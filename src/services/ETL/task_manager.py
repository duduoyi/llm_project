import asyncio
from src.services.ETL.mysql_checkpoint_repository import MysqlRunRepo, MysqlBatchRepo
from src.services.ETL.mongodb_checkpoint_repository import MongoRunRepo, MongoBatchRepo
from src.utils.logger import get_logger

class TaskManager:

    def __init__(
        self,
        max_concurrent_batches: int = 2,
        max_queue_size: int = 5,
        consumer_workers: int = 3
    ):
        self.semaphore = asyncio.Semaphore(max_concurrent_batches)
        self.max_queue_size = max_queue_size
        self.consumer_workers = consumer_workers

        # 实例化两套仓库
        self.mysql_run_repo = MysqlRunRepo()
        self.mongo_run_repo = MongoRunRepo()

        self.mysql_batch_repo = MysqlBatchRepo()
        self.mongo_batch_repo = MongoBatchRepo()
        self.logger = get_logger(self.__class__.__name__)

    def _get_repos(self, service):
        """根据ETL服务source_type 选择对应的RunRepo、BatchRepo"""
        source_type = getattr(service, "source_type", "mysql")
        if source_type == "mongodb":
            return self.mongo_run_repo, self.mongo_batch_repo
        return self.mysql_run_repo, self.mysql_batch_repo

    async def start_job(self, run_id: str, service):
        """启动任务"""
        self.logger.info(f"启动 {run_id} 任务")
        task = asyncio.create_task(self._run(run_id, service))
        return task

    async def _run(self, run_id: str, service):
        # 信号量：限制同时运行的ETL任务总数
        async with self.semaphore:
            queue = asyncio.Queue(maxsize=self.max_queue_size)  # 限制batch在内存
            stop_event = asyncio.Event()

            # ✅ 自动选择仓库
            run_repo, batch_repo = self._get_repos(service)

            await run_repo.create(run_id)
            self.logger.info(f"{run_id}ETL流程开始进行")

            producer = asyncio.create_task(
                self._producer(queue, stop_event, run_id, service, batch_repo)
            )

            # 启动 consumer worker
            consumers  = [
                asyncio.create_task(
                    self._consumer(queue, stop_event, run_id, service, batch_repo)
                )
                for _ in range(self.consumer_workers)  # 并发个数
            ]

            try:
                # producer
                await producer

                # 等待队列清空
                await queue.join()

                # 通知 consumers 退出
                for _ in range(self.consumer_workers):
                    await queue.put(None)
                for c in consumers:
                    await c

                await run_repo.update_status(run_id, "completed")
                self.logger.info(f"{run_id} ETL流程完成")

            except Exception as e:
                stop_event.set()
                await run_repo.update_status(run_id, "failed")
                self.logger.error(f"{run_id} ETL流程失败: {str(e)}", exc_info=True)
                raise

    async def _producer(self, queue: asyncio.Queue, stop_event: asyncio.Event, run_id: str, service, batch_repo):
        """生产者按批提取数据放入队列"""
        batch_no = 1
        last_id = 0

        # 断点续跑
        last_checkpoint = await batch_repo.get_last_completed(run_id)
        if last_checkpoint:
            last_id = last_checkpoint["last_id"]
            batch_no = last_checkpoint["batch_no"] + 1

        while not stop_event.is_set():
            self.logger.debug(f"按批提取数据放入队列：batch_no={batch_no}, last_id={last_id}")
            batch_data = await service.extract_batch(last_id)
            if not batch_data:
                self.logger.info(f"{run_id} 无待提取数据，生产者退出")
                break
            if not batch_data:
                break


            # 先持久化批次断点，再放入队列，避免阻塞丢失记录
            raw_last_id = batch_data[-1]["id"]
            # 创建 batch_checkpoint
            await batch_repo.create(
                run_id,
                batch_no,
                raw_last_id,
                len(batch_data)
            )
            # 🔥 背压关键点
            await queue.put((batch_no, batch_data))
            last_id = raw_last_id
            batch_no += 1

    async def _consumer(self, queue: asyncio.Queue, stop_event: asyncio.Event, run_id: str, service, batch_repo):
        """消费者处理 batch 并安全更新 checkpoint"""
        while True:

            item = await queue.get()

            if item is None:
                queue.task_done()
                break

            batch_no, batch_data = item
            batch_status = await batch_repo.get_status(run_id, batch_no)

            if batch_status == "completed":
                self.logger.debug(f"{run_id}_{batch_no}批次数据转换加载完成")
                queue.task_done()
                continue

            try:
                await batch_repo.update_status(run_id, batch_no, "running")
                self.logger.debug(f"开始进行{run_id}_{batch_no}批次数据转换加载")

                await service.process_batch(batch_data)

                await batch_repo.update_status(run_id, batch_no, "completed")
                self.logger.debug(f"{run_id}_{batch_no}批次数据转换加载完成")
            except Exception:
                stop_event.set()
                await batch_repo.mark_failed(run_id, batch_no)
                self.logger.error(f"{run_id}_{batch_no}批次数据转换加载失败", exc_info=True)
                raise
            finally:
                queue.task_done()

    async def rerun_failed(self, run_id: str, service):
        """
        只重跑失败批次
        """
        run_repo, batch_repo = self._get_repos(service)
        self.logger.info(f"{run_id} 开始重跑失败批次")

        failed_batches = await batch_repo.get_failed_batches(run_id)

        if not failed_batches:
            self.logger.info(f"{run_id} 没有失败批次")
            return

        for batch in failed_batches:
            batch_no = batch["batch_no"]
            self.logger.info(f"{run_id}_{batch_no} 开始重跑")

            prev_batch = await batch_repo.get_previous_batch(run_id, batch_no)
            start_id = prev_batch["last_id"] if prev_batch else 0

            batch_data = await service.extract_batch(start_id)
            if not batch_data:
                self.logger.warning(f"{run_id}_{batch_no} 未查询到数据，跳过")
                continue

            await batch_repo.update_status(run_id, batch_no, "running")

            try:
                await service.process_batch(batch_data)
                await batch_repo.update_status(run_id, batch_no, "completed")
                self.logger.info(f"{run_id}_{batch_no} 重跑成功")
            except Exception:
                await batch_repo.mark_failed(run_id, batch_no)
                self.logger.error(f"{run_id}_{batch_no} 重跑失败", exc_info=True)
                raise

        self.logger.info(f"{run_id} 失败批次重跑完成")