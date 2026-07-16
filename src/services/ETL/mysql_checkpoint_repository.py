import asyncio
from src.utils.singletons.database_singleton import AsyncMysqlSingleton
from src.config.mysql_patent_orm import OdsETLRun, OdsETLBatchCheckpoint
from src.services.ETL.base_etl_service import BaseRunRepo, BaseBatchRepo
from sqlalchemy import select, update
from src.utils.tz_time import get_current_str


class MysqlRunRepo(BaseRunRepo):
    def __init__(self):
        # 获取异步session工厂（lifespan已全局预热）
        self.session_factory = None
        self._lock = asyncio.Lock()  # 增加异步锁

    async def _get_factory(self):
        async with self._lock:
            if self.session_factory is None:
                self.session_factory = await AsyncMysqlSingleton.get_session("patent")
            return self.session_factory

    async def create(self, run_id):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLRun).where(OdsETLRun.run_id == run_id)
            res = await session.execute(stmt)
            exist = res.scalar_one_or_none()

            if exist:
                exist.status = "running"
                exist.update_time = get_current_str()
            else:
                new_row = OdsETLRun(
                    run_id=run_id,
                    status="running",
                    create_time=get_current_str()
                )
                session.add(new_row)
            await session.commit()

    async def update_status(self, run_id, status):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = (
                update(OdsETLRun)
                .where(OdsETLRun.run_id == run_id)
                .values(status=status, update_time=get_current_str())
            )
            await session.execute(stmt)
            await session.commit()

class MysqlBatchRepo(BaseBatchRepo):

    def __init__(self):
        # 获取异步session工厂（lifespan已全局预热）
        self.session_factory = None
        self._lock = asyncio.Lock()  # 增加异步锁

    async def _get_factory(self):
        async with self._lock:
            if self.session_factory is None:
                self.session_factory = await AsyncMysqlSingleton.get_session("patent")
            return self.session_factory

    async def create(self, run_id, batch_no, last_id, batch_size: int):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLBatchCheckpoint).where(
                OdsETLBatchCheckpoint.run_id == run_id,
                OdsETLBatchCheckpoint.batch_no == batch_no
            )
            res = await session.execute(stmt)
            exist = res.scalar_one_or_none()

            if exist:
                exist.status = "pending"
                exist.update_time = get_current_str()
            else:
                new_row = OdsETLBatchCheckpoint(
                    run_id=run_id,
                    batch_no=batch_no,
                    last_id=last_id,
                    status="pending",
                    batch_size=batch_size,
                    create_time=get_current_str()
                )
                session.add(new_row)
            await session.commit()

    async def update_status(self, run_id: str, batch_no: int, status: str):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = (
                update(OdsETLBatchCheckpoint)
                .where(
                    OdsETLBatchCheckpoint.run_id == run_id,
                    OdsETLBatchCheckpoint.batch_no == batch_no
                )
                .values(status=status, update_time=get_current_str())
            )
            await session.execute(stmt)
            await session.commit()

    async def mark_failed(self, run_id, batch_no):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLBatchCheckpoint).where(
                OdsETLBatchCheckpoint.run_id == run_id,
                OdsETLBatchCheckpoint.batch_no == batch_no
            )
            res = await session.execute(stmt)
            row = res.scalar_one_or_none()
            if not row:
                return
            retry_count = row.retry_count + 1
            update_stmt = (
                update(OdsETLBatchCheckpoint)
                .where(
                    OdsETLBatchCheckpoint.run_id == run_id,
                    OdsETLBatchCheckpoint.batch_no == batch_no
                )
                .values(
                    status="failed",
                    retry_count=retry_count,
                    update_time=get_current_str()
                )
            )
            await session.execute(update_stmt)
            await session.commit()

    async def get_status(self, run_id, batch_no):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLBatchCheckpoint).where(
                OdsETLBatchCheckpoint.run_id == run_id,
                OdsETLBatchCheckpoint.batch_no == batch_no
            )
            res = await session.execute(stmt)
            row = res.scalar_one_or_none()
            return row.status if row else None

    async def get_last_completed(self, run_id: str):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLBatchCheckpoint).where(
                OdsETLBatchCheckpoint.run_id == run_id,
                OdsETLBatchCheckpoint.status == "completed"
            )
            res = await session.execute(stmt)
            rows = res.scalars().all()
            if not rows:
                return None
            sorted_list_desc = sorted(rows, key=lambda x: x.batch_no, reverse=True)
            return {
                "run_id": sorted_list_desc[0].run_id,
                "batch_no": sorted_list_desc[0].batch_no,
                "last_id": sorted_list_desc[0].last_id
            }

    async def get_failed_batches(self, run_id):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLBatchCheckpoint).where(
                OdsETLBatchCheckpoint.run_id == run_id,
                OdsETLBatchCheckpoint.status == "failed"
            )
            res = await session.execute(stmt)
            rows = res.scalars().all()
            if not rows:
                return []
            sorted_list = sorted(rows, key=lambda x: x.batch_no)
            result = []
            for item in sorted_list:
                result.append({
                    "run_id": item.run_id,
                    "batch_no": item.batch_no,
                    "last_id": item.last_id,
                    "status": item.status
                })
            return result

    async def get_previous_batch(self, run_id, batch_no):
        factory = await self._get_factory()
        async with factory() as session:
            stmt = select(OdsETLBatchCheckpoint).where(
                OdsETLBatchCheckpoint.run_id == run_id,
                OdsETLBatchCheckpoint.batch_no < batch_no
            )
            res = await session.execute(stmt)
            rows = res.scalars().all()
            if not rows:
                return None
            sorted_list_desc = sorted(rows, key=lambda x: x.batch_no, reverse=True)
            return {
                "run_id": sorted_list_desc[0].run_id,
                "batch_no": sorted_list_desc[0].batch_no,
                "last_id": sorted_list_desc[0].last_id
            }


# if __name__ == '__main__':
    # run_repo = ETLRunRepo()
    # run_id = "Mysql2QdrantTypes.update_payload_56b01e9a"
    # run_repo.create(run_id)
    # run_repo.update_status(run_id, "completed")

    # batch_repo = BatchRepo()
    # batch_repo.update_status(run_id, 454, "completed")