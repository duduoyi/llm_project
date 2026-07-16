import asyncio
from src.utils.singletons.database_singleton import AsyncMongoSingleton
from src.services.ETL.base_etl_service import BaseRunRepo, BaseBatchRepo
from src.utils.tz_time import get_current_str


class MongoRunRepo(BaseRunRepo):
    """新增：Mongo存储etl_run"""

    def __init__(self):
        # 获取异步session工厂（lifespan已全局预热）
        self.mongo_client = None
        self._lock = asyncio.Lock()  # 增加异步锁

    async def _get_collection(self):
        async with self._lock:
            if self.mongo_client is None:
                self.mongo_client = await AsyncMongoSingleton.get_instance()
            coll = self.mongo_client["dquot"]["etl_run"]
            return coll

    async def create(self, run_id: str):
        coll = await self._get_collection()
        now_str = get_current_str()

        filter_cond = {"run_id": run_id}
        update_data = {
            "$set": {
                "status": "running",
                "update_time": now_str
            },
            "$setOnInsert": {
                "create_time": now_str
            }
        }
        await coll.update_one(filter_cond, update_data, upsert=True)

    async def update_status(self, run_id: str, status: str):
        coll = await self._get_collection()

        await coll.update_one(
            {"run_id": run_id},
            {
                "$set": {
                    "status": status,
                    "update_time": get_current_str()
                }
            }
        )


class MongoBatchRepo(BaseBatchRepo):
    """Mongo etl_batch_checkpoint"""
    def __init__(self):
        # 获取异步session工厂（lifespan已全局预热）
        self.mongo_client = None
        self._lock = asyncio.Lock()  # 增加异步锁

    async def _get_collection(self):
        async with self._lock:
            if self.mongo_client is None:
                self.mongo_client = await AsyncMongoSingleton.get_instance()
            coll = self.mongo_client["dquot"]["etl_batch_checkpoint"]
            return coll

    async def create(self, run_id: str, batch_no: int, last_id, batch_size: int):
        coll = await self._get_collection()
        now_str = get_current_str()

        filter_cond = {"run_id": run_id, "batch_no": batch_no}
        update_data = {
            "$set": {
                "status": "pending",
                "update_time": now_str
            },
            "$setOnInsert": {
                "run_id": run_id,
                "batch_no": batch_no,
                "last_id": last_id,
                "batch_size": batch_size,
                "create_time": now_str
            }
        }
        await coll.update_one(filter_cond, update_data, upsert=True)

    async def update_status(self, run_id: str, batch_no: int, status: str):
        coll = await self._get_collection()
        await coll.update_one(
            {"run_id": run_id, "batch_no": batch_no},
            {"$set": {"status": status, "update_time": get_current_str()}}
        )

    async def mark_failed(self, run_id: str, batch_no: int):
        coll = await self._get_collection()
        await coll.update_one(
            {"run_id": run_id, "batch_no": batch_no},
            {
                "$set": {
                    "status": "failed",
                    "update_time": get_current_str()
                },
                "$inc": {
                    "retry_count": 1
                }
            }
        )

    async def get_status(self, run_id: str, batch_no: int) -> str:
        coll = await self._get_collection()
        row = await coll.find_one({"run_id": run_id, "batch_no": batch_no})
        return row["status"] if row else None

    async def get_last_completed(self, run_id: str):
        coll = await self._get_collection()
        cursor = coll.find({"run_id": run_id, "status": "completed"}).sort("batch_no", -1).limit(1)
        rows = await cursor.to_list(length=1)
        return rows[0] if rows else None

    async def get_failed_batches(self, run_id: str) -> list:
        coll = await self._get_collection()
        cursor = coll.find({"run_id": run_id, "status": "failed"}).sort("batch_no", 1)
        return await cursor.to_list(length=None)

    async def get_previous_batch(self, run_id: str, batch_no: int):
        coll = await self._get_collection()
        cursor = coll.find(
            {"run_id": run_id, "batch_no": {"$lt": batch_no}}
        ).sort("batch_no", -1).limit(1)
        rows = await cursor.to_list(length=1)
        return rows[0] if rows else None



# if __name__ == '__main__':
    # run_repo = ETLRunRepo()
    # run_id = "Mysql2QdrantTypes.update_payload_56b01e9a"
    # run_repo.create(run_id)
    # run_repo.update_status(run_id, "completed")

    # batch_repo = BatchRepo()
    # batch_repo.update_status(run_id, 454, "completed")


