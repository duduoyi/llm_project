from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger
from src.utils.storage_manager import get_storage
from typing import Optional


class QdrantMigrationService(BaseResourceService):

    BATCH_SIZE = 10000

    def __init__(self):
        self.logger = get_logger(f"{self.__class__.__name__}")
        self.temp_storage = get_storage()
        self.filename_prefix = "patent_vector"
        self.subdir = "qdrant"
        self.dev_qdrant_client = None
        self.pro_qdrant_client = None

    async def init(self, from_qdrant_config, to_qdrant_config):
        """异步初始化，在 lifespan 或者任务启动时调用"""
        self.dev_qdrant_client = await self.get_qdrant_client(from_qdrant_config)
        self.pro_qdrant_client = await self.get_qdrant_client(to_qdrant_config)
        self.logger.info("✅ Qdrant 异步客户端初始化完成")

    async def export_qdrant_data(self, old_collection: str):
        """
        导出所有向量及id
        """
        try:
            # 1. 先获取集合总数量（可选，用于进度展示）
            collection_info = await self.dev_qdrant_client.get_collection(old_collection)
            total_count = collection_info.points_count
            self.logger.info(f"开始导出 {old_collection}，总数：{total_count}")

            # 2. 分批滚动查询+删除（每次处理 100 条，可根据性能调整）
            offset: Optional[int] = None
            batch_index = 0
            export_total  = 0

            while True:
                # 分批滚动查询数据
                records, next_offset = await self.dev_qdrant_client.scroll(
                    collection_name=old_collection,
                    limit=self.BATCH_SIZE,
                    offset=offset,
                    with_vectors=True,
                    with_payload=True
                )

                all_points = []
                for r in records:
                    all_points.append({
                        "id": r.id,
                        "vector": r.vector,
                        "payload": r.payload
                    })
                # 储存向量数据
                export_data = {"points": all_points}
                filename  = f"{self.filename_prefix}_{batch_index}"
                self.temp_storage.store_json(export_data, filename, self.subdir)

                batch_index += 1
                export_total += len(records)

                self.logger.debug(f"已处理 {export_total}/{total_count} 条数据")

                if offset is None:
                    break  # 没有更多数据，退出循环

            self.logger.info(f"导出完成，共 {export_total} 条")
            return export_total
        except Exception as e:
            self.logger.error(f"{old_collection}向量导出失败: {str(e)}", exc_info=True)
            # 记录该批次更新失败的point ID
            raise

    async def import_new_collection(self, new_collection: str):
        self.logger.info("导入数据到新 collection...")

        if not await self.pro_qdrant_client.collection_exists(new_collection):
            await self.pro_qdrant_client.create_collection(new_collection)
            self.logger.info(f"新集合{new_collection}创建成功")

        filename_list = self.temp_storage.list_filename(self.filename_prefix, self.subdir)

        for filename in filename_list:
            data = self.temp_storage.load_json(filename, self.subdir)
            points = data["points"]

            ids = [p["id"] for p in points]
            vectors = [p["vector"] for p in points]
            payload = [p["payload"] for p in points]

            self.logger.info(f"开始 upload_collection，{filename} -> {len(ids)} 条")

            await self.pro_qdrant_client.upload_collection(
                collection_name=new_collection,
                ids=ids,
                vectors=vectors,
                payload=payload
            )

        self.logger.info(f"新集合{new_collection}导入数据完成")

    async def verify_and_cleanup(self, old_collection: str, new_collection: str):
        old_count = await self.dev_qdrant_client.get_collection(old_collection).points_count

        new_count = await self.pro_qdrant_client.get_collection(new_collection).points_count

        if old_count != new_count:
            raise RuntimeError(f"数据不一致 old={old_count}, new={new_count}")

        # self.logger.info("校验通过，删除旧集合")
        # client.delete_collection(old_collection)

        self.temp_storage.cleanup_files(self.filename_prefix, self.subdir)
        self.logger.info("旧集合删除，临时文件清理完成")

    async def migrate(self, old_collection: str, new_collection: str):

        self.logger.info(
            f"开始迁移 {old_collection} -> {new_collection}"
        )

        exported = await self.export_qdrant_data(old_collection)

        if exported == 0:
            raise RuntimeError("导出数据为空，停止迁移")

        await self.import_new_collection(new_collection)

        await self.verify_and_cleanup(old_collection, new_collection)

        self.logger.info("迁移完成 🎉")

# ===================== 异步测试入口 =====================
async def main():
    service = QdrantMigrationService()
    # Qdrant配置
    from_qdrant_config = {
        'host': '127.0.0.1',
        'port': '6333',
        'api_key': ''
    }

    to_qdrant_config = {
        'host': '127.0.0.1',
        'port': '6333',
        'api_key': ''
    }
    await service.init(from_qdrant_config, to_qdrant_config)
    await service.migrate("patent_vector", "patent")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

