import asyncio
from src.utils.logger import get_logger
from typing import Optional, List
from src.utils.singletons.database_singleton import AsyncQdrantSingleton
from qdrant_client.http import models


class QdrantService:
    """Qdrant 数据内部处理"""
    BATCH_SIZE = 10000
    # 全局异步互斥锁：所有 Qdrant 变更操作统一加锁
    GLOBAL_QDRANT_LOCK = asyncio.Lock()

    def __init__(self):
        self.logger = get_logger(f"{self.__class__.__name__}")
        self.qdrant_client = None

    async def init(self):
        """异步初始化，在 lifespan 或者任务启动时调用"""
        self.qdrant_client = await AsyncQdrantSingleton.get_instance()
        self.logger.info("✅ Qdrant 异步客户端初始化完成")

    async def create_collection(
            self,
            collection_name: str,
            **kwargs
    ) -> bool:
        """
        创建Qdrant集合（向量表）
        :param collection_name: 集合名称
        :return: 创建成功返回True
        """
        async with self.GLOBAL_QDRANT_LOCK:
            # 定义默认的集合创建配置（可通用）
            default_collection_config = {
                "vectors_config": models.VectorParams(
                    size=384,  # paraphrase-multilingual-MiniLM-L12-v2 默认维度384
                    distance=models.Distance.COSINE,
                    datatype=models.Datatype.FLOAT32,  # 与嵌入模型生成数据格式一致
                    on_disk=True  # 向量存储到硬盘
                ),
                # 量化配置（核心降容：int8量化）
                "quantization_config": models.ScalarQuantization(
                    scalar=models.ScalarQuantizationConfig(
                        type=models.ScalarType.INT8,  # 标量量化
                        quantile=0.99,  # 避免极端值影响
                        always_ram=False  # 机器内存紧张，<8GB
                    )
                ),
                # HNSW索引配置（平衡速度与内存）
                "hnsw_config": models.HnswConfigDiff(
                    m=16,  # 标准生产值
                    ef_construct=100,  # 构建速度与质量平衡
                    full_scan_threshold=10000,
                    max_indexing_threads=4,
                    on_disk=False  # 索引常驻内存（推荐）
                ),
                # 优化器配置（控制碎片，目标1个segment）
                "optimizers_config": models.OptimizersConfigDiff(
                    deleted_threshold=0.01,
                    vacuum_min_vector_number=1000,
                    default_segment_number=1,  # 控制为 1 个 segment, 避免碎片
                    max_segment_size=500000,
                    memmap_threshold=200000,  # memmap_threshold 应该大于 segment 实际大小
                    indexing_threshold=100000,  # 每 10 万条触发一次索引构建
                    flush_interval_sec=30,
                    max_optimization_threads=4
                ),
                # WAL配置（限制日志堆积）
                "wal_config": models.WalConfigDiff(
                    wal_capacity_mb=64,
                    wal_segments_ahead=2
                ),
                # Payload配置（文本仅存磁盘）
                "on_disk_payload": True,
                # 分片/副本（单机默认配置）
                "shard_number": 1,
                "replication_factor": 1,
                "write_consistency_factor": 1,
                # 严格模式（关闭）
                "strict_mode_config": models.StrictModeConfig(enabled=False)
            }

            try:
                # 检查集合是否已存在
                if await self.qdrant_client.collection_exists(collection_name):
                    self.logger.warning(f"集合{collection_name}已存在，跳过创建")
                    return True

                # 合并默认配置 + 方法入参kwargs（kwargs优先级最高）
                final_config = {
                    **default_collection_config,
                    **kwargs
                }

                # 创建集合
                await self.qdrant_client.create_collection(
                    collection_name=collection_name,
                    **final_config
                )
                self.logger.info(f"集合{collection_name}创建成功")
                return True
            except Exception as e:
                self.logger.error(f"创建集合{collection_name}失败：{str(e)}")
                raise

    async def clear_patent_payload(self, collection_name: str):
        """
        清除所有点payload
        """
        async with self.GLOBAL_QDRANT_LOCK:
            try:
                # 1. 先获取集合总数量（可选，用于进度展示）
                collection_info = await self.qdrant_client.get_collection(collection_name)
                total_count = collection_info.points_count
                self.logger.info(f"开始删除集合 {collection_name} 的 所有payload 字段，总数据量：{total_count}")

                # 2. 分批滚动查询+删除（每次处理 100 条，可根据性能调整）
                offset: Optional[int] = None
                processed = 0

                while True:
                    # 分批滚动查询数据
                    records, next_offset = await self.qdrant_client.scroll(
                        collection_name=collection_name,
                        limit=self.BATCH_SIZE,
                        offset=offset
                    )
                    if not records:
                        break

                    # 构建删除 payload 的请求
                    point_ids = [record.id for record in records]

                    await self.qdrant_client.clear_payload(
                        collection_name=collection_name,
                        points_selector=point_ids  # 只删除当前批次的点
                    )

                    # 更新进度
                    processed += len(point_ids)
                    self.logger.debug(f"已处理 {processed}/{total_count} 条数据")

                    if offset is None:
                        break  # 没有更多数据，退出循环

                self.logger.info(f"删除完成：共处理 {processed} 条数据")
                return True
            except Exception as e:
                self.logger.error(f"{collection_name}删除所以负载失败: {str(e)}", exc_info=True)
                # 记录该批次更新失败的point ID
                raise

    async def delete_patent_payload(self, collection_name: str, payload_key: List[str]):
        """
        删除指定payload
        """
        async with self.GLOBAL_QDRANT_LOCK:
            try:
                # 1. 先获取集合总数量（可选，用于进度展示）
                collection_info = await self.qdrant_client.get_collection(collection_name)
                total_count = collection_info.points_count
                self.logger.info(f"开始删除集合 {collection_name} 的 payload 字段 {payload_key}，总数据量：{total_count}")

                # 2. 分批滚动查询+删除（每次处理 100 条，可根据性能调整）
                batch_size = 1000
                offset = None
                processed = 0

                while True:
                    # 分批滚动查询数据
                    scroll_result = await self.qdrant_client.scroll(
                        collection_name=collection_name,
                        limit=batch_size,
                        offset=offset
                    )
                    record_list = scroll_result['record']
                    next_page_offset = scroll_result['next_point_id']

                    # 构建删除 payload 的请求
                    point_ids = [record.id for record in record_list]

                    await self.qdrant_client.delete_payload(
                        collection_name=collection_name,
                        keys=payload_key,
                        points=point_ids  # 只删除当前批次的点
                    )

                    # 更新进度
                    processed += len(point_ids)
                    offset = next_page_offset
                    self.logger.debug(f"已处理 {processed}/{total_count} 条数据")

                    if not next_page_offset:
                        break  # 没有更多数据，退出循环

                self.logger.info(f"删除完成：共处理 {processed} 条数据，删除字段 {payload_key}")
                return True
            except Exception as e:
                self.logger.error(f"{collection_name}删除负载{payload_key}失败: {str(e)}", exc_info=True)
                # 记录该批次更新失败的point ID
                raise


# ===================== 异步测试入口 =====================
async def main():
    service = QdrantService()
    await service.init()
    await service.create_collection("teacher_profile")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())