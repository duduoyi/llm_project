import asyncio
from sqlalchemy import select
from src.utils.singletons.database_singleton import AsyncMysqlSingleton, AsyncQdrantSingleton
from qdrant_client.models import PointStruct
from src.services.ETL.base_etl_service import BaseETLService
from src.config.mysql_patent_orm import OdsCompanyPatentInfo
from src.utils.singletons.ai_singleton import EmbeddingFactory
from src.utils.data_cleaner import preprocess_all_string_fields
import hashlib


# ========================= 企业 数据向量化 ===========================
class QdrantCompanyPatent(BaseETLService):
    """
    企业专利向量化并上传到 Qdrant 的 ETL Service
    """

    name = "qdrant_company_patent"
    source_type = "mysql"

    async def extract_batch(self, last_id: int = 0):
        # MySQL：获取异步session工厂
        mysql_session_factory = await AsyncMysqlSingleton.get_session("patent")
        async with mysql_session_factory() as session:
            # ✅ SQLAlchemy 异步标准写法：使用 select()
            stmt = select(
                OdsCompanyPatentInfo.id,
                OdsCompanyPatentInfo.title,
                OdsCompanyPatentInfo.summary,
                OdsCompanyPatentInfo.application_number,
                OdsCompanyPatentInfo.application_date,
                OdsCompanyPatentInfo.publication_date,
                OdsCompanyPatentInfo.main_cls_number,
                OdsCompanyPatentInfo.applicant
            ).filter(
                OdsCompanyPatentInfo.create_time >= self.start_time,
                OdsCompanyPatentInfo.create_time < self.end_time,
                OdsCompanyPatentInfo.id > last_id
            ).order_by(OdsCompanyPatentInfo.id).limit(self.batch_size)

            res = await session.execute(stmt)
            results = res.all()

        # 将ORM对象转换为指定字段的字典
        batch_data = []
        for patent in results:
            patent_dict = {
                "id": patent.id,
                "title": patent.title,
                "summary": patent.summary,
                "application_number": patent.application_number,
                "application_date": patent.application_date,
                "publication_date": patent.publication_date,
                "main_cls_number": patent.main_cls_number,
                "applicant": patent.applicant
            }
            batch_data.append(patent_dict)

        return batch_data

    async def process_batch(self, batch_data):
        """数据转换"""
        if not batch_data:
            self.logger.info("当前批次无专利数据，跳过向量化入库")
            return

        # ===================== 1. 数据清洗 =====================
        clean_list = preprocess_all_string_fields(batch_data)

        # ===================== 2. 把字典 → Document 对象 =====================
        # 预分配列表：文本、id、元数据
        text_list = []
        point_ids = []
        payload_list = []
        for patent in clean_list:
            app_num = patent["application_number"]
            # 你要求的公式：sha256 → 取前16位 → 转整数
            patent_id = int(hashlib.sha256(app_num.encode('utf-8')).hexdigest()[:16], 16)  # 16进制转整数
            point_ids.append(patent_id)

            # 拼接 标题+摘要 作为向量内容（检索效果最好）
            content = f"专利名称：{patent['title']}。专利摘要：{patent['summary']}"
            text_list.append(content)
            # 构建 Payload
            payload_list.append({
                "application_number": patent["application_number"],
                "application_date": patent["application_date"],
                "publication_date": patent["publication_date"],
                "main_cls_number": patent["main_cls_number"],
                "applicant": patent["applicant"]
            })

        # ===================== 3. 存储到qdrant中 ====================
        # 2. 批量异步向量化（带信号量限流，我们包装好的方法）
        emb_wrapper = EmbeddingFactory.get_instance()
        # ⚠️ QdrantVectorStore必须传入原始embedding对象，不能传包装器
        vectors = await emb_wrapper.aembed_documents(text_list)

        # 3. 组装Qdrant Points
        points = []
        for pid, vec, payload in zip(point_ids, vectors, payload_list):
            points.append(PointStruct(
                id=pid,
                vector=vec,
                payload=payload
            ))

        # 3. Qdrant入库
        collection_name = "patent_test"
        # Qdrant 异步客户端
        qdrant_client = await AsyncQdrantSingleton.get_instance()
        # 原生异步API
        exists = await qdrant_client.collection_exists(collection_name)
        if not exists:
            self.logger.error(f"Qdrant集合 {collection_name} 不存在，跳过当前批次")
            return

        # ✅ 真正原生异步写入，无任何同步阻塞、不需要to_thread
        await qdrant_client.upsert(
            collection_name=collection_name,
            points=points
        )
        self.logger.info(f"✅ {len(points)} 条专利向量成功写入集合 {collection_name}")



# # 测试
# async def main():
#     config = {
#         "start_time": "2024-01-01 00:00:00",
#         "end_time": "2026-01-01 00:00:00",
#         "batch_size": 10
#     }
#     service = QdrantCompanyPatent(config)
#     result = await service.extract_batch(last_id=0)
#     print(result)
#
#
# if __name__ == '__main__':
#     asyncio.run(main())



