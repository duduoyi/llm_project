import asyncio
from sqlalchemy import select
from src.utils.singletons.database_singleton import AsyncMysqlSingleton, AsyncQdrantSingleton
from src.services.ETL.base_etl_service import BaseETLService
from src.config.mysql_patent_orm import OdsCompanyPatentInfo
from qdrant_client.http import models
from src.utils.data_cleaner import preprocess_all_string_fields
import hashlib

class QdrantCompanyPatentPayload(BaseETLService):

    name = "qdrant_company_patent_payload"
    source_type = "mysql"

    async def extract_batch(self, last_id: int = 0):
        # MySQL：获取异步session工厂
        mysql_session_factory = await AsyncMysqlSingleton.get_session("patent")
        async with mysql_session_factory() as session:
            # ✅ SQLAlchemy 异步标准写法：使用 select()
            stmt = select(
                OdsCompanyPatentInfo.id,
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
                "application_number": patent.application_number,
                "application_date": patent.application_date,
                "publication_date": patent.publication_date,
                "main_cls_number": patent.main_cls_number,
                "applicant": patent.applicant
            }
            batch_data.append(patent_dict)

        return batch_data


    async def process_batch(self, batch_data):
        """
        只处理一个批次
        """
        try:
            # ===================== 1. 数据清洗 =====================
            clean_list = preprocess_all_string_fields(batch_data)

            # ===================== 2. 把字典 → payload 对象 =====================
            payloads = []
            ids = []
            for patent in clean_list:
                app_num = patent["application_number"]
                # 你要求的公式：sha256 → 取前16位 → 转整数
                patent_id = int(hashlib.sha256(app_num.encode('utf-8')).hexdigest()[:16], 16)  # 16进制转整数
                ids.append(patent_id)

                # 构建 payload
                meta_payload = models.SetPayloadOperation(
                    set_payload=models.SetPayload(
                        payload={"metadata": patent},
                        points=[patent_id],
                    )
                )
                payloads.append(meta_payload)


            # ===================== 3. 存储到qdrant中 ====================
            collection_name = "patent_test"
            qdrant_client = await AsyncQdrantSingleton.get_instance()
            # 检查集合是否已存在
            if await qdrant_client.collection_exists(collection_name):
                # 批量获取point的存在性（仅查询ID，不返回向量/payload）
                points_exist = await qdrant_client.retrieve(
                    collection_name=collection_name,
                    ids=ids,  # Qdrant支持字符串ID
                    with_payload=False,
                    with_vectors=False
                )
                # 提取存在的point ID（确保为字符串）
                exist_ids = [p.id for p in points_exist]
                # 记录不存在的ID（仅日志提示，不报错）
                not_exist_ids = [pid for pid in ids if pid not in exist_ids]
                if not_exist_ids:
                    self.logger.info(f"检测到{len(exist_ids)}个point存在")

                result = await qdrant_client.batch_update_points(
                    collection_name=collection_name,
                    update_operations=payloads
                )
                self.logger.info(f"专利元数据已更新到qdrant集合{collection_name}：{result}")
            else:
                self.logger.error(f"集合{collection_name}不存在")
        except Exception as e:
            return False, str(e)
