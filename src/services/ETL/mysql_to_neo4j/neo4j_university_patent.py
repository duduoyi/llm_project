from sqlalchemy import select
from src.services.ETL.base_etl_service import BaseETLService
from src.config.mysql_patent_orm import OdsUniversityPatentInfo
from src.utils.data_cleaner import preprocess_all_string_fields, split_inventor_applicant


# ========================= 高校专利节点及关系 ===========================
class Neo4jUniversityPatent(BaseETLService):
    """
    高校专利节点及关系上传到 Neo4j 的 ETL Service
    """

    name = "neo4j_university_patent"
    source_type = "mysql"

    async def extract_batch(self, last_id: int = 0):
        # MySQL：获取异步session工厂
        mysql_session_factory = await self.get_mysql_session_factory("patent")
        async with mysql_session_factory() as session:
            # ✅ SQLAlchemy 异步标准写法：使用 select()
            stmt = select(
                OdsUniversityPatentInfo.id,
                OdsUniversityPatentInfo.application_number,
                OdsUniversityPatentInfo.main_cls_number,
                OdsUniversityPatentInfo.applicant,
                OdsUniversityPatentInfo.inventor
            ).filter(
                OdsUniversityPatentInfo.create_time >= self.start_time,
                OdsUniversityPatentInfo.create_time < self.end_time,
                OdsUniversityPatentInfo.id > last_id
            ).order_by(OdsUniversityPatentInfo.id).limit(self.batch_size)

            res = await session.execute(stmt)
            results = res.all()

        # 将ORM对象转换为指定字段的字典
        batch_data = []
        for patent in results:
            patent_dict = {
                "id": patent.id,
                "application_number": patent.application_number,
                "main_cls_number": patent.main_cls_number,
                "applicant": patent.applicant,
                "inventor": patent.inventor
            }
            batch_data.append(patent_dict)

        return batch_data

    async def process_batch(self, batch_data):
        """数据转换"""
        try:
            # ======================= transform =============================
            # 字符串列预处理
            data_clean = preprocess_all_string_fields(batch_data)
            self.logger.debug("数据清洗完成")
            # 拆分专利申请人和发明人
            transformed_data = split_inventor_applicant(data_clean)
            self.logger.debug("拆分专利申请人和发明人完成")
            # ======================= upload =============================
            #  无大学学者所属大学的专利不进入neo4j
            cypher_query = """
                CALL apoc.periodic.iterate(
                    "UNWIND $lists AS item RETURN item",
                    "
                        MATCH (u:university {university: item.applicant})
                        OPTIONAL MATCH (s:scholar {university: item.applicant, name: item.inventor})
                        MERGE (p:patent {application_number: item.application_number})
                        SET p.main_cls_number = item.main_cls_number
                        MERGE (p)-[r1:APPLIED_BY]->(u)
                        FOREACH (_ IN CASE WHEN s IS NOT NULL THEN [1] ELSE [] END |
                            MERGE (p)-[r2:INVENTED_BY]->(s)
                        )
                    ",
                    {
                        batchSize: 1000,
                        parallel: false,
                        retries: 1,
                        timeout: 60000,
                        batchMode: "BATCH",
                        reportStatistics: true,
                        params: { lists: $external_lists }
                    }
                ) YIELD batches, total, errorMessages, failedBatches
                RETURN batches, total, errorMessages, failedBatches
            """
            # ✅ 异步Neo4j关键改动：开启异步session + await run
            neo4j_driver = await self.get_neo4j_driver()
            async with neo4j_driver.session() as session:
                result = await session.run(cypher_query, external_lists=transformed_data)
                record = await result.single()

            if record:
                result_data = {
                    "batches": record["batches"],
                    "total": record["total"],
                    "failedBatches": record["failedBatches"],
                    "errorMessages": record["errorMessages"]
                }
                self.logger.info(f"✅ neo4j 写入执行结果：{result_data}")
            else:
                self.logger.info("ℹ️ neo4j 写入无返回结果")
        except Exception as e:
            self.logger.error(f"批次上传失败: {str(e)}")
            raise

# # 测试
# async def main():
#     config = {
#         "start_time": "2024-01-01 00:00:00",
#         "end_time": "2026-01-01 00:00:00",
#         "batch_size": 10
#     }
#     service = Neo4jUniversityPatent(config)
#     result = await service.extract_batch(last_id=0)
#     print(result)
#
#
# if __name__ == '__main__':
#     import asyncio
#     asyncio.run(main())
