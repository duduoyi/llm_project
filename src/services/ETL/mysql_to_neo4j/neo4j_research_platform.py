import asyncio
from sqlalchemy import select
from src.services.ETL.base_etl_service import BaseETLService
from src.config.mysql_patent_orm import DimUniversityResearchPlatform
from src.utils.singletons.database_singleton import AsyncMysqlSingleton, AsyncNeo4jSingleton
from src.utils.data_cleaner import preprocess_all_string_fields


# ========================= 实验室节点及关系 ===========================
class Neo4jResearchPlatform(BaseETLService):
    """
    实验室节点及关系上传到 Neo4j 的 ETL Service
    """

    name = "neo4j_research_platform"
    source_type = "mysql"

    async def extract_batch(self, last_id: int = 0):
        # MySQL：获取异步session工厂
        mysql_session_factory = await AsyncMysqlSingleton.get_session("patent")
        async with mysql_session_factory() as session:
            # ✅ SQLAlchemy 异步标准写法：使用 select()
            stmt = select(
                DimUniversityResearchPlatform.id,
                DimUniversityResearchPlatform.research_platform,
                DimUniversityResearchPlatform.type,
                DimUniversityResearchPlatform.university,
                DimUniversityResearchPlatform.college
            ).filter(
                DimUniversityResearchPlatform.create_time >= self.start_time,
                DimUniversityResearchPlatform.create_time < self.end_time,
                DimUniversityResearchPlatform.id > last_id
            ).order_by(DimUniversityResearchPlatform.id).limit(self.batch_size)

            res = await session.execute(stmt)
            results = res.all()
        
        # 将ORM对象转换为指定字段的字典
        batch_data = []
        for lab in results:
            lab_dict = {
                "id": lab.id,
                "research_platform": lab.research_platform,
                "type": lab.type,
                "university": lab.university,
                "college": lab.college
            }
            batch_data.append(lab_dict)

        return batch_data

    async def process_batch(self, batch_data):
        """数据转换"""
        try:
            # ======================= transform =============================
            # 字符串列预处理
            data_clean = preprocess_all_string_fields(batch_data)
            self.logger.debug("数据清洗完成")
            # ======================= upload =============================
            # 实验室所属学院无学者，则不新增实验室
            cypher_query = """
                CALL apoc.periodic.iterate(
                    "UNWIND $lists AS item RETURN item",
                    "
                        OPTIONAL MATCH (c:college {university: item.university, college: item.college})
                        MERGE (n:research_platform {research_platform: item.research_platform})
                        SET n.type = item.type
                        WITH c, n
                        WHERE c IS NOT NULL
                        MERGE (c)-[:CONTAINS]->(n)
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
            neo4j_driver = await AsyncNeo4jSingleton.get_driver()
            async with neo4j_driver.session() as session:
                result = await session.run(cypher_query, external_lists=data_clean)
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



# 测试
async def main():
    config = {
        "start_time": "2024-01-01 00:00:00",
        "end_time": "2026-01-01 00:00:00",
        "batch_size": 10
    }
    service = Neo4jResearchPlatform(config)
    result = await service.extract_batch(last_id=0)
    print(result)


if __name__ == '__main__':
    asyncio.run(main())
