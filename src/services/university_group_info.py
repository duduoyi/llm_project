from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger


# ========================= 高校团队 ===========================
class UniversityGroupInfo(BaseResourceService):
    """
    高校团队上传到 Mongodb
    """
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.collection = 'university_group_info'

    async def get_community_info(self):
        group_cypher = """
            MATCH (u:university)-[:CONTAINS]->(c:college)
            WITH c,
                u.university AS university,
                u.type AS university_type
            OPTIONAL MATCH (c)-[:CONTAINS]->(rp:research_platform)
            WITH c,
                university,
                university_type,
                collect(distinct {name: rp.research_platform, type: rp.type}) AS research_platform
            OPTIONAL MATCH (c)-[sr:CONTAINS]->(sj:subject)
            WITH c,
                university,
                university_type,
                research_platform,
                collect(distinct {name: sj.subject_name, type: sr.subject_level}) AS subject
            MATCH (c)<-[:WORKED_AT]-(s:scholar)
            WHERE s.community IS NOT NULL
            WITH
                university,
                university_type,
                c.college AS college,
                c.score AS college_score,
                research_platform,
                subject,
                s.group_name AS group_name,
                s.name AS scholar_name,
                s.per_patent_count AS per_patent_count,
                s.community_patent_count AS community_patent_count
            // 排序选 leader
            ORDER BY group_name, per_patent_count DESC       
            // 最终聚合
            RETURN
                group_name,
                university,
                university_type,
                college,
                college_score,
                research_platform,
                subject,
                community_patent_count,  // ✅ 精准总数
                collect(scholar_name + '(' + toString(per_patent_count) + ')') AS scholar_list
        """
        neo4j_driver = await self.get_neo4j_driver()
        result_data = []
        async with neo4j_driver.session() as session:
            group_result = await session.run(group_cypher)
            # 迭代读取必须在session存活范围内
            async for record in group_result:
                result_data.append(record.data())

        self.logger.info(f"高效团队获取完成")
        self.logger.info(f"共获取到 {len(result_data)} 条高校团队数据")
        return result_data

    async def save_to_mongodb(self, data_list):
        """存入MongoDB"""
        success = 0
        # 只更新/新增，不删除旧字段
        for item in data_list:
            group_name = item.get("group_name")
            if not group_name:
                continue

            # 只有需要更新时才执行
            mongo_db = await self.get_mongo_db()
            coll = mongo_db[self.collection]
            await coll.update_one(
                {"_id": group_name},
                {"$set": item},
                upsert=True
            )

            success += 1
        return success

    async def pipline(self):
        group_info = await self.get_community_info()
        if not group_info:
            self.logger.warning("未获取到高校团队数据")
            return

        save_result = await self.save_to_mongodb(group_info)
        self.logger.info(f"高校团队上传MongoDB完成，共 {save_result} 条")


# # ===================== 异步测试入口 =====================
# async def main():
#     service = UniversityGroupInfo()
#     await service.pipeline()
#
#
# if __name__ == '__main__':
#     import asyncio
#     asyncio.run(main())

