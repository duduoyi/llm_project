import asyncio
from src.utils.singletons.database_singleton import AsyncNeo4jSingleton, AsyncMongoSingleton
from src.utils.logger import get_logger


# ========================= 高校团队 ===========================
class UniversityGroupInfo:
    """
    高校团队上传到 Mongodb
    """
    # 全局异步互斥锁：与 Neo4j 写/删/图计算任务互斥，避免数据读写并发错乱
    _global_neo4j_lock = asyncio.Lock()

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.neo4j_client = None
        self._mongo_client = None
        self.collection = None

    async def init(self):
        """异步初始化，在 lifespan 或者任务启动时调用"""
        self.neo4j_client = await AsyncNeo4jSingleton.get_driver()
        self.logger.info("✅ Neo4j 异步客户端初始化完成")
        self._mongo_client = await AsyncMongoSingleton.get_instance()
        self.collection = self._mongo_client['dquot']['university_group_info']
        self.logger.info("✅ Mongo 异步集合初始化完成")

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
        group_result = await self.neo4j_client.run(group_cypher)
        self.logger.info(f"高效团队获取完成")
        # 将Neo4j结果 → 标准JSON格式（关键）
        result_data = []
        async for record in group_result:
            # 把每条记录转成字典
            result_data.append(record.data())
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
            await self.collection.update_one(
                {"_id": group_name},
                {"$set": item},
                upsert=True
            )

            success += 1
        return success

    async def pipline(self):
        async with self._global_neo4j_lock:
            group_info = await self.get_community_info()
            if not group_info:
                self.logger.warning("未获取到高校团队数据")
                return

            save_result = await self.save_to_mongodb(group_info)
            self.logger.info(f"高校团队上传MongoDB完成，共 {save_result} 条")


# ===================== 异步测试入口 =====================
async def main():
    service = UniversityGroupInfo()
    await service.init()
    await service.pipeline()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())

