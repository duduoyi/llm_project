from src.utils.singletons.ai_singleton import EmbeddingFactory
from src.utils.singletons.database_singleton import AsyncQdrantSingleton
from qdrant_client.models import Filter, FieldCondition, MatchValue


class SearchTechTeacherService:
    def __init__(self):
        self.qdrant_coll = "teacher_profile"
        self.mongo_dbname = "dquot"
        self.mongo_collection = "teacher_profile"
        self.score_threshold = 0.5

    async def search_vector(self, text, university, top_k = 20):
        # 批量异步向量化（自带信号量限流）
        emb_wrapper = EmbeddingFactory.get_instance()
        query_vector = await emb_wrapper.aembed_query(text)

        # 2. 构建过滤条件 payload.university = 指定大学
        search_filter = Filter(
            must=[
                FieldCondition(
                    key="university",
                    match=MatchValue(value=university)
                )
            ]
        )

        qdrant_client = await AsyncQdrantSingleton.get_instance("development")
        # 3. 执行向量检索
        resp = await qdrant_client.query_points(
            collection_name=self.qdrant_coll,
            query=query_vector,
            query_filter=search_filter,
            limit=top_k,
            score_threshold=self.score_threshold,
            with_payload=["mongo_id"],
            with_vectors=False
        )

        search_result = resp.points
        # 4. 提取 payload.mongo_id
        mongo_id_list = []
        for hit in search_result:
            payload = hit.payload
            if payload and "mongo_id" in payload:
                teacher_info = {
                    "_id": payload["mongo_id"],
                    "score": hit.score,
                }

                mongo_id_list.append(teacher_info)

        return mongo_id_list

    # async def get_teacher_by_mongo_ids(self, mongo_id_list):
    #     """
    #     根据mongo_id列表批量查询mongodb文档
    #     :param mongo_id_list: qdrant中取出的payload.mongo_id 字符串列表
    #     :return: mongodb完整文档列表
    #     """
    #     if not mongo_id_list:
    #         return []
    #
    #     from src.utils.singletons.database_singleton import AsyncMongoSingleton
    #     from bson import ObjectId
    #     mongo_client = await AsyncMongoSingleton.get_instance()
    #     coll = mongo_client[self.mongo_dbname][self.mongo_collection]
    #
    #     #mongo_id 是字符串，对应 mongo _id(ObjectId)，必须转ObjectId
    #     try:
    #         obj_id_list = [ObjectId(mid) for mid in mongo_id_list]
    #     except Exception as e:
    #         raise ValueError(f"非法mongo_id，无法转为ObjectId: {e}")
    #
    #     filter_query = {"_id": {"$in": obj_id_list}}
    #
    #     # 批量查询
    #     cursor = coll.find(filter_query)
    #     docs = await cursor.to_list(length=None)
    #
    #     return docs

# # ===================== 异步测试入口 =====================
# async def main():
#     service = SearchTechTeacherService()
#     result = await service.search_vector("材料", "西安交通大学")
#     # teachers = await service.get_teacher_by_mongo_ids(result)
#     # print(teachers)
#
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())