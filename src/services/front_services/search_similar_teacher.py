from src.services.base_resource_service import BaseResourceService
from qdrant_client.models import Filter, FieldCondition, MatchValue


class SearchSimilarTeacherService(BaseResourceService):
    def __init__(self):
        self.mongo_collection = "teacher_profiles"
        self.qdrant_coll = "teacher_profile"
        self.score_threshold = 0.7

    async def search_vector(self, mongo_id: str, top_k = 20):
        qdrant_client = await self.get_qdrant_client()
        # 1. 根据 payload.mongo_id 找到目标向量
        search_target = await qdrant_client.scroll(
            collection_name=self.qdrant_coll,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="mongo_id",
                        match=MatchValue(value=mongo_id)
                    )
                ]
            ),
            limit=1,
            with_vectors=True,
            with_payload=False
        )

        records, _ = search_target
        if not records:
            print(f"该老师无研究领域与应用领域，不存在 mongo_id = {mongo_id} 的向量")
        else:
            target_point = records[0]
            target_vector = target_point.vector

            # 第二步：使用该向量搜索相似向量，排除自身
            resp = await qdrant_client.query_points(
                collection_name=self.qdrant_coll,
                query=target_vector,
                query_filter=Filter(
                    must_not=[
                        FieldCondition(
                            key="mongo_id",
                            match=MatchValue(value=mongo_id)
                        )
                    ]
                ),
                limit=top_k,  # 返回topN相似向量
                score_threshold=self.score_threshold,  # 可选：相似度阈值，根据距离函数调整
                with_payload=["mongo_id"],
                with_vectors=False
            )

            # 打印结果
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
    #     mongo_db = await self.get_mongo_db()
    #     coll = mongo_db[self.mongo_collection]
    #
    #     from bson import ObjectId
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
#     service = SearchSimilarTeacherService()
#     result = await service.search_vector("6a4dff3bdc7b967171b3033c", 10)
#     # teachers = await service.get_teacher_by_mongo_ids(result)
#     print(result)
#
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())