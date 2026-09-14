import hashlib
from src.utils.singletons.ai_singleton import EmbeddingFactory
from src.services.ETL.base_etl_service import BaseETLService
from qdrant_client.http import models


class TeacherProfileVector(BaseETLService):
    """
    教师信息向量入库ETL
    Mongo teacher_profiles → 向量化 → Qdrant
    """
    name = "teacher_profile_vector"
    source_type = "mongodb"

    async def extract_batch(self, last_id=None):
        """分批从Mongo读取教师数据（游标分页）"""
        mongo_db = await self.get_mongo_db()
        coll = mongo_db["teacher_profiles"]
        filter_query = {}

        # 时间范围筛选条件
        time_filter = {}
        if self.start_time is not None:
            time_filter["$gte"] = self.start_time
        if self.end_time is not None:
            time_filter["$lt"] = self.end_time
        if time_filter:
            filter_query["createTime"] = time_filter

        if last_id not in (None, 0):
            filter_query["_id"] = {"$gt": last_id}

        # ===================== 新增：剔除两个字段同时为空的文档 =====================
        # 条件：不要【research_areas空 AND application_areas空】的数据
        filter_query["$nor"] = [
            {
                "$and": [
                    # research_areas 为空数组 / null / 不存在
                    {"research_areas": {"$in": [None, []]}},
                    # application_areas 为空数组 / null / 不存在
                    {"application_areas": {"$in": [None, []]}}
                ]
            }
        ]
        # ==========================================================================

        # 只读取需要的字段，减少网络传输
        cursor = coll.find(
            filter_query,
            {
                "_id": 1,
                "name": 1,
                "university": 1,
                "college": 1,
                "contact.email": 1,
                "research_areas": 1,
                "application_areas": 1,
                "createTime": 1
            }
        ).sort("_id", 1).limit(self.batch_size)

        batch_data = []
        async for doc in cursor:
            # 【兼容TaskManager！新增虚拟字段 id = _id】
            doc["id"] = doc["_id"]
            email = doc.get("contact", {}).get("email", None)
            doc.pop("contact", None)  # 移除嵌套contact
            doc["email"] = email  # 顶层增加email字段
            batch_data.append(doc)

        return batch_data

    async def process_batch(self, batch_data):
        """单批次处理：拼接文本 → 向量化 → 组装Qdrant Point"""
        if not batch_data:
            self.logger.info("当前批次无教师数据")
            return True, "empty"

        text_list = []
        point_meta = []
        point_ids = []
        skip_count = 0

        for doc in batch_data:
            mongo_obj_id = doc.get("_id")
            name = doc.get("name", "")
            university = doc.get("university", "")
            college = doc.get("college", "")
            email = doc.get("email", "")
            research_areas = str(doc.get("research_areas", "")).strip()
            application_areas = str(doc.get("application_areas", "")).strip()

            # ========= 过滤规则 =========
            # 两个字段同时为空/空白，直接跳过本条记录
            if not research_areas and not application_areas:
                skip_count += 1
                continue

            # 拼接向量化文本（可根据检索效果调整拼接模板）
            embed_text = f"研究领域：{research_areas}；应用领域：{application_areas}"
            text_list.append(embed_text)

            # 构造payload元数据
            payload = {
                "mongo_id": str(mongo_obj_id),
                "name": str(name),
                "university": str(university),
                "college": str(college),
                "email": str(email),
            }
            point_meta.append(payload)

            # 统一哈希ID规则：mongo_obj_id
            point_id = int(hashlib.sha256(str(mongo_obj_id).encode("utf-8")).hexdigest()[:16], 16)
            point_ids.append(point_id)

        if skip_count > 0:
            self.logger.info(f"本批次过滤丢弃【{skip_count}】条：研究领域、应用领域同时为空")

        # 全部被过滤，无需继续执行
        if not text_list:
            self.logger.info("本批次所有记录均被过滤，跳过向量入库")
            return True, "all_skipped"

        # 批量异步向量化（自带信号量限流）
        emb_wrapper = EmbeddingFactory.get_instance()
        vectors = await emb_wrapper.aembed_documents(text_list)
        # 连接 qdrant
        qdrant_collection = 'teacher_profile'
        qdrant_client = await self.get_qdrant_client()
        exist_flag = await qdrant_client.collection_exists(qdrant_collection)
        if not exist_flag:
            self.logger.error(f"Qdrant集合 {qdrant_collection} 不存在！请先创建集合")
            return False, f"集合{qdrant_collection}不存在"

        # 组装PointStruct
        # --------------------------
        # 新增逻辑：批量读取已存在point的 sent_mail_directions
        # --------------------------
        retrieve_res = await qdrant_client.retrieve(
            collection_name=qdrant_collection,
            ids=point_ids,
            with_payload=["sent_mail_directions"],
            with_vector=False
        )
        # id -> 历史已发送方向列表映射
        sent_dir_map = {}
        for point in retrieve_res:
            pid = int(point.id)
            pl = point.payload or {}
            sent_dir_map[pid] = pl.get("sent_mail_directions", [])

        points = []
        for pid, vec, meta in zip(point_ids, vectors, point_meta):
            final_payload = dict(meta)
            # 存在则取旧值，不存在初始化为空列表
            final_payload["sent_mail_directions"] = sent_dir_map.get(pid, [])
            points.append(models.PointStruct(
                id=pid,
                vector=vec,
                payload=final_payload
            ))

        # ========== Qdrant写入，加全局锁 ==========
        await qdrant_client.upsert(
            collection_name=qdrant_collection,
            points=points
        )
        self.logger.info(f"✅ 批次入库成功，数量：{len(points)}")
        return True, "success"

# # 测试
# async def main():
#     config = {
#         "start_time": None,
#         "end_time": None,
#         "batch_size": 10
#     }
#     service = TeacherProfileVector(config)
#     batch = await service.extract_batch(last_id=0)
#     ok, msg = await service.process_batch(batch)
#     if not ok:
#         print(f"批次处理结果：{ok}, {msg}")
#
# if __name__ == '__main__':
#     import asyncio
#     asyncio.run(main())