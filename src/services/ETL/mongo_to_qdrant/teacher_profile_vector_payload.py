import hashlib
from src.services.ETL.base_etl_service import BaseETLService
from qdrant_client.http import models


class TeacherProfilePayload(BaseETLService):
    """
    教师信息向量入库ETL
    Mongo teacher_profiles → 提取payload → Qdrant
    """
    name = "teacher_profile_payload"
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
                "contact.email": 1
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

        try:
            # ===================== 2. 把字典 → payload 对象 =====================
            payloads = []
            point_ids = []

            for doc in batch_data:
                mongo_obj_id = doc.get("_id")
                email = doc.get("email", "")
                # 统一哈希ID规则：mongo_obj_id
                point_id = int(hashlib.sha256(str(mongo_obj_id).encode("utf-8")).hexdigest()[:16], 16)
                point_ids.append(point_id)

                # 构造payload元数据
                update_payload = {
                    "email": str(email),
                }
                meta_payload = models.SetPayloadOperation(
                    set_payload=models.SetPayload(
                        payload=update_payload,
                        points=[point_id]
                    )
                )

                payloads.append(meta_payload)

            # 连接 qdrant
            qdrant_collection = 'teacher_profile'
            qdrant_client = await self.get_qdrant_client()
            # 检查集合是否已存在
            if not await qdrant_client.collection_exists(qdrant_collection):
                self.logger.error(f"集合{qdrant_collection}不存在")
                return False, f"集合{qdrant_collection}不存在"

            # 批量获取point的存在性（仅查询ID，不返回向量/payload）
            points_exist = await qdrant_client.retrieve(
                collection_name=qdrant_collection,
                ids=point_ids,  # Qdrant支持字符串ID
                with_payload=False,
                with_vectors=False
            )

            # 提取存在的point ID（确保为字符串）
            exist_ids = [p.id for p in points_exist]
            # 记录不存在的ID（仅日志提示，不报错）
            not_exist_ids = [pid for pid in point_ids if pid not in exist_ids]
            if not_exist_ids:
                self.logger.info(f"检测到{len(exist_ids)}个point存在")

            result = await qdrant_client.batch_update_points(
                collection_name=qdrant_collection,
                update_operations=payloads
            )
            self.logger.info(f"学者元数据已更新到qdrant集合{qdrant_collection}：{result}")
            return True, "success"

        except Exception as e:
            return False, str(e)


# 测试
async def main():
    config = {
        "start_time": None,
        "end_time": None,
        "batch_size": 1500
    }
    service = TeacherProfilePayload(config)
    batch = await service.extract_batch(last_id=0)
    ok, msg = await service.process_batch(batch)
    if not ok:
        print(f"批次处理结果：{ok}, {msg}")

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())