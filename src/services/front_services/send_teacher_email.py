from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger
from typing import List
import re

class SendTeacherEmail(BaseResourceService):
    def __init__(self):
        self.teacher_profiles = "teacher_profiles"
        self.email_template_collection = "email_templates"
        self.mail_send_log = "mail_send_log"
        self.score_threshold = 0.7
        self.batch_limit = 1000
        self.logger = get_logger(self.__class__.__name__)

    # async def search_vector(self, event_name, event_keyword, university_list: List[str] = None):
    #     # 批量异步向量化（自带信号量限流）
    #     emb_wrapper = await self.get_emb_wrapper()
    #     query_vector = await emb_wrapper.aembed_query(event_keyword)
    #
    #     # 2. 构建过滤条件 payload.university = 指定大学
    #     must_clauses = []
    #     must_not_clauses = []
    #
    #     # 1. email不为空：字段存在 email != ""
    #     must_not_clauses.append(
    #         FieldCondition(
    #             key="email",
    #             match=MatchValue(value="")
    #         )
    #     )
    #     # 2. university_list不为空，才加入university过滤；空则不生效
    #     if university_list:
    #         must_clauses.append(
    #             FieldCondition(
    #                 key="university",
    #                 match=MatchAny(any=university_list)
    #             )
    #         )
    #     # 3. sent_mail_directions 列表不包含 event_name
    #     # must_not：数组中不能出现该活动名称
    #     must_not_clauses.append(
    #         FieldCondition(
    #             key="sent_mail_directions",
    #             match=MatchAny(any=[event_name])
    #         )
    #     )
    #
    #     search_filter = Filter(
    #         must=must_clauses,
    #         must_not=must_not_clauses
    #     )
    #
    #     qdrant_client = await self.get_qdrant_client()
    #     # 3. 执行向量检索
    #     all_points  = []
    #     current_offset = 0
    #     while True:
    #         resp = await qdrant_client.query_points(
    #             collection_name=self.qdrant_coll,
    #             query=query_vector,
    #             query_filter=search_filter,
    #             limit=self.batch_limit,
    #             offset=current_offset,
    #             score_threshold=self.score_threshold,
    #             with_payload=["mongo_id", "name", "university", "email"],
    #             with_vectors=False
    #         )
    #         hits = resp.points
    #         hit_count = len(hits)
    #         all_points.extend(hits)
    #
    #         # 当前返回数量小于batch_limit，代表没有更多数据，退出循环
    #         if hit_count < self.batch_limit:
    #             self.logger.info(f"qdrant 相似度查询返回结果共计{hit_count} 条")
    #             break
    #
    #         # 手动累加offset，服务端不会返回给你
    #         current_offset += self.batch_limit
    #         self.logger.info(f"qdrant 相似度查询：current_offset：{current_offset}")
    #
    #         # 安全防护，防止死循环，根据业务调整最大值
    #         if current_offset >= 5000:
    #             break
    #
    #     # all_points 就是全部满足条件的命中列表
    #     result_list = []
    #     for point in all_points:
    #         qdrant_point_id = point.id
    #         payload = point.payload or {}
    #         item = {
    #             "qdrant_point_id": qdrant_point_id,
    #             "teacher_id": payload.get("mongo_id"),
    #             "university": payload.get("university"),
    #             "event_name": event_name,
    #             "event_keyword": event_keyword,
    #             "recipient_name": payload.get("name"),
    #             "recipient_email": payload.get("email")
    #         }
    #         result_list.append(item)
    #     self.logger.info(f"共计匹配到{len(result_list)}条数据")
    #
    #     return result_list
    async def search_college_teacher(self, university_list: List[str], college_list: List[str]):
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self.teacher_profiles]

        # 2. 查询条件：指定 university 和 college
        query_filter = {
            "university": {"$in": university_list},
            "college": {"$in": college_list},
            "contact.email": {
                "$exists": True,  # 字段存在
                "$ne": ""  # 不为空字符串
            }
        }

        # 3. 投影：只返回 _id, name, email
        projection = {
            "_id": 1,
            "name": 1,
            "contact.email": 1,
            "university": 1,
            "college": 1
        }
        cursor = collection.find(query_filter, projection=projection)
        # motor异步游标必须 await to_list
        result = await cursor.to_list(length=None)
        return result

    @classmethod
    def get_first_email(cls, recipient_str: str):
        """
        清洗多邮箱字符串 + 基础语法校验，取第一个邮箱
        不做a@b.c类可疑域名过滤；只做基础格式检查
        :param recipient_str: 原始邮箱字符串，支持逗号/分号分隔
        :return: str | None 清洗并校验通过的邮箱；格式错误/无数据返回None
        """
        if not isinstance(recipient_str, str):
            return None

        s = recipient_str
        # 清洗脏字符：换行、回车、tab、全角空格
        s = s.replace("\n", "").replace("\r", "").replace("\t", "").replace("　", "")
        # 分号统一转为逗号
        s = s.replace(";", ",")

        parts = [p.strip() for p in s.split(",")]
        parts = [p for p in parts if p]
        if not parts:
            return None

        first_raw = parts[0]
        # 基础校验
        _email_basic_reg = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
        if _email_basic_reg.match(first_raw):
            return first_raw
        else:
            return None

    async def email_content(self, event_name, teacher_list):
        """
        teacher_list 每条必须包含：name, email, school
        """
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self.email_template_collection]
        doc = await collection.find_one({"mail_event": event_name}, sort=[("updated_at", -1)])
        # 1. 模板不存在校验
        if doc is None:
            raise ValueError(f"找不到 mail_event={event_name} 的邮件模板")

        # 模板正文字符串，假设模板字段名是 body_template，请改成你真实库中的字段！
        template_body = doc["template_body"]


        mail_output = []
        for teacher in teacher_list:
            content_body = template_body.format(name=teacher["name"])
            contact_email = teacher["contact"]["email"]
            recipient_email = self.get_first_email(contact_email)

            mail_item = {
                "teacher_id": teacher["_id"],
                "university": teacher["university"],
                "college": teacher["college"],
                "event_name": event_name,
                "recipient_name": teacher["name"],
                "recipient_email": recipient_email,
                "mail_template_version": doc["_id"],
                "subject": doc["subject"],
                "body": content_body,
                "send_status": "to be sent"
            }
            mail_output.append(mail_item)
        return mail_output

    async def save_email_content(self, mail_output):
        if not mail_output:
            return []
        mongo_db = await self.get_mongo_db()
        log_coll = mongo_db[self.mail_send_log]
        try:
            # ✅ 批量插入
            result = await log_coll.insert_many(mail_output)
            insert_count = len(result.inserted_ids)
            return insert_count
        except Exception as e:
            raise RuntimeError(f"批量保存mail_send_log失败: {str(e)}") from e

    async def pipline(self, mail_event: str, university_list: List[str], college_list: List[str]):
        teacher_list = await self.search_college_teacher(university_list, college_list)
        email = await self.email_content(mail_event, teacher_list)
        insert_count = await self.save_email_content(email)
        self.logger.info(f"邮件日志批量保存完成，mail_event={mail_event}, 成功插入{insert_count}条记录")
        return {"send_status": "to be sent", "insert_count": insert_count}


# ===================== 异步测试入口 =====================
async def main():
    service = SendTeacherEmail()
    teacher_list = await service.search_college_teacher(["北京航空航天大学"], ["仪器科学与光电工程学院"])
    email =  await service.email_content("AOI", teacher_list)
    result = await service.save_email_content(email)
    print(result)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())