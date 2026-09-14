from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger
from src.utils.tz_time import get_current_str

class SaveContactUser(BaseResourceService):
    def __init__(self):
        self.contact_users = "contact_users"
        self.logger = get_logger(self.__class__.__name__)
    async def save_contact_user(self, cantact_name, contact_phone, contact_email, contact_type, contact_province):
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self.contact_users]
        now_str = get_current_str()
        doc = {
            "cantact_name": cantact_name,
            "contact_phone": contact_phone,
            "contact_email": contact_email,
            "contact_type": contact_type,
            "contact_province": contact_province,
            "updated_at": now_str
        }

        # contact_phone作为唯一匹配条件，存在则更新，不存在则插入（upsert）
        filter_query = {"contact_phone": contact_phone}
        # $set: 只更新传入字段，不会覆盖其他原有字段；如果希望全量覆盖直接用doc
        update_data = {"$set": doc}

        result = await collection.update_one(filter_query, update_data, upsert=True)

        # upsert结果判断：匹配到更新 / 新插入 都算成功
        # matched_count>0:已有数据更新；upserted_id不为None:新插入
        if result.matched_count > 0 or result.upserted_id is not None:
            return True
        else:
            return False


# ===================== 异步测试入口 =====================
async def main():
    service = SaveContactUser()
    contact_name = "齐圆缘"
    contact_phone = ""
    contact_email = "sunmenglin@dquot.com"
    contact_type = "高校端"

    contact_province2 = ["上海市", "江苏省", "浙江省", "安徽省"]
    contact_province = ["陕西省", "山西省", "湖北省", "湖南省", "甘肃省", "云南省"]
    contact_province4 = ["吉林省", "辽宁省", "黑龙江省", "福建省", "江西省", "广东省"]
    result = await service.save_contact_user(contact_name, contact_phone, contact_email, contact_type, contact_province)
    print(result)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())