from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger
from src.utils.tz_time import get_current_str
import re
from textwrap import dedent


class SaveEmailTemplate(BaseResourceService):
    def __init__(self):
        self.email_template_collection = "email_templates"
        self.logger = get_logger(self.__class__.__name__)

    @classmethod
    def clean_email_text(cls, raw_text: str) -> str:
        """
        邮件主题、邮件正文数据清洗
        只保留普通可见字符串 + 换行符 \n
        移除 \r \t 以及各类零宽/不可见控制字符
        """
        if raw_text is None:
            return ""
        # 转字符串
        text = str(raw_text)
        # ✅核心：去除多行共同行首缩进空格，专门处理 """多行字符串缩进
        text = dedent(text)

        # 将 \r 全部替换为 \n；tab\t替换为换行
        text = text.replace("\r", "\n").replace("\t", "\n")
        # 移除所有除换行符以外的其他ascii控制字符、零宽字符
        # [\x00-\x08\x0b\x0c\x0e-\x1f\x7f‑] 排除 \n(\x0a)
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u2000-\u200F\u2028-\u202F]', '', text)
        # 多个连续换行合并为单个换行，可选；如果不需要合并注释掉下一行
        text = re.sub(r'\n{3,}', r'\n\n', text)
        return text

    async def save_email_template(self, mail_event, mail_subject, mail_body):
        # ========== 数据清洗步骤 ==========
        cleaned_subject = self.clean_email_text(mail_subject).replace("\n", " ").strip()
        cleaned_body = self.clean_email_text(mail_body)
        # 只更新/新增，不删除旧字段
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self.email_template_collection]
        now_str = get_current_str()
        doc = {
            "mail_event": mail_event,
            "subject": cleaned_subject,
            "template_body": cleaned_body,
            "updated_at": now_str
        }

        # 只插入，不修改任何历史数据
        result = await collection.insert_one(doc)
        if result.inserted_id is not None:
            return True
        else:
            # 理论上insert_one不会出现inserted_id为空，防御性分支
            return False


# ===================== 异步测试入口 =====================
async def main():
    service = SaveEmailTemplate()
    mail_event = "AOI"
    mail_subject = "【产学研对接邀约】链上昆山-视界共创｜AOI视觉检测技术合作交流"
    mail_body = """
    {name}老师好，
    本次致信目的为昆山高新区开展高精度AOI算法领域的产学研合作邀请。
    江苏昆山集聚半导体、光电、精密电子等大量制造企业，可以将企业成熟硬件产品（工控机、边缘盒子、工业相机、BMS采集板等）开放接口，作为高校团队部署算法的标准化底座。经我司对昆山本地制造业企业走访调研发现，当地企业在生产量产环节普遍面临人工检测成本高、传统检测设备准确率、稳定性有待提升等产业痛点，对高精度 AOI 算法方案有较强的合作需求。
    我司为昆山高新区科技局产学研对接服务合作第三方平台——三螺旋科创服务平台，面向高校和本地企业提供免费公益的产学研信息对接服务。
    如果您有产学研合作意向，欢迎联系我方对接负责人，开展进一步沟通。
    冒昧打扰，期待您的反馈。
    """

    mail_body2 = """
    {name}老师好，
    本次致信诚邀您担任昆山高新区高精度AOI算法赛事评委。
    昆山集聚半导体、精密电子制造企业，量产环节普遍存在人工检测成本高、传统AOI设备精度稳定性不足等痛点。本次赛事依托本地产业场景，开放工控机、边缘盒子、工业相机等硬件底座，面向高校团队开展AOI视觉检测算法比拼。
    我司为昆山高新区科技局产学研对接第三方三螺旋科创服务平台。诚盼您以专家身份参与评审，从算法落地、工业实用性等维度给出专业评判。
    冒昧打扰，期待您的反馈。
    """

    result = await service.save_email_template(mail_event, mail_subject, mail_body)
    print(result)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())