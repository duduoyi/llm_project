from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from src.services.front_services.search_tech_teacher import SearchTechTeacherService
from src.services.front_services.search_similar_teacher import SearchSimilarTeacherService
from src.services.front_services.save_email_template import SaveEmailTemplate
from src.services.front_services.send_teacher_email import SendTeacherEmail
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List


# === FastAPI 路由 ===
router = APIRouter(
    prefix="/front/search",
    tags=["前端数据接口"],
    dependencies=[Depends(ETLAuthGuard())]
)

# 请求体模型
class SearchWordReq(BaseModel):
    tech_keyword: str          # 用户输入技术关键词
    university: str            # 指定大学名称
    top_k: Optional[int] = 20  # 返回top结果数量

@router.post("/tech_word_search_teacher")
async def run_word_search_teacher(req: SearchWordReq):
    teacher_search_service = SearchTechTeacherService()
    try:
        result = await teacher_search_service.search_vector(req.tech_keyword, req.university, req.top_k)
        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e

class SearchTeacherReq(BaseModel):
    mongo_id: str          # 转换为字符串的mongo_id
    top_k: Optional[int] = 20  # 返回top结果数量

@router.post("/search_similar_teacher")
async def run_search_similar_teacher(req: SearchTeacherReq):
    teacher_search_service = SearchSimilarTeacherService()
    try:
        result = await teacher_search_service.search_vector(req.mongo_id, req.top_k)
        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e


class EmailTemplateReq(BaseModel):
    mail_event: str = Field(description="活动类型")
    mail_subject: str = Field(description="邮件主题")
    mail_body: str = Field(description="邮件正文模板，支持 {name} {contact_name} 等占位符")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "mail_event": "AOI",
                    "mail_subject": "【产学研对接邀约】链上昆山-视界共创｜AOI视觉检测技术合作交流",
                    "mail_body": """{name}老师好，
本次致信目的为昆山高新区开展高精度AOI算法领域的产学研合作邀请。
江苏昆山集聚半导体、光电、精密电子等大量制造企业，可以将企业成熟硬件产品（工控机、边缘盒子、工业相机、BMS采集板等）开放接口，作为高校团队部署算法的标准化底座。经我司对昆山本地制造业企业走访调研发现，当地企业在生产量产环节普遍面临人工检测成本高、传统检测设备准确率、稳定性有待提升等产业痛点，对高精度 AOI 算法方案有较强的合作需求。
我司为昆山高新区科技局产学研对接服务合作第三方平台——三螺旋科创服务平台，面向高校和本地企业提供免费公益的产学研信息对接服务。
如果您有产学研合作意向，欢迎联系我方对接负责人，开展进一步沟通。
冒昧打扰，期待您的反馈。
三螺旋科创服务平台
"""
                }
            ]
        }
    }

@router.post("/save_email_templates")
async def save_email_templates_api(req: EmailTemplateReq):
    service = SaveEmailTemplate()
    try:
        result = await service.save_email_template(req.mail_event, req.mail_subject, req.mail_body)
        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e


class SendEmailReq(BaseModel):
    mail_event: str = Field(description="根据活动类型选取最新邮件模板")
    university_list: List[str] = Field(description="高校列表")
    college_list: List[str] = Field(description="学院列表")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "mail_event": "AOI",
                    "university_list": ["北京航空航天大学"],
                    "college_list": ["计算机学院"]
                }
            ]
        }
    }

@router.post("/send_email")
async def send_email_api(req: SendEmailReq):
    service = SendTeacherEmail()
    try:
        result = await service.pipline(req.mail_event, req.university_list, req.college_list)
        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e

## 邮件联系人接口
# class ContactUserReq(BaseModel):
#     contact_name: str = Field(description="联系人姓名")
#     contact_phone: str = Field(
#         pattern=r"^1[3-9]\d{9}$",
#         description="联系人电话，中国大陆11位手机号"
#     )
#     contact_email: EmailStr = Field(description="联系人邮箱")
#     contact_type: str = Field(description="高校端/企业端/政府端")
#     contact_province: List[str] = Field(description="负责省份")
#
#
# @router.post("/save_contact_user")
# async def save_contact_user_api(req: ContactUserReq):
#     service = SaveContactUser()
#     try:
#         result = await service.save_contact_user(req.contact_name, req.contact_phone, req.contact_email, req.contact_type, req.contact_province)
#         return JSONResponse(
#             content={
#                 "status": "success",
#                 "message": result
#             },
#             media_type="application/json; charset=utf-8"  # 明确指定编码
#         )
#     except HTTPException as e:
#         # 直接抛出已定义的HTTP异常（如数据库连接失败）
#         raise e