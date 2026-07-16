from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from src.services.front_services.search_tech_teacher import SearchTechTeacherService
from src.services.front_services.search_similar_teacher import SearchSimilarTeacherService
from pydantic import BaseModel
from typing import Optional


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