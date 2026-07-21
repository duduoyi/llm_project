from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from src.services.data_services.qdrant_service import QdrantService
from pydantic import BaseModel

# === FastAPI 路由 ===
router = APIRouter(
    prefix="/internal/qdrant",
    tags=["qdrant数据处理"],
    dependencies=[Depends(ETLAuthGuard())]
)

qdrant_service = QdrantService()

# 请求体模型
class QdrantCollectionName(BaseModel):
    collection_name: str


@router.post("/create_collection")
async def run_qdrant_create_collection(
        request: QdrantCollectionName
):
    try:
        # 安全判断：方法是否存在
        result = await qdrant_service.create_collection(request.collection_name)
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
