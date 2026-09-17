from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from src.services.company_base_update import CompanyBaseUpdate
from src.services.cos_to_mongo.company_base_info import CompanyBaseInfo
from src.utils.logger import get_logger
from pydantic import BaseModel, Field

logger = get_logger("qcc_company")

# === FastAPI 路由 ===
router = APIRouter(
    prefix="/internal/qcc_company",
    tags=["企查查企业更新"],
    dependencies=[Depends(ETLAuthGuard())]
)

async def qcc_company_download():
    """异步ETL定时任务，与FastAPI共用同一个事件循环"""

    service = CompanyBaseUpdate()
    service_name = service.name

    try:
        if not service:
            logger.error(f"[定时任务失败] {service_name} 不存在")
            return
        logger.info(f"[定时ETL日更] {service_name} 开始执行")
        await service.pipeline()
        logger.info(f"[定时ETL日更] {service_name} 提交成功")
    except Exception as e:
        logger.error(f"[定时ETL] {service_name} 执行异常: {str(e)}", exc_info=True)


# 启动任务接口
@router.post("/qcc_api_update")
async def start_download():
    service = CompanyBaseUpdate()

    try:

        success_count = await service.pipeline()
        return JSONResponse(
            content={
                "status": "success",
                "count": success_count
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e


class CosFileRequest(BaseModel):
    file_name: str = Field(description="company-report-1366204382/qcc_company 路径下文件名")

@router.post("/cos_base_insert")
async def start(req: CosFileRequest):
    service = CompanyBaseInfo()

    try:
        success_count = await service.qcc_company_to_mongodb(req.file_name)
        return JSONResponse(
            content={
                "status": "success",
                "count": success_count
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e