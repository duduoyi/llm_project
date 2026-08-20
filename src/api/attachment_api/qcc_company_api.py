from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from src.services.company_base_update import CompanyBaseUpdate
from src.utils.logger import get_logger

logger = get_logger("qcc_company")

# === FastAPI 路由 ===
router = APIRouter(
    prefix="/internal/qcc_company",
    tags=["政策附件下载"],
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
@router.post("/")
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
