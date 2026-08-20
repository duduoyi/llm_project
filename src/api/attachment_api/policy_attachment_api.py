from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from pydantic import BaseModel, Field
from src.services.policy_attachment_to_cos import AttachmentConsumer
from datetime import datetime, timedelta
from src.utils.logger import get_logger

logger = get_logger("policy_attachment_api")

# === FastAPI 路由 ===
router = APIRouter(
    prefix="/internal/policy_attachment",
    tags=["政策附件下载"],
    dependencies=[Depends(ETLAuthGuard())]
)

async def policy_attachment_download():
    """异步ETL定时任务，与FastAPI共用同一个事件循环"""

    service = AttachmentConsumer()
    service_name = service.name

    try:
        # 定时任务自动填充前一日时间范围
        now = datetime.now()
        start_time = datetime(now.year, now.month, now.day, 0, 0, 0)
        end_time = start_time + timedelta(days=1)

        if not service:
            logger.error(f"[定时任务失败] {service_name} 不存在")
            return
        logger.info(f"[定时ETL日更(前一日数据)] {service_name} 开始执行，参数: start_time = {start_time}, end_time = {end_time}")
        await service.batch_scan_today_task(start_time, end_time)
        logger.info(f"[定时ETL日更(前一日数据)] {service_name} 提交成功")
    except Exception as e:
        logger.error(f"[定时ETL] {service_name} 执行异常: {str(e)}", exc_info=True)


class DownloadRequest(BaseModel):
    start_time: str = Field(
        pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
        description="格式：yyyy-MM-dd HH:mm:ss",
        examples=["2026-07-01 00:00:00"]
    )
    end_time: str = Field(
        pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
        description="格式：yyyy-MM-dd HH:mm:ss",
        examples=["2026-07-20 00:00:00"]
    )

# 启动任务接口
@router.post("/download")
async def start_download(
        request: DownloadRequest
):
    service = AttachmentConsumer()

    try:
        start_dt = datetime.strptime(request.start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(request.end_time, "%Y-%m-%d %H:%M:%S")

        result = await service.batch_scan_today_task(start_dt, end_dt)
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
