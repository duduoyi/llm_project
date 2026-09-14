import asyncio
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
import uuid
from pydantic import BaseModel, Field
from enum import Enum
from src.services.ETL.task_manager import TaskManager
from src.services.ETL.service_registry import ServiceRegistry
from datetime import datetime, timedelta
from src.utils.logger import get_logger

logger = get_logger("etl_api")

# === FastAPI 路由 ===
router = APIRouter(
    prefix="/internal/etl",
    tags=["ETL任务管理"],
    dependencies=[Depends(ETLAuthGuard())]
)

# 全局单例任务管理器（可复用，内部自带信号量限制并发任务）
task_manager = TaskManager(
    max_concurrent_batches=2,
    max_queue_size=5,
    consumer_workers=3
)

class ETLTypes(str, Enum):
    # qdrant_company_patent = 'qdrant_company_patent'
    # qdrant_university_patent = 'qdrant_university_patent'
    # qdrant_company_patent_payload = 'qdrant_company_patent_payload'
    # qdrant_university_patent_payload = 'qdrant_university_patent_payload'
    # neo4j_scholar = 'neo4j_scholar'
    # neo4j_university_patent = 'neo4j_university_patent'
    # neo4j_company_patent = 'neo4j_company_patent'
    teacher_profile_vector = 'teacher_profile_vector'
    teacher_profile_payload = 'teacher_profile_payload'

class ETLRequest(BaseModel):
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
    batch_size: int = 1000

async def scheduled_etl_task(service_name: str, cfg: dict):
    """异步ETL定时任务，与FastAPI共用同一个事件循环"""
    run_id = f"{service_name}_daily_{uuid.uuid4().hex[:8]}"
    try:
        # 定时任务自动填充前一日时间范围
        now = datetime.now()
        yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        task_cfg = cfg.copy()
        if "start_time" not in task_cfg or task_cfg["start_time"] is None:
            task_cfg["start_time"] = yesterday_start.strftime("%Y-%m-%d %H:%M:%S")
        if "end_time" not in task_cfg or task_cfg["end_time"] is None:
            task_cfg["end_time"] = today_start.strftime("%Y-%m-%d %H:%M:%S")

        req_model = ETLRequest(**task_cfg)
        service = ServiceRegistry.get_instance(service_name, req_model.model_dump())
        if not service:
            logger.error(f"[定时任务失败] {run_id} service {service_name} 不存在")
            return
        logger.info(f"[定时ETL日更(前一日数据)] {run_id} 开始执行，参数: {req_model.model_dump()}")
        await task_manager.start_job(run_id, service)
        logger.info(f"[定时ETL日更(前一日数据)] {run_id} 提交成功")
    except Exception as e:
        logger.error(f"[定时ETL] {service_name} 执行异常: {str(e)}", exc_info=True)


# 启动任务接口
@router.post("/start")
async def start_task(
        service_name: ETLTypes,
        service_config: ETLRequest
):
    run_id = f"{service_name.value}_{uuid.uuid4().hex[:8]}"

    service = ServiceRegistry.get_instance(
        service_name.value,
        service_config.dict()
    )

    if not service:
        raise HTTPException(404, "service not found")

    # 启动任务管理器
    await task_manager.start_job(run_id, service)

    return {"run_id": run_id, "message": "etl task started"}

class ResumeETLRequest(BaseModel):
    run_id: str = Field(..., description="需要断点续跑的任务ID")
    service_name: ETLTypes
    service_config: ETLRequest


@router.post("/resume")
async def resume_task(req: ResumeETLRequest):
    run_id = req.run_id

    service = ServiceRegistry.get_instance(
        req.service_name,
        req.service_config.dict()
    )

    if not service:
        raise HTTPException(404, "service not found")

    # 启动任务管理器
    await task_manager.start_job(run_id, service)

    return {
        "run_id": run_id,
        "message": "resume started"
    }


@router.post("/rerun_failed")
async def rerun_failed_task(req: ResumeETLRequest):

    service = ServiceRegistry.get_instance(
        req.service_name,
        req.service_config.dict()
    )

    if not service:
        raise HTTPException(404, "service not found")

    # 后台执行重跑
    asyncio.create_task(task_manager.rerun_failed(req.run_id, service))

    return {
        "run_id": req.run_id,
        "message": "failed batch rerun started"
    }

