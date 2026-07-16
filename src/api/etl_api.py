import asyncio
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
import uuid
from pydantic import BaseModel, Field
from enum import Enum
from src.services.ETL.task_manager import TaskManager
from src.services.ETL.service_registry import ServiceRegistry
from datetime import datetime

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

class ETLRequest(BaseModel):
    start_time: str = None
    end_time: str = None
    batch_size: int = 1000  # 直接设置batch_size默认值为1000

    def __init__(self, **data):
        # 获取当前时间
        now = datetime.now()

        # 计算上个月1号0点：先回退到上个月，再取1号0点0分0秒
        # 处理1月回退到去年12月的情况
        last_month = now.month - 1 if now.month > 1 else 12
        last_month_year = now.year if now.month > 1 else now.year - 1
        # 构造上个月1号0点的datetime对象
        default_start = datetime(last_month_year, last_month, 1, 0, 0, 0)

        # 计算当月1号0点
        default_end = datetime(now.year, now.month, 1, 0, 0, 0)

        # 格式化为字符串（默认用常见的"%Y-%m-%d %H:%M:%S"格式，可根据需求调整）
        default_start_str = default_start.strftime("%Y-%m-%d %H:%M:%S")
        default_end_str = default_end.strftime("%Y-%m-%d %H:%M:%S")

        # 如果用户未传start_time/end_time，使用默认值
        if "start_time" not in data or data["start_time"] is None:
            data["start_time"] = default_start_str
        if "end_time" not in data or data["end_time"] is None:
            data["end_time"] = default_end_str

        # 调用父类的初始化方法
        super().__init__(**data)


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