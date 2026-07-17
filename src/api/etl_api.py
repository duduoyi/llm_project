import asyncio
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
import uuid
from pydantic import BaseModel, Field
from enum import Enum
from src.core.scheduler import scheduler, CronTrigger
from src.services.ETL.task_manager import TaskManager
from src.services.ETL.service_registry import ServiceRegistry
from datetime import datetime, timedelta

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

# ====================== ETL定时任务配置【仅ETL模块自己的定时任务】 ======================
# 每个ETL服务独立配置【定时cron + 运行参数】
SCHEDULE_ETL_TASKS = {
    "teacher_profile_vector": {
        "cron": CronTrigger(hour=2, minute=30),
        "config": {
            "batch_size": 1000
        }
    }
    # 可继续添加其他服务，各自独立定时
    # "xxx_service": {
    #     "cron": CronTrigger(hour=4, minute=0),
    #     "config": {"batch_size":2000}
    # }
}


def scheduled_etl_task(service_name: str, cfg: dict):
    """APScheduler 同步包装器，执行异步ETL任务【每日更新前一天数据】"""
    async def run():
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
                print(f"[定时任务失败] {run_id} service {service_name} 不存在")
                return
            print(f"[定时ETL日更(前一日数据)] {run_id} 开始执行，参数: {req_model.model_dump()}")
            await task_manager.start_job(run_id, service)
            print(f"[定时ETL日更(前一日数据)] {run_id} 提交成功")
        except Exception as e:
            print(f"[定时ETL] {service_name} 执行异常: {str(e)}")

    asyncio.run(run())


def register_all_schedule_jobs():
    """遍历配置注册所有独立定时任务"""
    for srv_name, task_info in SCHEDULE_ETL_TASKS.items():
        job_id = f"daily_etl_{srv_name}"
        cron_trigger = task_info["cron"]
        task_cfg = task_info["config"]

        scheduler.add_job(
            func=scheduled_etl_task,
            trigger=cron_trigger,
            args=[srv_name, task_cfg],
            id=job_id,
            replace_existing=True,
            max_instances=1
        )
        print(f"✅ 注册定时任务 {job_id} | cron: {cron_trigger}")
    print(f"✅ 总计加载 {len(SCHEDULE_ETL_TASKS)} 个ETL定时任务")

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
    start_time: str
    end_time: str
    batch_size: int = 1000  # 直接设置batch_size默认值为1000

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


# ====================== 定时任务查询接口 ======================
@router.get("/schedule/list")
async def list_schedule_jobs():
    """查看当前所有ETL定时任务及下次执行时间"""
    jobs = scheduler.get_jobs()
    res = []
    for job in jobs:
        res.append({
            "job_id": job.id,
            "service_name": job.args[0] if len(job.args) > 0 else None,
            "next_run_time": job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    return {"schedule_jobs": res}