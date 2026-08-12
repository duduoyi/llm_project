from fastapi import APIRouter
from datetime import datetime
from typing import Dict, Any
from src.core.scheduler import scheduler

router = APIRouter(tags=["Health Check"])


# 基础健康检查 - 轻量快速
@router.get("/health", summary="基础健康检查", response_model=Dict[str, Any])
async def health_check():
    """
    基础健康检查接口，快速判断服务是否存活
    不涉及耗时操作，适合负载均衡器或监控系统的高频检查
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "fastapi_etl"
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