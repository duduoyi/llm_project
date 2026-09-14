from apscheduler.triggers.cron import CronTrigger
from src.utils.logger import get_logger
from src.api.attachment_api.policy_attachment_api import policy_attachment_download
from src.api.attachment_api.qcc_company_api import qcc_company_download

logger = get_logger("download_scheduler")

# 每个ETL服务独立配置【定时cron + 运行参数】
SCHEDULE_DOWNLOAD_TASKS = [
    {
        "job_id": "policy_attachment_download",
        "func": policy_attachment_download,
        "trigger": CronTrigger(hour=3, minute=30),
        "args": [],
        "max_instances": 1
    },
    {
        "job_id": "qcc_company_download",
        "func": qcc_company_download,
        "trigger": CronTrigger(hour=1, minute=30),
        "args": [],  # 函数传参
        "max_instances": 1
    },
]

def register_download_schedule_jobs(scheduler):
    """注册通知模块所有定时任务"""
    for job_cfg in SCHEDULE_DOWNLOAD_TASKS:
        scheduler.add_job(
            func=job_cfg["func"],
            trigger=job_cfg["trigger"],
            args=job_cfg["args"],
            id=job_cfg["job_id"],
            replace_existing=True,
            max_instances=job_cfg["max_instances"]
        )
        logger.info(f"✅ 注册通知定时任务 {job_cfg['job_id']}")
    logger.info(f"✅ 通知模块加载 {len(SCHEDULE_DOWNLOAD_TASKS)} 个定时任务")