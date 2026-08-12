from src.core.scheduler import scheduler, CronTrigger
from src.api.etl_api.etl_api import scheduled_etl_task
from src.utils.logger import get_logger

logger = get_logger("etl_scheduler")

# ====================== ETL定时任务配置【仅ETL模块自己的定时任务】 ======================
# 每个ETL服务独立配置【定时cron + 运行参数】
SCHEDULE_ETL_TASKS = {
    "teacher_profile_vector": {
        "cron": CronTrigger(hour=2, minute=30),
        "config": {
            "batch_size": 1000
        }
    },
    # 可继续添加其他服务，各自独立定时
    # "xxx_service": {
    #     "cron": CronTrigger(hour=4, minute=0),
    #     "config": {"batch_size":2000}
    # }
}


def register_etl_schedule_jobs():
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
        logger.info(f"✅ 注册定时任务 {job_id} | cron: {cron_trigger}")
    logger.info(f"✅ 总计加载 {len(SCHEDULE_ETL_TASKS)} 个ETL定时任务")

