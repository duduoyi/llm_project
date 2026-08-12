from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from src.api.etl_api.etl_scheduler import register_etl_schedule_jobs
from src.api.attachment_api.download_scheduler import register_download_schedule_jobs
from src.utils.logger import get_logger

logger = get_logger("scheduler")
# 后续新增任何业务模块，只需要在这里import对应的注册函数

# 全局异步调度器
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
# 对外导出CronTrigger保持原有业务代码不变
__all__ = ["scheduler", "CronTrigger"]


def register_all_scheduled_tasks():
    """全局统一注册全部模块定时任务"""
    # 逐个业务模块注册
    register_etl_schedule_jobs()
    register_download_schedule_jobs()
    # register_xxx_schedule_jobs() 新增模块在这里追加

async def init_scheduler():
    # 先注册所有job，再启动scheduler（最佳顺序）
    register_all_scheduled_tasks()
    if not scheduler.running:
        scheduler.start()
    logger.info("⏰ 全局APScheduler调度器启动完成")

async def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.info("⏰ 全局APScheduler调度器正常关闭")