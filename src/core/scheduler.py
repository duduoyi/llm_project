from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from src.utils.logger import get_logger

logger = get_logger("scheduler")
# 后续新增任何业务模块，只需要在这里import对应的注册函数

# 全局异步调度器
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
# 对外导出CronTrigger保持原有业务代码不变
__all__ = ["scheduler", "CronTrigger"]


async def init_scheduler():
    if not scheduler.running:
        scheduler.start()
    logger.info("⏰ 全局APScheduler调度器启动完成")

async def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.info("⏰ 全局APScheduler调度器正常关闭")