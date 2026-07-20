from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 全局异步调度器
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
# 对外导出CronTrigger保持原有业务代码不变
__all__ = ["scheduler", "CronTrigger"]