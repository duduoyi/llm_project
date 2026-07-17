from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# 使用上海时区全局统一
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

__all__ = ["scheduler", "CronTrigger"]