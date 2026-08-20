from src.core.scheduler import scheduler
from src.api.etl_api.etl_scheduler import register_etl_schedule_jobs
from src.api.attachment_api.download_scheduler import register_download_schedule_jobs


def register_all_scheduled_tasks():
    """全局统一注册全部模块定时任务
    后续新增业务模块，在这里import并调用注册函数
    """
    # 将全局scheduler实例传给各业务注册函数
    register_etl_schedule_jobs(scheduler)
    register_download_schedule_jobs(scheduler)
    # register_xxx_schedule_jobs(scheduler) 新增模块在这里追加