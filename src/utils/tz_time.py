# 统一时区
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any
from starlette.responses import JSONResponse


# 设置系统时区为上海
os.environ['TZ'] = 'Asia/Shanghai'
try:
    time.tzset()  # Unix/Linux 系统有效
except AttributeError:
    # Windows 系统不支持 tzset()，无需处理
    pass

# 上海时区对象 (UTC+8)
SHANGHAI_TZ = timezone(timedelta(hours=8))

# 标准时间格式
STD_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# 标准日期格式
STD_DATE_FORMAT = "%Y-%m-%d"


def to_shanghai_time(dt: datetime) -> datetime:
    """将任意datetime转换为上海时区"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=SHANGHAI_TZ)
    return dt.astimezone(SHANGHAI_TZ)

def format_datetime(dt: datetime) -> str:
    """格式化datetime为标准格式字符串"""
    return to_shanghai_time(dt).strftime(STD_DATETIME_FORMAT)

def get_current_time() -> datetime:
    """获取当前上海时区的datetime对象"""
    return datetime.now(SHANGHAI_TZ)

def get_current_str() -> str:
    current_time = get_current_time()
    return format_datetime(current_time)

def get_today_str() -> str:
    """获取当前上海时区的datetime对象"""
    return datetime.now(SHANGHAI_TZ).strftime(STD_DATE_FORMAT)

def get_yesterday_str() -> str:
    """获取当前上海时区的datetime对象"""
    return datetime.now(SHANGHAI_TZ).strftime(STD_DATE_FORMAT)

def get_today_start_str() -> str:
    """获取当天0点的字符串格式（YYYY-MM-DD 00:00:00）"""
    today_start = get_current_time().replace(hour=0, minute=0, second=0, microsecond=0)
    return format_datetime(today_start)

def get_yesterday_start_str() -> str:
    """获取前一天0点的字符串格式（YYYY-MM-DD 00:00:00）"""
    yesterday_start = (get_current_time() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return format_datetime(yesterday_start)

def get_3_years_ago_jan1() -> str:
    """
    获取当前年份减3的1月1日，格式：YYYY-01-01
    示例：当前2025年 → 返回2022-01-01；当前2024年 → 返回2021-01-01
    """
    # 获取当前年份
    current_year = datetime.now().year
    # 计算目标年份（当前年份-3），拼接固定日期1月1日
    target_year = current_year - 3
    start_date = f"{target_year}-01-01"
    return start_date

def get_last_year_start_str() -> str:
    """
    获取去年起始时间，格式：YYYY-01-01 00:00:00
    """
    # 获取当前年份
    current_year = datetime.now().year
    last_year = current_year - 1
    datetime_str = f"{last_year}-01-01 00:00:00"
    return datetime_str

def get_current_year_start_str() -> str:
    """
    获取今年起始时间，格式：YYYY-01-01 00:00:00
    """
    # 获取当前年份
    current_year = datetime.now().year
    datetime_str = f"{current_year}-01-01 00:00:00"
    return datetime_str

def get_next_year_start_str() -> str:
    """
    获取明年起始时间，格式：YYYY-01-01 00:00:00
    """
    # 获取当前年份
    current_year = datetime.now().year
    next_year = current_year + 1
    datetime_str = f"{next_year}-01-01 00:00:00"
    return datetime_str

def parse_datetime(s: str) -> datetime:
    """解析标准格式字符串为上海时区datetime"""
    dt = datetime.strptime(s, STD_DATETIME_FORMAT)
    return dt.replace(tzinfo=SHANGHAI_TZ)

def time_diff_seconds(start_time_str: str, end_time_str: str) -> float:
    """计算两个时间字符串的差值（秒），返回float类型"""
    if not start_time_str or not end_time_str:
        return 0.0

    try:
        start_dt = parse_datetime(start_time_str)
        end_dt = parse_datetime(end_time_str)
        return round((end_dt - start_dt).total_seconds(), 2)
    except (ValueError, TypeError):
        return 0.0

class ShanghaiTimezoneJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        """将所有datetime对象格式化为上海时区标准格式"""

        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return format_datetime(obj)
            elif isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime(item) for item in obj]
            return obj

        content = convert_datetime(content)
        return super().render(content)


def month_update():
    # 获取当前时间
    now = datetime.now()

    # 计算上个月1号0点
    if now.month == 1:
        # 处理1月份的特殊情况（上个月是去年12月）
        last_month_first_day = datetime(now.year - 1, 12, 1, 0, 0, 0)
    else:
        last_month_first_day = datetime(now.year, now.month - 1, 1, 0, 0, 0)

    # 计算本月1号0点（上个月的结束时间）
    current_month_first_day = datetime(now.year, now.month, 1, 0, 0, 0)

    # 格式化时间字符串
    start_time = last_month_first_day.strftime("%Y-%m-%d %H:%M:%S")
    end_time = current_month_first_day.strftime("%Y-%m-%d %H:%M:%S")
    return start_time, end_time