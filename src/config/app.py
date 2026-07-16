import os
from . import ENVIRONMENT  # 从 __init__.py 导入环境标识


# 日志配置
LOG_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'backup_count': os.getenv('LOG_BACKUP_DAYS', 30),  # 保留30天的日志
    # 'console_level': logging.WARNING  # 可选，控制台只显示警告及以上
}

# 跨域配置: 从环境变量获取允许的域名，多个域名用逗号分隔
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "")
# 处理域名列表（分割并去除空值）
origins = [origin.strip() for origin in ALLOWED_ORIGINS.split(",") if origin.strip()]
# 开发环境如果未配置，默认允许所有（方便开发调试）
if not origins:
    origins = ["*"]

# 生产环境校验（仅在prod环境生效）
if ENVIRONMENT == "production" and not origins:
    # 生产环境必须跨域配置
    raise ValueError("生产环境必须配置 ALLOWED_ORIGINS 环境变量")
