# 日志管理
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from src.config.app import LOG_CONFIG


def get_logger(name):
    """获取配置好的日志器，按日切割，自动保留最近7天日志"""
    logger = logging.getLogger(name)

    # 设置日志级别，默认为INFO
    log_level = LOG_CONFIG.get('level')
    logger.setLevel(log_level)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 项目根目录（假设当前文件在src/utils下，向上两级为项目根目录）
    project_root = Path(__file__).parent.parent.parent
    log_dir = project_root / 'logs'

    # 确保日志目录存在
    log_dir.mkdir(parents=True, exist_ok=True)

    # 日志文件名
    log_filename = 'db.log'
    log_file_path = log_dir / log_filename

    # 格式化器，增加了模块和行号信息
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # ========== 按天轮转处理器，保留7天 ==========
    # when='D' 每天切割；interval=1；backupCount=7 最多保留7份旧日志，超过自动删除
    file_handler = TimedRotatingFileHandler(
        str(log_file_path),
        when='D',
        interval=1,
        backupCount=7,
        encoding='utf-8',
        utc=False  # 使用本地时间，False=系统本地时间；True=UTC时间
    )
    # 日志后缀格式：db.log.2026-08-21
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(formatter)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 可以为控制台设置不同的日志级别（如只输出WARNING及以上）
    if 'console_level' in LOG_CONFIG:
        console_handler.setLevel(LOG_CONFIG['console_level'])

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger