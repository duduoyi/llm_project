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

    # 关键：防止重复添加handler，必须保留！
    if logger.handlers:
        return logger

    # ========= 只执行一次，初始化handler挂载到root logger =========
    # 项目根目录（假设当前文件在src/utils下，向上两级为项目根目录）
    project_root = Path(__file__).parent.parent.parent
    log_dir = project_root / 'logs'

    # 确保日志目录存在
    log_dir.mkdir(parents=True, exist_ok=True)

    # 日志文件名
    log_filename = 'etl.log'
    log_file_path = log_dir / log_filename

    # 格式化器，增加了模块和行号信息
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # ========== 按天轮转处理器，保留7天 ==========
    # when='D' 每天切割；interval=1；backupCount=7 最多保留7份旧日志，超过自动删除
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file_path),
        when='D',
        interval=1,
        backupCount=7,
        encoding='utf-8',
        utc=False  # 使用本地时间，False=系统本地时间；True=UTC时间
    )
    # 日志后缀格式：etl.log.2026-08-21
    file_handler.suffix = "%Y-%m-%d"

    # ========== 自定义 rotator：实现 etl_2026-10-14.log 命名 ==========
    def custom_rotator(source: str, dest: str):
        """
        source: 当前日志文件路径 logs/etl.log
        dest: 原生默认生成 logs/etl.log.2026-10-14
        我们修改目标文件名： logs/etl_2026-10-14.log
        """
        src_path = Path(source)
        # 取出日期部分
        date_str = Path(dest).suffix.lstrip(".")
        new_name = f"{src_path.stem}_{date_str}{src_path.suffix}"
        new_dest = str(src_path.parent / new_name)
        import os
        os.rename(source, new_dest)

    file_handler.rotator = custom_rotator

    file_handler.setFormatter(formatter)
    # 文件handler日志级别，和全局logger保持一致
    file_handler.setLevel(log_level)

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