import os
import pathlib
from dotenv import load_dotenv

# -------------------------- 全局环境初始化（只执行一次） --------------------------
# 1. 从.env确定当前运行环境（优先级：系统环境变量 > 默认值）
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# 2. 定位项目根目录，加载对应环境的 .env. 文件
BASE_DIR = pathlib.Path(__file__).parent.parent.parent  # 项目根目录
env_file = BASE_DIR / f".env.{ENVIRONMENT}"

# 3. 检查配置文件是否存在
if not env_file.exists():
    raise FileNotFoundError(f"未找到 {ENVIRONMENT} 环境的配置文件：{env_file}")

# 4. 加载 .env 文件（全局生效，覆盖系统环境变量）
load_dotenv(dotenv_path=env_file, override=True)

# -------------------------- 统一导出所有配置和环境变量 --------------------------
# 导出环境标识
__all__ = ["ENVIRONMENT"]
# 应用配置
from src.config.app import LOG_CONFIG, ALLOWED_ORIGINS
# 认证配置
from src.config.auth import ETL_API_KEY, FRONTEND_API_KEY, ETL_API_KEY_MAP
# 数据库配置
from src.config.database import MYSQL_MULTI_CONFIG, NEO4J_CONFIG, QDRANT_CONFIG, QDRANT_PRODUCTION_CONFIG, MONGODB_CONFIG
# LLM模型配置

# 明确对外暴露的配置列表（修复：调用extend方法，而非赋值）
__all__.extend([
    # 数据库
    "MYSQL_MULTI_CONFIG", "NEO4J_CONFIG", "QDRANT_CONFIG", "QDRANT_PRODUCTION_CONFIG", "MONGODB_CONFIG"
    # 应用
    "LOG_CONFIG", "ALLOWED_ORIGINS",
    # 模型
    "zhipu_ai", "qwen_ai"
    # 认证
    "ETL_API_KEY", "FRONTEND_API_KEY", "ETL_API_KEY_MAP"
])