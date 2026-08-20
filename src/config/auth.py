import os
from typing import Dict, Optional
from . import ENVIRONMENT  # 从 __init__.py 导入环境标识

# 认证机制
# --------------------------
# 1. 加载独立的API Key变量
# --------------------------
# ETL流程（读写权限）
ETL_API_KEY: Optional[str] = os.getenv("ETL_API_KEY")
# 前端数据返回（仅读权限）
FRONTEND_API_KEY: Optional[str] = os.getenv("FRONTEND_API_KEY")
# --------------------------
# 2. 建立“API Key → 元信息”的映射
# --------------------------
# 结构：{api_key: {"service": "服务名称", "permissions": ["权限列表"]}}
ETL_API_KEY_MAP: Dict[str, Dict[str, any]] = {}
# 逐个添加密钥与元信息的关联（确保变量存在才添加）
if ETL_API_KEY:
    ETL_API_KEY_MAP[ETL_API_KEY] = {
        "service": "ETL服务",
        "permissions": ["read", "write"],  # 该密钥的权限
        "variable": "ETL_API_KEY"     # 关联的变量名（便于日志审计）
    }
if FRONTEND_API_KEY:
    ETL_API_KEY_MAP[FRONTEND_API_KEY] = {
        "service": "前端数据返回服务",
        "permissions": ["read"],
        "variable": "FRONTEND_API_KEY"
    }
# --------------------------
# 3. 验证配置有效性（启动时检查）
# --------------------------
if ENVIRONMENT == "production" and not ETL_API_KEY_MAP:
    raise ValueError("未配置任何有效的ETL API Key（检查.env文件或环境变量）")


# 企查查
QCC_APP_KEY: Optional[str] = os.getenv("QCC_APP_KEY", "")
# 前端数据返回（仅读权限）
QCC_SECRECT_KEY: Optional[str] = os.getenv("QCC_SECRECT_KEY", "")
