import os
from src.config import ENVIRONMENT  # 从 __init__.py 导入环境标识

# MySQL配置
MYSQL_BASE_CONFIG  = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'slx88888888'),
    'pool_size': int(os.getenv('MYSQL_POOL_SIZE', 5)),
    'max_overflow': int(os.getenv('MYSQL_MAX_OVERFLOW', 10)),
    'pool_recycle': int(os.getenv('MYSQL_POOL_RECYCLE', 3600)),
}

# 1. 从 .env 中获取规范配置的数据库名字符串（改动：MYSQL_DATABASES 替代 MYSQL_DATABASE）
mysql_dbs_str = os.getenv('MYSQL_DATABASES', '')
mysql_db_names = [db.strip() for db in mysql_dbs_str.split(',') if db.strip()]

# 2. 构建多数据库完整配置（不变）
MYSQL_MULTI_CONFIG = {
    db_name: {**MYSQL_BASE_CONFIG, 'database': db_name}
    for db_name in mysql_db_names
}

# MongoDB配置
MONGODB_CONFIG = {
    'host': os.getenv('MONGODB_HOST'),
    'port': int(os.getenv('MONGODB_PORT')),
    "user": os.getenv('MONGODB_USER', ''),  # 用户名，无则为空
    "password": os.getenv('MONGODB_PASSWORD', ''),  # 密码，无则为空
    "database": os.getenv('MONGODB_DATABASE', ''),
    "max_pool_size": 10,  # 连接池最大连接数
    "min_pool_size": 1,
    "socket_timeout": 30000  # 套接字超时时间（毫秒）
}

# Neo4j配置
NEO4J_CONFIG = {
    'uri': os.getenv('NEO4J_URI'),
    'user': os.getenv('NEO4J_USER'),
    'password': os.getenv('NEO4J_PASSWORD')
}


# Qdrant配置
QDRANT_CONFIG = {
    'host': os.getenv('QDRANT_HOST'),
    'port': int(os.getenv('QDRANT_PORT')),
    'api_key': os.getenv('QDRANT_API_KEY', ''),
}

QDRANT_PRODUCTION_CONFIG = {
    'host': os.getenv('QDRANT_PRODUCTION_HOST'),
    'port': int(os.getenv('QDRANT_PRODUCTION_PORT')),
    'api_key': os.getenv('QDRANT_PRODUCTION_API_KEY', ''),
}

# Cos 配置
COS_CONFIG = {
    'secret_id': os.getenv('COS_SECRET_ID'),
    'secret_key': os.getenv('COS_SECRET_KEY'),
    'region': os.getenv('COS_REGION'),
    'token': os.getenv('COS_TOKEN', None),
}

# 生产环境校验（仅在prod环境生效）
if ENVIRONMENT == "production":
    # 生产环境必须配置密码，禁止默认值
    if not MYSQL_BASE_CONFIG['password']:
        raise ValueError("生产环境必须配置 MYSQL_PASSWORD 环境变量")
    if not NEO4J_CONFIG['password']:
        raise ValueError("生产环境必须配置 NEO4J_PASSWORD 环境变量")
    if not os.getenv('QDRANT_API_KEY'):
        raise ValueError("生产环境必须配置 QDRANT_API_KEY 环境变量")
    if not os.getenv('MONGODB_PASSWORD'):
        raise ValueError("生产环境必须配置 MONGODB_PASSWORD 环境变量")