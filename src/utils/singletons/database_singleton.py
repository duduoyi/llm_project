
class MongoSingleton:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            from pymongo import MongoClient
            from src.config.database import MONGODB_CONFIG
            config = MONGODB_CONFIG
            # 构建连接URI
            if config.get("user") and config.get("password"):
                uri = (
                    f"mongodb://{config['user']}:{config['password']}@"
                    f"{config['host']}:{config['port']}/"
                    f"?authSource={config.get('auth_source', 'admin')}&authMechanism=SCRAM-SHA-1"
                )
            else:
                uri = f"mongodb://{config['host']}:{config['port']}/"

            cls._instance = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=config.get('max_pool_size', 10),
                minPoolSize=config.get('min_pool_size', 1)
            )
        return cls._instance



from typing import Dict, Optional
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


class AsyncNeo4jSingleton:
    """Neo4j 异步单例：缓存 AsyncDriver，不缓存session"""
    _driver: Optional["AsyncDriver"] = None

    @classmethod
    async def get_driver(cls):
        if cls._driver is None:
            from src.config.database import NEO4J_CONFIG
            from neo4j import AsyncGraphDatabase
            config = NEO4J_CONFIG
            cls._driver = AsyncGraphDatabase.driver(
                uri=config["uri"],
                auth=(config["user"], config["password"]),
                max_connection_pool_size=50,
                max_connection_lifetime=3600,
                connection_acquisition_timeout=30,
            )
        return cls._driver

    @classmethod
    async def close(cls):
        if cls._driver is not None:
            await cls._driver.close()
            cls._driver = None


class AsyncMongoSingleton:
    """Mongo 异步单例 motor"""
    _client: Optional["AsyncIOMotorClient"] = None

    @classmethod
    async def get_instance(cls):
        if cls._client is None:
            from motor.motor_asyncio import AsyncIOMotorClient
            from src.config.database import MONGODB_CONFIG
            config = MONGODB_CONFIG
            if config.get("user") and config.get("password"):
                uri = (
                    f"mongodb://{config['user']}:{config['password']}@"
                    f"{config['host']}:{config['port']}/"
                    f"?authSource={config.get('auth_source', 'admin')}&authMechanism=SCRAM-SHA-1"
                )
            else:
                uri = f"mongodb://{config['host']}:{config['port']}/"

            cls._client = AsyncIOMotorClient(
                uri,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=config.get('max_pool_size', 10),
                minPoolSize=config.get('min_pool_size', 1)
            )
        return cls._client

    @classmethod
    async def close(cls):
        if cls._client is not None:
            cls._client.close()
            cls._client = None


class AsyncMysqlSingleton:
    """多库 MySQL 异步单例，缓存 async engine + sessionmaker"""
    _engines: Dict[str, async_sessionmaker] = {}
    _raw_engines: Dict[str, "AsyncEngine"] = {}

    @classmethod
    async def get_session(cls, db_name: str):
        if db_name not in cls._engines:
            from src.config.database import MYSQL_MULTI_CONFIG
            config = MYSQL_MULTI_CONFIG[db_name]
            # 异步连接串：mysql+asyncmy
            dsn = (
                f"mysql+asyncmy://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
            )
            engine = create_async_engine(
                dsn,
                pool_size=config.get('pool_size', 10),
                max_overflow=config.get('max_overflow', 20),
                pool_recycle=config.get('pool_recycle', 3600),
                pool_pre_ping=True,
            )
            session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
            cls._raw_engines[db_name] = engine
            cls._engines[db_name] = session_factory
        return cls._engines[db_name]

    @classmethod
    async def close_all(cls):
        for eng in cls._raw_engines.values():
            await eng.dispose()
        cls._engines.clear()
        cls._raw_engines.clear()


class AsyncQdrantSingleton:
    """Qdrant 异步客户端单例，区分生产/测试环境"""
    _instances: Dict[Optional[str], "AsyncQdrantClient"] = {}

    @classmethod
    async def get_instance(cls, qdrant_type: str = None):
        if qdrant_type not in cls._instances:
            from qdrant_client import AsyncQdrantClient
            from src.config.database import QDRANT_CONFIG, QDRANT_PRODUCTION_CONFIG
            if qdrant_type == "production":
                cfg = QDRANT_PRODUCTION_CONFIG
            else:
                cfg = QDRANT_CONFIG
            client = AsyncQdrantClient(
                host=cfg["host"],
                port=cfg["port"],
                api_key=cfg["api_key"],
                https=cfg.get("https", False),
                prefer_grpc=cfg.get("prefer_grpc", False),
                timeout=cfg.get("timeout", 300)
            )
            cls._instances[qdrant_type] = client
        return cls._instances[qdrant_type]

    @classmethod
    async def close_all(cls):
        for cli in cls._instances.values():
            await cli.close()
        cls._instances.clear()

# 无异步接口
class CosSingleton:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            from qcloud_cos import CosConfig, CosS3Client
            from src.config.database import COS_CONFIG
            config = CosConfig(
                Region=COS_CONFIG['region'],
                SecretId=COS_CONFIG['secret_id'],
                SecretKey=COS_CONFIG['secret_key'],
                Token=COS_CONFIG['token']
            )
            cls._instance = CosS3Client(config)
        return cls._instance