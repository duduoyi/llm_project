import asyncio
from typing import Optional, Dict
from abc import ABC
from neo4j import AsyncDriver
from qdrant_client.async_qdrant_client import AsyncQdrantClient
from sqlalchemy.ext.asyncio import async_sessionmaker
from motor.motor_asyncio import AsyncIOMotorDatabase
from src.utils.singletons.database_singleton import AsyncMysqlSingleton, AsyncNeo4jSingleton, AsyncMongoSingleton, AsyncQdrantSingleton, CosSingleton
from src.utils.singletons.ai_singleton import EmbeddingFactory, LLMFactory
from src.utils.logger import get_logger

class BaseResourceService(ABC):
    """通用资源基类：提供所有数据库、向量模型全局单例，无业务抽象方法"""
    # 保护数据库初始化进程，为单独任务运行准备，生产环境做兜底
    # ====================== 全局统一资源：MySQL Session工厂 ======================
    _mysql_lock: Optional[asyncio.Lock] = None
    _mysql_session_factory_map: dict[str, async_sessionmaker] = {}  # key: db_name

    # ========== 全局统一资源：Neo4j单例+锁（所有子类共享） ==========
    _neo4j_lock: Optional[asyncio.Lock] = None
    _neo4j_driver: Optional[AsyncDriver] = None

    # ====================== 全局统一资源：Mongo ======================
    _mongo_lock: Optional[asyncio.Lock] = None
    _mongo_db: Optional[AsyncIOMotorDatabase] = None

    # ====================== 全局统一资源：Qdrant ======================
    _qdrant_lock: Optional[asyncio.Lock] = None
    _qdrant_client: Optional[AsyncQdrantClient] = None

    # ====================== 全局统一资源：嵌入式模型 ======================
    _emb_lock: Optional[asyncio.Lock] = None
    _emb_wrapper: Optional[EmbeddingFactory]= None

    # ===================== LLM 全局单例 =====================
    _llm_lock: Optional[asyncio.Lock] = None
    # key: model_name, value: llm实例
    _llm_map: Dict[str, object] = {}

    # ===================== COS 对象存储 =====================
    _cos_lock: Optional[asyncio.Lock] = None
    _cos_client = None

    # -------------------------- MySQL 统一获取Session工厂 --------------------------
    @classmethod
    async def get_mysql_session_factory(cls, db_name: str) -> async_sessionmaker:
        if cls._mysql_lock is None:
            cls._mysql_lock = asyncio.Lock()
        async with cls._mysql_lock:
            if db_name not in cls._mysql_session_factory_map:
                factory = await AsyncMysqlSingleton.get_session(db_name)
                cls._mysql_session_factory_map[db_name] = factory
        return cls._mysql_session_factory_map[db_name]

    @classmethod
    async def get_neo4j_driver(cls) -> AsyncDriver:
        """全局统一获取Neo4j异步驱动，内置延迟锁+双重校验，解决跨loop问题"""
        # 异步上下文内延迟创建锁，绑定当前运行loop
        if cls._neo4j_lock is None:
            cls._neo4j_lock = asyncio.Lock()

        async with cls._neo4j_lock:
            # 双重校验，防止并发重复初始化驱动
            if cls._neo4j_driver is None:
                cls._neo4j_driver = await AsyncNeo4jSingleton.get_driver()
        return cls._neo4j_driver

    @classmethod
    async def get_mongo_db(cls, db_name = None) -> AsyncIOMotorDatabase:
        if cls._mongo_lock is None:
            cls._mongo_lock = asyncio.Lock()
        async with cls._mongo_lock:
            if cls._mongo_db is None:
                cls._mongo_db = await AsyncMongoSingleton.get_db(db_name)
        return cls._mongo_db

    # -------------------------- Qdrant 统一获取Client --------------------------
    @classmethod
    async def get_qdrant_client(cls, cfg = None) -> AsyncQdrantClient:
        if cls._qdrant_lock is None:
            cls._qdrant_lock = asyncio.Lock()
        async with cls._qdrant_lock:
            if cls._qdrant_client is None:
                cls._qdrant_client = await AsyncQdrantSingleton.get_instance(cfg)
        return cls._qdrant_client

    @classmethod
    async def get_emb_wrapper(cls):
        if cls._emb_lock is None:
            cls._emb_lock = asyncio.Lock()
        async with cls._emb_lock:
            if cls._emb_wrapper is None:
                # 兜底重建逻辑，lifespan正常走不到这里
                cls._emb_wrapper = EmbeddingFactory.get_instance()
        return cls._emb_wrapper

    @classmethod
    async def get_llm_instance(cls, model_name: str):
        if cls._llm_lock is None:
            cls._llm_lock = asyncio.Lock()
        async with cls._llm_lock:
            if model_name not in cls._llm_map:
                # 同步构造模型实例，仅首次加载互斥
                cls._llm_map[model_name] = LLMFactory.get_instance(model_name)
        return cls._llm_map[model_name]

    @classmethod
    async def get_cos_client(cls):
        if cls._cos_lock is None:
            cls._cos_lock = asyncio.Lock()
        async with cls._cos_lock:
            if cls._cos_client is None:
                cls._cos_client = CosSingleton.get_instance()
        return cls._cos_client

    # ===================== 统一关闭所有资源 =====================
    @classmethod
    async def close_all_resource(cls):
        logger = get_logger("resource_close_global")
        logger.info("开始统一关闭全部数据库、模型资源")

        # 1. 关闭 Qdrant Client
        if cls._qdrant_client is not None:
            try:
                await AsyncQdrantSingleton.close()
                logger.info("✅ QdrantClient 关闭完成")
            except Exception as e:
                logger.error(
                    f"关闭Qdrant客户端失败 err={str(e)}",
                    exc_info=True
                )
            finally:
                cls._qdrant_client = None

        # 2. 关闭 Neo4j Driver
        if cls._neo4j_driver is not None:
            try:
                await AsyncNeo4jSingleton.close()
                logger.info("✅ Neo4jDriver 关闭完成")
            except Exception as e:
                logger.error(
                    f"关闭Neo4j驱动失败 err={str(e)}",
                    exc_info=True
                )
            finally:
                cls._neo4j_driver = None

        # 3. 关闭 Mongo Client
        if cls._mongo_db  is not None:
            try:
                AsyncMongoSingleton.close()
                logger.info("✅ MongoClient 关闭完成")
            except Exception as e:
                logger.error(
                    f"关闭Mongo客户端失败 err={str(e)}",
                    exc_info=True
                )
            finally:
                cls._mongo_db = None

        # 4. 释放 Embedding 向量模型
        if cls._emb_wrapper is not None:
            try:
                if hasattr(cls._emb_wrapper, "destroy"):
                    cls._emb_wrapper.destroy()
                logger.info("✅ Embedding模型释放完成")
            except Exception as e:
                logger.error(
                    f"释放Embedding模型失败 err={str(e)}",
                    exc_info=True
                )
            finally:
                cls._emb_wrapper = None

        # 5. 释放所有LLM大模型
        if cls._llm_map:
            try:
                for name, llm in cls._llm_map.items():
                    if hasattr(llm, "destroy"):
                        llm.destroy()
                cls._llm_map.clear()
                logger.info("✅ 全部LLM模型释放完成")
            except Exception as e:
                logger.error(
                    f"释放LLM模型失败 err={str(e)}",
                    exc_info=True
                )

        # 6. MySQL session_factory 无需close，直接清空缓存
        if cls._mysql_session_factory_map:
            await AsyncMysqlSingleton.close_all()
            logger.info("✅ MySQL session工厂缓存清空")

        # 7. COS同步客户端无连接，仅置空
        cls._cos_client = None
        logger.info("✅ COS客户端置空完成")

        logger.info("===== 全部资源关闭/释放流程执行完毕 =====")