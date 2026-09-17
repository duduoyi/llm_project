# 应用入口
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
import uvicorn
import os
from src.utils.logger import get_logger
from src.config.app import origins
# 导入异步数据库单例
from src.services.base_resource_service import BaseResourceService
# 导入路由模块
from src.api import system_router, etl_router, front_router, qdrant_router, qcc_router
# ===== 新增：导入etl定时调度器相关 =====
from src.core.scheduler import init_scheduler, shutdown_scheduler
from src.core.job_register import register_all_scheduled_tasks

# 配置日志
logger = get_logger(__name__)

# ========== 生命周期管理器：启动预热DB / 关闭释放连接 ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 【启动阶段】所有接口进来之前，一次性初始化全部数据库连接池
    logger.info("===== 服务启动，开始预热所有数据库连接 =====")
    try:
        # # 1. MySQL patent 库异步连接池
        # await BaseResourceService.get_mysql_session_factory("patent")
        # logger.info("✅ MySQL Session工厂预加载完成")
        #
        # # 2. Neo4j 异步驱动
        # await BaseResourceService.get_neo4j_driver()
        # logger.info("✅ Neo4j Driver 预加载完成")

        # 3. Qdrant development 向量库客户端
        await BaseResourceService.get_qdrant_client()
        logger.info("✅ Qdrant Client 预加载完成")

        # 4. MongoDB 异步连接
        await BaseResourceService.get_mongo_db()
        logger.info("✅ MongoDB motor 异步客户端预加载完成")

        # # COS 同步客户端，无await，触发懒加载初始化
        # await BaseResourceService.get_cos_client()
        # logger.info("✅ COS 对象存储客户端初始化完成")

        # 嵌入式模型（防止模型加载阻塞事件循环）
        await BaseResourceService.get_emb_wrapper()
        emb_wrapper = await BaseResourceService.get_emb_wrapper()
        # 可选：预热一次推理（真正加载模型权重到内存）
        _ = await emb_wrapper.aembed_query("warm up")
        logger.info("✅ Embedding 向量模型加载完成")

        # # 预热 LLM（只是实例创建，不会发起网络请求；真正调用才会访问大模型接口）
        # await BaseResourceService.get_llm_instance("qwen")
        # logger.info("✅ LLM客户端初始化完成")
        # 1.先注册所有job
        register_all_scheduled_tasks()
        logger.info("✅ register_all_scheduled_tasks 注册定时任务")
        await init_scheduler()
        logger.info("✅ APScheduler ETL定时调度器启动成功")

        logger.info("===== 所有数据库资源预热完毕，开始接收请求 =====")

        # ===================== 启动ETL定时调度器 =====================
        # register_all_schedule_jobs()
        # if not scheduler.running:
        #     scheduler.start()
        #     logger.info("✅ APScheduler ETL定时调度器启动成功")

        logger.info("===== 服务就绪，开始接收请求 =====")

    except Exception as e:
        logger.error(f"数据库预热失败，服务终止启动: {str(e)}", exc_info=True)
        # 预热失败直接抛出异常，服务启动失败，避免带故障运行
        raise e

    # yield 分割：前面是启动逻辑，后面是关闭逻辑
    yield

    # 【关闭阶段】服务停止时优雅释放所有异步连接
    logger.info("===== 收到关闭信号，开始销毁数据库连接 =====")
    try:
        # ===================== 关闭定时调度器 =====================
        # if scheduler.running:
        #     scheduler.shutdown()
        #     logger.info("✅ APScheduler ETL定时调度器已关闭")
        await shutdown_scheduler()
        # 可在这里补充统一关闭所有数据库连接/模型资源的静态方法
        await BaseResourceService.close_all_resource()
        logger.info("✅ 全部异步数据库连接已正常关闭")
    except Exception as e:
        logger.error(f"关闭数据库资源异常: {str(e)}", exc_info=True)

# 初始化 FastAPI 应用，绑定 lifespan
app = FastAPI(
    title="Multi-step ETL Service API",
    version="0.0.1",
    docs_url=None,  # 禁用默认 CDN 版 docs
    redoc_url=None,
    lifespan=lifespan  # 绑定生命周期
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 获取项目根目录（fastApi_ETL）的绝对路径
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 拼接静态资源目录的绝对路径
STATIC_DIR = os.path.join(ROOT_DIR, "static")
# 挂载静态资源（使用绝对路径）
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# 自定义 Swagger UI 路由，加载本地资源
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        swagger_js_url="/static/dist/swagger-ui-bundle.js",
        swagger_css_url="/static/dist/swagger-ui.css",
        swagger_favicon_url="/static/dist/favicon-32x32.png"
    )

# 挂载路由
app.include_router(system_router)
app.include_router(etl_router)
app.include_router(front_router)
app.include_router(qdrant_router)
app.include_router(qcc_router)

# 应用入口
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        loop="uvloop",
        reload=False  # 生产环境改为False
    )