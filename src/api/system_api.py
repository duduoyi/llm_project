from fastapi import APIRouter
from datetime import datetime
from typing import Dict, Any


router = APIRouter(tags=["Health Check"])


# 基础健康检查 - 轻量快速
@router.get("/health", summary="基础健康检查", response_model=Dict[str, Any])
async def health_check():
    """
    基础健康检查接口，快速判断服务是否存活
    不涉及耗时操作，适合负载均衡器或监控系统的高频检查
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "fastapi_etl"
    }

