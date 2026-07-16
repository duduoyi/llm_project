# 内部接口访问权限
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict
from src.config.auth import ETL_API_KEY_MAP
from src.utils.logger import get_logger


logger = get_logger(__name__)


class ETLAuthGuard(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super().__init__(auto_error=auto_error)

    async def __call__(self, request: Request) -> Dict[str, any]:
        # 1. 获取Bearer令牌
        credentials: Optional[HTTPAuthorizationCredentials] = await super().__call__(request)
        client_ip = request.client.host if request.client else "unknown"

        # 2. 验证认证方案
        if not credentials or credentials.scheme != "Bearer":
            logger.warning(f"无效认证：未使用Bearer方案，IP: {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="认证方案必须为Bearer"
            )

        # 3. 验证API Key是否在映射中
        api_key = credentials.credentials
        key_info = ETL_API_KEY_MAP.get(api_key)
        if not key_info:
            logger.warning(f"无效API Key：密钥不存在，IP: {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="无效的ETL接口访问令牌"
            )

        # 4. 日志记录（审计用，包含关联的变量名）
        logger.info(
            f"认证成功：服务={key_info['service']}，"
            f"变量名={key_info['variable']}，IP={client_ip}"
        )

        # 5. 返回密钥元信息（后续接口可按需使用权限）
        return key_info
