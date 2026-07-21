from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from src.utils.security import ETLAuthGuard
from enum import Enum
from src.services.data_services.neo4j_service import Neo4jService
from pydantic import Field, BaseModel
from typing import List, Optional


# === FastAPI 路由 ===
router = APIRouter(
    prefix="/internal/neo4j",
    tags=["Neo4j数据处理"],
    dependencies=[Depends(ETLAuthGuard())]
)

neo4j_service = Neo4jService()


class Neo4jDataProcess(str, Enum):
    create_patent_with = 'create_patent_with'
    college_community_detection = 'college_community_detection'


@router.post("/{process_type}")
async def run_neo4j_data_process(
        process_type: Neo4jDataProcess
):
    # 核心：根据枚举值动态调用 neo4j_service 里的同名方法
    method_name = process_type.value
    try:
        # 安全判断：方法是否存在
        if not hasattr(neo4j_service, method_name):
            return {"error": f"方法 {method_name} 不存在"}

        # 动态调用！！！
        func = getattr(neo4j_service, method_name)
        result = await func()  # 如果需要传参，写 func(参数1, 参数2...)
        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e


# ------------- Pydantic 模型：注册用户（非三螺旋用户）-------------
class Users(BaseModel):
    """单个内部用户的参数模型（强类型校验）"""
    # 必填字段（无默认值 = 必填）
    username: str = Field(..., description="用户账号（唯一标识）")
    name: str = Field(..., description="用户真实姓名")
    institutionType: str = Field(..., description="所属机构类型")
    institutionName: str = Field(..., description="所属机构名称")

    # 可选字段（Optional + 默认值None = 可选，支持null）
    department: Optional[str] = Field(None, description="所属机构部门")
    weixin: Optional[str] = Field(None, description="微信")
    phone: Optional[str] = Field(None, description="电话")
    email: Optional[str] = Field(None, description="邮箱")
    remarks: Optional[str] = Field(None, description="备注")

@router.post("/create_contact_user")
def run_create_contact_user(params: List[Users]):
    """创建联系人节点及关系"""
    try:
        dict_list = [user.dict() for user in params]
        # 内部已通过上下文管理器处理数据库连接，无需外部干预
        result = neo4j_service.create_contact_user(
            data=dict_list
        )
        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e


# ------------- 接口路由：更新注册人员信息-------------
@router.post("/update_users")
def run_create_user(params: List[Users]):
    """创建内部用户节点"""
    try:
        # 1. 模型转字典列表
        dict_list = [user.dict() for user in params]
        # 3. 调用Neo4j创建函数
        result = neo4j_service.update_users(data=dict_list)

        return JSONResponse(
            content={
                "status": "success",
                "message": result
            },
            media_type="application/json; charset=utf-8"  # 明确指定编码
        )
    except HTTPException as e:
        # 直接抛出已定义的HTTP异常（如数据库连接失败）
        raise e