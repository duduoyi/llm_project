import abc
from typing import Dict, Any
from src.utils.logger import get_logger
from src.utils.tz_time import get_current_year_start_str, get_next_year_start_str
from src.services.base_resource_service import BaseResourceService

class BaseETLService(BaseResourceService):
    """
    所有 ETL Service 必须继承这个基类
    """

    name: str = None  # 每个子类必须定义服务名
    source_type: str = None  # 每个子类必须定义数据源

    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)
        self.start_time = config.get("start_time") or get_current_year_start_str()
        self.end_time = config.get("end_time") or get_next_year_start_str()
        self.batch_size = config.get("batch_size") or 1000

    # ========== 关键修改：加上 async ==========
    @abc.abstractmethod
    async def extract_batch(self, last_id) -> Any:
        """
        抽象方法：提取数据
        子类必须实现（不同ETL服务取的数不同）
        """
        pass

    @abc.abstractmethod
    async def process_batch(self, batch_data):
        pass




class BaseRunRepo(BaseResourceService):
    """ETL 任务主记录仓库抽象（ods_etl_run）"""
    @abc.abstractmethod
    async def create(self, run_id: str):
        pass

    @abc.abstractmethod
    async def update_status(self, run_id: str, status: str):
        pass



class BaseBatchRepo(BaseResourceService):
    @abc.abstractmethod
    async def create(self, run_id: str, batch_no: int, last_id, batch_size: int):
        ...
    @abc.abstractmethod
    async def update_status(self, run_id: str, batch_no: int, status: str):
        ...
    @abc.abstractmethod
    async def mark_failed(self, run_id: str, batch_no: int):
        ...
    @abc.abstractmethod
    async def get_status(self, run_id: str, batch_no: int) -> str:
        ...
    @abc.abstractmethod
    async def get_last_completed(self, run_id: str):
        ...
    @abc.abstractmethod
    async def get_failed_batches(self, run_id: str) -> list:
        ...
    @abc.abstractmethod
    async def get_previous_batch(self, run_id: str, batch_no: int):
        ...