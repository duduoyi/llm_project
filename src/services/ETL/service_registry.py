import inspect
import pkgutil
import importlib
from typing import Dict, Type, Any
from src.services.ETL.base_etl_service import BaseETLService


class ServiceRegistry:
    """
    工业级 ServiceRegistry
    - 自动扫描并注册
    - 支持插件式扩展
    - 线程安全
    """

    _services: Dict[str, Type[BaseETLService]] = {}
    _initialized: bool = False

    @classmethod
    def register(cls, service_cls: Type[BaseETLService]):
        """
        手动注册（通常不需要）
        """
        if not service_cls.name:
            raise ValueError(f"{service_cls} 必须定义 name 属性")

        cls._services[service_cls.name] = service_cls

    @classmethod
    def get_instance(cls, service_name: str, config: Dict[str, Any]):
        """
        获取 service 实例
        """
        if not cls._initialized:
            cls.auto_discover()

        service_cls = cls._services.get(service_name)
        if not service_cls:
            raise ValueError(f"Service '{service_name}' 未注册")

        return service_cls(config)

    @classmethod
    def list_services(cls):
        if not cls._initialized:
            cls.auto_discover()
        return list(cls._services.keys())

    @classmethod
    def auto_discover(cls, package: str = "src.services.ETL"):
        """
        自动扫描 src.services.ETL 目录及其所有子目录下的 service 类
        最小改动：增加递归扫描逻辑
        """
        if cls._initialized:
            return

        package_module = importlib.import_module(package)

        for importer, module_name, is_pkg in pkgutil.walk_packages(
            path=package_module.__path__,
            prefix=f"{package}.",
            onerror=lambda x: None
        ):
            # 导入当前模块（包括子目录的模块）
            module = importlib.import_module(module_name)

            # 遍历模块内的类，逻辑和原有一致
            for name, obj in inspect.getmembers(module):
                if (
                    inspect.isclass(obj)
                    and issubclass(obj, BaseETLService)
                    and obj is not BaseETLService
                ):
                    if not obj.name:
                        raise ValueError(
                            f"{obj.__name__} 必须定义 name 属性"
                        )

                    cls._services[obj.name] = obj

        cls._initialized = True