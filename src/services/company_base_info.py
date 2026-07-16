import re
import asyncio
from src.utils.singletons.database_singleton import AsyncMongoSingleton
import os
from pathlib import Path
from bs4 import BeautifulSoup
from src.utils.logger import get_logger
from typing import Union, List
import pandas as pd


class CompanyBaseInfo:
    # 全局异步写入锁：防止多任务并发写入Mongo造成竞争
    _mongo_write_lock = asyncio.Lock()

    def __init__(self):
        """
        初始化服务类
        """
        self.logger = get_logger(self.__class__.__name__)
        self.project_root = Path(__file__).parent.parent.parent
        self.qcc_folder = self.project_root / "files" / "qcc_company"
        self.listed_folder = self.project_root / "files" / "suzhou_listed_company"
        # 初始化客户端
        self._mongo_client = None
        self.collection = None

    async def init(self):
        """异步初始化，在 lifespan / 任务启动时调用"""
        self._mongo_client = await AsyncMongoSingleton.get_instance()
        self.collection = self._mongo_client['dquot']['company_info']
        self.logger.info("✅ Async Mongo 客户端初始化完成")

    async def listed_company_to_mongodb(self):
        # 1. 获取文件列表（文件遍历是同步操作，to_thread隔离）
        file_path_list = await asyncio.to_thread(
            self._get_files_list,
            folder_path=self.listed_folder,
            file_format=".txt"
        )
        self.logger.info(f"文件列表: {file_path_list}")
        # 2. 读取文件内容
        all_companies_data = []
        for file_path in file_path_list:
            company_data = await asyncio.to_thread(self._read_txt_file, file_path)
            all_companies_data.extend(company_data)
        # 3. 过滤 ST 公司
        company_filtered = await asyncio.to_thread(self._filter_st_companies, all_companies_data)
        # 4. 格式转换（重命名字段 + 区县 + 包裹结构 + _id）
        final_data = await asyncio.to_thread(
            lambda data: [self._transform_listed_company(c) for c in data],
            company_filtered
        )
        # 5. 存入MongoDB
        success = await self._save_to_mongodb(final_data)
        self.logger.info(f"✅ 完成同步：{success} 条数据（仅给企业基本信息新增字段，所有旧数据完全保留）")

    async def qcc_company_to_mongodb(self, file_name=None):
        # 1. 获取文件列表（根据 file_name 判断）
        if file_name is not None:
            # 有指定文件名 → 只读取这一个文件
            file_path_list = [self.qcc_folder / file_name]  # 拼接完整路径
        else:
            # 无指定文件名 → 遍历文件夹所有 xlsx
            file_path_list = await asyncio.to_thread(
                self._get_files_list,
                folder_path=self.qcc_folder,
                file_format=".xlsx"
            )

        self.logger.info(f"待处理文件列表: {file_path_list}")

        # 2. 读取文件内容
        company_data_list = []
        for file_path in file_path_list:
            company_data = await asyncio.to_thread(self._read_excel_file, file_path)
            company_data_list.extend(company_data)
        # 3. 格式转换
        update_data = await asyncio.to_thread(
            lambda data: [self._transform_qcc_company(c) for c in data],
            company_data_list
        )
        # 3. 存入MongoDB
        success = await self._save_to_mongodb(update_data)
        self.logger.info(f"✅ 完成同步：{success} 条数据（仅给企业基本信息新增字段，所有旧数据完全保留）")

    @staticmethod
    def _get_files_list(folder_path, file_format: tuple | str):
        """读取文件夹下指定格式文件"""
        file_list = [f for f in os.listdir(folder_path) if f.lower().endswith(file_format)]
    
        file_path_list = []
        for filename in file_list:
            file_path = os.path.join(folder_path, filename)
            file_path_list.append(file_path)
    
        return file_path_list

    def _read_txt_file(self, file_path: str):
        """读取.txt 文件"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            data = self._parse_table_to_json(content)
            return data
        except Exception as e:
            self.logger.error(f"⚠️ 处理 {file_path} 失败：{e}")

    @staticmethod
    def _parse_table_to_json(txt_content):
        """ 全新 HTML 解析，用 BeautifulSoup 正确提取表格数据 """
        soup = BeautifulSoup(txt_content, "html.parser")
        table = soup.find("table")
        if not table:
            return []
    
        data = []
    
        # 提取表头
        th_cells = table.find("tr").find_all("th")
    
        headers = [str(th.get_text()).strip() for th in th_cells if str(th.get_text()).strip()]
    
        # 提取数据行
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            row_data = [str(td.get_text()).strip() for td in tds]
    
            # 对齐列数
            if len(row_data) >= len(headers):
                item = dict(zip(headers, row_data))
                data.append(item)
    
        return data

    def _filter_st_companies(self, company_list):
        """
        过滤所有 ST 公司
        包含：ST、*ST、SST、ST* 等全部风险警示股票
        """
        filtered = []
        for company in company_list:
            # 获取证券简称
            sec_name = str(company.get("证券简称", "")).strip()
    
            # 匹配所有 ST 开头的公司
            if not re.match(r'^(ST|\*ST|SST)', sec_name):
                filtered.append(company)
    
        self.logger.info(f"✅ 过滤完成：原始 {len(company_list)} 条，保留 {len(filtered)} 条正常公司")
        return filtered

    @staticmethod
    def _transform_listed_company(company):
        """
        核心格式转换：
        1. _id = 机构名称, 英文括号替换为中文
        2. 保留指定字段
        3. 包裹进 企业基本信息
        """
        # 只保留需要的证券字段
        keep_fields = [
            "证券代码",
            "证券简称",
            "证券类别",
            "上市日期",
            "上市地点",
            "上市状态",
            "办公地址",
            "董事长",
            "总经理"
        ]
        company_name = company.get("机构名称", "")
        # 替换英文括号为中文
        clean_company_name = str(company_name).replace("(", "（").replace(")", "）")
        # 新建干净的企业信息字典，只拿指定字段
        new_company = {k: company[k] for k in keep_fields if k in company}
        # 生成最终结构（包含 _id）
        final_data = {
            "_id": clean_company_name,  # MongoDB id 使用公司名称
            "企业基本信息": new_company
        }
        return final_data

    def _read_excel_file(
            self,
            file_path: str,
            sheet_name: Union[str, int, None] = 0,
            header: Union[int, None] = 0,
            usecols: Union[int, str, List[Union[int, str]], None] = None
    ):
        """读取文件夹下所有 .xlsx 文件，合并数据"""
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=header,
                usecols=usecols,
                engine='openpyxl'
            )
            self.logger.debug(f"文件路径: {file_path} (表名: {sheet_name}, 行数: {len(df)})")
            jsons = df.to_dict(orient='records')
            return jsons
        except Exception as e:
            self.logger.error(f"{sheet_name} 文件读取失败: {e}")
            raise

    @staticmethod
    def _transform_qcc_company(company):
        """
        核心格式转换：
        1. _id = 企业名称, 英文括号替换为中文
        3. 包裹进 企业基本信息
        """
        company_name = company.get("企业名称", "")
        # 替换英文括号为中文
        clean_company_name = str(company_name).replace("(", "（").replace(")", "）")
        # 生成最终结构（包含 _id）
        final_data = {
            "_id": clean_company_name,  # MongoDB id 使用公司名称
            "企业基本信息": company
        }
        return final_data


    async def _save_to_mongodb(self, data_list):
        """存入MongoDB"""
        success = 0
        # 只更新/新增，不删除旧字段
        async with self._mongo_write_lock:
            for item in data_list:
                company_name = item.get("_id")
                if not company_name:
                    continue

                # ==============================================
                # 核心：只构建【企业基本信息】的新增字段
                # ==============================================
                update_query = {}
                honor_value = None

                # 只处理 企业基本信息，并且只新增不存在的键
                if "企业基本信息" in item and isinstance(item["企业基本信息"], dict):
                    for key, value in item["企业基本信息"].items():
                        # 格式：企业基本信息.xxx
                        field_path = f"企业基本信息.{key}"
                        if key == "企业荣誉":
                            # 单独处理荣誉：用于追加
                            honor_value = value
                        else:
                            # 其他字段正常更新
                            update_query[field_path] = value

                try:
                    # 1. 更新普通字段
                    if update_query:
                        await self.collection.update_one(
                            {"_id": company_name},
                            {"$set": update_query},
                            upsert=True
                        )

                    # 2. 追加荣誉（已经修复成数组，直接push）
                    if honor_value is not None:
                        await self.collection.update_one(
                            {"_id": company_name},
                            {"$push": {
                                "企业基本信息.企业荣誉": honor_value
                            }},
                            upsert=True
                        )

                    success += 1
                except Exception as e:
                    self.logger.error(f"更新失败 {company_name}: {str(e)}")
            return success


# # ====================== 异步测试入口 ======================
# async def main():
#     service = CompanyBaseInfo()
#     await service.init()
#     # await service.listed_company_to_mongodb()
#     await service.qcc_company_to_mongodb("苏州市专精特新“小巨人”企业及上市企业.xlsx")
#
#
# if __name__ == "__main__":
#     asyncio.run(main())