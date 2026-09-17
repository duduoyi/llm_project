import re
import asyncio
from src.services.base_resource_service import BaseResourceService
import io
from bs4 import BeautifulSoup
from src.utils.logger import get_logger
from typing import Union, List
import pandas as pd


class CompanyBaseInfo(BaseResourceService):
    def __init__(self):
        """
        初始化服务类
        """
        self.logger = get_logger(self.__class__.__name__)
        # 初始化客户端（懒加载）
        self._mongo_collection = "company_info"
        self._bucket_name = "company-report-1366204382"
        self._qcc_folder = "qcc_company/"


    async def listed_company_to_mongodb(self, listed_folder):
        try:
            cos_client = await self.get_cos_client()
            # 1. 获取文件列表（文件遍历是同步操作，to_thread隔离）
            file_key_list = await asyncio.to_thread(
                self._get_files_list,
                cos_client,
                bucket=self._bucket_name,
                prefix=listed_folder,
                file_format=".txt"
            )
            self.logger.info(f"待处理上市企业文件列表: {file_key_list}")

            # 2. 读取文件内容
            all_companies_data = []
            for file_key in file_key_list:
                company_data = await asyncio.to_thread(
                    self._read_txt_file,
                    await self.get_cos_client(),
                    self._bucket_name,
                    file_key
                )
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
        except Exception as e:
            self.logger.error(
                f"上市企业同步任务异常 err={str(e)}",
                exc_info=True
            )
            raise
    async def qcc_company_to_mongodb(self, file_name=None):
        try:
        # 1. 获取文件列表（根据 file_name 判断）
            cos_client = await self.get_cos_client()
            if file_name is not None:
                # 有指定文件名 → 只读取这一个文件
                file_key_list = [f"{self._qcc_folder}{file_name}"]
            else:
                # 无指定文件名 → 遍历文件夹所有 xlsx
                file_key_list = await asyncio.to_thread(
                    self._get_files_list,
                    cos_client,
                    self._bucket_name,
                    self._qcc_folder,
                    ".xlsx"
                )
            self.logger.info(f"待处理文件列表: {file_key_list}")

            # 2. 读取文件内容
            company_data_list = []
            for file_key in file_key_list:
                company_data = await asyncio.to_thread(
                    self._read_excel_file,
                    cos_client,
                    self._bucket_name,
                    file_key
                )
                company_data_list.extend(company_data)

            # 3. 格式转换
            update_data = await asyncio.to_thread(
                lambda data: [self._transform_qcc_company(c) for c in data],
                company_data_list
            )
            # 3. 存入MongoDB
            success = await self._save_to_mongodb(update_data)
            self.logger.info(f"✅ 完成同步：{success} 条数据（仅给企业基本信息新增字段，所有旧数据完全保留）")
        except Exception as e:
            self.logger.error(
                f"企查查企业同步任务异常 file_name={file_name}, err={str(e)}",
                exc_info=True
            )
            raise

    @staticmethod
    def _get_files_list(cos_client, bucket: str, prefix: str, file_format: str):
        """同步获取COS指定前缀文件列表"""
        file_list = []
        marker = ""
        while True:
            resp = cos_client.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
            contents = resp.get("Contents", [])
            if not contents:
                break
            for item in contents:
                key = item["Key"]
                if key.lower().endswith(file_format):
                    file_list.append(key)
            if resp.get("IsTruncated") == "true":
                marker = resp.get("NextMarker", "")
            else:
                break
        return file_list

    def _read_txt_file(self, cos_client, bucket: str, file_key: str, encoding: str = "utf-8"):
        """同步读取COS txt并解析HTML表格"""
        try:
            resp = cos_client.get_object(Bucket=bucket, Key=file_key)
            raw_bytes = resp["Body"].get_raw_stream().read()
            try:
                content = raw_bytes.decode(encoding)
                return CompanyBaseInfo._parse_table_to_json(content)
            except UnicodeDecodeError:
                content = raw_bytes.decode("gbk")
                return CompanyBaseInfo._parse_table_to_json(content)
        except cos_client.exceptions.NoSuchKey:
            self.logger.error(f"COS文件不存在 bucket={bucket}, key={file_key}")
            return []
        except Exception as e:
            self.logger.error(f"读取TXT文件失败 key={file_key}, err={str(e)}", exc_info=True)
            return []

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
            cos_client, bucket: str, file_key: str,
            sheet_name: Union[str, int, None] = 0,
            header: Union[int, None] = 0,
            usecols: Union[int, str, List[Union[int, str]], None] = None
    ):
        """
        从COS读取Excel文件，内存流交给pandas解析(适用于结构规范、数据量小的表格)，兼容原有入参和返回格式
        :param file_key: COS 对象完整Key
        """
        try:
            # 1. 从COS下载二进制数据流
            resp = cos_client.get_object(Bucket=bucket, Key=file_key)
            binary_data = resp["Body"].get_raw_stream().read()
            stream = io.BytesIO(binary_data)

            df = pd.read_excel(
                stream,
                sheet_name=sheet_name,
                header=header,
                usecols=usecols,
                engine='openpyxl'
            )
            self.logger.debug(f"COS文件Key: {file_key} (表名: {sheet_name}, 行数: {len(df)})")
            jsons = df.to_dict(orient='records')
            return jsons
        except cos_client.exceptions.NoSuchKey:
            self.logger.error(f"COS 文件不存在 key={file_key}")
            raise
        except Exception as e:
            self.logger.error(f"{sheet_name} 读取COS Excel失败: {e}", exc_info=True)
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
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self._mongo_collection]
        # 1. 更新普通字段
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
                if update_query:
                    await collection.update_one(
                        {"_id": company_name},
                        {"$set": update_query},
                        upsert=True
                    )

                # 2. 追加荣誉（已经修复成数组，直接push）
                if honor_value is not None:
                    await collection.update_one(
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
#     await service.listed_company_to_mongodb("nantong_listed_company/")
#     await service.qcc_company_to_mongodb("【企查查】昆山_制造业和科学_20260401-20260824.xlsx")
#
#
# if __name__ == "__main__":
#     asyncio.run(main())