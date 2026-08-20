"""
企业工商信息新增 - 实现企业模糊匹配 + 工商信息补充
工作流程：MongoDB读取新增企业 → API调用 → 更新企业基本信息 → 缺失企业提示
"""

from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger
import time
import hashlib
import json
import aiohttp
import asyncio
from src.config.auth import QCC_APP_KEY, QCC_SECRECT_KEY


class CompanyBaseUpdate(BaseResourceService):
    """数据更新类 - 新增企业更新信息"""
    name = "qcc_company_update"
    def __init__(self, max_workers: int = 3):
        """
        初始化服务类
        :param max_workers: 最大并发数，默认5
        """
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)  # 并发信号量

        # 初始化客户端（懒加载）
        self._mongo_collection = "company_info"

    # ==================== 获取新增企业列表 ====================
    async def _get_new_company(self):
        mongo_db = await self.get_mongo_db()
        company_coll = mongo_db[self._mongo_collection]

        company_filter = {
            "企业基本信息": {"$exists": False},
            "企业查询错误": {"$exists": False}
        }
        # 投影：只返回cos路径
        report_proj = {
            "_id": 1
        }

        company_list = []
        cursor = company_coll.find(company_filter, projection=report_proj).batch_size(1000)
        async for doc in cursor:
            company_name = doc["_id"]
            company_list.append(company_name)

        self.logger.info(f"从{company_coll}拿到 {len(company_list)} 个需要更新的企业")
        return company_list

    # ==================== 调用天眼查API ====================
    async def _get_company_info(self, _id):
        #  token可以从 数据中心 -> 我的接口 中获取
        #  请求参数
        encode = 'utf-8'
        # Http请求头设置
        timespan = str(int(time.time()))
        token = QCC_APP_KEY + timespan + QCC_SECRECT_KEY
        hl = hashlib.md5()
        hl.update(token.encode(encoding=encode))
        token = hl.hexdigest().upper()
        self.logger.debug('MD5加密后为 ：' + token)

        # 设置请求Url-请自行设置Url
        req_inter_name = "https://api.qichacha.com/ECIV4/GetBasicDetailsByName"
        param_str = f"keyword={_id}"
        url = req_inter_name + "?key=" + QCC_APP_KEY + "&" + param_str
        headers = {'Token': token, 'Timespan': timespan}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    self.logger.info(f"[{_id}] http_status={response.status}")
                    if response.status != 200:
                        return False, {}, f"http请求异常 status:{response.status}"
                    resp_dict = await response.json()
                    self.logger.info(f"[{_id}] api_response: {json.dumps(resp_dict, ensure_ascii=False)}")
                    return True, resp_dict, ""

        except Exception as e:
            err = f"请求企查查异常: {str(e)}"
            self.logger.error(f"[{_id}] {err}")
            return False, {}, err

    async def _transform_company_info(self, company_response: dict):
        result = company_response["Result"]
        if not result:
            raise ValueError("Result为空，无企业数据")

        # 处理营业期限 TermStart / TermEnd
        term_start_raw = result.get("TermStart", "")
        term_end_raw = result.get("TermEnd", "")

        start = term_start_raw[:10] if term_start_raw else ""
        if term_end_raw:
            end = term_end_raw[:10]
        else:
            end = "无固定期限"
        start_end = f"{start} 至 {end}"

        # 成立日期截取前10位
        establish_date_raw = result.get("StartDate", "")
        establish_date = establish_date_raw[:10] if establish_date_raw else ""

        area = result.get("Area", {})

        # ========== OriginalName 对象数组 → 提取Name拼接字符串 ==========
        original_name_list = result.get("OriginalName", [])
        name_items = []
        if isinstance(original_name_list, list):
            for item in original_name_list:
                if isinstance(item, dict) and item.get("Name"):
                    name_items.append(item["Name"])
                elif isinstance(item, str) and item.strip():
                    name_items.append(item)

        former_name = ",".join(name_items) if name_items else ""


        company_map = {
            "企业基本信息": {
                "企业名称": result.get("Name", ""),
                "登记状态": result.get("Status", ""),
                "法定代表人": result.get("OperName", ""),
                "注册资本": result.get("RegistCapi", ""),
                "实缴资本": result.get("RecCap", ""),
                "成立日期": establish_date,
                "统一社会信用代码": result.get("CreditCode", ""),
                "企业地址": result.get("Address", ""),
                "所属省份": area.get("Province", ""),
                "所属城市": area.get("City", ""),
                "所属区县": area.get("County", ""),
                "企业（机构）类型": result.get("EconKind", ""),
                "营业期限": start_end,
                "曾用名": former_name,
                "经营范围": result.get("Scope", ""),
                "登记机关": result.get("BelongOrg", ""),
            }
        }
        return company_map

    async def _save_to_mongodb(self, company_info):
        """存入MongoDB"""
        # 只更新/新增，不删除旧字段
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self._mongo_collection]
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        # 1. 单条更新
        _id = company_info["企业基本信息"]["企业名称"]
        try:
            if company_info:
                await collection.update_one(
                    {"_id": _id},
                    {
                        "$set": {
                            **company_info,
                            "updateTime": now_str
                        }
                    },
                    upsert=True
                )
            return _id, True
        except Exception as e:
            self.logger.error(f"更新mongo失败 {_id}: {str(e)}")
            return _id, False

    async def _save_fetch_error(self, company_name: str, status: str, message: str):
        """
        将查询错误写入数据库（Status=201无结果 / 网络异常都走这里）
        写入字段：企业查询错误，同时更新顶层updateTime
        """
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self._mongo_collection]
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        error_doc = {
            "状态码": status,
            "错误信息": message,
            "查询时间": now_str
        }
        try:
            await collection.update_one(
                {"_id": company_name},
                {
                    "$set": {
                        "企业查询错误": error_doc,
                        "updateTime": now_str
                    }
                },
                upsert=True
            )
            self.logger.warning(f"[{company_name}] 已标记查询错误到mongo: {message}")
            return True
        except Exception as e:
            self.logger.error(f"[{company_name}] 保存错误标记失败: {str(e)}")
            return False


    async def _single_task(self, company_name):
        """单个企业任务，信号量控制并发"""
        async with self._semaphore:
            try:
                ok, resp_data, err_msg = await self._get_company_info(company_name)
                # http/网络层面失败
                if not ok:
                    return "error"

                business_status = resp_data.get("Status")
                business_msg = resp_data.get("Message", "")

                # ✅企查查业务状态201：有效请求，但查询无结果，写入错误标记
                if business_status == "201":
                    await self._save_fetch_error(company_name, business_status, business_msg)
                    return "no_result"

                # ✅正常业务200，解析保存企业信息
                if business_status == "200":
                    company_map = await self._transform_company_info(resp_data)
                    _, save_ok = await self._save_to_mongodb(company_map)
                    return "success" if save_ok else "error"

                # 其他业务状态码（301、401等）统一记错误
                return "error"

            except Exception as e:
                self.logger.exception(f"处理企业 {company_name} 发生未知异常")
                return "error"

    async def pipeline(self):
        """主流程，支持并发；max_workers控制并发数量"""
        company_list = await self._get_new_company()
        if not company_list:
            self.logger.info("无待处理企业，直接返回")
            return 0

        # ----------并发版本（推荐，使用max_workers）----------
        tasks = [self._single_task(name) for name in company_list]
        results = await asyncio.gather(*tasks)
        success = sum(1 for r in results if r is True)

        total = len(company_list)
        fail = total - success
        self.logger.info(f"任务结束 total:{total}, success:{success}, fail:{fail}")
        return success


# ====================== 异步测试入口 ======================
async def main():
    service = CompanyBaseUpdate()
    await service.pipeline()


if __name__ == "__main__":
    asyncio.run(main())
