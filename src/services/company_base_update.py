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
import re
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

    @staticmethod
    def clean_bad_unicode(text: str) -> str:
        if not text:
            return text
        # 移除 Unicode 替换字符 � (U+FFFD)
        text = text.replace("\ufffd", "")
        # 可选：保留中文、英文、数字、常用标点，过滤掉其他特殊不可见字符
        pattern = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9，。；：、（）()《》【】%“”‘’——\s]')
        text = pattern.sub('', text)
        return text.strip()

    async def _transform_company_info(self, company_id: str, company_response: dict):
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
        clean_scope = self.clean_bad_unicode(result.get("Scope", ""))
        company_map = {
            "_id": company_id,
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
                "经营范围": clean_scope,
                "登记机关": result.get("BelongOrg", ""),
            }
        }
        return company_map

    async def _save_to_mongodb(self, company_info):
        """存入MongoDB"""
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self._mongo_collection]
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        _id = company_info["_id"]

        # 调试：确认实际连的库和集合（排查连错库/集合的情况）
        self.logger.info(
            f"[Mongo写入] db={mongo_db.name}, collection={collection.name}, _id={_id!r}"
        )

        try:
            if not company_info:
                self.logger.error(f"company_info 为空，跳过写入: {_id}")
                return _id, False

            result = await collection.update_one(
                {"_id": _id},
                {
                    "$set": {
                        **company_info,
                        "updateTime": now_str
                    }
                },
                upsert=True
            )

            # 关键：检查真实写入结果
            matched = result.matched_count
            modified = result.modified_count
            upserted_id = result.upserted_id

            self.logger.info(
                f"[Mongo写入结果] _id={_id!r}, "
                f"matched={matched}, modified={modified}, upserted_id={upserted_id}"
            )

            # upsert 成功：upserted_id 不为 None
            # 更新成功：matched > 0（modified 可能为0表示数据无变化）
            if upserted_id is not None or matched > 0:
                return _id, True
            else:
                self.logger.error(
                    f"写入未生效！matched={matched}, modified={modified}, "
                    f"upserted_id={upserted_id}, _id={_id!r}"
                )
                return _id, False

        except Exception as e:
            self.logger.exception(f"更新mongo异常 {_id}: {str(e)}")
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


    async def _single_task(self, company_id):
        """单个企业任务，信号量控制并发"""
        async with self._semaphore:
            try:
                ok, resp_data, err_msg = await self._get_company_info(company_id)
                # http/网络层面失败
                if not ok:
                    return "error"

                business_status = resp_data.get("Status")
                business_msg = resp_data.get("Message", "")

                # ✅企查查业务状态201：有效请求，但查询无结果，写入错误标记
                if business_status == "201":
                    await self._save_fetch_error(company_id, business_status, business_msg)
                    return "no_result"

                # ✅正常业务200，解析保存企业信息
                if business_status == "200":
                    company_map = await self._transform_company_info(company_id, resp_data)
                    self.logger.info(f"对{company_id}进行数据转换，转换结果：{company_map}")
                    _, save_ok = await self._save_to_mongodb(company_map)
                    self.logger.info(f"将{company_id}存入MongoDB，存储结果：{save_ok}")
                    return "success" if save_ok else "error"

                # 其他业务状态码（301、401等）统一记错误
                return "error"

            except Exception as e:
                self.logger.exception(f"处理企业 {company_id} 发生未知异常")
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


# # ====================== 异步测试入口 ======================
# async def main():
#     api_response = {"Status": "200", "Message": "【有效请求】查询成功", "OrderNumber": "ECI2026091201300085930169",
#      "Result": {"KeyNo": "8baaby3rvprpvjajijsjvvap3mjhapbhha", "Name": "柯萨化妆品陈列设备（昆山）有限公司",
#                 "No": "320583400036374", "BelongOrg": "昆山市数据局", "OperId": "pikb5ykk33vharaj39pppvel39s3ssrabr",
#                 "OperName": "秦勤", "DesignatedRepresentativeList": [], "StartDate": "2005-12-05 00:00:00",
#                 "EndDate": "", "Status": "在业", "Province": "JS", "UpdatedDate": "2026-09-04 05:16:57",
#                 "CreditCode": "91320583782096704J", "RegistCapi": "2000万元", "RegisteredCapital": "2000",
#                 "RegisteredCapitalUnit": "万", "RegisteredCapitalCCY": "CNY",
#                 "EconKind": "有限责任公司（自然人投资或控股的法人独资）", "Address": "江苏省昆山开发区泾浦路18号",
#                 "Scope": "许可项目：建设工程设计；建设工程施工；建筑劳务分包（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准）一般项目：通用设备制造（不含特种设备制造）；家具制造；家具零配件生产；家具零配件销售；家具销售；家具安装和维修服务；建筑用木料及木材组件加工；日用木制品制造；日用木制品销售；人造板销售；木制玩具销售；模具制造；模具销售；塑料制品制造；塑料制品销售；工艺美术品及礼仪用品销��（象牙及其制品除外）；游艺用品及室内游艺器材销售；金属制品销售；灯具销售；建筑材料销售；橡胶制品销售；纸制品销售；建筑用金属配件销售；建筑装饰材料销售；五金产品零售；电子产品销售；国内货物运输代理；技术服务、技术开发、技术咨询、技术交流、技术转让、技术推广；专业设计服务；数字文化创意技术装备销售；工程管理服务；进出口代理；会议及展览服务（出国办展须经相关部门审批）（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）",
#                 "TermStart": "2005-12-05 00:00:00", "TermEnd": "2030-12-04 00:00:00",
#                 "CheckDate": "2026-08-11 00:00:00", "OrgNo": "78209670-4", "IsOnStock": "0",
#                  "OriginalName": [],
#                 "ImageUrl": "https://image.qcc.com/logo/8c0e16c05aad6e5806f50d0645e1a10e.jpg", "EntType": "0",
#                 "RecCap": "45万欧元", "PaidUpCapital": "45", "PaidUpCapitalUnit": "万", "PaidUpCapitalCCY": "EUR",
#                 "Area": {"Province": "江苏省", "City": "苏州市", "County": "昆山市"},
#                 "AreaCode": "320583", "TeamEnd": ""}}
#
#
#
#     service = CompanyBaseUpdate()
#
#     company_map = await service._transform_company_info("柯萨化妆品陈列设备(昆山)有限公司", api_response)
#
#     print(company_map)
#
#     _, save_ok = await service._save_to_mongodb(company_map)
#     print(_, save_ok)
#
#
# if __name__ == "__main__":
#     asyncio.run(main())
