import re
from src.utils.singletons.database_singleton import MongoSingleton
from src.utils.singletons.ai_singleton import LLMFactory
from langchain_core.messages import SystemMessage, HumanMessage
from src.utils.logger import get_logger
import asyncio
from collections import defaultdict
import json


class MatchCompanySubject:
    def __init__(self, max_workers: int = 5):
        """
        初始化服务类
        """
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = max_workers
        # 初始化客户端
        self._mongo_client = MongoSingleton.get_instance()
        self.university_collection = self._mongo_client['dquot']['university_group_info']
        self.company_collection = self._mongo_client['dquot']['company_info']
        # 初始化大模型
        self._llm_client = LLMFactory.get_instance('qwen')

    async def match_company_college(self):
        self.logger.info(f"开始异步批量处理企业学院匹配 | LLM最大并发: {self.max_workers}")
        # ========== 步骤1：获取文件（线程池执行） ==========
        research_subject_dict = self.get_university_research_subject()
        if not research_subject_dict:
            self.logger.error("未获取到任何一级学科/实验平台数据")
            return {"success": False, "message": "无可用学科/实验平台数据"}

        subject_list = research_subject_dict["一级学科"]
        research_list = research_subject_dict["实验平台"]

        cooperation_need = await asyncio.to_thread(
            self.get_company_cooperation_need
        )
        self.logger.info(f"找到待处理企业合作需求：{len(cooperation_need)}")

        if not cooperation_need:
            return {"success": True, "message": "无待处理企业合作需求", "processed": 0, "failed": 0}

        # 2. 使用线程池并发处理
        results = defaultdict(list)
        # 核心：LLM并发控制器（信号量）
        semaphore = asyncio.Semaphore(self.max_workers)

        # ============================
        # 单个企业匹配完整处理流程
        # ============================
        async def process_one_company(company_info: dict):
            async with semaphore:
                company_name = company_info['_id']
                try:
                    # 1. 获取企业信息
                    self.logger.info(f"开始匹配企业：{company_name} 的一级学科及实验平台")
                    company_match_result = await asyncio.to_thread(
                        self._analyze_with_llm,
                        company_info,
                        subject_list,
                        research_list,
                        2
                    )
                    if not company_match_result or "error" in company_match_result:
                        return {"success": False, "company_name": company_name, "error": "匹配一级学科和实验平台失败"}

                    match_subject_list = company_match_result['subject']
                    match_research_list = company_match_result['research_platform']
                    self.logger.info(f"匹配一级学科{match_subject_list}及实验平台{match_research_list}对应的高校学院")
                    match_university_college = await asyncio.to_thread(
                        self.get_match_subject_college,
                        match_subject_list,
                        match_research_list
                    )
                    if not match_university_college:
                        return {"success": False, "company_name": company_match_result['_id'], "error": "匹配高校学院失败"}
                    self.logger.info(f"匹配高校学院{match_university_college}")

                    # 6. 保存MongoDB
                    # pymongo 同步存储，丢线程池
                    save_result = await asyncio.to_thread(
                        self._save_json_to_mongodb, company_name, match_university_college
                    )
                    if save_result.get("success"):
                        self.logger.info(f"[成功] {company_name}")
                        return {"success": True, "company_name": company_name}
                    else:
                        return {"success": False, "company_name": company_name, "error": "存储失败"}

                except Exception as e:
                    self.logger.error(f"[异常] {company_name}: {str(e)}")
                    return {"success": False, "company_name": company_name, "error": str(e)}

        # 执行所有任务
        tasks = [process_one_company(company_info) for company_info in cooperation_need]
        final_results = await asyncio.gather(*tasks)

        # 整理结果
        for res in final_results:
            if res.get("success"):
                results["success"].append(res)
            else:
                results["failed"].append(res)

        # 汇总
        summary = {
            "success": True,
            "total": len(cooperation_need),
            "processed": len(results["success"]),
            "failed": len(results["failed"]),
            "details": results
        }

        self.logger.info(f"✅ 处理完成 | 成功:{summary['processed']} | 失败:{summary['failed']}")
        return summary

    def get_university_research_subject(self):
        search = [
            # 展开数组
            {"$unwind": {"path": "$research_platform", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$subject", "preserveNullAndEmptyArrays": True}},

            # 分组去重
            {
                "$group": {
                    "_id": None,
                    "实验平台": {"$addToSet": "$research_platform.name"},
                    "一级学科": {"$addToSet": "$subject.name"}
                }
            },

            # 不显示 _id
            {"$project": {"_id": 0}}
        ]

        return next(self.university_collection.aggregate(search), {})

    def get_company_cooperation_need(self):
        search = [
            {"$match": {
                "合作需求与方向.可联合攻关方向": {
                    "$exists": True,
                    "$ne": None,
                },
                "可合作高校学院": {"$exists": False}
            }},
            {"$project": {
                "_id": 1,
                "可联合攻关方向": "$合作需求与方向.可联合攻关方向"
            }},
            # # 限制一个测试
            # {"$limit": 1}
        ]

        return list(self.company_collection.aggregate(search))

    def _analyze_with_llm(self, company_info: dict, subject_list, research_list, max_retries: int = 2) -> dict:
        """调用千问大模型进行企业合作方向分析（带重试机制）"""
        for attempt in range(max_retries):
            try:
                system_prompt = f"""
                    你需要根据用户提供的企业可联合攻关方向，结合内置科研资源，筛选匹配度最高的5个一级学科、10个实验平台并返回结果。
                    筛选规则：
                    1. 严格贴合企业可联合攻关方向，可联合攻关方向列表中越靠前的方向，权重越大。工学类学科优先， 仅匹配明确相关的实验平台，仅从内置科研资源库中匹配学科和实验平台，剔除所有无关学科与实验平台；
                    2. 最终结果按匹配度从高到低排序；
                    3. 输出格式为一组标准Python字典，包含学科列表、实验室列表，学科列表字段名为“subject”，实验室列表字段名为“research_platform”，
                    列表内为对应名称字符串，无任何多余文字、解释、备注、注释冗余内容。
                    【内置科研资源库】
                    可用一级学科：{subject_list}
                    可用实验平台：{research_list}
                """
                clean_system_prompt = self._compact_text(system_prompt)

                company_name = company_info["_id"]
                company_research = company_info["可联合攻关方向"]
                user_prompt = f'''
                    {company_name}企业可联合攻关方向为{company_research}
                    请开始分析：'''
                clean_user_prompt = self._compact_text(user_prompt)

                messages = [
                    SystemMessage(content=clean_system_prompt),
                    HumanMessage(content=clean_user_prompt)
                ]

                self.logger.info(f"正在调用千问大模型... (第{attempt + 1}次尝试)")
                response = self._llm_client.invoke(messages)
                self.logger.info(f"LLM调用成功，开始解析响应...")

                if not hasattr(response, 'content'):
                    self.logger.error(f"LLM响应格式异常，缺少content属性: {type(response)}")
                    return {"error": f"LLM响应格式异常: {type(response)}"}

                # 关键清洗：把单引号强制转成双引号
                content = response.content.strip().replace("'", '"')
                # 自动提取 {...} 格式，过滤多余文字
                match = re.search(r"\{.*}", content, re.DOTALL)
                if not match:
                    self.logger.error("LLM返回内容中未找到有效的JSON")
                    if attempt == max_retries - 1:
                        return {"error": "LLM返回格式异常，缺少JSON"}
                    continue
                json_str = match.group()
                # 转字典
                try:
                    result_dict = json.loads(json_str)
                except json.JSONDecodeError as e:
                    self.logger.error(f"LLM返回的JSON解析失败: {e}")
                    return {"error": f"JSON解析失败: {str(e)}"}

                result_dict['_id'] = company_name

                return result_dict

            except Exception as e:
                self.logger.error(f"LLM调用失败: {e}", exc_info=True)
                if attempt == max_retries - 1:
                    return {"error": f"LLM调用失败: {str(e)}"}

    @staticmethod
    def _compact_text(text: str) -> str:
        """压缩文本"""
        text = re.sub(r'\n+', '\n', text)
        text = re.sub(r' +', ' ', text)
        return '\n'.join(line.strip() for line in text.split('\n'))

    def get_match_subject_college(self, subject_list: list, research_list: list):
        search = [
            {
                "$match": {
                    "$and": [
                        {"research_platform.name": {"$in": research_list}},
                        {"subject.name": {"$in": subject_list}}
                    ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "university": "$university",
                        "college": "$college"
                    }
                }
            },
            {
                "$replaceRoot": {"newRoot": "$_id"}
            },
            {"$sort": {"university": 1, "college": 1}}
        ]
        # 获取结果
        return list(self.university_collection.aggregate(search))

    def _save_json_to_mongodb(
            self,
            company_name: str,
            match_university: list
    ) -> dict:
        """
        存储列表到MongoDB
        唯一键固定为 _id
        只更新/新增字段，旧字段 100% 保留
        特别处理：企业基本信息 只新增不存在的键，不覆盖已有键
        """
        try:
            # 执行更新：只新增/更新，不覆盖旧字段
            result = self.company_collection.update_one(
                {"_id": company_name},  # 按 _id 匹配
                {"$set": {"可合作高校学院": match_university}},  # 只更新需要的字段
                upsert=True  # 不存在则新增
            )

            return {
                "success": True,
                "message": "Upsert成功",
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }

        except Exception as e:
            self.logger.error(f"MongoDB存储失败: {e}")
            return {"success": False, "message": f"存储失败: {str(e)}"}


if __name__ == '__main__':
    service = MatchCompanySubject()
    asyncio.run(service.match_company_college())