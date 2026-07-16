import re
from src.utils.singletons.database_singleton import AsyncMongoSingleton
from src.utils.singletons.ai_singleton import LLMFactory
from langchain_core.messages import SystemMessage, HumanMessage
from src.utils.logger import get_logger
import asyncio
from collections import defaultdict
import json


class MatchCompanySubject:
    def __init__(self, max_workers: int = 5):
        """
        初始化服务类（同步构造，仅声明变量）
        """
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = max_workers
        # 异步客户端延后init初始化
        self._mongo_client = None
        self.db_name = "dquot"
        self.university_coll_name = "university_group_info"
        self.company_coll_name = "company_info"
        # 初始化大模型（需支持ainvoke异步调用）
        self._llm_client = LLMFactory.get_instance('qwen')

    async def init(self):
        """异步初始化Mongo连接（必须await调用）"""
        self._mongo_client = await AsyncMongoSingleton.get_instance()
        self.logger.info("✅ MatchCompanySubject Mongo异步客户端初始化完成")

    async def match_company_college(self):
        if self._mongo_client is None:
            raise RuntimeError("请先执行 await service.init() 初始化Mongo客户端！")

        self.logger.info(f"开始异步批量处理企业学院匹配 | LLM最大并发: {self.max_workers}")
        # ========== 步骤1：获取学科、实验平台数据【异步mongo】 ==========
        research_subject_dict = await self.get_university_research_subject()
        if not research_subject_dict:
            self.logger.error("未获取到任何一级学科/实验平台数据")
            return {"success": False, "message": "无可用学科/实验平台数据"}

        subject_list = research_subject_dict.get("一级学科", [])
        research_list = research_subject_dict.get("实验平台", [])

        cooperation_need = await self.get_company_cooperation_need()
        self.logger.info(f"找到待处理企业合作需求：{len(cooperation_need)}")

        if not cooperation_need:
            return {"success": True, "message": "无待处理企业合作需求", "processed": 0, "failed": 0}

        # 并发控制
        results = defaultdict(list)
        semaphore = asyncio.Semaphore(self.max_workers)

        async def process_one_company(company_info: dict):
            async with semaphore:
                company_name = company_info['_id']
                try:
                    self.logger.info(f"开始匹配企业：{company_name} 的一级学科及实验平台")
                    # 【异步LLM，不再套to_thread】
                    company_match_result = await self._analyze_with_llm_async(
                        company_info, subject_list, research_list, max_retries=2
                    )
                    if not company_match_result or "error" in company_match_result:
                        return {
                            "success": False,
                            "company_name": company_name,
                            "error": company_match_result.get("error", "匹配一级学科和实验平台失败")
                        }

                    match_subject_list = company_match_result['subject']
                    match_research_list = company_match_result['research_platform']
                    self.logger.info(f"匹配一级学科{match_subject_list}及实验平台{match_research_list}对应的高校学院")

                    # 【异步查询高校】
                    match_university_college = await self.get_match_subject_college(
                        match_subject_list, match_research_list
                    )
                    if not match_university_college:
                        return {
                            "success": False,
                            "company_name": company_name,
                            "error": "匹配高校学院结果为空"
                        }
                    self.logger.info(f"匹配高校学院数量：{len(match_university_college)}")

                    # 【异步入库】
                    save_result = await self._save_json_to_mongodb(
                        company_name, match_university_college
                    )
                    if save_result.get("success"):
                        self.logger.info(f"[成功] {company_name}")
                        return {"success": True, "company_name": company_name}
                    else:
                        return {
                            "success": False,
                            "company_name": company_name,
                            "error": save_result.get("message", "存储失败")
                        }

                except Exception as e:
                    self.logger.error(f"[异常] {company_name}: {str(e)}", exc_info=True)
                    return {"success": False, "company_name": company_name, "error": str(e)}

        # 启动任务
        tasks = [process_one_company(company_info) for company_info in cooperation_need]
        final_results = await asyncio.gather(*tasks)

        for res in final_results:
            if res.get("success"):
                results["success"].append(res)
            else:
                results["failed"].append(res)

        summary = {
            "success": True,
            "total": len(cooperation_need),
            "processed": len(results["success"]),
            "failed": len(results["failed"]),
            "details": results
        }
        self.logger.info(f"✅ 处理完成 | 成功:{summary['processed']} | 失败:{summary['failed']}")
        return summary

    # ====================== 异步Mongo查询方法 ======================
    async def get_university_research_subject(self):
        pipeline = [
            {"$unwind": {"path": "$research_platform", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$subject", "preserveNullAndEmptyArrays": True}},
            {
                "$group": {
                    "_id": None,
                    "实验平台": {"$addToSet": "$research_platform.name"},
                    "一级学科": {"$addToSet": "$subject.name"}
                }
            },
            {"$project": {"_id": 0}}
        ]
        university_coll = self._mongo_client[self.db_name][self.university_coll_name]
        cursor = university_coll.aggregate(pipeline)
        data_list = await cursor.to_list(length=1)
        return data_list[0] if data_list else {}

    async def get_company_cooperation_need(self):
        pipeline = [
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
            # {"$limit": 1}
        ]
        company_coll = self._mongo_client[self.db_name][self.company_coll_name]
        cursor = company_coll.aggregate(pipeline)
        return await cursor.to_list(length=None)

    async def get_match_subject_college(self, subject_list: list, research_list: list):
        pipeline = [
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
            {"$replaceRoot": {"newRoot": "$_id"}},
            {"$sort": {"university": 1, "college": 1}}
        ]
        university_coll = self._mongo_client[self.db_name][self.university_coll_name]
        cursor = university_coll.aggregate(pipeline)
        return await cursor.to_list(length=None)

    async def _save_json_to_mongodb(
            self,
            company_name: str,
            match_university: list
    ) -> dict:
        try:
            company_coll = self._mongo_client[self.db_name][self.company_coll_name]
            result = await company_coll.update_one(
                {"_id": company_name},
                {"$set": {"可合作高校学院": match_university}},
                upsert=True
            )
            return {
                "success": True,
                "message": "Upsert成功",
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except Exception as e:
            self.logger.error(f"MongoDB存储失败: {e}", exc_info=True)
            return {"success": False, "message": f"存储失败: {str(e)}"}

    # ====================== 异步LLM调用 ======================
    async def _analyze_with_llm_async(self, company_info: dict, subject_list, research_list, max_retries: int = 2) -> dict:
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

                self.logger.info(f"正在异步调用千问大模型... (第{attempt + 1}次尝试)")
                response = await self._llm_client.ainvoke(messages)
                self.logger.info(f"LLM异步调用成功，开始解析响应...")

                if not hasattr(response, 'content'):
                    self.logger.error(f"LLM响应格式异常，缺少content属性: {type(response)}")
                    return {"error": f"LLM响应格式异常: {type(response)}"}

                content = response.content.strip().replace("'", '"')
                match = re.search(r"\{.*}", content, re.DOTALL)
                if not match:
                    self.logger.error("LLM返回内容中未找到有效的JSON")
                    if attempt == max_retries - 1:
                        return {"error": "LLM返回格式异常，缺少JSON"}
                    continue
                json_str = match.group()
                result_dict = json.loads(json_str)
                result_dict['_id'] = company_name
                return result_dict

            except json.JSONDecodeError as e:
                self.logger.error(f"LLM返回的JSON解析失败: {e}", exc_info=True)
                if attempt == max_retries - 1:
                    return {"error": f"JSON解析失败: {str(e)}"}
            except Exception as e:
                self.logger.error(f"LLM异步调用失败: {e}", exc_info=True)
                if attempt == max_retries - 1:
                    return {"error": f"LLM调用失败: {str(e)}"}

    @staticmethod
    def _compact_text(text: str) -> str:
        """压缩文本"""
        text = re.sub(r'\n+', '\n', text)
        text = re.sub(r' +', ' ', text)
        return '\n'.join(line.strip() for line in text.split('\n'))


async def main():
    service = MatchCompanySubject(max_workers=5)
    await service.init()
    result = await service.match_company_college()
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    asyncio.run(main())