"""
报告服务模块 - 实现异步高并发的企业年报MDA分析流程
工作流程：COS读取PDF → 提取MDA章节 → 保存MDA PDF → 转换为Markdown → 调用LLM解析 → 存储MongoDB
"""
from src.utils.singletons.database_singleton import MongoSingleton, CosSingleton
from src.utils.singletons.ai_singleton import LLMFactory
from langchain_core.messages import SystemMessage, HumanMessage
from src.utils.logger import get_logger
from typing import List
from datetime import datetime
import json
import re
import io
import os
from collections import defaultdict
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
import asyncio


class CompanyReportServices:
    """报告服务类 - 封装校企匹配报告生成的核心逻辑（支持异步高并发）"""

    def __init__(self, max_workers: int = 5):
        """
        初始化服务类
        :param max_workers: 最大并发数，默认5
        """
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = max_workers

        # 初始化客户端（懒加载）
        self._mongo_client = MongoSingleton.get_instance()
        self._llm_client = LLMFactory.get_instance('qwen')
        self._cos_client = CosSingleton.get_instance()

    # ==================== 核心异步工作流 ====================
    async def process_batch_async(
            self,
            bucket_name: str = "company-report-1366204382",
            source_prefix: str = "2025年/",
            mda_prefix: str = "2025年/MDA/",
            collection_name: str = "company_info",
            db_name: str = "dquot"
    ) -> dict:
        """
        异步批量处理：完整工作流程（读取→提取MDA→转换→解析→存储）
        :return: 处理结果汇总
        """
        self.logger.info(f"开始异步批量处理 | LLM最大并发: {self.max_workers} | 源目录: {source_prefix}")

        # ========== 步骤1：获取文件（线程池执行） ==========
        pdf_files = await asyncio.to_thread(
            self._get_company_report_list, bucket_name, source_prefix
        )
        self.logger.info(f"找到待处理文件: {len(pdf_files)} 个")

        if not pdf_files:
            return {"success": True, "message": "无待处理文件", "processed": 0, "failed": 0}

        # 2. 使用线程池并发处理
        results = defaultdict(list)
        # 核心：LLM并发控制器（信号量）
        semaphore = asyncio.Semaphore(self.max_workers)

        # ============================
        # 单个文件完整处理流程
        # ============================
        async def process_one_file(file_key):
            async with semaphore:
                try:
                    file_name = file_key.split('/')[-1]
                    base_name = os.path.splitext(file_name)[0]
                    mda_pdf_key = f"{mda_prefix}{base_name}_mda.pdf"

                    self.logger.info(f"[MDA提取] {file_name}")

                    extracted_key = await self._extract_and_save_mda(mda_pdf_key, bucket_name)
                    if not extracted_key:
                        return {"success": False, "file_key": file_key, "error": "提取MDA失败"}

                    self.logger.info(f"[MDA解析] {mda_pdf_key}")
                    save_result = await self._analyze_and_save_mda(mda_pdf_key, bucket_name, collection_name, db_name)
                    if save_result.get("success"):
                        self.logger.info(f"[成功] {mda_pdf_key}")
                        return {"success": True, "file_key": mda_pdf_key}
                    else:
                        return {"success": False, "file_key": mda_pdf_key, "error": "MDA解析失败"}

                except Exception as e:
                    self.logger.error(f"[异常] {file_key}: {str(e)}")
                    return {"success": False, "file_key": file_key, "error": str(e)}

        # 执行所有任务
        tasks = [process_one_file(file_key) for file_key in pdf_files]
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
            "total": len(pdf_files),
            "processed": len(results["success"]),
            "failed": len(results["failed"]),
            "details": results
        }

        self.logger.info(f"✅ 处理完成 | 成功:{summary['processed']} | 失败:{summary['failed']}")
        return summary

    # ==================== 获取年报pdf列表 ====================
    def _get_company_report_list(self, bucket_name: str, prefix: str) -> List[str]:
        """从COS获取文件列表"""
        pdf_files = []
        marker = ""

        while True:
            response = self._cos_client.list_objects(Bucket=bucket_name, Prefix=prefix, Marker=marker)
            contents = response.get('Contents', [])
            if not contents:
                break

            for item in contents:
                key = item['Key']
                if key.lower().endswith('.pdf') and '_mda' not in key:
                    pdf_files.append(key)

            if response.get('IsTruncated') == 'true':
                marker = response.get('NextMarker', '')
            else:
                break

        return pdf_files

    # ==================== 提取并存储MDA.pdf ====================
    async def _extract_and_save_mda(
            self,
            mda_file_key: str,
            bucket_name: str,
    ):
        """
        异步安全：提取MDA章节并保存为PDF
        :return: 保存后的文件key，失败返回None
        """
        def _extract_and_save_mda_sync():
            # 检查文件是否已存在，存在则直接返回
            try:
                self._cos_client.head_object(Bucket=bucket_name, Key=mda_file_key)
                self.logger.info(f"[提取跳过] {mda_file_key} 已存在")
                return mda_file_key
            except FileNotFoundError:
                # 文件不存在，继续生成
                pass

            response = self._cos_client.get_object(Bucket=bucket_name, Key=mda_file_key)
            pdf_data = response['Body'].get_raw_stream().read()
            
            toc_text = self._find_toc_page(pdf_data)
            if not toc_text:
                self.logger.warning(f"未找到目录页: {mda_file_key}")
                return None

            page_range = self._parse_mda_page_range(toc_text)
            if not page_range or page_range[0] is None:
                self.logger.warning(f"未找到MDA章节: {mda_file_key}")
                return None

            start_page, end_page = page_range

            # 提取指定页面
            reader = PdfReader(io.BytesIO(pdf_data))
            writer = PdfWriter()

            # 边界检查（处理 end_page 为 None 的情况）
            start_page = max(0, min(start_page, len(reader.pages) - 1))
            if end_page is None:
                end_page = len(reader.pages) - 1
            else:
                end_page = max(start_page, min(end_page, len(reader.pages) - 1))

            for pg in range(start_page, end_page + 1):
                writer.add_page(reader.pages[pg])

            # 保存到COS
            output = io.BytesIO()
            writer.write(output)
            output.seek(0)

            self._cos_client.put_object(
                Bucket=bucket_name,
                Key=mda_file_key,
                Body=output.read(),
                ContentType='application/pdf'
            )

            self.logger.info(f"MDA PDF已保存: {mda_file_key}")
            return mda_file_key

        return await asyncio.to_thread(
            _extract_and_save_mda_sync
        )

    @staticmethod
    def _find_toc_page(pdf_data: bytes) -> str:
        """查找目录页（支持跨页目录）"""
        toc_keywords = ["目录", "目 录", "CONTENTS", "索引"]
        end_keywords = ["公司治理", "重要事项", "重大事件", "股份变动"]
        max_check_pages = 10
        toc_text = ""
        found_toc = False

        with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
            for i, page in enumerate(pdf.pages[:max_check_pages]):
                text = page.extract_text() or ""

                if any(kw in text for kw in toc_keywords):
                    found_toc = True

                if found_toc:
                    toc_text += text + "\n"

                    # 检查是否已找到结束页的关键词
                    if any(kw in text for kw in end_keywords):
                        break

                    # 检查页码是否已进入正文区域（通常目录页页码在底部，且前后页码连续）
                    page_num_pattern = re.compile(r'^第\s*(\d+)\s*页')
                    if page_num_pattern.search(text):
                        break

        return toc_text.strip() if toc_text.strip() else ""

    @staticmethod
    def _parse_mda_page_range(toc_text: str):
        """解析MDA页码范围"""
        start_keywords = ["管理层讨论与分析", "经营情况讨论与分析"]
        end_keywords = ["公司治理", "重要事项", "重大事件", "股份变动"]

        page_pattern = re.compile(r'(.*?)[\s.…\-_]*(\d+)$')

        start_page = None
        end_page = None
        found_start = False

        for line in toc_text.split('\n'):
            line = line.strip()
            if not line:
                continue

            match = page_pattern.search(line)
            if not match:
                continue

            title = match.group(1).strip()
            page_num = int(match.group(2))
            page_index = page_num - 1

            if not found_start:
                for kw in start_keywords:
                    if kw in title:
                        start_page = page_index
                        found_start = True
                        break

            if found_start and end_page is None:
                for kw in end_keywords:
                    if kw in title:
                        end_page = page_index - 1
                        break

                if end_page is not None:
                    break

        return start_page, end_page

    # ==================== 大模型分析并存储 MDA ====================
    async def _analyze_and_save_mda(
            self,
            mda_pdf_key: str,
            bucket_name: str,
            collection_name: str,
            db_name: str
    ):
        """异步安全：将MDA PDF转换为Markdown格式"""
        # pymongo 同步查询，丢线程池执行
        exists = await asyncio.to_thread(
            self._check_mongodb_exists, collection_name, db_name, mda_pdf_key
        )
        if exists:
            self.logger.info(f"[解析跳过] {mda_pdf_key} 已入库")
            return {"success": True}

        markdown_content = await asyncio.to_thread(self._convert_mda_to_markdown, mda_pdf_key, bucket_name)

        if not markdown_content:
            return {"success": False, "message": "MD转换失败"}

        self.logger.info(f"[LLM解析] {mda_pdf_key}")
        company_info = await asyncio.to_thread(self._analyze_with_llm, markdown_content, 2)
        if not company_info or "error" in company_info:
            return {"success": False, "file_key": mda_pdf_key, "error": company_info.get("error", "LLM解析失败")}

        # 5. 元数据
        company_info["_metadata"] = {
            "mda_file": mda_pdf_key,
            "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "analysis_model": "qwen"
        }

        # 6. 保存MongoDB
        # pymongo 同步存储，丢线程池
        return await asyncio.to_thread(
            self._save_json_to_mongodb, company_info, collection_name, db_name
        )

    def _check_mongodb_exists(self, collection_name: str, db_name: str, mda_file_key: str) -> bool:
        """
        异步安全版：检查MongoDB记录是否存在
        """
        try:
            collection = self._mongo_client[db_name][collection_name]

            record = collection.find_one({
                "_metadata.mda_file": mda_file_key
            })
            return record is not None

        except Exception as e:
            self.logger.warning(f"检查MongoDB记录失败: {e}")
            return False

    def _convert_mda_to_markdown(self, mda_pdf_key: str, bucket_name: str):
        response = self._cos_client.get_object(Bucket=bucket_name, Key=mda_pdf_key)
        mda_pdf_data = response['Body'].get_raw_stream().read()
        if len(mda_pdf_data) < 100:
            return None

        chunks = []
        current_chunk = ""

        with pdfplumber.open(io.BytesIO(mda_pdf_data)) as pdf:
            for i, page in enumerate(pdf.pages):
                page_content = f"\n--- 第 {i + 1} 页 ---\n"

                # 提取表格
                tables = page.extract_tables(table_settings={
                    "vertical_strategy": "text",
                    "horizontal_strategy": "text",
                    "intersection_tolerance": 3,
                    "snap_tolerance": 3,
                })

                if tables and any(t for t in tables if self._validate_table(t)):
                    for t_idx, table in enumerate(tables):
                        if not self._validate_table(table):
                            continue
                        page_content += f"\n【表格 {t_idx + 1}】\n"
                        for row in table:
                            row = [self._clean_text(str(c).replace("\n", " ").strip()) if c else "" for c in row]
                            page_content += "| " + " | ".join(row) + " |\n"

                # 提取文本
                text = page.extract_text()
                if text:
                    page_content += self._clean_text(text) + "\n"

                # 分块
                if len(current_chunk) + len(page_content) > 3500:
                    chunks.append(current_chunk)
                    current_chunk = page_content
                else:
                    current_chunk += page_content

            if current_chunk.strip():
                chunks.append(current_chunk)

        return "\n".join(chunks) if chunks else None


    @staticmethod
    def _clean_text(text: str) -> str:
        """清理乱码字符"""
        if not text:
            return ""
        replacements = {'□': '', '▯': '', '■': '', '�': '', '\x00': ''}
        for old, new in replacements.items():
            text = text.replace(old, new)
        return re.sub(r'\s+', ' ', text).strip()

    @staticmethod
    def _validate_table(table: list) -> bool:
        """验证表格有效性"""
        if not table:
            return False
        total_chars = sum(len(str(c)) for row in table for c in row if c)
        garbled = sum(len(re.findall(r'[□▯■�]', str(c))) for row in table for c in row if c)
        return total_chars > 5 and (garbled / total_chars < 0.2 if total_chars > 0 else True)

    # ==================== LLM调用 ====================
    def _analyze_with_llm(self, markdown_content: str, max_retries: int = 2) -> dict:
        """调用千问大模型进行MDA分析（带重试机制）"""
        if not markdown_content or len(markdown_content.strip()) < 50:
            self.logger.error(f"Markdown内容太短或为空，长度: {len(markdown_content) if markdown_content else 0}")
            return {"error": "Markdown内容太短或为空"}

        for attempt in range(max_retries):
            result_text = ""
            try:
                system_prompt = """
                    你是资深企业技术情报分析师和产学研合作顾问，从年报MDA章节深度挖掘产学研合作信息。
                    【核心原则】
                    1. 数据必须严格来自报告原文，不允许编造
                    2. 数值型数据必须精确到报告所述
                    3. 未披露信息填写"未披露"
                    4. 重点关注可与高校/科研院所结合的技术点
                """
                clean_system_prompt = self._compact_text(system_prompt)

                user_prompt = f'''
                    请仔细阅读以下MDA年报内容，严格按照JSON格式输出分析结果。只返回JSON，不要其他文字。
                    === 内容 ===
                    {markdown_content}
                    === JSON格式 ===
                    {{
                      "企业基本信息": {{
                        "所属领域": "主要技术/业务领域",
                        "核心方向": "企业核心技术和业务方向",
                        "技术定位": "在行业中的技术地位"
                      }},
                      "主营构成（产学研合作方向）": {{
                        "营业收入（万元）": 数值(万元),
                        "营业收入同比增长率": 数值,
                        "营业成本（万元）": 数值(万元),
                        "毛利率": 数值,
                        "产品结构": [{{"产品类别": "名称", "核心产品": ["产品1"], "营收权重": "主力/重要", "核心应用": ["应用场景"]}}],
                        "成本上升原因": ["原因"]
                      }},
                      "核心技术能力（产学研合作核心）": {{
                        "共性关键技术平台": ["技术1"],
                        "产品核心技术": ["技术1"],
                        "技术优势": ["优势1"],
                        "研发能力": {{
                          "研发投入占比": 数值,
                          "研发人员数": 人数,
                          "累计授权专利": 数量,
                          "在研攻关方向": ["方向1"]
                        }}
                      }},
                      "产业链与创新资源": {{
                        "上游支撑资源": ["资源1"],
                        "中游制造能力": ["能力1"],
                        "下游需求场景": ["场景1"]
                      }},
                      "研发机构与平台": {{
                        "内部研发平台": "平台名称",
                        "核心能力": "描述",
                        "可开放资源": ["资源"]
                      }},
                      "合作需求与方向": {{
                        "可联合攻关方向": ["方向1"],
                        "可承接成果转化": "描述",
                        "可共建平台": "类型"
                      }},
                      "数据来源与校验": "来源说明"
                    }}
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

                result_text = response.content.strip()
                self.logger.info(f"LLM响应长度: {len(result_text)} 字符")

                if not result_text:
                    self.logger.error("LLM返回了空内容")
                    if attempt == max_retries - 1:
                        return {"error": "LLM返回空内容"}
                    continue

                # 解析JSON
                json_str = result_text
                if "```json" in result_text:
                    json_str = result_text.split("```json")[1].split("```")[0].strip()
                elif "```" in result_text:
                    json_str = result_text.split("```")[1].split("```")[0].strip()

                self.logger.info("JSON解析成功")
                return json.loads(json_str)

            except TimeoutError:
                self.logger.warning(f"LLM调用超时（300秒），重试剩余次数: {max_retries - attempt - 1}")
                if attempt == max_retries - 1:
                    return {"error": "LLM调用超时（已重试）"}
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {e}, 原始内容: {result_text[:500] if 'result_text' in dir() else 'N/A'}")
                if attempt == max_retries - 1:
                    return {"error": f"JSON解析失败: {str(e)}"}
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

    # ==================== MongoDB存储 ====================
    def _save_json_to_mongodb(
            self,
            json_data: dict,
            collection_name: str,
            db_name: str = 'dquot'
    ) -> dict:
        """
        存储JSON到MongoDB
        唯一键固定为 _id
        只更新/新增字段，旧字段 100% 保留
        特别处理：企业基本信息 只新增不存在的键，不覆盖已有键
        """
        try:
            collection = self._mongo_client[db_name][collection_name]

            # 固定使用 _id 作为唯一键
            if "_id" in json_data:
                # ==============================================
                # 核心修改：拆分更新逻辑，不覆盖企业基本信息原有字段
                # ==============================================
                update_query = {}

                # 1. 提取 企业基本信息（如果有），使用 $set 合并，不会覆盖原有键
                company_info = json_data.pop("企业基本信息", None)
                if company_info and isinstance(company_info, dict):
                    for key, value in company_info.items():
                        update_query[f"企业基本信息.{key}"] = value

                # 2. 剩余顶层字段正常更新（不包含企业基本信息）
                if json_data:
                    for key, value in json_data.items():
                        update_query[key] = value

                # 执行更新：只新增/更新，不覆盖旧字段
                result = collection.update_one(
                    {"_id": json_data["_id"]},  # 按 _id 匹配
                    {"$set": update_query},  # 只更新需要的字段
                    upsert=True  # 不存在则新增
                )

                return {
                    "success": True,
                    "message": "Upsert成功（企业基本信息只新增字段，不覆盖旧字段）",
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                    "upserted_id": str(result.upserted_id) if result.upserted_id else None
                }
            else:
                # 无 _id 时直接插入
                result = collection.insert_one(json_data)
                return {"success": True, "message": "插入成功", "inserted_id": str(result.inserted_id)}

        except Exception as e:
            self.logger.error(f"MongoDB存储失败: {e}")
            return {"success": False, "message": f"存储失败: {str(e)}"}



if __name__ == "__main__":
    service = CompanyReportServices(max_workers=5)
    # asyncio.run(service.process_batch_async())
