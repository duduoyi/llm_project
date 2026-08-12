"""
报告服务模块 - 实现异步高并发的企业年报MDA分析流程
工作流程：COS读取PDF → 提取MDA章节 → 保存MDA PDF → 转换为Markdown → 调用LLM解析 → 存储MongoDB
"""
from src.services.base_resource_service import BaseResourceService
from langchain_core.messages import SystemMessage, HumanMessage
from src.utils.logger import get_logger
from typing import List, Dict, Any
from datetime import datetime
import json
import re
import io
import os
from collections import defaultdict
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
import asyncio
from qcloud_cos import CosServiceError
import textwrap
import difflib


class CompanyReportServices(BaseResourceService):
    """报告服务类 - 封装校企匹配报告生成的核心逻辑（支持异步高并发）"""
    def __init__(self, max_workers: int = 3):
        """
        初始化服务类
        :param max_workers: 最大并发数，默认5
        """
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = max_workers
        # 修复6：分层独立信号量，隔离IO与LLM并发
        self.io_semaphore = asyncio.Semaphore(max_workers * 2)  # COS/PDF IO并发
        self.llm_semaphore = asyncio.Semaphore(max_workers)     # LLM调用并发

        # 分块阈值配置（两层分块分离，独立可调）
        self.pdf_memory_chunk_limit = 3500  # PDF解析内存分块
        self.llm_chunk_max_len = 100000  # LLM上下文输入分片

        # 初始化客户端（懒加载）
        self._mongo_collection = "company_info"
        self._bucket_name = "company-report-1366204382"
        self._llm_model = "qwen"

    # ==================== 核心异步工作流 ====================
    async def process_batch_async(
            self,
    ) -> dict:
        """
        异步批量处理：完整工作流程（读取→提取MDA→转换→解析→存储）
        :return: 处理结果汇总
        """
        try:
            self.logger.info(f"获取需解析的年报路径")
            pdf_files = await self._get_report_cos_path()

            self.logger.info(f"开始异步批量处理 | LLM最大并发: {self.max_workers * 2}")
            cos_client = await self.get_cos_client()

            self.logger.info(f"找到待处理文件: {len(pdf_files)} 个")

            if not pdf_files:
                return {"success": True, "message": "无待处理文件", "processed": 0, "failed": 0}

            # 2. 使用线程池并发处理
            results = defaultdict(list)

            # ============================
            # 单个文件完整处理流程（分层信号量）
            # ============================
            async def process_one_file(file_key):
                try:
                    source_prefix = file_key.split('/')[0]
                    file_name = file_key.split('/')[-1]
                    base_name = os.path.splitext(file_name)[0]
                    mda_pdf_key = f"{source_prefix}/MDA/{base_name}_mda.pdf"

                    self.logger.info(f"[MDA提取] {file_name}")

                    # 修复6：IO操作加IO信号量限流
                    async with self.io_semaphore:
                        extracted_key = await asyncio.wait_for(
                            asyncio.to_thread(
                                self._extract_and_save_mda,
                                cos_client,
                                self._bucket_name,
                                file_key,
                                mda_pdf_key
                            ),
                            timeout=180
                        )
                    if not extracted_key:
                        return {"success": False, "file_key": file_key, "error": "提取MDA失败"}

                    self.logger.info(f"[MDA解析] {mda_pdf_key}")
                    save_result = await self._analyze_and_save_mda(mda_pdf_key, file_key)
                    if save_result.get("success"):
                        self.logger.info(f"[成功] {mda_pdf_key}")
                        return {"success": True, "file_key": mda_pdf_key}
                    else:
                        return {"success": False, "file_key": mda_pdf_key, "error": "MDA解析失败"}

                except Exception as e:
                    self.logger.error(f"单个文件处理异常 file_key={file_key}, err={str(e)}", exc_info=True)
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

        except Exception as e:
            self.logger.error(f"MDA批量处理整体异常 err={str(e)}", exc_info=True)
            raise

    # ==================== 获取年报pdf列表 ====================
    async def _get_report_cos_path(self):
        mongo_db = await self.get_mongo_db()
        company_coll = mongo_db[self._mongo_collection]

        # ==========第一步：company_coll 查询未解析的年报==========
        report_filter = {
            "_metadata.source_file": {"$exists": True},
            # 暂时修正更新
            "企业基本信息.所属区县": "海门区"
            # "_metadata.mda_file": {"$exists": False}
        }
        # 投影：只返回cos路径
        report_proj = {
            "_id": 0,
            "_metadata.source_file": 1
        }

        report_list = []
        cursor = company_coll.find(report_filter, projection=report_proj).batch_size(1000)
        async for doc in cursor:
            report_path = doc["_metadata"]["source_file"]
            report_list.append(report_path)

        self.logger.info(f"从{company_coll}拿到 {len(report_list)} 个年报路径")
        return report_list


    # ==================== 提取并存储MDA.pdf ====================
    def _extract_and_save_mda(self, cos_client, bucket: str, origin_file_key: str, mda_file_key: str):
        """
        同步安全：提取MDA章节并保存为PDF
        :return: 保存后的文件key，失败返回None
        """
        # 判断MDA文件是否已存在
        try:
            cos_client.head_object(Bucket=bucket, Key=mda_file_key)
            self.logger.info(f"[提取跳过] {mda_file_key} 已存在")
            return mda_file_key
        except CosServiceError as e:
            if e.get_error_code() == "NoSuchResource":
                pass

        resp = cos_client.get_object(Bucket=bucket, Key=origin_file_key)
        pdf_data = resp['Body'].get_raw_stream().read()

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

        cos_client.put_object(
            Bucket=bucket,
            Key=mda_file_key,
            Body=output.read(),
            ContentType='application/pdf'
        )

        self.logger.info(f"MDA PDF已保存: {mda_file_key}")
        return mda_file_key


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
            mda_file: str,
            source_file: str
    ):
        """异步安全：将MDA PDF转换为Markdown格式"""
        try:
            # 2. PDF转Markdown
            cos_client = await self.get_cos_client()
            async with self.io_semaphore:
                markdown_content = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._convert_mda_to_markdown,
                        cos_client,
                        self._bucket_name,
                        mda_file
                    ),
                    timeout=120
                )

            if not markdown_content:
                return {"success": False, "message": "MD转换失败"}

            # 修复5：长文本分片处理
            md_chunks = self._split_markdown_chunks(markdown_content)
            llm_client = await self.get_llm_instance(self._llm_model)

            # 逐分片调用LLM，分层LLM信号量限流【改造：分片并发执行】
            tasks = []
            for idx, chunk in enumerate(md_chunks):
                self.logger.info(f"LLM分片解析 {mda_file} 第{idx + 1}/{len(md_chunks)}块")

                # 🔧 核心修改：task_func 只返回 chunk_res，不再返回元组
                async def task_func(text=chunk, chunk_idx=idx):
                    async with self.llm_semaphore:
                        try:
                            chunk_res = await asyncio.wait_for(
                                self._analyze_with_llm_async(llm_client, text, max_retries=2),
                                timeout=150
                            )
                        except asyncio.TimeoutError:
                            chunk_res = {"error": "LLM请求超时"}
                    return chunk_res  # ✅ 直接返回结果字典

                tasks.append(task_func())

            # 1. 并发执行（gather 保证返回顺序与 tasks 一致，无需手动排序）
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

            merged_result = None
            for idx, chunk_res in enumerate(chunk_results):
                # 2. 健壮的错误处理（防止单个分片异常导致整体崩溃）
                if isinstance(chunk_res, Exception):
                    return {
                        "success": False,
                        "file_key": mda_file,
                        "error": f"分片{idx + 1}LLM调用异常：{str(chunk_res)}"
                    }

                if isinstance(chunk_res, dict) and "error" in chunk_res:
                    return {
                        "success": False,
                        "file_key": mda_file,
                        "error": f"分片{idx + 1}LLM失败：{chunk_res['error']}"
                    }

                # 3. 执行智能合并
                if merged_result is None:
                    merged_result = chunk_res
                else:
                    merged_result = self._merge_chunk_llm_result(merged_result, chunk_res)

            # 🔧 防御性编程：确保 merged_result 一定是字典
            if not isinstance(merged_result, dict):
                self.logger.error(f"[类型异常] merged_result 类型为 {type(merged_result)}，强制重置为空字典")
                merged_result = {}

            company_info = merged_result

            # 5. 元数据
            company_info["_metadata"] = {
                "source_file": source_file,
                "mda_file": mda_file,
                "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "analysis_model": "qwen"
            }

            # 6. 保存MongoDB
            return await self._save_json_to_mongodb(company_info)
        except Exception as e:
            self.logger.error(f"MDA解析入库异常 mda_pdf_key={mda_file}, err={str(e)}", exc_info=True)
            raise

    def _convert_mda_to_markdown(self, cos_client, bucket: str, mda_pdf_key: str):
        resp = cos_client.get_object(Bucket=bucket, Key=mda_pdf_key)
        mda_pdf_data = resp['Body'].get_raw_stream().read()
        if len(mda_pdf_data) < 100:
            return None

        # 常量：PDF内存分块阈值（控制单块内存占用，和LLM分片阈值独立）
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
                        # 表头分隔线
                        header_row = table[0]
                        sep_line = "| " + " | ".join(["---"] * len(header_row)) + " |\n"
                        page_content += sep_line
                        for row in table:
                            row = [self._clean_text(str(c).replace("\n", " ").strip()) if c else "" for c in row]
                            page_content += "| " + " | ".join(row) + " |\n"

                text = page.extract_text()
                if text:
                    page_content += self._clean_text(text) + "\n"

                # ========== 第一层分块：PDF分页内存分块 ==========
                # 作用：防止单块页面文本过大占用大量内存，输出后拼接成完整全文MD
                if len(current_chunk) + len(page_content) > self.pdf_memory_chunk_limit:
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

    # ==================== 长文本分片工具（修复5） ====================
    def _split_markdown_chunks(self, md_content: str) -> List[str]:
        """将超长Markdown按字符阈值切分为多段，保证单段不超限"""
        chunks = []
        start_idx = 0
        content_len = len(md_content)
        chunk_overlap = 0 # RAG需要，长文本分块不需要
        while start_idx < content_len:
            end_idx = min(start_idx + self.llm_chunk_max_len, content_len)
            # 尽量在换行处截断，避免句子割裂
            if end_idx < content_len:
                newline_pos = md_content.rfind("\n", start_idx, end_idx)
                if newline_pos > start_idx:
                    end_idx = newline_pos
            chunk = md_content[start_idx:end_idx].strip()
            if chunk:
                chunks.append(chunk)

            # 核心改动：下一块起始位置 = 当前结尾 - 重叠长度
            next_start = end_idx - chunk_overlap
            # 防止原地卡死无限循环：至少向前推进一小段
            if next_start <= start_idx:
                next_start = start_idx + int(self.llm_chunk_max_len * 0.2)
            start_idx = next_start

        self.logger.info(f"Markdown文本分片完成，共 {len(chunks)} 块")
        return chunks

    def _merge_chunk_llm_result(self, base: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """
        智能合并两个分片的 JSON 提取结果
        规则：
        1. 字典类型：递归合并
        2. 列表类型：基于文本相似度去重后追加（防止语义重复）
        3. 基础类型：优先保留非空、非 null 的值
        """
        if not isinstance(new, dict):
            return base

        for key, new_val in new.items():
            if key not in base:
                base[key] = new_val
                continue

            base_val = base[key]

            # 1. 递归处理嵌套字典（如 "企业基本信息", "研发能力"）
            if isinstance(base_val, dict) and isinstance(new_val, dict):
                base[key] = self._merge_chunk_llm_result(base_val, new_val)

            # 2. 列表去重合并（核心防重复逻辑）
            elif isinstance(base_val, list) and isinstance(new_val, list):
                base[key] = self._deduplicate_list(base_val, new_val)

            # 3. 基础值覆盖（优先保留有实际内容的值）
            else:
                if base_val in [None, "", "未披露", "null"]:
                    base[key] = new_val
                # 如果 base 已经有值，保持 base 不变（避免被后序分片的 null 覆盖）

        return base

    @classmethod
    def _deduplicate_list(cls, base_list: List, new_list: List, threshold: float = 0.85) -> List:
        """
        基于文本相似度对列表进行去重合并
        threshold: 相似度阈值，超过此值视为重复
        """
        merged = base_list[:]

        for new_item in new_list:
            is_duplicate = False
            # 仅对字符串类型的列表项进行相似度比对
            if isinstance(new_item, str):
                for base_item in merged:
                    if isinstance(base_item, str):
                        ratio = difflib.SequenceMatcher(None, base_item, new_item).ratio()
                        if ratio >= threshold:
                            is_duplicate = True
                            # 如果新项更长/更完整，替换旧项
                            if len(new_item) > len(base_item):
                                merged[merged.index(base_item)] = new_item
                            break

            # 如果是字典列表（如 "产品结构"），可在此处扩展基于特定 Key 的去重逻辑
            # 这里简化处理：若完全相同则跳过，否则追加
            elif isinstance(new_item, dict):
                if new_item not in merged:
                    merged.append(new_item)
            else:
                if new_item not in merged:
                    merged.append(new_item)

            if not is_duplicate and new_item not in merged:
                merged.append(new_item)

        return merged

    # ==================== LLM调用 ====================
    async def _analyze_with_llm_async(self, llm_client, markdown_content: str, max_retries: int = 2) -> dict:
        """调用千问大模型进行MDA分析（带重试机制）"""
        if not markdown_content or len(markdown_content.strip()) < 50:
            self.logger.error(f"Markdown内容太短或为空，长度: {len(markdown_content) if markdown_content else 0}")
            return {"error": "Markdown内容太短或为空"}

        for attempt in range(max_retries):
            result_text = ""
            try:
                # 使用dedent去除字符串缩进，使多行字符串在代码中美观，但运行时不包含额外空格
                #
                clean_system_prompt = textwrap.dedent("""
                    你是资深企业技术情报分析师和产学研合作顾问。你的任务是从用户输入中提取产学研合作信息，并严格按照指定的JSON格式输出。
                    【核心原则】
                    1. 绝对忠实原文：数据必须严格来自报告原文，严禁编造、推测或补充外部知识。
                    2. 精确数值提取：数值型数据必须精确到报告所述，保留原始单位。
                    3. 缺失值处理：若原文未披露某项信息，该字段必须严格填写为 null，禁止填写"未披露"、"无"等文字，以保证JSON类型的一致性。
                    4. 产学研导向：重点关注并精准提取可与高校/科研院所结合的技术点、共建平台及联合攻关方向。
                    【输出格式硬性要求】
                    1. 仅输出合法的JSON对象，严禁包含任何Markdown标记（如 ```json 等）、前缀说明或后缀解释。
                    2. 所有字符串必须使用双引号，确保JSON语法绝对正确。
                    3. 严格遵循以下JSON结构，不得增删任何字段：
                    {
                      "企业基本信息": {
                        "所属领域": "string",
                        "核心方向": "string",
                        "技术定位": "string"
                      },
                      "主营构成（产学研合作方向）": {
                        "营业收入（万元）": "number | null",
                        "营业收入同比增长率": "number | null",
                        "营业成本（万元）": "number | null",
                        "毛利率": "number | null",
                        "产品结构": [
                          {
                            "产品类别": "string", 
                            "核心产品": ["string"], 
                            "营收权重": "string", 
                            "核心应用": ["string"]
                          }
                        ],
                        "成本上升原因": ["string"]
                      },
                      "核心技术能力（产学研合作核心）": {
                        "共性关键技术平台": ["string"],
                        "产品核心技术": ["string"],
                        "技术优势": ["string"],
                        "研发能力": {
                          "研发投入占比": "number | null",
                          "研发人员数": "number | null",
                          "累计授权专利": "number | null",
                          "在研攻关方向": ["string"]
                        }
                      },
                      "产业链与创新资源": {
                        "上游支撑资源": ["string"],
                        "中游制造能力": ["string"],
                        "下游需求场景": ["string"]
                      },
                      "研发机构与平台": {
                        "内部研发平台": "string | null",
                        "核心能力": "string | null",
                        "可开放资源": ["string"]
                      },
                      "合作需求与方向": {
                        "可联合攻关方向": ["string"],
                        "可承接成果转化": "string | null",
                        "可共建平台": "string | null"
                      },
                      "数据来源与校验": "string"
                    }
                """)

                clean_user_prompt = textwrap.dedent(f'''
                    请仔细阅读以下MDA年报内容，严格按照系统提示词中的JSON格式返回。
                    {markdown_content}
                    ''')

                messages = [
                    SystemMessage(content=clean_system_prompt),
                    HumanMessage(content=clean_user_prompt)
                ]

                self.logger.info(f"正在调用千问大模型... (第{attempt + 1}次尝试)")
                # 单次启用JSON输出
                response = await llm_client.ainvoke(messages, response_format={"type": "json_object"})
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


    # ==================== MongoDB存储 ====================
    async def _save_json_to_mongodb(
            self,
            json_data: dict
    ) -> dict:
        """
        存储JSON到MongoDB
        唯一键固定为 _id
        只更新/新增字段，旧字段 100% 保留
        特别处理：企业基本信息 只新增不存在的键，不覆盖已有键
        """
        mda_file = json_data["_metadata"]["mda_file"]
        source_file = json_data["_metadata"]["source_file"]

        try:
            mongo_db = await self.get_mongo_db()
            collection = mongo_db[self._mongo_collection]
            filter_stock = {"_metadata.source_file": source_file}

            # 查询是否存在该企业
            exist_doc = await collection.find_one(filter_stock)
            if not exist_doc:
                msg = f"未匹配到企业年报路径【{source_file}】存量文档，跳过入库"
                self.logger.warning(msg)
                return {"success": True, "skip": True, "message": msg, "source_file": source_file}

            self.logger.info(f"匹配到存量企业【{source_file}】，执行增量更新")
            # 拆分企业基本信息，打点更新，只新增不覆盖原有字段
            new_company_base = json_data.pop("企业基本信息", {})
            update_set = {}
            # 企业基本信息子字段打点更新，不会覆盖库内原有key
            for key, value in new_company_base.items():
                update_set[f"企业基本信息.{key}"] = value

            # 剩余顶层业务字段正常覆盖更新
            update_set.update(json_data)

            # upsert=False 无匹配不会新增，这里必然匹配到文档
            result = await collection.update_one(
                filter_stock,
                {"$set": update_set},
                upsert=False
            )

            return {
                "success": True,
                "skip": False,
                "matched": result.matched_count,
                "modified": result.modified_count,
                "source_file": source_file,
                "mda_file": mda_file,
                "message": "存量企业增量更新完成"
            }

        except Exception as e:
            err_msg = f"Mongo入库异常 source_file={source_file}, mda={json_data.get('_metadata', {}).get('mda_file')}, err={str(e)}"
            self.logger.error(err_msg, exc_info=True)
            return {"success": False, "error": err_msg}



# ==================== 测试入口 ==========================
async def main():
    service = CompanyReportServices()
    await service.process_batch_async()

if __name__ == '__main__':
    asyncio.run(main())