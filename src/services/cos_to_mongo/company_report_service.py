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


class CompanyReportServices(BaseResourceService):
    """报告服务类 - 封装校企匹配报告生成的核心逻辑（支持异步高并发）"""
    def __init__(self, max_workers: int = 5):
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
        self.llm_chunk_max_len = 3200  # LLM上下文输入分片

        # 初始化客户端（懒加载）
        self._mongo_collection = "company_info"
        self._bucket_name = "company-report-1366204382"
        self._llm_model = "qwen"

    # ==================== 核心异步工作流 ====================
    async def process_batch_async(
            self,
            source_prefix: str = "2025年/"
    ) -> dict:
        """
        异步批量处理：完整工作流程（读取→提取MDA→转换→解析→存储）
        :return: 处理结果汇总
        """
        try:
            self.logger.info(f"开始异步批量处理 | LLM最大并发: {self.max_workers * 2} | 源目录: {source_prefix}")
            cos_client = await self.get_cos_client()

            # ========== 步骤1：获取文件（线程池执行，增加超时） ==========
            pdf_files = await asyncio.wait_for(
                asyncio.to_thread(
                    self._get_company_report_list,
                    cos_client,
                    self._bucket_name,
                    source_prefix
                ),
                timeout=120
            )
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
                    file_name = file_key.split('/')[-1]
                    base_name = os.path.splitext(file_name)[0]
                    # 兼容中英文冒号截取证券简称
                    stock_short_name = base_name.split(":", 1)[0] if ":" in base_name else (
                        base_name.split("：", 1)[0] if "：" in base_name else base_name
                    )
                    mda_pdf_key = f"{source_prefix}MDA/{base_name}_mda.pdf"

                    self.logger.info(f"[MDA提取] {file_name} | 证券简称：{stock_short_name}")

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
                    save_result = await self._analyze_and_save_mda(mda_pdf_key, stock_short_name)
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
            self.logger.error(f"MDA批量处理整体异常 source_prefix={source_prefix}, err={str(e)}", exc_info=True)
            raise

    # ==================== 获取年报pdf列表 ====================
    @staticmethod
    def _get_company_report_list(cos_client, bucket: str, prefix: str) -> List[str]:
        pdf_files = []
        marker = ""
        while True:
            resp = cos_client.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
            contents = resp.get('Contents', [])
            if not contents:
                break
            for item in contents:
                key = item['Key']
                if key.lower().endswith('.pdf') and '_mda' not in key:
                    pdf_files.append(key)
            if resp.get('IsTruncated') == 'true':
                marker = resp.get('NextMarker', '')
            else:
                break
        return pdf_files

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
        except cos_client.exceptions.NoSuchKey:
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
            mda_pdf_key: str,
            stock_short_name: str
    ):
        """异步安全：将MDA PDF转换为Markdown格式"""
        try:
            mongo_db = await self.get_mongo_db()
            coll = mongo_db[self._mongo_collection]
            # 1. 判断是否已入库
            record = await coll.find_one({"_metadata.mda_file": mda_pdf_key})
            if record:
                self.logger.info(f"[解析跳过] {mda_pdf_key} 已入库")
                return {"success": True}

            # 2. PDF转Markdown
            cos_client = await self.get_cos_client()
            async with self.io_semaphore:
                markdown_content = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._convert_mda_to_markdown,
                        cos_client,
                        self._bucket_name,
                        mda_pdf_key
                    ),
                    timeout=120
                )

            if not markdown_content:
                return {"success": False, "message": "MD转换失败"}

            # 修复5：长文本分片处理
            md_chunks = self._split_markdown_chunks(markdown_content)
            llm_client = await self.get_llm_instance(self._llm_model)
            merged_result = None

            # 逐分片调用LLM，分层LLM信号量限流
            for idx, chunk in enumerate(md_chunks):
                self.logger.info(f"LLM分片解析 {mda_pdf_key} 第{idx+1}/{len(md_chunks)}块")
                async with self.llm_semaphore:
                    chunk_res = await self._analyze_with_llm_async(llm_client, chunk, max_retries=2)
                if "error" in chunk_res:
                    return {
                        "success": False,
                        "file_key": mda_pdf_key,
                        "error": f"分片{idx+1}LLM失败：{chunk_res['error']}"
                    }
                # 合并分片结果
                if merged_result is None:
                    merged_result = chunk_res
                else:
                    merged_result = self._merge_chunk_llm_result(merged_result, chunk_res)


            company_info =  merged_result

            # 把文件名截取的证券简称写入企业基本信息
            if "企业基本信息" not in company_info:
                company_info["企业基本信息"] = {}
            company_info["企业基本信息"]["证券简称"] = stock_short_name

            # 5. 元数据
            company_info["_metadata"] = {
                "mda_file": mda_pdf_key,
                "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "analysis_model": "qwen"
            }

            # 6. 保存MongoDB
            return await self._save_json_to_mongodb(company_info)
        except Exception as e:
            self.logger.error(f"MDA解析入库异常 mda_pdf_key={mda_pdf_key}, err={str(e)}", exc_info=True)
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
            start_idx = end_idx
        self.logger.info(f"Markdown文本分片完成，共 {len(chunks)} 块")
        return chunks

    def _merge_chunk_llm_result(self, base_data: Dict[str, Any], new_chunk_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        合并多段LLM分片结果：存量字段保留，新分片补充数据
        规则：
        1. 列表字段取并集去重
        2. 基础字符串字段：原数据为"未披露"则覆盖，否则保留原值
        3. 嵌套字典递归合并
        """
        merged = base_data.copy()
        for k, v in new_chunk_data.items():
            if k not in merged:
                merged[k] = v
                continue
            origin_val = merged[k]
            # 嵌套字典递归合并
            if isinstance(origin_val, dict) and isinstance(v, dict):
                merged[k] = self._merge_chunk_llm_result(origin_val, v)
            # 列表字段合并去重
            elif isinstance(origin_val, list) and isinstance(v, list):
                combine_list = origin_val + v
                merged[k] = list({json.dumps(item, ensure_ascii=False) for item in combine_list})
                merged[k] = [json.loads(s) for s in merged[k]]
            # 普通文本：原数据未披露则更新
            elif isinstance(origin_val, str) and origin_val == "未披露":
                merged[k] = v
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
                response = await llm_client.ainvoke(messages)
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
        mda_file_key = json_data["_metadata"]["mda_file"]
        stock_short_name = json_data["企业基本信息"]["证券简称"]
        try:
            mongo_db = await self.get_mongo_db()
            collection = mongo_db[self._mongo_collection]
            filter_stock = {"企业基本信息.证券简称": stock_short_name}

            # 查询是否存在该证券简称的企业
            exist_doc = await collection.find_one(filter_stock)
            if not exist_doc:
                msg = f"未匹配到证券简称【{stock_short_name}】存量文档，跳过入库"
                self.logger.warning(msg)
                return {"success": True, "skip": True, "message": msg, "stock_short_name": stock_short_name}

            self.logger.info(f"匹配到存量企业【{stock_short_name}】，执行增量更新")
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
                "stock_short_name": stock_short_name,
                "mda_file": mda_file_key,
                "message": "存量企业增量更新完成"
            }

        except Exception as e:
            err_msg = f"Mongo入库异常 证券简称={stock_short_name}, mda={json_data.get('_metadata', {}).get('mda_file')}, err={str(e)}"
            self.logger.error(err_msg, exc_info=True)
            return {"success": False, "error": err_msg}



# # 测试入口
# async def main():
#     cfg = {"start_time": "2025-01-01 00:00:00", "end_time": "2025-12-31 00:00:00", "batch_size": 100}
#     service = CompanyReportServices(cfg, max_workers=3)
#     await service.process_batch_async(source_prefix="2025年/")
#
# if __name__ == '__main__':
#     asyncio.run(main())