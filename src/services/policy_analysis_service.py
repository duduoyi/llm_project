"""
报告服务模块 - 实现同步政策解析
工作流程：读取政策文件 → 转换为json →存入MongoDB
"""
import asyncio
from src.utils.singletons.database_singleton import AsyncMongoSingleton
from src.utils.logger import get_logger
from pathlib import Path
import re
import os


class PolicyAnalysisServices:
    """政策解析服务类 - 封装政策文件解析结果"""
    # 全局Mongo写入互斥锁
    _mongo_write_lock = asyncio.Lock()

    def __init__(self):
        """
        初始化服务类
        """
        self.logger = get_logger(self.__class__.__name__)
        self.project_root = Path(__file__).parent.parent.parent
        self.policy_folder = self.project_root / "files" / "policy"
        # 初始化客户端
        self._mongo_client = None
        self.db_name = "dquot"
        self.policy_collection_name = "policy"
        self.attachments_collection_name = "attachments"

    async def init(self):
        """异步初始化，在 lifespan / 任务启动时调用"""
        self._mongo_client = await AsyncMongoSingleton.get_instance()
        self.logger.info("✅ Async Mongo 客户端初始化完成")

    async def policy_to_mongodb(self, file_name=None):
        # 1. 获取文件列表（根据 file_name 判断）
        if file_name is not None:
            # 有指定文件名 → 只读取这一个文件
            file_path_list = [self.policy_folder / file_name]  # 拼接完整路径
        else:
            # 无指定文件名 → 遍历文件夹所有 xlsx
            file_path_list = await asyncio.to_thread(
                self._get_files_list,
                folder_path=self.policy_folder,
                file_format=".md"
            )

        self.logger.info(f"文件列表: {file_path_list}")

        # 2. 读取文件内容
        policy_list = []
        for file_path in file_path_list:
            content = await asyncio.to_thread(self._read_markdown_file, file_path)
            policy_list.append(content)
        # 3. 格式转换
        update_data = []
        for md_text in policy_list:
            block_dicts = await asyncio.to_thread(self._split_markdown_to_json, md_text)
            update_data.extend(block_dicts)
        # 4. 存入MongoDB
        async with self._mongo_write_lock:
            result = await self._save_json_to_mongodb(
                update_data,
                collection_name=self.policy_collection_name,
                db_name=self.db_name
            )
        self.logger.info(f"✅ 完成同步：{result} 条数据")
        return result

    async def policy_attachments_to_mongodb(self):
        """同步政策附件到MongoDB"""
        async with self._mongo_write_lock:
            # 读取政策集合附件数据
            policy_attachment_list = await self.get_policy_attachments(
                collection_name=self.policy_collection_name,
                db_name=self.db_name
            )
            # 结构转换
            clear_attachment_list = await asyncio.to_thread(
                self.transform_policy_attachments,
                policy_attachment_list
            )
            # 附件写入
            result = await self._save_json_to_mongodb(
                clear_attachment_list,
                collection_name=self.attachments_collection_name,
                db_name=self.db_name
            )
        self.logger.info(f"✅ 政策附件同步完成：{result}")
        return result

    @staticmethod
    def _get_files_list(folder_path, file_format: tuple | str):
        """读取文件夹下指定格式文件"""
        file_list = [f for f in os.listdir(folder_path) if f.lower().endswith(file_format)]

        file_path_list = []
        for filename in file_list:
            file_path = os.path.join(folder_path, filename)
            file_path_list.append(file_path)

        return file_path_list

    @staticmethod
    def _read_markdown_file(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()

        # 1. 移除所有制表符
        text = text.replace("\t", "")
        # 2. 【关键】只删除「行内所有半角/全角空格」，保留换行符 \n
        text = re.sub(r'[ \u3000]', '', text)
        # 3. 合并连续空行，只保留单个换行，保证分行结构正常
        text = re.sub(r'\n+', '\n', text)
        # 4. 去除首尾空白
        return text.strip()

    @staticmethod
    def _split_markdown_to_json(content):
        # 按 ### 分割成独立模块
        block_list = re.split(r'(?=###)', content)
        # 匹配markdown链接 [title](url)
        link_pattern = re.compile(r'^\[(.*?)]\((.*?)\)$')
        # 匹配批次+截止时间：批次：第一批，截止时间：2026-04-30
        batch_pattern = re.compile(
            r'批次[:：](.*?)(?:[,，]开始时间[:：](\d{4}-\d{2}-\d{2}))?[,，]截止时间[:：](\d{4}-\d{2}-\d{2})'
        )

        result = []

        # 字段映射前缀
        field_map = {
            "-政策归属：": "department",
            "-政策等级：": "level",
            "-面向对象：": "target",
            "-奖励：": "reward",
            "-申报流程：": "process",
            "-政策来源：": "sourceUrl"
        }


        for block in block_list:
            block = block.strip()
            if not block:
                continue

            # 按换行分割每一行
            lines = block.split("\n")

            # 单条数据结构初始化
            data = {
                "name": lines[0].replace("###", "").strip(),
                "department": "",
                "level": "",
                "target": "",
                "reward": "",
                "applyTime": [],
                "conditions": "",
                "process": "",
                "sourceUrl": [],
                "status": "active",
                "sourceFile":[]
            }

            current_list = None
            conditions_buffer = []  # 临时存储申报条件的多条内容
            in_apply_time = False  # 标记是否进入申报时间子项

            for line in lines[1:]:
                # 匹配常规字段
                matched = False
                for prefix, field in field_map.items():
                    if line.startswith(prefix):
                        raw_val = line.replace(prefix, "").strip()
                        # 单独处理 sourceUrl 字段，解析markdown链接
                        if field == "sourceUrl":
                            match = link_pattern.match(raw_val)
                            if match:
                                title_text = match.group(1).strip()
                                url_text = match.group(2).strip()
                                data["sourceUrl"].append({
                                    "title": title_text,
                                    "url": url_text
                                })
                            else:
                                # 不是标准markdown链接，title存原文，url空
                                data["sourceUrl"].append({
                                    "title": raw_val,
                                    "url": ""
                                })
                        else:
                            # 普通字段直接赋值字符串
                            data[field] = raw_val
                        current_list = None
                        in_apply_time = False
                        matched = True
                        break
                if matched:
                    continue

                # 进入申报时间区块
                if line == "-申报时间：":
                    in_apply_time = True
                    current_list = None
                    continue

                # 切换到 申报条件 列表
                if re.match(r'^-申报条件[:：]$', line):
                    current_list = "conditions"
                    conditions_buffer = []
                    in_apply_time = False
                    continue

                # 切换到 政策文件 列表
                if re.match(r'^-政策文件[:：]$', line):
                    current_list = data["sourceFile"]
                    continue

                # 解析申报时间下的批次行
                if in_apply_time and line.startswith("-"):
                    sub_content = line.lstrip("-").strip()
                    batch_match = batch_pattern.search(sub_content)
                    if batch_match:
                        batch_name = batch_match.group(1).strip()
                        start_str = batch_match.group(2)
                        end_str = batch_match.group(3).strip()

                        start_date = start_str.strip() if start_str else None
                        end_date = end_str.strip() if end_str else None

                        time_item = {
                            "batch_no": batch_name,
                            "start_date": start_date,
                            "end_date": end_date
                        }
                        data["applyTime"].append(time_item)
                    continue

                # 收集子项（以 - 开头的内容）
                if current_list is not None and line.startswith("-"):
                    sub_content = line.lstrip("-").strip()
                    if not sub_content:
                        continue
                    if current_list == "conditions":
                        # 申报条件存入临时列表
                        conditions_buffer.append(sub_content)
                    else:
                        # 政策文件正常追加列表
                        current_list.append(sub_content)

            # 循环结束后，将多条申报条件用换行拼接成完整字符串
            if conditions_buffer:
                data["conditions"] = "\n".join(conditions_buffer)

            result.append(data)
        return result

    async def get_policy_attachments(self, collection_name: str, db_name: str = "dquot"):
        collection = self._mongo_client[db_name][collection_name]
        pipeline = [
            # 只筛选 sourceFile 非空数组、不为null的文档
            {"$match": {"sourceFile": {"$exists": True, "$ne": []}}},
            {"$project": {"_id": 1, "sourceFile": 1}},
            # 拆分数组，此时sourceFile一定有值
            {"$unwind": "$sourceFile"},
            # 二次过滤单条文件路径不为空字符串
            {"$match": {"sourceFile": {"$ne": ""}}}
        ]
        # 执行聚合
        cursor = collection.aggregate(pipeline)
        result = await cursor.to_list(length=None)
        return result

    def transform_policy_attachments(self, attachments: list):
        file_type_map = {
            "pdf": 'application/pdf',
            "doc": 'application/msword',
            "docx": 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            "xls": 'application/vnd.ms-excel',
            "xlsx": 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            "ppt": 'application/vnd.ms-powerpoint',
            "pptx": 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            "jpg": 'image/jpeg',
            "jpeg": 'image/jpeg',
            "png": 'image/png',
            "gif": 'image/gif',
            "txt": 'text/plain',
            "zip": 'application/zip',
            "rar": 'application/x-rar-compressed',
        }
        attachment_list = []
        for item in attachments:
            source_file = item["sourceFile"]
            parts = source_file.split("/")
            file_name = parts[-1]
            # 移除第一层 dquot-xxx 前缀目录
            rel_path = "/".join(parts[1:])
            # 提取文件后缀
            if "." in file_name:
                suffix = file_name.split(".")[-1].lower()
            else:
                suffix = ""
            mime_type = file_type_map.get(suffix, "application/octet-stream")

            # 循环内新建独立字典，避免引用覆盖
            attach = {
                "name": file_name,
                "path": rel_path,
                "type": mime_type,
                "module": self.policy_collection_name,
                "moduleId": str(item["_id"])
            }
            attachment_list.append(attach)

        return attachment_list

    # ==================== MongoDB存储 ====================
    async def _save_json_to_mongodb(
            self,
            policy_list: list,
            collection_name: str,
            db_name: str = "dquot"
    ):
        """
        存储JSON到MongoDB
        唯一键固定为 _id
        只更新/新增字段，旧字段 100% 保留
        特别处理：企业基本信息 只新增不存在的键，不覆盖已有键
        """
        collection = self._mongo_client[db_name][collection_name]

        try:
            if not policy_list:
                print("未解析到任何政策数据")
                return  {"success": True, "inserted_count": 0, "message": "无数据"}

            insert_res = await collection.insert_many(policy_list)
            return {
                "success": True,
                "message": "Insert成功",
                "inserted_count": len(insert_res.inserted_ids)
            }

        except Exception as e:
            self.logger.error(f"MongoDB存储失败: {e}")
            return {"success": False, "message": f"存储失败: {str(e)}"}


# async def main():
#     service = PolicyAnalysisServices()
#     await service.init()  # 千万不要漏！
#     res = await service.policy_attachments_to_mongodb()
#     print(res)
#
# if __name__ == "__main__":
#     asyncio.run(main())


