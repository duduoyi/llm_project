import asyncio
import aiohttp
from io import BytesIO
from typing import Optional
import re
from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger


class PolicyAttachmentDownload(BaseResourceService):
    DOWNLOAD_TIMEOUT = 15
    MAX_RETRY = 2
    name = "policy_attachment_download"

    def __init__(self):
        """
        初始化服务类
        """
        self.logger = get_logger(self.__class__.__name__)
        # 初始化客户端（懒加载）
        self._mongo_collection = "policies"
        self._bucket_name = "policy-1366204382"

    @staticmethod
    def get_safe_keyword_prefix(doc) -> str:
        """从文档 match_keywords 提取关键词，清洗成合法COS路径前缀"""
        raw_keywords = doc.get("match_keywords", "")
        if isinstance(raw_keywords, list):
            keyword_str = "_".join(raw_keywords)
        else:
            keyword_str = str(raw_keywords)

        unsafe_pattern = re.compile(r'[\\/:*?"<>|#\s]')
        safe_text = unsafe_pattern.sub("_", keyword_str)
        safe_text = safe_text[:60].strip("_")
        if not safe_text:
            safe_text = "default_policy"
        return safe_text

    async def download_attach(self, url: str) -> Optional[BytesIO]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        for retry in range(self.MAX_RETRY + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, timeout=self.DOWNLOAD_TIMEOUT) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            return BytesIO(data)
            except Exception as e:
                print(f"下载失败 {url}, retry={retry}, err:{str(e)}")
                await asyncio.sleep(0.5)
        return None

    async def upload_cos(self, stream: BytesIO, save_key: str) -> Optional[str]:
        try:
            cos_client = await self.get_cos_client()
            cos_client.put_object(
                Bucket=self._bucket_name,
                Body=stream,
                Key=save_key
            )
            return f"cos://{self._bucket_name}/{save_key}"
        except Exception as e:
            print(f"COS上传失败 key={save_key}, err={str(e)}")
            return None

    async def process_single_doc(self, doc):
        doc_id = doc["_id"]
        attachments = doc.get("attachments", [])
        keyword_prefix = self.get_safe_keyword_prefix(doc)

        # 遍历附件处理
        for att in attachments:
            # 已有cos路径，跳过
            if att.get("cos_path") is not None:
                continue

            att_url = att["url"]
            att_name = att["name"]
            fname = doc["file_name"]
            file_key = f"{keyword_prefix}/{fname}/{att_name}"

            stream = await self.download_attach(att_url)
            if not stream:
                print(f"文档 {doc_id} 附件 {att_url} 下载失败，跳过")
                continue

            cos_path = await self.upload_cos(stream, file_key)
            if cos_path:
                att["cos_path"] = cos_path
                print(f"文档 {doc_id} 附件上传成功: {cos_path}")

        mongo_db = await self.get_mongo_db("crawl")
        collection = mongo_db[self._mongo_collection]
        # 所有附件循环完毕，统一更新状态为 success
        await collection.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "attachments": attachments,
                    "attach_task_status": "success"
                }
            }
        )
        print(f"【完成】文档 {doc_id} 全部附件处理完毕，状态更新为 success\n")

    async def batch_scan_today_task(self, start_time, end_time):
        """扫描当日 update_time、状态 != success 的文档"""

        query = {
            "update_time": {"$gte": start_time, "$lt": end_time},
            "attach_task_status": {"$ne": "success"}
        }
        print(f"\n===== 开始扫描当日任务，日期范围 {start_time.date()} =====")
        mongo_db = await self.get_mongo_db("crawl")
        collection = mongo_db[self._mongo_collection]
        cursor = collection.find(
            query,
            {
                "_id": 1,
                "file_name": 1,
                "update_time": 1,
                "match_keywords": 1,
                "attachments": 1,
                "cos_path": 1
            }
        )
        total = 0
        async for doc in cursor:
            try:
                await self.process_single_doc(doc)
                total += 1
            except Exception as e:
                print(f"文档 {doc['_id']} 处理异常: {str(e)}")

        print(f"===== 扫描结束，本轮处理文档总数：{total} =====\n")


# ====================== 异步测试入口 ======================
# async def main():
#     service = AttachmentConsumer()
#     await service.batch_scan_today_task("2026-08-01 00:00:00", "2026-08-02 00:00:00")
#
#
# if __name__ == "__main__":
#     asyncio.run(main())