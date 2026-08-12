import asyncio
import aiohttp
from playwright.async_api import async_playwright
from io import BytesIO
from typing import Optional, Tuple
import re
from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger


class ListedCompanyReport(BaseResourceService):
    DOWNLOAD_TIMEOUT = 15
    MAX_RETRY = 2
    name = "listed_company_report"

    def __init__(self):
        """
        初始化服务类
        """
        self.logger = get_logger(self.__class__.__name__)
        # 初始化客户端（懒加载）
        self._stocks_collection = "stocks"
        self._company_collection = "company_info"
        self._bucket_name = "company-report-1366204382"

    async def get_stock_list(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        url = "https://www.cninfo.com.cn/new/data/szse_stock.json"
        for retry in range(self.MAX_RETRY + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, timeout=self.DOWNLOAD_TIMEOUT) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return data
            except Exception as e:
                self.logger.error(f"下载失败 {url}, retry={retry}, err:{str(e)}")
                await asyncio.sleep(0.5)
        return None

    async def save_stock_list_to_mongo(self, data):
        """存入MongoDB"""
        # 只更新/新增，不删除旧字段
        mongo_db = await self.get_mongo_db()
        collection = mongo_db[self._stocks_collection]
        insert_count = 0
        # 3. 提取stockList数组
        stock_list = data["stockList"]

        # 4. 批量插入MongoDB
        # drop=True 可选：清空旧数据再导入；不需要删除就注释
        try:
            await collection.delete_many({})
            result = await collection.insert_many(stock_list)
            insert_count = len(result.inserted_ids)
            # 股票代码唯一索引（查询股票必用）
            await collection.create_index("code", unique=True)
            # orgId索引（爬公告接口匹配用）
            await collection.create_index("orgId")
            self.logger.info(f"导入完成，共{insert_count}条股票数据")
        except Exception as e:
            self.logger.error(f"导入失败: {str(e)}")
        return insert_count

    async def get_stock_code_and_orgid(self, county):
        mongo_db = await self.get_mongo_db()
        company_coll = mongo_db[self._company_collection]
        stocks_coll = mongo_db[self._stocks_collection]

        # ==========第一步：company_coll 查询证券代码列表==========
        company_filter = {
            "企业基本信息.所属区县": county,
            "企业基本信息.上市状态": "正常上市",
            "_metadata": {"$exists": False}
        }
        # 投影：只返回证券代码，屏蔽 _id
        company_proj = {
            "_id": 0,
            "企业基本信息.证券代码": 1
        }

        code_list = []
        cursor = company_coll.find(company_filter, projection=company_proj).batch_size(1000)
        async for doc in cursor:
            sec_code = doc["企业基本信息"]["证券代码"]
            code_list.append(sec_code)

        self.logger.info(f"从{self._company_collection}拿到 {len(code_list)} 个证券代码")

        # ==========第二步：批量查询stocks_coll，code匹配证券代码拿orgId==========
        # $in批量查询，减少IO，性能远优于循环单条查询
        stock_cursor = stocks_coll.find(
            {"code": {"$in": code_list}},
            {"_id": 0, "code": 1, "orgId": 1}
        )

        code_orgid_map = {}
        async for s in stock_cursor:
            code_orgid_map[s["code"]] = s["orgId"]

        # 组装结果：证券代码 -> orgId
        result = []
        for code in code_list:
            result.append({
                "stockCode": code,
                "orgId": code_orgid_map.get(code)  # 找不到返回None
            })

        return result

    async def download_report(self, stockcode: str, orgid: str, year: str) -> Tuple[Optional[str], Optional[BytesIO]]:
        base_domain = "https://www.cninfo.com.cn"
        page_url = f"{base_domain}/new/disclosure/stock?stockCode={stockcode}&orgId={orgid}#periodicReports"
        target_detail_href: Optional[str] = None
        target_title: Optional[str] = None

        # 使用异步playwright，不要sync_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(accept_downloads=True)  # 允许浏览器下载文件
            try:
                # ==========第一步：进入定期报告列表页，找到指定年份年度报告公告链接==========
                page = await browser.new_page()
                await page.goto(page_url, timeout=60000)
                # 等待公告表格渲染
                await page.wait_for_selector('table tbody', timeout=20000)

                # 定位所有公告a标签
                a_locator = page.locator('//table/tbody//a[span[@class="r-title"]]')
                total = await a_locator.count()

                for idx in range(total):
                    current_a = a_locator.nth(idx)
                    # 获取标题文本
                    title_text = await current_a.locator("span.r-title").text_content()
                    title_text = title_text.strip()

                    if title_text.endswith(f"{year}年年度报告"):
                        rel_href = await current_a.get_attribute("href")
                        if rel_href:
                            target_detail_href = base_domain + rel_href
                            target_title = title_text
                        break

                if not target_detail_href:
                    self.logger.warning(f"[{stockcode}] 未找到 [{year}年年度报告]")
                    return None, None

                # ==========第二步：打开公告详情页面，点击下载按钮==========
                detail_page = await context.new_page()
                await detail_page.goto(target_detail_href, timeout=60000)
                # 改用相对xpath，稳定性大幅提升
                download_btn_xpath = '//button[normalize-space()="公告下载"]'
                await detail_page.wait_for_selector(download_btn_xpath, timeout=15000)
                await asyncio.sleep(1)  # 等待预览加载

                # 监听下载事件，必须先注册监听再点击按钮（防止竞态丢失下载）
                async with detail_page.expect_download(timeout=30000) as download_info:
                    await detail_page.click(download_btn_xpath)
                download = await download_info.value

                # ==========第三步：读取下载的PDF二进制流到BytesIO（不保存本地）==========
                output = BytesIO()
                temp_file = await download.path()
                with open(temp_file, "rb") as fp:
                    output.write(fp.read())
                output.seek(0)
                return target_title, output

            except Exception as e:
                self.logger.info(f"[{stockcode}] 页面解析异常: {str(e)}", exc_info=True)
                return None, None
            finally:
                # 找到链接立刻关闭浏览器，节约资源
                await browser.close()

    async def upload_cos(self, stream: BytesIO, save_key: str) -> Optional[str]:
        try:
            cos_client = await self.get_cos_client()
            cos_client.put_object(
                Bucket=self._bucket_name,
                Body=stream,
                Key=save_key
            )
            return save_key
        except Exception as e:
            self.logger.error(f"COS上传失败 key={save_key}, err={str(e)}")
            return None


    async def process_single_doc(self, county, year):
        data = await self.get_stock_list()
        self.logger.info("获取stock_list映射")
        insert_count = await self.save_stock_list_to_mongo(data)
        self.logger.info(f"更新存储stock_list映射{insert_count}条")

        code_list = await self.get_stock_code_and_orgid(county)
        success = 0
        for code in code_list:
            stockcode = code["stockCode"]
            orgid = code["orgId"]

            title, stream = await self.download_report(stockcode, orgid, year)
            if not stream:
                self.logger.info(f"文档 {title} 下载失败，跳过")
                continue

            file_key = f"{year}年/{title}.pdf"
            self.logger.info(f"{file_key}文档下载成功")
            cos_path = await self.upload_cos(stream, file_key)
            if cos_path:
                mongo_db = await self.get_mongo_db()
                company_coll = mongo_db[self._company_collection]
                # 所有附件循环完毕，统一更新状态为 success
                await company_coll.update_one(
                    {"企业基本信息.证券代码": stockcode},
                    {
                        "$set": {
                            "_metadata.source_file": cos_path
                        }
                    }
                )
                success += 1
                self.logger.info(f"{title} 存储cos路径已更新到MongoDB")

        return {
            "total": len(code_list),
            "success": success,
        }




# # ====================== 异步测试入口 ======================
# async def main():
#     service = ListedCompanyReport()
#     await service.process_single_doc("海门区", "2025")
#
#
# if __name__ == "__main__":
#     asyncio.run(main())