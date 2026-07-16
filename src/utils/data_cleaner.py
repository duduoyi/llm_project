import re
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

def preprocess_all_string_fields(dict_list: list[dict], fill_na: str = "") -> list[dict]:
    """
    自动识别字典列表中所有字符串类型字段并执行预处理（替代DataFrame版本）
    预处理规则：
    1. 空值填充 → 2. 清理换行符 → 3. 清理空白字符 → 4. 中文分号转英文 → 5. 英文括号转中文
    适配混合类型值：避免非字符串对象导致的异常
    :param dict_list: 输入的字典列表（如 [{"name": " 张三\n", "age": 18}, ...]）
    :param fill_na: 空值填充值，默认空字符串
    :return: 处理后的字典列表
    """
    # 空值处理
    if not dict_list:
        logger.info("输入字典列表为空，直接返回空列表")
        return []

    # 步骤1：识别所有可能包含字符串的字段（遍历所有字典的key）
    all_fields = set()
    for d in dict_list:
        all_fields.update(d.keys())
    string_fields = list(all_fields)  # 字典列表的字段不区分类型，全部纳入处理（按需过滤非字符串值）

    logger.info(f"自动识别到 {len(string_fields)} 个待检查字段：{string_fields}")

    # 步骤2：批量处理每个字典的字符串字段（纯Python向量化思路）
    processed_list = []
    for idx, item in enumerate(dict_list):
        processed_item = item.copy()  # 复制原字典，避免修改原数据

        for field in string_fields:
            # 字段不存在则填充空值
            if field not in processed_item:
                processed_item[field] = fill_na
                continue

            # 获取原始值
            val = processed_item[field]

            # 步骤1：空值填充（兼容None/空字符串/NaN）
            if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
                val = fill_na

            # 步骤2：强制转为字符串（处理混合类型）
            # 特殊处理：避免把空值转为"nan"
            if val != fill_na:
                val = str(val) if not pd.isna(val) else fill_na
            else:
                val = fill_na

            # 步骤3：清理回车/换行符（使用预编译正则提升效率）
            val = re.compile(r"\r|\n|\r\n").sub("", val)

            # 步骤4：清理所有空白字符（空格/制表符/全角空格等）
            val = re.compile(r"\s+").sub("", val)

            # 步骤5：中文分号； → 英文分号;
            val = val.replace("；", ";")

            # 步骤6：英文括号()→ 中文括号（）
            val = val.replace("(", "（").replace(")", "）")

            # 更新处理后的值
            processed_item[field] = val

        processed_list.append(processed_item)

    logger.info(f"完成 {len(processed_list)} 条数据的字符串字段预处理")
    return processed_list


def split_inventor_applicant(data_list):
    """计算专利与发明人之间的关系并转换为 JSON，保留原始所有字段

    Args:
        data_list: 字典列表，每个字典包含 patent 的字段信息

    Returns:
        展开后的字典列表（笛卡尔积结果）
    """
    try:
        patent_data = []
        # 遍历每个字典
        for row_dict in data_list:
            # 分割并清洗申请人列表
            applicant_str = str(row_dict.get("applicant", "")).strip()
            applicants = [app.strip() for app in applicant_str.split(";") if app.strip()]
            # 无申请人时用空值占位，避免丢失数据
            applicants = applicants if applicants else [None]

            # 分割并清洗发明人列表
            inventor_str = str(row_dict.get("inventor", "")).strip()
            inventors = [inv.strip() for inv in inventor_str.split(";") if inv.strip()]
            # 无发明人时用空值占位
            inventors = inventors if inventors else [None]

            # 笛卡尔积：申请人 × 发明人
            for applicant in applicants:
                for inventor_name in inventors:
                    # 复制原始行数据，保留所有字段
                    new_item = row_dict.copy()
                    # 覆盖为拆分后的单个申请人/发明人
                    new_item["applicant"] = applicant
                    new_item["inventor"] = inventor_name

                    patent_data.append(new_item)

        return patent_data
    except Exception as e:
        print(f"发明人/申请人分割失败: {str(e)}")
        raise


