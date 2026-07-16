# ETL 临时文件存储管理器
import json
import pandas as pd
from typing import Any, Dict, List, Union, BinaryIO, Optional, Tuple
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger
import shutil
from PIL import Image


class StorageManager:
    """文件存储管理器"""

    _instance = None
    _storage_dir = None
    logger = get_logger(__name__)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StorageManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """初始化共享存储目录"""
        if self._storage_dir is None:
            project_root = Path(__file__).parent.parent.parent
            self._storage_dir = project_root / 'files'
            self._storage_dir.mkdir(parents=True, exist_ok=True)  # 确保目录存在
            self.logger.info(f"Initialized shared storage at: {self._storage_dir}")

    @property
    def base_dir(self) -> Path:
        """获取基础共享存储目录"""
        return self._storage_dir

    def create_subdir(self, subdir_name: str) -> Path:
        """创建子目录"""
        subdir_path = self._storage_dir / subdir_name
        subdir_path.mkdir(parents=True, exist_ok=True)
        return subdir_path

    def store_json(self, data: Any, filename: str, subdir: str = None) -> Path:
        """存储 json 数据"""
        file_path = self._get_file_path(filename, subdir, '.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # 新增：打印完整存储路径
        self.logger.debug(f"Stored JSON data to shared path: {file_path}")
        return file_path

    def load_json(self, filename: str, subdir: str = None) -> Any:
        """加载 json 数据"""
        file_path = self._get_file_path(filename, subdir, '.json')
        if not file_path.exists():
            # 新增：文件不存在时打印当前目录下的文件列表，辅助排查
            dir_path = self._storage_dir / subdir if subdir else self._storage_dir
            existing_files = [str(f) for f in dir_path.glob('*.json')]
            self.logger.error(
                f"File not found: {file_path}\n"
                f"Existing JSON files in {dir_path}: {existing_files}"
            )
            raise FileNotFoundError(f"No such file or directory: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def store_dataframe(self, df: pd.DataFrame, filename: str,
                        subdir: str = None, format: str = 'parquet') -> Path:
        """存储 DataFrame"""
        file_path = self._get_file_path(filename, subdir, f'.{format}')

        if format == 'parquet':
            df.to_parquet(file_path)
        elif format == 'csv':
            df.to_csv(file_path, index=False)
        elif format == 'json':
            df.to_json(file_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.debug(f"Stored DataFrame to: {file_path}")
        return file_path

    def load_dataframe(self, filename: str, subdir: str = None,
                       format: str = 'parquet') -> pd.DataFrame:
        """加载 DataFrame"""
        file_path = self._get_file_path(filename, subdir, f'.{format}')

        if format == 'parquet':
            return pd.read_parquet(file_path)
        elif format == 'csv':
            return pd.read_csv(file_path)
        elif format == 'json':
            return pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    # -------------------------- 扩展：Excel (.xlsx/.xls) 读写 --------------------------
    def store_excel(self,
                    data: Union[pd.DataFrame, List[List[Any]], Dict[str, List[Any]]],
                    filename: str,
                    subdir: str = None,
                    format: str = 'xlsx',
                    sheet_name: str = 'Sheet1',
                    index: bool = False) -> Path:
        """
        存储 Excel 文件（支持 .xlsx/.xls）
        :param data: 数据来源（DataFrame / 二维列表 / 字典（列名->列数据））
        :param filename: 文件名（无需扩展名）
        :param subdir: 子目录
        :param format: Excel 格式（xlsx/xls）
        :param sheet_name: 工作表名称
        :param index: 是否写入 DataFrame 索引
        :return: 存储后的文件路径
        """
        # 格式校验与统一
        format = format.lower()
        if format not in ['xlsx', 'xls']:
            raise ValueError(f"Unsupported Excel format: {format} (only 'xlsx'/'xls' allowed)")
        if format == 'xls' and not pd.io.excel._xlrd.is_available():
            raise ImportError("Install xlrd==1.2.0 to support .xls format (newer xlrd does not support .xls)")

        # 获取目标文件路径
        file_path = self._get_file_path(filename, subdir, f'.{format}')
        # 数据转换为 DataFrame（统一处理）
        try:
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list) and all(isinstance(row, list) for row in data):
                # 二维列表转换为 DataFrame（无列名）
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # 字典转换为 DataFrame（键为列名，值为列数据）
                df = pd.DataFrame(data)
            else:
                raise TypeError(f"Unsupported data type for Excel: {type(data)}")
        except Exception as e:
            self.logger.error(f"Failed to convert data to DataFrame for Excel: {e}")
            raise

        # 存储 Excel 文件（核心修改：移除 encoding 参数）
        try:
            with pd.ExcelWriter(file_path, engine='openpyxl' if format == 'xlsx' else 'xlrd') as writer:
                # 移除 encoding='utf-8'，该参数对 to_excel 无效
                df.to_excel(writer, sheet_name=sheet_name, index=index)
            self.logger.debug(f"Stored Excel file to: {file_path} (sheet: {sheet_name})")
            return file_path
        except Exception as e:
            self.logger.error(f"Failed to store Excel file: {e}")
            raise

    def load_excel(self,
                   filename: str,
                   subdir: str = None,
                   format: str = 'xlsx',
                   sheet_name: Optional[Union[str, int]] = 0,
                   header: Optional[int] = 0,
                   usecols: Optional[Union[List[str], List[int]]] = None) -> pd.DataFrame:
        """
        加载 Excel 文件（支持 .xlsx/.xls）
        :param filename: 文件名（可带/不带扩展名）
        :param subdir: 子目录
        :param format: Excel 格式（xlsx/xls，若文件名带扩展名可自动忽略）
        :param sheet_name: 工作表名称/索引（默认第1个工作表）
        :param header: 列名所在行索引（None=无列名）
        :param usecols: 要加载的列（列名列表/列索引列表）
        :return: 加载后的 DataFrame
        """
        # 自动匹配文件扩展名（优先从文件名提取）
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir
        file_path = None

        # 1. 文件名已带扩展名，直接校验
        if any(filename.lower().endswith(ext) for ext in ['.xlsx', '.xls']):
            file_path = dir_path / filename
            if file_path.exists():
                format = file_path.suffix[1:].lower()
            else:
                raise FileNotFoundError(f"Excel file not found: {file_path}")
        # 2. 文件名不带扩展名，按指定格式匹配
        else:
            file_path = self._get_file_path(filename, subdir, f'.{format}')

        # 格式兼容性校验
        format = format.lower()
        if format not in ['xlsx', 'xls']:
            raise ValueError(f"Unsupported Excel format: {format} (only 'xlsx'/'xls' allowed)")
        if not file_path.exists():
            existing_excels = [str(f.name) for f in dir_path.glob('*.xlsx') + dir_path.glob('*.xls')]
            self.logger.error(
                f"Excel file not found: {file_path}\n"
                f"Existing Excel files in {dir_path}: {existing_excels}"
            )
            raise FileNotFoundError(f"No Excel file found for {filename} in {dir_path}")

        # 加载 Excel 文件（核心修改：移除 encoding 参数）
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=header,
                usecols=usecols,
                engine='openpyxl' if format == 'xlsx' else 'xlrd'
            )
            self.logger.debug(f"Loaded Excel file from: {file_path} (sheet: {sheet_name}, rows: {len(df)})")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load Excel file: {e}")
            raise

    # -------------------------- 扩展：CSV (.csv) 读写 --------------------------
    def store_csv(self,
                  data: Union[pd.DataFrame, List[List[Any]], Dict[str, List[Any]]],
                  filename: str,
                  subdir: str = None,
                  sep: str = ',',
                  header: bool = True,
                  index: bool = False,
                  encoding: str = 'utf-8') -> Path:
        """
        存储 CSV 文件
        :param data: 数据来源（DataFrame / 二维列表 / 字典（列名->列数据））
        :param filename: 文件名（无需扩展名）
        :param subdir: 子目录
        :param sep: 分隔符（默认逗号）
        :param header: 是否写入列名（仅 DataFrame/字典有效）
        :param index: 是否写入 DataFrame 索引
        :param encoding: 文件编码
        :return: 存储后的文件路径
        """
        # 获取目标文件路径
        file_path = self._get_file_path(filename, subdir, '.csv')
        # 数据转换为 DataFrame（统一处理）
        try:
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list) and all(isinstance(row, list) for row in data):
                df = pd.DataFrame(data)
                header = False  # 二维列表无列名，强制关闭 header
            elif isinstance(data, dict):
                df = pd.DataFrame(data)
            else:
                raise TypeError(f"Unsupported data type for CSV: {type(data)}")
        except Exception as e:
            self.logger.error(f"Failed to convert data to DataFrame for CSV: {e}")
            raise

        # 存储 CSV 文件
        try:
            df.to_csv(
                file_path,
                sep=sep,
                header=header,
                index=index,
                encoding=encoding,
                na_rep=''
            )
            self.logger.debug(f"Stored CSV file to: {file_path} (sep: '{sep}', encoding: {encoding})")
            return file_path
        except Exception as e:
            self.logger.error(f"Failed to store CSV file: {e}")
            raise

    def load_csv(self,
                 filename: str,
                 subdir: str = None,
                 sep: str = ',',
                 header: Optional[int] = 0,
                 usecols: Optional[Union[List[str], List[int]]] = None,
                 encoding: str = 'utf-8') -> pd.DataFrame:
        """
        加载 CSV 文件
        :param filename: 文件名（可带/不带.csv扩展名）
        :param subdir: 子目录
        :param sep: 分隔符（默认逗号）
        :param header: 列名所在行索引（None=无列名）
        :param usecols: 要加载的列（列名列表/列索引列表）
        :param encoding: 文件编码
        :return: 加载后的 DataFrame
        """
        # 定位 CSV 文件路径
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir
        file_path = None

        if filename.lower().endswith('.csv'):
            file_path = dir_path / filename
        else:
            file_path = dir_path / f"{filename}.csv"

        if not file_path.exists():
            existing_csvs = [str(f.name) for f in dir_path.glob('*.csv')]
            self.logger.error(
                f"CSV file not found: {file_path}\n"
                f"Existing CSV files in {dir_path}: {existing_csvs}"
            )
            raise FileNotFoundError(f"No CSV file found for {filename} in {dir_path}")

        # 加载 CSV 文件
        try:
            df = pd.read_csv(
                file_path,
                sep=sep,
                header=header,
                usecols=usecols,
                encoding=encoding,
                na_filter=False
            )
            self.logger.debug(f"Loaded CSV file from: {file_path} (sep: '{sep}', rows: {len(df)})")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load CSV file: {e}")
            raise

    def store_text(self,
                   text: Union[str, List[str], List[Tuple[Any, ...]]],
                   filename: str,
                   subdir: str = None,
                   encoding: str = 'utf-8',
                   newline: str = '\n',
                   is_jieba_dict: bool = False,
                   default_word_freq: int = 200) -> Path:
        """
        存储文本数据（强化支持 jieba 自定义词典格式）
        :param text: 文本数据
                     - 普通模式：字符串 / 字符串列表（每行一个元素）
                     - jieba 词典模式：纯词语列表 / 二元组列表(词语, 词频) / 三元组列表(词语, 词频, 词性)
        :param filename: 文件名（无需扩展名）
        :param subdir: 子目录
        :param encoding: 文件编码（必须为 utf-8，适配 jieba）
        :param newline: 换行符
        :param is_jieba_dict: 是否按 jieba 自定义词典格式存储
        :param default_word_freq: jieba 词典默认词频（仅纯词语列表时生效，建议 100-500）
        :return: 存储后的文件路径
        """
        # 1. jieba 词典模式处理（核心改造部分）
        if is_jieba_dict:
            try:
                text_content = self._format_jieba_dict(text, default_word_freq, newline)
            except Exception as e:
                self.logger.error(f"Failed to format jieba dictionary: {e}")
                raise
        # 2. 普通文本模式（保持原有功能兼容）
        else:
            try:
                if isinstance(text, list):
                    text_content = newline.join([str(line).strip() for line in text])
                else:
                    text_content = str(text)
            except Exception as e:
                self.logger.error(f"Failed to process text data: {e}")
                raise

        # 3. 获取文件路径（强制 .txt 扩展名，适配 jieba）
        file_path = self._get_file_path(filename, subdir, '.txt')

        # 4. 写入文件（保证 utf-8 编码，jieba 要求）
        with open(file_path, 'w', encoding=encoding, newline=newline) as f:
            f.write(text_content)

        # 5. 日志区分输出
        if is_jieba_dict:
            self.logger.info(f"Stored jieba custom dictionary to: {file_path} (encoding: {encoding}, default freq: {default_word_freq})")
        else:
            self.logger.info(f"Stored text file to: {file_path} (encoding: {encoding})")

        return file_path

    def _format_jieba_dict(self,
                           data: Union[List[str], List[Tuple[Any, ...]]],
                           default_freq: int,
                           newline: str) -> str:
        """
        内部辅助方法：格式化 jieba 自定义词典内容
        :param data: 词典数据（纯词语列表 / 二元组 / 三元组）
        :param default_freq: 默认词频
        :param newline: 换行符
        :return: 格式化后的 jieba 词典文本内容
        """
        # 校验输入数据类型
        if not isinstance(data, list):
            raise TypeError(f"jieba dictionary data must be list, got {type(data)}")
        if len(data) == 0:
            raise ValueError("jieba dictionary data cannot be empty")

        jieba_lines = []
        for item in data:
            # 过滤空值
            if not item:
                continue

            # 场景 1：纯词语（字符串）
            if isinstance(item, str):
                word = item.strip()
                if not word:
                    continue
                # 格式：词语 + 默认词频（省略词性）
                jieba_line = f"{word} {default_freq}"
                jieba_lines.append(jieba_line)

            # 场景 2：二元组/三元组（词语, 词频[, 词性]）
            elif isinstance(item, (tuple, list)):
                if len(item) < 1:
                    continue
                # 提取词语（必填）
                word = str(item[0]).strip()
                if not word:
                    continue
                # 提取词频（可选，默认使用 default_freq）
                try:
                    word_freq = int(item[1]) if len(item) >= 2 and item[1] else default_freq
                except (ValueError, TypeError):
                    word_freq = default_freq
                # 提取词性（可选，省略则不写入）
                pos = str(item[2]).strip() if len(item) >= 3 and item[2] else None

                # 拼接 jieba 词典行
                if pos:
                    jieba_line = f"{word} {word_freq} {pos}"
                else:
                    jieba_line = f"{word} {word_freq}"
                jieba_lines.append(jieba_line)

            # 不支持的条目类型
            else:
                self.logger.warning(f"Unsupported item type in jieba dictionary: {type(item)}, skipped")
                continue

        # 去重（保留顺序，避免重复词条影响 jieba 分词）
        unique_jieba_lines = list(dict.fromkeys(jieba_lines))

        # 拼接为完整文本内容
        return newline.join(unique_jieba_lines)

    def load_text(self,
                  filename: str,
                  subdir: str = None,
                  encoding: str = 'utf-8',
                  split_lines: bool = False,
                  strip_lines: bool = True) -> Union[str, List[str]]:
        """
        加载文本数据（支持加载 jieba 词典文件并返回原始内容）
        :param filename: 文件名（可带/不带.txt扩展名）
        :param subdir: 子目录
        :param encoding: 文件编码
        :param split_lines: 是否按行拆分返回列表
        :param strip_lines: 是否去除每行首尾空格/换行符
        :return: 完整文本字符串 / 按行拆分的字符串列表
        """
        # 定位 TXT 文件路径
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir
        file_path = None

        if filename.lower().endswith('.txt'):
            file_path = dir_path / filename
        else:
            file_path = dir_path / f"{filename}.txt"

        if not file_path.exists():
            existing_txts = [str(f.name) for f in dir_path.glob('*.txt')]
            self.logger.error(
                f"TXT file not found: {file_path}\n"
                f"Existing TXT files in {dir_path}: {existing_txts}"
            )
            raise FileNotFoundError(f"No TXT file found for {filename} in {dir_path}")

        # 加载文件
        with open(file_path, 'r', encoding=encoding) as f:
            content = f.read()

        # 处理返回格式
        if split_lines:
            lines = content.splitlines()
            if strip_lines:
                lines = [line.strip() for line in lines if line.strip()]
            self.logger.debug(f"Loaded TXT file from: {file_path} (lines: {len(lines)})")
            return lines
        else:
            self.logger.debug(f"Loaded TXT file from: {file_path} (size: {len(content)} bytes)")
            return content

    def store_binary(self, data: bytes, filename: str, subdir: str = None) -> Path:
        """存储二进制数据"""
        file_path = self._get_file_path(filename, subdir, '.bin')
        with open(file_path, 'wb') as f:
            f.write(data)
        self.logger.debug(f"Stored binary data to: {file_path}")
        return file_path

    def load_binary(self, filename: str, subdir: str = None) -> bytes:
        """加载二进制数据"""
        file_path = self._get_file_path(filename, subdir, '.bin')
        with open(file_path, 'rb') as f:
            return f.read()

    # -------------------------- 新增：图片存储/加载功能 --------------------------
    def store_image(self,
                    image_source: Union[str, Path, bytes, Image.Image],
                    filename: str,
                    subdir: str = None,
                    format: str = 'png') -> Path:
        """
        存储图片文件
        :param image_source: 图片来源（文件路径/二进制数据/PIL Image对象）
        :param filename: 文件名（无需扩展名）
        :param subdir: 子目录
        :param format: 图片格式（png/jpg/jpeg/bmp等）
        :return: 存储后的文件路径
        """
        # 处理格式（统一小写，兼容jpg/jpeg）
        format = format.lower()
        if format == 'jpg':
            format = 'jpeg'

        # 获取目标文件路径
        file_path = self._get_file_path(filename, subdir, f'.{format}')

        try:
            # 场景1：图片来源是文件路径
            if isinstance(image_source, (str, Path)):
                image_source = Path(image_source)
                if not image_source.exists():
                    raise FileNotFoundError(f"Source image not found: {image_source}")
                # 拷贝文件到目标路径
                shutil.copy2(image_source, file_path)

            # 场景2：图片来源是二进制数据
            elif isinstance(image_source, bytes):
                with open(file_path, 'wb') as f:
                    f.write(image_source)

            # 场景3：图片来源是PIL Image对象
            elif isinstance(image_source, Image.Image):
                image_source.save(file_path, format=format)

            else:
                raise TypeError(f"Unsupported image source type: {type(image_source)}")

            self.logger.info(f"Stored image to: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to store image: {e}")
            raise

    def load_image(self,
                   filename: str,
                   subdir: str = None,
                   return_type: str = 'path') -> Union[Path, bytes, Image.Image]:
        """
        加载图片文件
        :param filename: 文件名（可带/不带扩展名）
        :param subdir: 子目录
        :param return_type: 返回类型（path:文件路径 / bytes:二进制数据 / pil:PIL Image对象）
        :return: 对应类型的图片数据
        """
        # 自动匹配图片扩展名（优先png/jpg/jpeg/bmp）
        file_path = None
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir

        # 1. 如果文件名带扩展名，直接检查
        if '.' in filename:
            file_path = dir_path / filename
            if not file_path.exists():
                raise FileNotFoundError(f"Image file not found: {file_path}")
        # 2. 文件名不带扩展名，自动匹配常见格式
        else:
            for ext in ['png', 'jpg', 'jpeg', 'bmp']:
                candidate = dir_path / f"{filename}.{ext}"
                if candidate.exists():
                    file_path = candidate
                    break
            if not file_path:
                existing_images = [str(f) for f in dir_path.glob(f"{filename}.*") if
                                   f.suffix in ['.png', '.jpg', '.jpeg', '.bmp']]
                self.logger.error(
                    f"Image file not found for {filename}\n"
                    f"Existing images in {dir_path}: {existing_images}"
                )
                raise FileNotFoundError(f"No image file found for {filename} in {dir_path}")

        try:
            # 返回文件路径
            if return_type == 'path':
                self.logger.debug(f"Loaded image path: {file_path}")
                return file_path

            # 返回二进制数据
            elif return_type == 'bytes':
                with open(file_path, 'rb') as f:
                    data = f.read()
                self.logger.debug(f"Loaded image bytes (size: {len(data)} bytes)")
                return data

            # 返回PIL Image对象
            elif return_type == 'pil':
                img = Image.open(file_path)
                self.logger.debug(f"Loaded PIL Image (format: {img.format}, size: {img.size})")
                return img

            else:
                raise ValueError(f"Unsupported return_type: {return_type} (must be 'path'/'bytes'/'pil')")

        except Exception as e:
            self.logger.error(f"Failed to load image: {e}")
            raise

    # -------------------------- 新增：PDF 存储/加载功能 --------------------------
    def store_pdf(self,
                  pdf_source: Union[str, Path, bytes, BinaryIO],
                  filename: str,
                  subdir: str = None) -> Path:
        """
        存储PDF文件
        :param pdf_source: PDF来源（文件路径/二进制数据/二进制流对象）
        :param filename: 文件名（无需扩展名）
        :param subdir: 子目录
        :return: 存储后的PDF文件路径
        """
        # 固定PDF扩展名
        file_path = self._get_file_path(filename, subdir, '.pdf')

        try:
            # 场景1：PDF来源是文件路径（str/Path）
            if isinstance(pdf_source, (str, Path)):
                pdf_source = Path(pdf_source)
                if not pdf_source.exists():
                    raise FileNotFoundError(f"Source PDF not found: {pdf_source}")
                # 拷贝文件到目标路径（保留元数据）
                shutil.copy2(pdf_source, file_path)

            # 场景2：PDF来源是二进制数据（bytes）
            elif isinstance(pdf_source, bytes):
                with open(file_path, 'wb') as f:
                    f.write(pdf_source)

            # 场景3：PDF来源是二进制流（如io.BytesIO）
            elif hasattr(pdf_source, 'read') and callable(getattr(pdf_source, 'read')):
                with open(file_path, 'wb') as f:
                    # 若流已读取过，重置指针到开头
                    if hasattr(pdf_source, 'seek') and callable(getattr(pdf_source, 'seek')):
                        pdf_source.seek(0)
                    shutil.copyfileobj(pdf_source, f)

            else:
                raise TypeError(f"Unsupported PDF source type: {type(pdf_source)}")

            self.logger.info(f"Stored PDF to: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to store PDF: {e}")
            raise

    def load_pdf(self,
                 filename: str,
                 subdir: str = None,
                 return_type: str = 'path') -> Union[Path, bytes, BinaryIO]:
        """
        加载PDF文件
        :param filename: 文件名（可带/不带.pdf扩展名）
        :param subdir: 子目录
        :param return_type: 返回类型
                            - path: 文件路径（Path对象）
                            - bytes: 二进制数据
                            - stream: 二进制流（io.BytesIO，可直接传给PDF库处理）
        :return: 对应类型的PDF数据
        """
        # 定位PDF文件路径
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir

        # 1. 文件名带扩展名，直接检查
        if filename.lower().endswith('.pdf'):
            file_path = dir_path / filename
            if not file_path.exists():
                raise FileNotFoundError(f"PDF file not found: {file_path}")
        # 2. 文件名不带扩展名，自动补.pdf
        else:
            file_path = dir_path / f"{filename}.pdf"
            if not file_path.exists():
                # 列出目录内所有PDF文件，辅助排查
                existing_pdfs = [str(f.name) for f in dir_path.glob('*.pdf')]
                self.logger.error(
                    f"PDF file not found: {file_path}\n"
                    f"Existing PDFs in {dir_path}: {existing_pdfs}"
                )
                raise FileNotFoundError(f"No PDF file found for {filename} in {dir_path}")

        try:
            # 返回文件路径（最常用，适合直接打开/嵌入）
            if return_type == 'path':
                self.logger.debug(f"Loaded PDF path: {file_path}")
                return file_path

            # 返回二进制数据（适合网络传输/内存处理）
            elif return_type == 'bytes':
                with open(file_path, 'rb') as f:
                    data = f.read()
                self.logger.debug(f"Loaded PDF bytes (size: {len(data)} bytes)")
                return data

            # 返回二进制流（适合reportlab/PyPDF2等库直接处理）
            elif return_type == 'stream':
                import io
                stream = io.BytesIO()
                with open(file_path, 'rb') as f:
                    stream.write(f.read())
                stream.seek(0)  # 重置指针到开头
                self.logger.debug(f"Loaded PDF stream (size: {stream.getbuffer().nbytes} bytes)")
                return stream

            else:
                raise ValueError(
                    f"Unsupported return_type: {return_type} "
                    f"(must be 'path'/'bytes'/'stream')"
                )

        except Exception as e:
            self.logger.error(f"Failed to load PDF: {e}")
            raise

    # -------------------------------------------------------------------------

    def _get_file_path(self, filename: str, subdir: str = None, extension: str = '') -> Path:
        """获取文件路径"""
        if subdir:
            dir_path = self._storage_dir / subdir
            dir_path.mkdir(parents=True, exist_ok=True)
        else:
            dir_path = self._storage_dir

        # 确保文件名有正确的扩展名
        if not filename.endswith(extension):
            filename += extension

        return dir_path / filename

    def list_files(self, filename_prefix: str = '*', subdir: str = None) -> List[Path]:
        """列出文件"""
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir
        return list(dir_path.glob(f'{filename_prefix}*.*'))

    def list_filename(self, filename_prefix: str = '*', subdir: str = None) -> List[str]:
        """列出文件名"""
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir
        # 遍历匹配的文件，提取纯文件名
        file_names = []
        for file_path in dir_path.glob(f'{filename_prefix}*.*'):
            # 使用name属性获取仅文件名（含后缀），如 "data.csv"
            file_names.append(file_path.name)

        return file_names

    def get_file_info(self, filename: str, subdir: str = None) -> Dict:
        """获取文件信息"""
        file_path = self._get_file_path(filename, subdir, '')
        if file_path.exists():
            stat = file_path.stat()
            return {
                'path': str(file_path),
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'created_time': datetime.fromtimestamp(stat.st_ctime)
            }
        return {}

    def cleanup_files(self, filename_prefix: str = '*', subdir: str = None):
        """仅清理指定文件，不删除整个目录"""
        if not self._storage_dir or not self._storage_dir.exists():
            return

        # 拼接当前批次的文件路径模式（匹配该批次的所有文件）
        dir_path = self._storage_dir / subdir if subdir else self._storage_dir
        # 匹配如 "extracted_data.json"、"processed_data.json" 的文件
        batch_files = dir_path.glob(f'{filename_prefix}*.*')

        for file in batch_files:
            if file.is_file():  # 仅删除文件，不删除目录
                file.unlink()
                self.logger.debug(f"Cleaned up file: {file}")

    def cleanup(self):
        """安全清理：仅删除空目录，避免误删其他批次文件"""
        if self._storage_dir and self._storage_dir.exists():
            # 遍历子目录，删除空目录
            for subdir in self._storage_dir.iterdir():
                if subdir.is_dir() and not any(subdir.iterdir()):
                    subdir.rmdir()
                    self.logger.debug(f"Cleaned up empty subdir: {subdir}")
            self.logger.info(f"Safe cleanup completed for storage: {self._storage_dir}")


# 工具函数
def get_storage() -> StorageManager:
    """获取共享存储存储管理器实例"""
    return StorageManager()


def cleanup_storage():
    """清理共享存储存储"""
    StorageManager().cleanup()