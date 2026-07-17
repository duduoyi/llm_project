import asyncio
import torch
import os
from pathlib import Path
from typing import List


# ===================== 自动 GPU/CPU =====================
def get_auto_device():
    return "cuda" if torch.cuda.is_available() else "cpu"

DEVICE = get_auto_device()
# 根据显存调整，16G推荐2，24G可以3~4
EMBED_SEM = asyncio.Semaphore(2)

# ===================== 【全局单例】工厂 =====================

# 包装层：增加信号量限流，不破坏原生HuggingFaceEmbeddings对象
class SemEmbeddingWrapper:
    def __init__(self, emb_obj):
        self._emb = emb_obj

    async def aembed_query(self, text: str):
        async with EMBED_SEM:
            return await self._emb.aembed_query(text)

    async def aembed_documents(self, texts: List[str]):
        async with EMBED_SEM:
            return await self._emb.aembed_documents(texts)

    # 暴露原始对象，需要传给LangChain VectorStore时使用
    @property
    def raw_embedding(self):
        return self._emb

class EmbeddingFactory:
    _instances = {}

    @classmethod
    def get_instance(cls, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        """
        只需要传模型名称
        自动使用GPU/CPU
        """
        if model_name not in cls._instances:
            from langchain_huggingface import HuggingFaceEmbeddings
            project_root = Path(__file__).parent.parent.parent.parent
            model_path = os.path.join(project_root, "models", "sentence-transformers", model_name)

            raw_emb = HuggingFaceEmbeddings(
                model_name=model_path,
                model_kwargs={"device": DEVICE},
                encode_kwargs={
                    "normalize_embeddings": True,
                    "num_workers": 0
                }
            )

            # 不要修改raw_emb！用包装类包裹
            wrapped = SemEmbeddingWrapper(raw_emb)
            cls._instances[model_name] = wrapped
        return cls._instances[model_name]


class LLMFactory:
    """远端大模型调用，ChatOpenAI 原生支持异步 ainvoke，无需额外包装"""

    _instances = {}

    @classmethod
    def get_instance(cls, provider: str = "qwen"):
        """
        获取 LLM 实例
        :param provider: "zhipu" 或 "qwen"
        """
        if provider not in cls._instances:
            from langchain_openai import ChatOpenAI
            if provider == "zhipu":
                cls._instances[provider] = ChatOpenAI(
                    model=os.getenv("ZHIPU_MODEL"),
                    api_key=os.getenv("ZHIPU_API_KEY"),
                    base_url=os.getenv("ZHIPU_BASE_URL"),
                    temperature=0,
                    max_tokens=1024,
                )
            elif provider == "qwen":
                cls._instances[provider] = ChatOpenAI(
                    model=os.getenv("QWEN_MODEL"),
                    api_key=os.getenv("QWEN_API_KEY"),
                    base_url=os.getenv("QWEN_BASE_URL"),
                    temperature=0.1,
                    max_tokens=4096,
                )
            else:
                raise ValueError(f"不支持的 LLM 提供商: {provider}")

        return cls._instances[provider]



# ===================== 异步测试脚本 =====================
# async def embedding():
#     emb = EmbeddingFactory.get_instance()
#     vec = await emb.aembed_query("测试文本")
#     print(f"向量长度：{len(vec)}")
#
#
# async def llm():
#     llm = LLMFactory.get_instance("qwen")
#     resp = await llm.ainvoke("你好")
#     print(resp.content)
#
#
# if __name__ == "__main__":
#     asyncio.run(embedding())