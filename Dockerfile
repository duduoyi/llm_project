# 多阶段构建：第一阶段用于构建依赖（精简构建环境，减少冗余）
FROM python:3.11-slim-bookworm AS builder
# 强制UTF-8，避免编码乱码
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    # 关闭python字节码生成、关闭日志缓冲
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
# 只在builder拷贝wheels，wheels不会进入最终镜像历史
COPY ./wheels /app/wheels
COPY requirements.txt .

# 离线安装，--no-index，不联网
RUN pip install \
    --no-index \
    --find-links /app/wheels \
    --no-cache-dir \
    -r requirements.txt \
    && find /usr/local/lib/python3.11/site-packages \
    \( -name "__pycache__" -o -name "*.pyc" -o -name "tests" -o -name "test" -o -name "docs" -o -name "examples" \) \
    -type d -prune -exec rm -rf {} +

# 阶段2：运行环境（核心精简，最大化压缩体积）
FROM python:3.11-slim-bookworm
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
# 优化4：运行阶段不更新apt源（非必需，移除冗余操作；若需安装运行依赖再保留）
# 仅保留创建非root用户和目录权限配置
RUN useradd -m fastapi && \
    mkdir -p /app && \
    chown -R fastapi:fastapi /app && \
    # 新增：清理系统默认冗余文件（日志、缓存等）
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /var/tmp/*

USER fastapi

WORKDIR /app
ENV PATH="/home/fastapi/.local/bin:${PATH}"

# 修复核心1：将wheels复制到/app目录下（而非系统根目录）
# 【关键】直接复制builder已经安装好的python库，全程不出现wheels
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# 业务代码
COPY --chown=fastapi:fastapi src/ /app/src/
COPY --chown=fastapi:fastapi static/ /app/static/

EXPOSE 8001
# 优化8：使用exec格式启动命令，减少shell进程，间接减小镜像运行时内存占用（不影响镜像体积，优化附加价值）
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8001"]