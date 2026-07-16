# 多阶段构建：第一阶段用于构建依赖（精简构建环境，减少冗余）
FROM python:3.11-slim AS builder

# 优化1：精简apt安装，仅保留必要构建工具（原build-essential包含冗余组件，按需缩减）
RUN sed -i \
    -e 's|deb.debian.org|mirrors.ustc.edu.cn|g' \
    -e 's|security.debian.org|mirrors.ustc.edu.cn|g' \
    /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/cache/apt/archives/*

WORKDIR /app
COPY requirements.txt .

# 优化2：精简wheel构建参数，移除无效配置，明确torch CPU源（避免构建GPU依赖）
# 原--no-index在已有-i指定源时可保留，但补充torch CPU额外索引，确保纯CPU包构建
RUN pip wheel -i https://pypi.mirrors.ustc.edu.cn/simple/ \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    --trusted-host pypi.mirrors.ustc.edu.cn \
    --trusted-host download.pytorch.org \
    --default-timeout=600 \
    --retries=10 \
    --no-cache-dir \
    --wheel-dir /app/wheels \
    --prefer-binary \
    --no-deps \
    -r requirements.txt

# 优化3：构建阶段提前清理wheel包冗余（如.dist-info中的无用文件，可选）
RUN find /app/wheels -name "*.whl" -exec sh -c 'unzip -q {} -d temp_wheel && rm -rf temp_wheel/*/docs temp_wheel/*/tests && zip -q -r {} temp_wheel && rm -rf temp_wheel' \; || true


# 阶段2：运行环境（核心精简，最大化压缩体积）
FROM python:3.11-slim

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
# 路径：/app/wheels（fastapi有权限），同时--chown确保文件归属
COPY --from=builder --chown=fastapi:fastapi /app/wheels /app/wheels
COPY --from=builder --chown=fastapi:fastapi /app/requirements.txt /app/requirements.txt

# 修复核心2：安装时指向/app/wheels，删除时也指向该路径
RUN pip install \
    -i https://pypi.tuna.tsinghua.edu.cn/simple/ \
    --trusted-host pypi.tuna.tsinghua.edu.cn \
    --no-cache-dir \
    --find-links /app/wheels \
    -r requirements.txt && \
    # 全面清理冗余文件
    find /home/fastapi/.local \
        \( -name "__pycache__" -o -name "*.pyc" -o -name "*.pyo" -o -name "*.pyd" \) \
        -type f -delete && \
    find /home/fastapi/.local \
        \( -name "__pycache__" -o -name "tests" -o -name "test" -o -name "docs" -o -name "examples" -o -name "example" \) \
        -type d -prune -exec rm -rf {} + && \
    rm -rf /home/fastapi/.cache /tmp/* && \
    rm -rf /app/wheels

# 优化7：仅复制必要项目文件，配合.dockerignore进一步排除无用文件
COPY --chown=fastapi:fastapi src/ /app/src/
COPY --chown=fastapi:fastapi static/ /app/static/

EXPOSE 8001
# 优化8：使用exec格式启动命令，减少shell进程，间接减小镜像运行时内存占用（不影响镜像体积，优化附加价值）
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8001"]