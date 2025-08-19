FROM python:3.12-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 安装 uv
RUN pip install uv

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
ENV LOG_LEVEL=INFO


# 复制 MongoDB 数据库工具
COPY mongodb-database-tools/ ./mongodb-database-tools/

# 复制项目文件
COPY pyproject.toml .
COPY uv.lock .
COPY src/ ./src/
COPY config.ini.sample ./config.ini
COPY README.md .

# 使用 uv 安装项目
RUN uv sync --frozen

# 创建 dumps 目录（如果不存在）
RUN mkdir -p /app/dumps

# 运行命令
CMD ["uv", "run", "python", "src/main.py"]
