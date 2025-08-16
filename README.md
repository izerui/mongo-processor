# MongoDB 数据迁移工具

> 工具下载地址(注意要与mongodb版本对应): https://www.mongodb.com/try/download/database-tools
> 版本对应: https://www.mongodb.com/docs/database-tools/mongodump/

下载地址：
https://fastdl.mongodb.org/tools/db/mongodb-database-tools-macos-arm64-100.9.0.zip
其他版本可以类似修改版本号和平台标志解决下载地址找不到的问题

## 环境准备

本项目使用 [uv](https://docs.astral.sh/uv/) 进行依赖管理。

### MongoDB 数据库工具

项目已包含 MongoDB 数据库工具 **100.13.0** 版本，支持以下平台：
- Windows x86_64
- macOS ARM64
- Linux x86_64

工具已打包在 `mongodb-database-tools/` 目录中，无需额外下载。

### 安装 uv
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 设置项目
```bash
# 克隆项目后，安装依赖
uv sync

# 激活虚拟环境
source .venv/bin/activate  # macOS/Linux
# 或
.venv\Scripts\activate     # Windows
```

## 配置

在根目录下创建 `config.ini`:
```ini
[global]
databases=code2,ddd,xxx

[source]
host=161.189.137.33
port=27171
username=business
password=xxx

[target]
host=118.145.81.22
port=3717
username=root
password=xxx
```

## 运行

### 使用 uv 运行
```bash
uv run python main.py
```

### 或直接运行
```bash
python main.py
```

## 开发

### 添加开发依赖
```bash
uv add --dev pytest
```

### 平台支持
- ✅ Windows 10/11 (x86_64)
- ✅ macOS (Intel & Apple Silicon)
- ✅ Linux (x86_64 & ARM64)
- ✅ Docker (自动包含所需工具)

### 添加运行时依赖
```bash
uv add pymongo
```

## 部署指南

### 构建并推送
```bash
# 构建并推送到私有仓库
./build.sh

# 从私有仓库拉取运行
docker pull harbor.yunjizhizao.com/library/mongo-processor:latest
docker run -v $(pwd)/config.ini:/app/config.ini harbor.yunjizhizao.com/library/mongo-processor:latest
```

### 更新依赖
```bash
uv lock
```

### 推送选项
```bash
./build.sh          # 构建并推送
SKIP_PUSH=true ./build.sh  # 仅构建不推送
```

### 导出 requirements.txt
```bash
uv export --format requirements-txt > requirements.txt
```
