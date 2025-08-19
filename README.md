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
# 线程池并发配置（同时处理的数据库数量）
maxThreads = 4
# 单库导出导入并行处理collection的并发数
numParallelCollections = 8
# 每个集合的插入工作线程数
numInsertionWorkersPerCollection = 8


## 目录结构

导出后的目录结构如下（mongodump默认行为）：

```
dumps/
├── 数据库名1/
│   ├── collection1.bson.gz
│   ├── collection1.metadata.json.gz
│   ├── collection2.bson.gz
│   └── collection2.metadata.json.gz
├── 数据库名2/
│   ├── collection3.bson.gz
│   ├── collection3.metadata.json.gz
│   └── ...
```

**重要说明：**
- mongodump会在`--out`指定的目录下自动创建数据库名子目录
- 因此最终路径为：`dumps/数据库名/文件`
- 每个数据库作为一个整体导出，所有集合的文件直接放在数据库目录下


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

### 部署指南
### 构建并推送
```bash
# 构建并推送到 Docker Hub
./build.sh

# 从 Docker Hub 拉取运行
docker pull your-username/mongo-processor:latest
docker run -v $(pwd)/config.ini:/app/config.ini your-username/mongo-processor:latest
```

### 更新依赖
```bash
uv lock
```

### 推送选项
```bash
./build.sh          # 构建并推送到 Docker Hub
SKIP_PUSH=true ./build.sh  # 仅构建不推送
```

### GitHub Actions 自动构建
项目已配置 GitHub Actions，支持智能触发：

#### 触发条件
当以下文件变更时，推送到 `main` 分支自动构建：
- `main.py`, `dump.py` - 核心代码
- `Dockerfile` - 镜像配置
- `pyproject.toml` - 依赖配置
- `mongodb-database-tools/` - MongoDB工具

#### 镜像地址
`docker.io/your-username/mongo-processor:latest`

#### 配置步骤
1. 在 GitHub 仓库设置中添加 Secrets：
   - `DOCKER_USERNAME`: Docker Hub 用户名
   - `DOCKER_PASSWORD`: Docker Hub 密码
2. 修改相关文件并推送即可自动构建

#### 手动触发
在 GitHub Actions 页面手动运行工作流
### 导出 requirements.txt
```bash
uv export --format requirements-txt > requirements.txt
```
