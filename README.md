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
numParallelCollections = 16
# 每个集合的插入工作线程数
numInsertionWorkersPerCollection = 8

# 分片相关配置
# 是否启用分片功能（默认true）
enableSharding = true
# 触发分片的最小文档数量（默认100万）
minDocumentsForShard = 1000000
# 默认分片数量（默认4）
defaultShardCount = 4
# 最大分片数量（默认16）
maxShardCount = 16
# 分片导出并发数（默认4）
shardConcurrency = 4


## 分片功能

### ObjectId 分片导出

本工具支持对大型collection进行ObjectId分片导出，提供以下特性：

**自动分片检测：**
- 自动检测超过阈值的collection（默认100万文档）
- 智能计算最优分片数量
- 支持手动配置分片参数

**并行分片导出：**
- 按ObjectId时间戳范围分片
- 每个分片独立并行导出
- 自动生成分片文件命名

**分片数据管理：**
- 分片文件命名格式：`collection_shard_001.bson`
- 自动生成分片元数据文件：`collection_shards.json`
- 支持导入时自动识别和合并分片

### 分片配置说明

```ini
# 是否启用分片功能
enableSharding = true

# 触发分片的最小文档数量（默认100万）
minDocumentsForShard = 1000000

# 默认分片数量（建议2-8个）
defaultShardCount = 4

# 最大分片数量（防止过度分片）
maxShardCount = 16

# 分片导出并发数（建议不超过CPU核心数）
shardConcurrency = 4
```

## 目录结构

### 常规导出结构：
```
dumps/
├── 数据库名1/
│   ├── collection1.bson.gz
│   ├── collection1.metadata.json.gz
│   ├── collection2.bson.gz
│   └── collection2.metadata.json.gz
```

### 分片导出结构：
```
dumps/
├── 数据库名1/
│   ├── small_collection.bson.gz          # 小collection（常规导出）
│   ├── small_collection.metadata.json.gz
│   ├── large_collection_shard_000.bson   # 大collection分片文件
│   ├── large_collection_shard_000.metadata.json
│   ├── large_collection_shard_001.bson
│   ├── large_collection_shard_001.metadata.json
│   ├── large_collection_shard_002.bson
│   ├── large_collection_shard_002.metadata.json
│   ├── large_collection_shard_003.bson
│   ├── large_collection_shard_003.metadata.json
│   └── large_collection_shards.json      # 分片元数据文件
```

**重要说明：**
- mongodump会在`--out`指定的目录下自动创建数据库名子目录
- 分片文件使用序号命名：`collection_shard_000`, `collection_shard_001` 等
- 分片元数据文件包含分片范围和恢复信息
- 导入时会自动识别分片并合并到目标collection


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

### 测试分片功能
```bash
# 运行分片功能测试
uv run python src/test_shard.py
```

## 分片功能使用示例

### 基本用法
分片功能默认启用，无需额外配置。工具会自动：

1. **检测大型collection**：文档数量超过阈值时自动启用分片
2. **计算最优分片数**：根据数据量智能确定分片数量
3. **并行导出分片**：多个分片同时导出，提升性能
4. **自动合并导入**：导入时识别分片文件并自动合并

### 高级配置
```ini
[global]
# 关闭分片功能（使用传统导出）
enableSharding = false

# 调整分片阈值（50万文档触发分片）
minDocumentsForShard = 500000

# 设置固定分片数量
defaultShardCount = 8

# 限制最大分片数量
maxShardCount = 32

# 提高分片并发数（适用于高性能服务器）
shardConcurrency = 8
```

### 性能优化建议

**分片数量选择：**
- 小型collection（< 100万文档）：不分片
- 中型collection（100万-500万文档）：2-4个分片
- 大型collection（500万-2000万文档）：4-8个分片
- 超大collection（> 2000万文档）：8-16个分片

**并发配置：**
- `shardConcurrency`：建议设置为CPU核心数的50%-75%
- `numParallelCollections`：控制同时处理的collection数
- `numInsertionWorkersPerCollection`：影响导入性能

**最佳实践：**
- 在非业务高峰期进行分片导出
- 监控磁盘I/O和网络带宽使用情况
- 大型数据库建议先测试小部分数据

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
