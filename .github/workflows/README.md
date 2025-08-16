# GitHub Actions 配置说明

## 工作流文件

### `build-latest.yml`
- **触发条件**: 当核心文件变更时自动构建
- **功能**: 构建并推送 `latest` 标签到 Docker Hub
- **镜像地址**: `docker.io/your-username/mongo-processor:latest`

## 触发文件
当以下文件变更时，推送到 `main` 分支会触发构建：
- `main.py` - 核心程序
- `dump.py` - MongoDB工具封装
- `Dockerfile` - 镜像配置
- `pyproject.toml` - 依赖配置
- `mongodb-database-tools/` - MongoDB工具目录
- `.github/workflows/build-latest.yml` - 工作流配置

## 配置步骤

### 1. 添加 GitHub Secrets
在 GitHub 仓库设置中添加：
| Secret 名称 | 说明 | 示例 |
|-------------|------|------|
| `DOCKER_USERNAME` | Docker Hub 用户名 | `your-username` |
| `DOCKER_PASSWORD` | Docker Hub 密码或访问令牌 | `your-password` |

### 2. 更新镜像名称
将工作流文件中的 `your-username` 替换为你的实际 Docker Hub 用户名：
- 在 `.github/workflows/build-latest.yml` 中
- 在 `build.sh` 脚本中

### 3. 验证结果
推送成功后，可以通过以下命令验证：
```bash
docker pull your-username/mongo-processor:latest
```

## 手动触发
在 GitHub Actions 页面手动运行工作流。

## 故障排除
- 检查 GitHub Secrets 是否正确配置
- 确保 Docker Hub 用户有权限推送
- 检查网络连接是否正常