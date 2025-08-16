#!/bin/bash

# MongoDB 数据迁移工具构建脚本

set -e

echo "🚀 MongoDB 数据迁移工具构建脚本"
echo "================================"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Docker Hub 配置
DOCKER_REGISTRY="docker.io"
IMAGE_NAME="your-username/mongo-processor"
IMAGE_TAG="latest"
SKIP_PUSH="${SKIP_PUSH:-false}"

# 检查命令是否存在
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# 检查 uv
if ! command_exists uv; then
    echo -e "${RED}❌ uv 未安装${NC}"
    echo "请安装 uv:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# 检查 Docker
if ! command_exists docker; then
    echo -e "${YELLOW}⚠️  Docker 未安装，跳过 Docker 构建${NC}"
    SKIP_DOCKER=true
else
    SKIP_DOCKER=false
fi

echo -e "${GREEN}✅ 环境检查通过${NC}"

# 使用 uv 同步依赖
echo "📦 同步依赖..."
uv sync

# 验证项目可以运行
echo "🔍 验证项目..."
uv run python -c "import main; print('项目验证通过')"

# 构建 Docker 镜像
if [ "$SKIP_DOCKER" = false ]; then
    echo "🐳 构建 Docker 镜像..."
    docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .
    echo -e "${GREEN}✅ Docker 镜像构建完成${NC}"

    # 检查是否需要推送
    if [ "$SKIP_PUSH" = "true" ]; then
        echo -e "${YELLOW}⏭️  跳过推送${NC}"
    else
        # 检查是否已登录
        if ! docker info | grep -q "harbor.yunjizhizao.com"; then
            echo -e "${YELLOW}⚠️  未检测到 Harbor 登录${NC}"
            echo "请先登录: docker login harbor.yunjizhizao.com"
            echo -e "${YELLOW}跳过推送，仅构建镜像${NC}"
        else
            # 推送到 Harbor
            echo "📤 推送到 Harbor..."
            FULL_IMAGE_NAME="${HARBOR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"

            # 打标签
            echo "🏷️  打标签: ${FULL_IMAGE_NAME}"
            docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${FULL_IMAGE_NAME}

            # 推送
            echo "🚀 推送到 ${HARBOR_REGISTRY}..."
            docker push ${FULL_IMAGE_NAME}

            echo -e "${GREEN}✅ 推送完成！${NC}"
            echo "镜像地址: ${FULL_IMAGE_NAME}"
        fi
    fi
else
    echo -e "${YELLOW}⏭️  跳过 Docker 构建${NC}"
fi

echo ""
echo -e "${GREEN}🎉 构建完成！${NC}"
echo ""
echo "使用方法:"
echo "  本地运行: uv run python main.py"
echo "  Docker运行: docker run -v \$(pwd)/config.ini:/app/config.ini mongo-processor:latest"
echo "  Docker Hub镜像: docker run -v \$(pwd)/config.ini:/app/config.ini ${DOCKER_REGISTRY}/${IMAGE_NAME}:latest"
echo ""
echo "配置说明:"
echo "  1. 复制 config.ini.sample 为 config.ini"
echo "  2. 修改 config.ini 中的数据库连接信息"
echo "  3. MongoDB 数据库工具已包含在项目中"
echo ""
echo "推送选项:"
echo "  仅构建不推送: SKIP_PUSH=true ./build.sh"
echo "  登录 Docker Hub: docker login docker.io"
