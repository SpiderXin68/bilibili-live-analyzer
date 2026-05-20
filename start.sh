#!/bin/bash
# B站直播弹幕实时分析平台 - 启动脚本

cd "$(dirname "$0")"

echo "🚀 启动 B站直播弹幕实时分析平台..."
echo ""

# 检查 Python 依赖
python3 -c "import fastapi, uvicorn, aiohttp, aiosqlite" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "📦 安装 Python 依赖..."
    pip3 install -r backend/requirements.txt --break-system-packages
fi

# 创建 data 目录
mkdir -p data

echo "📺 服务地址: http://127.0.0.1:8000"
echo ""

# 启动服务
python3 backend/main.py "$@"
