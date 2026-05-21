#!/usr/bin/env python3
"""
B站直播弹幕实时分析平台 - 启动入口

用法:
    python main.py                    # 启动服务 (默认端口 8000)
    python main.py --port 8080       # 自定义端口
    python main.py --room 123456     # 自定义房间号
"""
import argparse
import logging
import sys
import os

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("aiosqlite").setLevel(logging.WARNING)

from server import app, SERVER_HOST, SERVER_PORT
from config import settings


def main():
    parser = argparse.ArgumentParser(
        description="B站直播弹幕实时分析平台")
    parser.add_argument("--host", type=str, default=SERVER_HOST,
                        help=f"监听地址 (默认: {SERVER_HOST})")
    parser.add_argument("--port", type=int, default=SERVER_PORT,
                        help=f"监听端口 (默认: {SERVER_PORT})")
    parser.add_argument("--room", type=str, default=None,
                        help="默认直播间 ID (覆盖 config.py)")
    parser.add_argument("--debug", action="store_true",
                        help="调试模式 (自动重载)")
    args = parser.parse_args()

    # 通过显式的 settings 对象注入运行时参数，而非修改 os.environ
    # ✅ 避免隐式副作用，利于测试和并发安全
    if args.room:
        settings.DEFAULT_ROOM_ID = args.room

    import uvicorn
    uvicorn.run(
        "server:app",
        host=args.host,
        port=args.port,
        reload=args.debug,
        log_level="debug" if args.debug else "info",
    )


if __name__ == "__main__":
    main()
