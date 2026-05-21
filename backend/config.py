"""
B站直播弹幕实时分析平台 - 配置
"""
import os
from pathlib import Path

# 项目根目录
ROOT_DIR = Path(__file__).resolve().parent.parent

# 数据目录
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# SQLite 数据库
DB_PATH = str(DATA_DIR / "live_analytics.db")

# 默认直播间（优先读取环境变量，支持 --room 命令行覆盖）
DEFAULT_ROOM_ID = os.environ.get("DEFAULT_ROOM_ID", "23255738")

# Bilibili API
BILI_API_GET_DANMU_INFO = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo"
BILI_API_ROOM_INFO = "https://api.live.bilibili.com/room/v1/Room/get_info"
BILI_WS_HOST = "broadcastlv.chat.bilibili.com"

# Protover: 0=plain json, 1=heartbeat_int, 2=zlib, 3=brotli
# Use 2 (zlib) to avoid brotli dependency
BILI_PROTO_VER = 2

# WebSocket 采集配置
WS_HEARTBEAT_INTERVAL = 30  # 心跳间隔（秒）
WS_RECONNECT_DELAY = 3      # 重连延迟（秒）
WS_MAX_RECONNECT_DELAY = 60 # 最大重连延迟（秒）

# 服务器配置
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000

# 分析配置
KEYWORDS = [
    "666", "牛逼", "主播", "哈哈哈", "加油", "好听",
    "233", "笑死", "真的", "不是", "卧槽", "来了",
    "冲冲冲", "哈人", "绷不住了", "绝了", "逆天"
]
