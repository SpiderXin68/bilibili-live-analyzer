"""
B站直播弹幕实时分析平台 - 配置
"""
import os
from pathlib import Path


class Settings:
    """应用配置类

    用显式的对象属性代替模块级全局变量 + os.environ 修改，
    避免隐式副作用 (Side Effect) 和多模块耦合。
    """

    def __init__(self):
        # 项目路径
        self.ROOT_DIR = Path(__file__).resolve().parent.parent
        self.DATA_DIR = self.ROOT_DIR / "data"
        self.DATA_DIR.mkdir(exist_ok=True)

        # SQLite 数据库
        self.DB_PATH = str(self.DATA_DIR / "live_analytics.db")

        # 默认直播间（可通过 --room 命令行覆盖）
        self.DEFAULT_ROOM_ID = "23255738"

        # Bilibili API
        self.BILI_API_GET_DANMU_INFO = (
            "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo"
        )
        self.BILI_API_ROOM_INFO = (
            "https://api.live.bilibili.com/room/v1/Room/get_info"
        )
        self.BILI_WS_HOST = "broadcastlv.chat.bilibili.com"

        # Protover: 0=plain json, 1=heartbeat_int, 2=zlib, 3=brotli
        self.BILI_PROTO_VER = 2

        # WebSocket 采集配置
        self.WS_HEARTBEAT_INTERVAL = 30
        self.WS_RECONNECT_DELAY = 3
        self.WS_MAX_RECONNECT_DELAY = 60

        # 服务器配置
        self.SERVER_HOST = "0.0.0.0"
        self.SERVER_PORT = 8000

        # 分析配置
        self.KEYWORDS = [
            "666", "牛逼", "主播", "哈哈哈", "加油", "好听",
            "233", "笑死", "真的", "不是", "卧槽", "来了",
            "冲冲冲", "哈人", "绷不住了", "绝了", "逆天",
        ]


# 全局单例
settings = Settings()

# ── 向下兼容导出（使用 settings 对象替换旧模块级常量） ──
ROOT_DIR = settings.ROOT_DIR
DATA_DIR = settings.DATA_DIR
DB_PATH = settings.DB_PATH
DEFAULT_ROOM_ID = settings.DEFAULT_ROOM_ID
BILI_API_GET_DANMU_INFO = settings.BILI_API_GET_DANMU_INFO
BILI_API_ROOM_INFO = settings.BILI_API_ROOM_INFO
BILI_WS_HOST = settings.BILI_WS_HOST
BILI_PROTO_VER = settings.BILI_PROTO_VER
WS_HEARTBEAT_INTERVAL = settings.WS_HEARTBEAT_INTERVAL
WS_RECONNECT_DELAY = settings.WS_RECONNECT_DELAY
WS_MAX_RECONNECT_DELAY = settings.WS_MAX_RECONNECT_DELAY
SERVER_HOST = settings.SERVER_HOST
SERVER_PORT = settings.SERVER_PORT
KEYWORDS = settings.KEYWORDS
