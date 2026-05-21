"""
B站直播 WebSocket 采集器
基于 bilibili-api-python 库，处理协议细节和认证

依赖: pip install bilibili-api-python
"""
import asyncio
import json
import logging
import os
import time
from typing import Optional, Callable, Awaitable

from bilibili_api import Credential
from bilibili_api.live import LiveDanmaku

logger = logging.getLogger(__name__)

# Cookie 文件管理

def _find_cookie_file() -> Optional[str]:
    """自动查找 Cookie 文件"""
    candidates = [
        os.environ.get("BILI_COOKIE_FILE", ""),
        os.path.join(os.path.dirname(os.path.dirname(__file__)),
                     ".bilibili_cookie.txt"),
        os.path.join(os.getcwd(), ".bilibili_cookie.txt"),
        ".bilibili_cookie.txt",
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None


def _load_cookie_header() -> dict:
    """加载 Cookie 文件，返回 {Cookie: str}"""
    path = _find_cookie_file()
    if not path:
        return {}
    try:
        with open(path, "r") as f:
            content = f.read().strip()
        if content and ";" in content:
            logger.info(f"📄 加载 Cookie 文件: {path}")
            return {"Cookie": content}
    except Exception as e:
        logger.warning(f"读取 Cookie 文件失败: {e}")
    return {}


def _load_credential() -> Optional[Credential]:
    """从 Cookie 文件创建 Credential 对象"""
    path = _find_cookie_file()
    if not path:
        return None
    try:
        with open(path, "r") as f:
            content = f.read().strip()
        cookies = {}
        for pair in content.split("; "):
            if "=" in pair:
                k, v = pair.split("=", 1)
                cookies[k] = v
        return Credential(
            sessdata=cookies.get("SESSDATA", ""),
            bili_jct=cookies.get("bili_jct", ""),
            dedeuserid=cookies.get("DedeUserID", ""),
        )
    except Exception as e:
        logger.warning(f"创建 Credential 失败: {e}")
        return None


def _save_cookie_header(cookie_str: str) -> str:
    """保存 Cookie 字符串到文件"""
    from config import ROOT_DIR
    path = ROOT_DIR / ".bilibili_cookie.txt"
    with open(path, "w") as f:
        f.write(cookie_str.strip() + "\n")
    logger.info(f"💾 Cookie 已保存到: {path}")
    return str(path)


class BiliLiveCollector:
    """B站直播采集器 - 基于 bilibili-api"""

    def __init__(self, room_id: str):
        self.room_id = int(room_id)
        self._running = False
        self._dm: Optional[LiveDanmaku] = None
        self._task: Optional[asyncio.Task] = None

        # 回调
        self.on_danmaku: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_gift: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_super_chat: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_like: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_enter: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_popularity: Optional[Callable[[int]], Awaitable[None]] = None
        self.on_connected: Optional[Callable[[], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[], Awaitable[None]]] = None

    async def get_room_info(self) -> dict:
        """获取房间信息"""
        try:
            import aiohttp
            from bilibili_api.live import LiveRoom
            cred = _load_credential()
            room = LiveRoom(self.room_id, cred) if cred else LiveRoom(self.room_id)
            info = await room.get_room_info()
            room_info = info.get("room_info", {})

            # 粉丝数需要单独从旧 API 获取
            attention = 0
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://api.live.bilibili.com/room/v1/Room/get_info",
                        params={"room_id": self.room_id},
                        headers={"User-Agent": "Mozilla/5.0"},
                    ) as resp:
                        data = await resp.json()
                        if data.get("code") == 0:
                            attention = data["data"].get("attention", 0)
            except Exception:
                pass

            return {
                "title": room_info.get("title", ""),
                "live_status": room_info.get("live_status", 0),
                "online": room_info.get("online", 0),
                "attention": attention,
                "area_name": room_info.get("area_name", ""),
                "tags": room_info.get("tags", ""),
            }
        except Exception as e:
            logger.warning(f"获取房间信息失败: {e}")
            return {}

    async def _run(self):
        """运行采集器"""
        cred = _load_credential()
        if not cred:
            logger.error("❌ 未找到 Cookie，请先扫码登录")
            return

        self._dm = LiveDanmaku(self.room_id, credential=cred, debug=False)

        # 注册事件处理
        # bilibili-api 事件格式: callback_info = {room_display_id, room_real_id, type, data}
        # 实际数据在 callback_info["data"] 中

        @self._dm.on("DANMU_MSG")
        async def on_danmaku(cb):
            try:
                raw = cb.get("data", {}) if isinstance(cb, dict) else {}
                info = raw.get("info", [])
                if isinstance(info, list) and len(info) >= 3 and self.on_danmaku:
                    text = info[1] if len(info) > 1 else ""
                    uid = info[2][0] if len(info) > 2 and isinstance(info[2], list) else 0
                    username = info[2][1] if len(info) > 2 and isinstance(info[2], list) else ""
                    dm_type = info[0][1] if isinstance(info[0], list) and len(info[0]) > 1 else 0
                    logger.debug(f"收到弹幕: {username}: {text}")
                    await self.on_danmaku({
                        "uid": uid,
                        "username": username,
                        "text": text,
                        "dm_type": dm_type,
                    })
                elif isinstance(info, dict):
                    # DANMU_MSG 格式变化：info 可能是一个 dict
                    logger.debug(f"DANMU_MSG info is dict: {str(info)[:200]}")
            except Exception as e:
                logger.error(f"处理弹幕事件失败: {e}")

        @self._dm.on("SEND_GIFT")
        async def on_gift(cb):
            raw = cb.get("data", {}) if isinstance(cb, dict) else {}
            d = raw.get("data", raw) if isinstance(raw, dict) else {}
            if self.on_gift:
                await self.on_gift({
                    "uid": d.get("uid", 0),
                    "username": d.get("uname", ""),
                    "gift_name": d.get("giftName", ""),
                    "gift_id": d.get("giftId", 0),
                    "price": d.get("price", 0),
                    "num": d.get("num", 1),
                    "total_price": d.get("total_coin", 0),
                })

        @self._dm.on("SUPER_CHAT_MESSAGE")
        async def on_sc(cb):
            raw = cb.get("data", {}) if isinstance(cb, dict) else {}
            d = raw.get("data", raw) if isinstance(raw, dict) else {}
            if self.on_super_chat:
                await self.on_super_chat({
                    "uid": d.get("uid", 0),
                    "username": d.get("user_info", {}).get("uname", ""),
                    "text": d.get("message", ""),
                    "price": d.get("price", 0),
                    "keep_time": d.get("time", 0),
                })

        @self._dm.on("LIKE_INFO_V3_CLICK")
        async def on_like(cb):
            raw = cb.get("data", {}) if isinstance(cb, dict) else {}
            d = raw.get("data", raw) if isinstance(raw, dict) else {}
            if self.on_like:
                await self.on_like({
                    "uid": d.get("uid", 0),
                    "username": d.get("uname", ""),
                    "like_count": 1,
                })

        @self._dm.on("INTERACT_WORD")
        async def on_interact(cb):
            raw = cb.get("data", {}) if isinstance(cb, dict) else {}
            d = raw.get("data", raw) if isinstance(raw, dict) else {}
            if d.get("msg_type") == 1 and self.on_enter:
                await self.on_enter({
                    "uid": d.get("uid", 0),
                    "username": d.get("uname", ""),
                })

        # 心跳回复 -> 人气值（事件名是 VIEW 不是 _HEARTBEAT）
        @self._dm.on("VIEW")
        async def on_view(cb):
            # VIEW 的 data 字段直接是 int
            popularity = cb.get("data", 0) if isinstance(cb, dict) else 0
            if self.on_popularity:
                await self.on_popularity(popularity)

        try:
            logger.info(f"🔗 开始构建连接 {self.room_id}...")
            # 连接前触发 on_connected 事件通知上层准备就绪
            if self.on_connected:
                asyncio.create_task(self.on_connected())
            await self._dm.connect()
            logger.info(f"💡 直播间 {self.room_id} 连接已正常关闭")
        except Exception as e:
            logger.error(f"采集器运行时异常: {e}")
            if self.on_disconnected:
                await self.on_disconnected()
            raise
        finally:
            self._running = False
            try:
                await self._dm.disconnect()
            except Exception:
                pass

    async def run(self):
        """启动采集器"""
        self._running = True
        self._task = asyncio.create_task(self._run())
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"采集器已停止: {e}")
        finally:
            self._running = False

    async def stop(self):
        """停止采集器"""
        self._running = False
        if self._dm:
            try:
                await self._dm.disconnect()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
        logger.info(f"🛑 已断开房间 {self.room_id}")
