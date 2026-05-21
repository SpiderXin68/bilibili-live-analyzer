"""
FastAPI 服务器
- REST API: 数据查询
- WebSocket: 实时推送到前端

高并发优化：
- 写入全走异步队列（Queue），1s 积攒后 bulk insert，避免单条 commit 阻塞
- 广播改用 asyncio.gather 并发发送，消除队头阻塞（Head-of-Line Blocking）
- 所有高频回调只做广播（零等待），写库由后台消费者汇总
"""
import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Optional

import aiohttp

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from collector import BiliLiveCollector
from storage import Storage
from config import settings

# 强制 bilibili-api 使用 aiohttp 而非 curl_cffi
# curl_cffi 在 Python 3.14 下 C 扩展层兼容性不佳，会段错误
import bilibili_api.utils.network as bili_net
bili_net.select_client("aiohttp")

logger = logging.getLogger(__name__)

# ── 全局实例 ──
storage = Storage()
current_collector: Optional[BiliLiveCollector] = None
collector_task: Optional[asyncio.Task] = None
metrics_task: Optional[asyncio.Task] = None

# 全局 aiohttp ClientSession（应用生命周期管理，避免频繁 TCP 握手）
http_session: Optional[aiohttp.ClientSession] = None

# WebSocket 连接池
frontend_connections: set = set()

# ── 异步写入队列 ──
# 弹幕/事件/指标不直接写库，而是打入内存队列
# database_writer_loop 每 1s 积攒后批量 executemany + 一次 commit
message_queue: asyncio.Queue = asyncio.Queue()
event_queue: asyncio.Queue = asyncio.Queue()
metric_queue: asyncio.Queue = asyncio.Queue()


async def broadcast(data: dict):
    """
    向所有前端广播消息

    优化：使用 asyncio.gather 并发发送，避免顺序发送时某个慢连接
    阻塞全部（队头阻塞 Head-of-Line Blocking）。
    """
    if not frontend_connections:
        return
    msg = json.dumps(data, ensure_ascii=False)

    # ✅ 一次性拍快照，避免两次迭代 set 出现竞态错位
    conns = list(frontend_connections)
    tasks = [ws.send_text(msg) for ws in conns]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    dead = set()
    for ws, result in zip(conns, results):
        if isinstance(result, Exception):
            dead.add(ws)
    if dead:
        frontend_connections.difference_update(dead)


async def database_writer_loop():
    """
    后台常驻：异步队列消费者

    每 1s 或积攒满 200 条时，从三个队列 drain 数据，
    批量写入数据库（一个事务一次 commit），大幅降低磁盘 I/O。
    """
    while True:
        try:
            await asyncio.sleep(1.0)

            # 攒够 3 种队列的数据
            # ⚠️ 必须用 get_nowait()，不可先 empty() 再 get():
            #    empty() 返回后协程可能被切换，item 被取走，
            #    此时 get() 会永远阻塞 ⇒ 写入消费者死锁
            msg_batch = []
            while not message_queue.empty() and len(msg_batch) < 200:
                try:
                    msg_batch.append(message_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            evt_batch = []
            while not event_queue.empty() and len(evt_batch) < 200:
                try:
                    evt_batch.append(event_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            met_batch = []
            while not metric_queue.empty() and len(met_batch) < 50:
                try:
                    met_batch.append(metric_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if msg_batch:
                await storage.add_messages_bulk(msg_batch)
            if evt_batch:
                await storage.add_events_bulk(evt_batch)
            if met_batch:
                await storage.add_metrics_bulk(met_batch)

        except Exception as e:
            logger.error(f"批量写库异常: {e}")


# ── 事件回调（只广播 + 入队，不直接写库） ──

async def record_danmaku(data: dict):
    """处理弹幕：广播 + 入队（不直接写库）"""
    room_id = str(current_collector.room_id) if current_collector \
              else settings.DEFAULT_ROOM_ID

    # 广播（零延迟推送大屏）
    await broadcast({
        "type": "danmaku",
        "data": {
            "uid": data.get("uid", 0),
            "username": data.get("username", ""),
            "text": data.get("text", ""),
            "time": time.time(),
        }
    })

    # 入队待批量写入
    await message_queue.put((
        int(room_id),
        data.get("uid", 0),
        data.get("username", ""),
        data.get("text", ""),
        "danmaku",
        time.time(),
        json.dumps({"dm_type": data.get("dm_type", 0)},
                    ensure_ascii=False),
    ))


async def record_gift(data: dict):
    """处理礼物：广播 + 入队"""
    room_id = str(current_collector.room_id) if current_collector \
              else settings.DEFAULT_ROOM_ID

    await broadcast({
        "type": "gift",
        "data": {
            "username": data.get("username", ""),
            "gift_name": data.get("gift_name", ""),
            "num": data.get("num", 1),
            "price": data.get("price", 0),
        }
    })

    await event_queue.put((
        int(room_id),
        "gift",
        data.get("uid", 0),
        data.get("username", ""),
        json.dumps(data, ensure_ascii=False),
        time.time(),
    ))


async def record_super_chat(data: dict):
    """处理 SC：广播 + 入队"""
    room_id = str(current_collector.room_id) if current_collector \
              else settings.DEFAULT_ROOM_ID

    await broadcast({
        "type": "super_chat",
        "data": {
            "username": data.get("username", ""),
            "text": data.get("text", ""),
            "price": data.get("price", 0),
        }
    })

    await event_queue.put((
        int(room_id),
        "super_chat",
        data.get("uid", 0),
        data.get("username", ""),
        json.dumps(data, ensure_ascii=False),
        time.time(),
    ))


async def record_like(data: dict):
    """处理点赞：广播 + 入队"""
    room_id = str(current_collector.room_id) if current_collector \
              else settings.DEFAULT_ROOM_ID

    await broadcast({
        "type": "like",
        "data": {"username": data.get("username", "")}
    })

    await event_queue.put((
        int(room_id),
        "like",
        data.get("uid", 0),
        data.get("username", ""),
        json.dumps(data, ensure_ascii=False),
        time.time(),
    ))


async def record_enter(data: dict):
    """处理进入：广播 + 入队"""
    room_id = str(current_collector.room_id) if current_collector \
              else settings.DEFAULT_ROOM_ID

    await broadcast({
        "type": "enter",
        "data": {"username": data.get("username", "")}
    })

    await event_queue.put((
        int(room_id),
        "enter",
        data.get("uid", 0),
        data.get("username", ""),
        json.dumps(data, ensure_ascii=False),
        time.time(),
    ))


# ── 采集器控制 ──

async def start_collector(room_id: str):
    """启动/切换采集器"""
    global current_collector, collector_task, metrics_task

    # 停止当前采集器
    if current_collector:
        await current_collector.stop()
        if collector_task:
            collector_task.cancel()
            try:
                await collector_task
            except asyncio.CancelledError:
                pass
    # 终止旧指标采集任务，防止泄漏和数据污染
    if metrics_task:
        metrics_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass
        metrics_task = None

    # 创建新采集器 → 注入全局 http_session 复用 TCP 连接
    current_collector = BiliLiveCollector(room_id, session=http_session)

    # 注册回调
    current_collector.on_danmaku = record_danmaku
    current_collector.on_gift = record_gift
    current_collector.on_super_chat = record_super_chat
    current_collector.on_like = record_like
    current_collector.on_enter = record_enter

    async def on_popularity(pop: int):
        await broadcast({
            "type": "metrics",
            "data": {
                "online": pop,
                "time": time.time(),
            }
        })
    current_collector.on_popularity = on_popularity

    collector_task = asyncio.create_task(current_collector.run())

    # 启动房间指标采集（保存句柄防止泄漏）
    metrics_task = asyncio.create_task(collect_metrics_periodically(room_id))

    return current_collector


async def collect_metrics_periodically(room_id: str):
    """定期采集房间指标"""
    while True:
        try:
            if current_collector and current_collector._running:
                info = await current_collector.get_room_info()
                online = info.get("online", 0)
                attention = info.get("attention", 0)
                live_status = info.get("live_status", 0)
                title = info.get("title", "")
                area_name = info.get("area_name", "")

                # 入队批量写入
                await metric_queue.put((
                    int(room_id),
                    time.time(),
                    online, attention, live_status, title, area_name,
                ))

                await broadcast({
                    "type": "metrics",
                    "data": {
                        "online": online,
                        "attention": attention,
                        "live_status": live_status,
                        "title": title,
                        "area_name": area_name,
                        "time": time.time(),
                    }
                })
                logger.info(
                    f"📊 房间指标 - 在线:{online} 关注:{attention} "
                    f"状态:{'直播中' if live_status else '未开播'}"
                )
        except Exception as e:
            logger.warning(f"采集房间指标失败: {e}")
        await asyncio.sleep(60)


# ── FastAPI 应用 ──

@asynccontextmanager
async def lifespan(app: FastAPI):
    """生命周期"""
    global http_session

    logger.info("🚀 启动服务...")

    # 创建全局 aiohttp ClientSession（整个应用共享一个）
    http_session = aiohttp.ClientSession()
    logger.info("🔌 全局 HTTP Session 已创建")

    # 启动数据库批量写入消费者
    writer_task = asyncio.create_task(database_writer_loop())
    logger.info("📦 异步写入消费者已启动")

    await storage.init()

    # 自动启动默认房间
    logger.info(f"📺 自动连接房间: {settings.DEFAULT_ROOM_ID}")
    await start_collector(settings.DEFAULT_ROOM_ID)

    yield

    logger.info("🛑 关闭服务...")
    if current_collector:
        await current_collector.stop()
    if collector_task:
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass
    if metrics_task:
        metrics_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass

    # 排空队列：优雅停机，确保最后 1 秒的数据不丢失
    logger.info("📦 排空写入队列...")
    for queue, bulk_method in [
        (message_queue, storage.add_messages_bulk),
        (event_queue,   storage.add_events_bulk),
        (metric_queue,  storage.add_metrics_bulk),
    ]:
        remaining = []
        while not queue.empty():
            try:
                remaining.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            await bulk_method(remaining)
            logger.info(f"   ↪ 已写入 {len(remaining)} 条残留数据")

    writer_task.cancel()
    await storage.close()

    # 清理全局 HTTP Session
    if http_session and not http_session.closed:
        await http_session.close()
        logger.info("🔌 全局 HTTP Session 已关闭")


app = FastAPI(
    title="B站直播弹幕分析平台",
    description="实时弹幕数据采集、分析与可视化",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 静态文件 ──

from fastapi.staticfiles import StaticFiles
static_dir = settings.ROOT_DIR / "backend" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── 前端页面 ──

@app.get("/", response_class=HTMLResponse)
async def index():
    """前端主页面"""
    html_path = settings.ROOT_DIR / "frontend" / "index.html"
    if not html_path.exists():
        return "<h1>前端文件未找到</h1><p>请确保 frontend/index.html 存在</p>"
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


# ── Cookie 管理（B站扫码登录） ──


@app.get("/api/cookie/qrcode-image")
async def cookie_qrcode_image(url: str = Query(...)):
    """生成二维码图片（PNG），直接返回给前端"""
    from fastapi.responses import Response
    try:
        import qrcode
        from io import BytesIO
        qr = qrcode.QRCode(
            version=4,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=8,
            border=2,
        )
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = BytesIO()
        img.save(buf, format="PNG")
        return Response(
            content=buf.getvalue(),
            media_type="image/png",
            headers={"Cache-Control": "no-cache"},
        )
    except Exception as e:
        logger.error(f"生成二维码图片失败: {e}")
        return Response(status_code=500)


@app.get("/api/cookie/qrcode")
async def cookie_qrcode():
    """
    获取 B站 扫码登录二维码
    返回: {url (二维码图片地址), key (轮询密钥)}
    """

    try:
        session = http_session
        async with session.get(
            "https://passport.bilibili.com/x/passport-login/web/qrcode/generate",
            params={"source": "main-fe-header"},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/131.0.0.0 Safari/537.36",
            },
        ) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {"ok": False, "error": data.get("message", "生成二维码失败")}
            result = data["data"]
            return {
                "ok": True,
                "url": result["url"],
                "key": result["qrcode_key"],
            }
    except Exception as e:
        logger.error(f"生成二维码失败: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/api/cookie/poll")
async def cookie_poll(key: str = Query(...)):
    """轮询扫码登录状态"""
    from collector import _save_cookie_header
    try:
        session = http_session
        async with session.get(
            "https://passport.bilibili.com/x/passport-login/web/qrcode/poll",
            params={"qrcode_key": key},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/131.0.0.0 Safari/537.36",
            },
        ) as resp:
            data = await resp.json()

            # ⚠️ B站 QR 轮询接口返回: {"code":0, "data":{"code":86038, ...}}
            #    根级 code 永远为 0（HTTP 成功），实际状态在 data.data.code
            qr_data = data.get("data", {})
            qr_code = qr_data.get("code", -1) if data.get("code") == 0 else -1
            logger.info(f"扫码轮询: qr_code={qr_code}")

            if qr_code == 0:
                set_cookies = resp.headers.getall("Set-Cookie", [])
                if set_cookies:
                    cookie_parts = []
                    for sc in set_cookies:
                        parts = sc.split(";")[0]
                        if "=" in parts and "Path=" not in parts:
                            cookie_parts.append(parts)
                    cookie_str = "; ".join(cookie_parts)
                    _save_cookie_header(cookie_str)
                    logger.info("✅ 扫码登录成功，Cookie 已保存")

                    room_id = current_collector.room_id \
                        if current_collector else settings.DEFAULT_ROOM_ID
                    await start_collector(room_id)

                    return {
                        "ok": True,
                        "status": "ok",
                        "message": "登录成功！正在连接弹幕...",
                    }
                else:
                    logger.warning("Set-Cookie 为空")
                    return {
                        "ok": True,
                        "status": "partial",
                        "message": "登录成功但 Cookie 不完整，请手动导出",
                    }
            elif qr_code == 86038:
                return {"ok": False, "status": "expired",
                        "message": "二维码已失效，请重新获取"}
            elif qr_code == 86090:
                return {"ok": True, "status": "scanned",
                        "message": "已扫码，请在手机上确认"}
            else:
                return {"ok": True, "status": "pending",
                        "message": "等待扫码...", "code": qr_code}
    except Exception as e:
        logger.error(f"扫码轮询失败: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/api/cookie/status")
async def cookie_status():
    """检查 Cookie 状态"""
    from collector import _find_cookie_file, _load_cookie_header
    path = _find_cookie_file()
    if path:
        header = _load_cookie_header()
        has_login = "SESSDATA" in str(header) if header else False
        return {
            "exists": True,
            "path": path,
            "has_login": has_login,
            "cookie_preview": str(header)[:80] if header else None,
        }
    return {"exists": False, "has_login": False}


# ── REST API ──

@app.get("/api/status")
async def api_status():
    """服务状态"""
    from collector import _find_cookie_file, _load_cookie_header
    room_id = current_collector.room_id if current_collector else None
    has_cookie = bool(_find_cookie_file())
    has_login = False
    if has_cookie:
        header = _load_cookie_header()
        has_login = "SESSDATA" in str(header) if header else False
    running = current_collector and current_collector._running
    info = await current_collector.get_room_info() \
        if current_collector else {}
    return {
        "running": running,
        "has_cookie": has_cookie,
        "has_login": has_login,
        "room_id": room_id,
        "room_info": {
            "title": info.get("title", ""),
            "live_status": info.get("live_status", 0),
            "online": info.get("online", 0),
            "attention": info.get("attention", 0),
            "area_name": info.get("area_name", ""),
            "tags": info.get("tags", ""),
        },
        "connections": len(frontend_connections),
        "time": time.time(),
    }


@app.get("/api/room/{room_id}")
async def api_room_info(room_id: str):
    """获取指定房间信息"""
    collector = BiliLiveCollector(room_id, session=http_session)
    info = await collector.get_room_info()
    return {"room_id": room_id, "data": info}


@app.post("/api/room/{room_id}/connect")
async def api_connect_room(room_id: str):
    """切换到指定直播间"""
    if current_collector and current_collector.room_id == room_id:
        return {"status": "already_connected", "room_id": room_id}
    await start_collector(room_id)
    return {"status": "connected", "room_id": room_id}


@app.post("/api/disconnect")
async def api_disconnect():
    """断开当前连接"""
    if current_collector:
        await current_collector.stop()
    return {"status": "disconnected"}


@app.get("/api/danmaku")
async def api_danmaku(room_id: str = None, limit: int = 100):
    """获取弹幕历史"""
    rid = room_id or settings.DEFAULT_ROOM_ID
    return {"data": await storage.get_recent_messages(rid, limit=limit)}


@app.get("/api/events")
async def api_events(room_id: str = None, event_type: str = None,
                     limit: int = 50):
    """获取事件列表"""
    rid = room_id or settings.DEFAULT_ROOM_ID
    return {"data": await storage.get_recent_events(
        rid, event_type=event_type, limit=limit)}


@app.get("/api/event-distribution")
async def api_event_distribution(room_id: str = None, since: float = 0):
    """事件分布"""
    rid = room_id or settings.DEFAULT_ROOM_ID
    return {"data": await storage.get_event_distribution(rid, since=since)}


@app.get("/api/metrics")
async def api_metrics(room_id: str = None, since: float = 0):
    """房间指标时序"""
    rid = room_id or settings.DEFAULT_ROOM_ID
    return {"data": await storage.get_metrics(rid, since=since)}


@app.get("/api/stats")
async def api_stats(room_id: str = None, bucket: int = 60):
    """聚合统计（6路并发查询，总耗时 = 最慢的单一路径）"""
    rid = room_id or settings.DEFAULT_ROOM_ID
    since = time.time() - 3600

    results = await asyncio.gather(
        storage.get_total_interactions(rid, since=since),
        storage.get_top_users(rid, since=since, top_n=20),
        storage.get_top_phrases(rid, since=since, top_n=20),
        storage.get_top_keywords(rid, since=since, top_n=30),
        storage.get_danmaku_density(rid, since=since,
                                     bucket_seconds=bucket),
        storage.get_event_distribution(rid, since=since),
    )
    total, top_users, top_phrases, top_keywords, density, events = results

    return {
        "room_id": rid,
        "message_count": total["danmaku"],
        "total_interactions": total["total"],
        "event_counts": total,
        "top_users": top_users,
        "top_phrases": top_phrases,
        "top_keywords": top_keywords,
        "density": density,
        "event_distribution": events,
        "since": since,
    }


@app.get("/api/export")
async def api_export(room_id: str = None,
                     since: float = Query(0),
                     until: float = Query(0),
                     fmt: str = "json"):
    """导出弹幕数据"""
    from fastapi.responses import PlainTextResponse
    rid = room_id or settings.DEFAULT_ROOM_ID
    since_val = since or (time.time() - 3600)
    data = await storage.get_export_data(rid, since=since_val,
                                         until=until or time.time())
    if fmt == "csv":
        import csv, io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["时间", "用户ID", "用户名", "类型", "内容"])
        for r in data:
            w.writerow([r["time"], r["uid"], r["username"],
                        r["type"], r["text"]])
        return PlainTextResponse(
            buf.getvalue(),
            media_type="text/csv",
            headers={
                "Content-Disposition":
                f"attachment; filename=danmaku_export_{rid}.csv"
            },
        )
    return {"ok": True, "room_id": rid, "count": len(data), "data": data}


@app.get("/api/activity")
async def api_activity(room_id: str = None, bucket: int = 60):
    """活跃度和未进入人数统计"""
    rid = room_id or settings.DEFAULT_ROOM_ID
    since = time.time() - 3600
    data = await storage.get_active_users_density(
        rid, since=since, bucket_seconds=bucket)
    return {"room_id": rid, "bucket_seconds": bucket, **data}


# ── WebSocket 实时推送 ──

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 实时推流"""
    await websocket.accept()
    frontend_connections.add(websocket)
    logger.info(f"📱 前端连接: {websocket.client} "
                f"(共 {len(frontend_connections)} 个连接)")
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action", "")
                if action == "connect":
                    room_id = msg.get("room_id", "")
                    if room_id:
                        await start_collector(room_id)
                        await websocket.send_text(json.dumps({
                            "type": "status",
                            "data": {"room_id": room_id, "status": "connected"}
                        }))
                elif action == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"WebSocket 错误: {e}")
    finally:
        frontend_connections.discard(websocket)
        logger.info(f"📱 前端断开 (共 {len(frontend_connections)} 个连接)")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host=settings.SERVER_HOST,
        port=settings.SERVER_PORT,
        reload=False,
        log_level="info",
    )
