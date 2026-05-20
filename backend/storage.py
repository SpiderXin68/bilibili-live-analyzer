"""
SQLite 存储层 - 弹幕、事件、房间指标持久化
不依赖 Redis，零配置开箱即用
"""
import asyncio
import json
import logging
import time
from typing import Optional

import aiosqlite

from config import DB_PATH

logger = logging.getLogger(__name__)


class Storage:
    """异步 SQLite 存储"""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def init(self):
        """初始化数据库和表"""
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id INTEGER NOT NULL,
                uid INTEGER DEFAULT 0,
                username TEXT DEFAULT '',
                text TEXT NOT NULL,
                msg_type TEXT DEFAULT 'danmaku',
                timestamp REAL NOT NULL,
                extra TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS live_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                uid INTEGER DEFAULT 0,
                username TEXT DEFAULT '',
                content TEXT DEFAULT '{}',
                timestamp REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS room_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                online INTEGER DEFAULT 0,
                attention INTEGER DEFAULT 0,
                live_status INTEGER DEFAULT 0,
                title TEXT DEFAULT '',
                area_name TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages(room_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_room_ts ON live_events(room_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_metrics_room_ts ON room_metrics(room_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_type ON live_events(event_type);
        """)
        await self.db.commit()
        logger.info(f"✅ 数据库初始化: {self.db_path}")

    async def close(self):
        """关闭数据库"""
        if self.db:
            await self.db.close()

    # ── 弹幕 ──

    async def add_message(self, room_id: str, uid: int, username: str,
                          text: str, msg_type: str = "danmaku",
                          extra: dict = None) -> int:
        """添加一条弹幕"""
        async with self._lock:
            cursor = await self.db.execute(
                """INSERT INTO messages (room_id, uid, username, text, msg_type, timestamp, extra)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (int(room_id), uid, username, text, msg_type,
                 time.time(), json.dumps(extra or {}, ensure_ascii=False))
            )
            await self.db.commit()
            return cursor.lastrowid

    async def get_recent_messages(self, room_id: str, limit: int = 100,
                                  offset: int = 0) -> list:
        """获取最近弹幕"""
        cursor = await self.db.execute(
            """SELECT id, uid, username, text, msg_type, timestamp, extra
               FROM messages WHERE room_id = ?
               ORDER BY id DESC LIMIT ? OFFSET ?""",
            (int(room_id), limit, offset)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in reversed(rows)]

    async def get_message_count(self, room_id: str,
                                since: float = 0) -> int:
        """获取弹幕总数"""
        cursor = await self.db.execute(
            "SELECT COUNT(*) FROM messages WHERE room_id = ? AND timestamp >= ?",
            (int(room_id), since)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_messages_timeline(self, room_id: str,
                                    since: float, until: float) -> list:
        """获取时间范围内的弹幕（用于时序图）"""
        cursor = await self.db.execute(
            """SELECT timestamp FROM messages
               WHERE room_id = ? AND timestamp >= ? AND timestamp <= ?
               ORDER BY timestamp ASC""",
            (int(room_id), since, until)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── 事件（礼物/SC/点赞/进入/关注） ──

    async def add_event(self, room_id: str, event_type: str,
                        uid: int, username: str,
                        content: dict = None) -> int:
        """添加一条事件"""
        async with self._lock:
            cursor = await self.db.execute(
                """INSERT INTO live_events (room_id, event_type, uid, username, content, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (int(room_id), event_type, uid, username,
                 json.dumps(content or {}, ensure_ascii=False),
                 time.time())
            )
            await self.db.commit()
            return cursor.lastrowid

    async def get_recent_events(self, room_id: str, event_type: str = None,
                                limit: int = 50) -> list:
        """获取最近事件"""
        if event_type:
            cursor = await self.db.execute(
                """SELECT id, event_type, uid, username, content, timestamp
                   FROM live_events
                   WHERE room_id = ? AND event_type = ?
                   ORDER BY id DESC LIMIT ?""",
                (int(room_id), event_type, limit)
            )
        else:
            cursor = await self.db.execute(
                """SELECT id, event_type, uid, username, content, timestamp
                   FROM live_events
                   WHERE room_id = ?
                   ORDER BY id DESC LIMIT ?""",
                (int(room_id), limit)
            )
        rows = await cursor.fetchall()
        results = []
        for r in reversed(rows):
            d = dict(r)
            d["content"] = json.loads(d["content"])
            results.append(d)
        return results

    async def get_event_distribution(self, room_id: str,
                                     since: float = 0) -> dict:
        """获取事件分布统计"""
        cursor = await self.db.execute(
            """SELECT event_type, COUNT(*) as cnt
               FROM live_events
               WHERE room_id = ? AND timestamp >= ?
               GROUP BY event_type""",
            (int(room_id), since)
        )
        rows = await cursor.fetchall()
        return {r[0]: r[1] for r in rows}

    # ── 房间指标 ──

    async def add_metric(self, room_id: str, online: int = 0,
                         attention: int = 0, live_status: int = 0,
                         title: str = "", area_name: str = "") -> int:
        """添加一条房间指标"""
        async with self._lock:
            cursor = await self.db.execute(
                """INSERT INTO room_metrics (room_id, timestamp, online, attention, live_status, title, area_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (int(room_id), time.time(), online, attention,
                 live_status, title, area_name)
            )
            await self.db.commit()
            return cursor.lastrowid

    async def get_metrics(self, room_id: str, since: float = 0) -> list:
        """获取房间指标时序数据"""
        cursor = await self.db.execute(
            """SELECT timestamp, online, attention, live_status, title, area_name
               FROM room_metrics
               WHERE room_id = ? AND timestamp >= ?
               ORDER BY timestamp ASC""",
            (int(room_id), since)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── 聚合查询 ──

    async def get_top_keywords(self, room_id: str, since: float = 0,
                               min_len: int = 2, top_n: int = 30) -> list:
        """获取高频关键词（基于分词后的短语统计）"""
        cursor = await self.db.execute(
            """SELECT text FROM messages
               WHERE room_id = ? AND timestamp >= ? AND msg_type = 'danmaku'""",
            (int(room_id), since)
        )
        rows = await cursor.fetchall()

        from collections import Counter
        counter = Counter()
        for (text,) in rows:
            # 简单分词：按空格/标点分割，过滤短词和纯数字
            words = text.replace(",", " ").replace("，", " ") \
                        .replace("!", " ").replace("！", " ") \
                        .replace("?", " ").replace("？", " ") \
                        .replace(".", " ").replace("。", " ") \
                        .split()
            for w in words:
                w = w.strip()
                if len(w) >= min_len and not w.isdigit():
                    counter[w] += 1

        return [{"name": w, "value": c}
                for w, c in counter.most_common(top_n)]

    async def get_top_phrases(self, room_id: str, since: float = 0,
                              min_len: int = 2, top_n: int = 20) -> list:
        """获取高频弹幕原句"""
        cursor = await self.db.execute(
            """SELECT text, COUNT(*) as cnt FROM messages
               WHERE room_id = ? AND timestamp >= ? AND msg_type = 'danmaku'
               AND length(text) >= ?
               GROUP BY text ORDER BY cnt DESC LIMIT ?""",
            (int(room_id), since, min_len, top_n)
        )
        rows = await cursor.fetchall()
        return [{"text": r[0], "count": r[1]} for r in rows]

    async def get_top_users(self, room_id: str, since: float = 0,
                            top_n: int = 20) -> list:
        """获取活跃用户排行（弹幕+事件）"""
        cursor = await self.db.execute(
            """SELECT username, COUNT(*) as cnt FROM (
                   SELECT username, timestamp FROM messages
                   WHERE room_id = ? AND timestamp >= ? AND username != ''
                   UNION ALL
                   SELECT username, timestamp FROM live_events
                   WHERE room_id = ? AND timestamp >= ? AND username != ''
               ) GROUP BY username ORDER BY cnt DESC LIMIT ?""",
            (int(room_id), since, int(room_id), since, top_n)
        )
        rows = await cursor.fetchall()
        return [{"username": r[0], "count": r[1]} for r in rows]

    async def get_total_interactions(self, room_id: str,
                                     since: float = 0) -> dict:
        """获取总互动量"""
        cursor = await self.db.execute(
            "SELECT COUNT(*) FROM messages WHERE room_id = ? AND timestamp >= ?",
            (int(room_id), since))
        msgs = (await cursor.fetchone())[0]

        cursor = await self.db.execute(
            """SELECT event_type, COUNT(*) FROM live_events
               WHERE room_id = ? AND timestamp >= ?
               GROUP BY event_type""",
            (int(room_id), since))
        events = {r[0]: r[1] for r in await cursor.fetchall()}

        return {
            "danmaku": msgs,
            "total": msgs + sum(events.values()),
            **events,
        }

    async def get_danmaku_density(self, room_id: str, since: float = 0,
                                  bucket_seconds: int = 60) -> list:
        """获取弹幕密度时序（用于曲线图）"""
        cursor = await self.db.execute(
            """SELECT timestamp FROM messages
               WHERE room_id = ? AND timestamp >= ? AND msg_type = 'danmaku'
               ORDER BY timestamp ASC""",
            (int(room_id), since)
        )
        rows = await cursor.fetchall()
        timestamps = [r[0] for r in rows]
        if not timestamps:
            return []

        # 按 bucket 聚合
        min_ts = timestamps[0]
        max_ts = timestamps[-1]
        buckets = []
        bucket_start = min_ts
        while bucket_start < max_ts:
            bucket_end = bucket_start + bucket_seconds
            count = sum(1 for t in timestamps if bucket_start <= t < bucket_end)
            buckets.append({
                "time": bucket_start,
                "count": count
            })
            bucket_start = bucket_end
        return buckets

    async def get_active_users_density(self, room_id: str, since: float = 0,
                                         bucket_seconds: int = 60) -> dict:
        """
        获取各时段活跃用户数和活跃比
        返回: {
            "density": [{"time": ts, "active": n, "total": m, "ratio": r}, ...]
            "total_online_avg": x  (时段平均在线人数)
        }
        """
        # 1. 获取互动用户（弹幕+礼物+SC+点赞）
        cursor = await self.db.execute(
            """SELECT username, timestamp FROM messages
               WHERE room_id = ? AND timestamp >= ? AND username != ''
               UNION ALL
               SELECT username, timestamp FROM live_events
               WHERE room_id = ? AND timestamp >= ?
               AND event_type IN ('gift', 'super_chat', 'like')
               AND username != ''""",
            (int(room_id), since, int(room_id), since)
        )
        rows = await cursor.fetchall()

        # 2. 获取进入人数（唯一访客）
        cursor2 = await self.db.execute(
            """SELECT username, timestamp FROM live_events
               WHERE room_id = ? AND timestamp >= ?
               AND event_type = 'enter' AND username != ''""",
            (int(room_id), since)
        )
        enter_rows = await cursor2.fetchall()

        # 3. 获取在线人数时序
        metrics = await self.get_metrics(room_id, since=since)

        if not rows and not metrics:
            return {"density": [], "total_online_avg": 0}

        # 确定时间范围
        all_ts = [r[1] for r in rows] + [r[1] for r in enter_rows]
        if metrics:
            all_ts.extend(m["timestamp"] for m in metrics)
        if not all_ts:
            return {"density": [], "total_online_avg": 0}

        min_ts = min(all_ts)
        max_ts = max(all_ts)

        result = []
        bucket_start = min_ts
        while bucket_start < max_ts:
            bucket_end = bucket_start + bucket_seconds

            # 活跃用户（互动的）
            active_users = set(
                r[0] for r in rows
                if bucket_start <= r[1] < bucket_end
            )

            # 进入用户
            enter_users = set(
                r[0] for r in enter_rows
                if bucket_start <= r[1] < bucket_end
            )

            # 时段平均在线
            online_values = [
                m["online"] for m in metrics
                if bucket_start <= m["timestamp"] < bucket_end
            ]
            avg_online = round(
                sum(online_values) / len(online_values)
            ) if online_values else 0

            active_count = len(active_users)
            enter_count = len(enter_users)
            ratio = round(active_count / avg_online, 4) \
                if avg_online > 0 else 0

            result.append({
                "time": bucket_start,
                "active": active_count,
                "enter": enter_count,
                "online": avg_online,
                "ratio": ratio,
            })
            bucket_start = bucket_end

        total_online_avg = round(
            sum(m["online"] for m in metrics) / len(metrics)
        ) if metrics else 0

        return {
            "density": result,
            "total_online_avg": total_online_avg,
        }

    async def get_export_data(self, room_id: str,
                              since: float = 0, until: float = 0,
                              limit: int = 10000) -> list:
        """导出弹幕数据（时间、用户、内容等）"""
        until = until or time.time()
        cursor = await self.db.execute(
            """SELECT datetime(timestamp, 'unixepoch') as time_str,
                      username, text, uid, msg_type, timestamp
               FROM messages
               WHERE room_id = ? AND timestamp >= ? AND timestamp <= ?
               ORDER BY timestamp ASC LIMIT ?""",
            (int(room_id), since, until, limit)
        )
        rows = await cursor.fetchall()
        return [{
            "time": r[0],
            "username": r[1] or "",
            "text": r[2] or "",
            "uid": r[3],
            "type": r[4],
            "timestamp": r[5],
        } for r in rows]

    async def delete_old_data(self, keep_seconds: int = 86400 * 7):
        """清理旧数据（默认保留7天）"""
        cutoff = time.time() - keep_seconds
        async with self._lock:
            for table in ["messages", "live_events", "room_metrics"]:
                await self.db.execute(
                    f"DELETE FROM {table} WHERE timestamp < ?", (cutoff,))
            await self.db.commit()
