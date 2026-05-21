"""
SQLite 存储层 - 弹幕、事件、房间指标持久化
不依赖 Redis，零配置开箱即用

高并发优化：
- 批量写入 (add_*_bulk) 供 server.py 的异步队列消费者调用
- get_danmaku_density 改用 SQL GROUP BY 直接聚合，免除 Python 双重循环
- get_active_users_density 同样利用 SQL 分桶，减少内存压力
- _process_keywords 接入 jieba 中文分词，词云数据真实有效
"""
import asyncio
import json
import logging
import time
from typing import Optional

import aiosqlite

from config import DB_PATH

logger = logging.getLogger(__name__)


def _process_keywords(rows: list, min_len: int, top_n: int) -> list:
    """
    纯 CPU 计算：中文词频统计（在线程池中执行）

    使用 jieba 精确模式分词，替代原 haskell 风格的 split() 分词，
    因为 B 站弹幕是纯中文，空格分割对中文无效。
    预加载 jieba 词典可能影响首次调用，之后极快。
    """
    import jieba
    from collections import Counter

    jieba.setLogLevel(logging.WARNING)

    counter = Counter()
    for (text,) in rows:
        words = jieba.lcut(text)
        for w in words:
            w = w.strip()
            if len(w) >= min_len and not w.isdigit():
                counter[w] += 1

    return [{"name": w, "value": c}
            for w, c in counter.most_common(top_n)]


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

    # ── 批量写入（高频场景核心入口） ──

    async def add_messages_bulk(self, records: list) -> int:
        """
        批量插入弹幕（一个事务，一次 commit）

        record 格式：(room_id, uid, username, text, msg_type, timestamp, extra_json)
        """
        if not records:
            return 0
        async with self._lock:
            await self.db.executemany(
                """INSERT INTO messages
                   (room_id, uid, username, text, msg_type, timestamp, extra)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                records
            )
            await self.db.commit()
            return len(records)

    async def add_events_bulk(self, records: list) -> int:
        """
        批量插入事件

        record 格式：(room_id, event_type, uid, username, content_json, timestamp)
        """
        if not records:
            return 0
        async with self._lock:
            await self.db.executemany(
                """INSERT INTO live_events
                   (room_id, event_type, uid, username, content, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                records
            )
            await self.db.commit()
            return len(records)

    async def add_metrics_bulk(self, records: list) -> int:
        """
        批量插入房间指标

        record 格式：(room_id, timestamp, online, attention, live_status, title, area_name)
        """
        if not records:
            return 0
        async with self._lock:
            await self.db.executemany(
                """INSERT INTO room_metrics
                   (room_id, timestamp, online, attention, live_status, title, area_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                records
            )
            await self.db.commit()
            return len(records)

    # ── 单条写入（保留向后兼容，但高频路径建议走批量） ──

    async def add_message(self, room_id: str, uid: int, username: str,
                          text: str, msg_type: str = "danmaku",
                          extra: dict = None) -> int:
        """添加一条弹幕"""
        return await self.add_messages_bulk([
            (int(room_id), uid, username, text, msg_type,
             time.time(), json.dumps(extra or {}, ensure_ascii=False))
        ])

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
        return await self.add_events_bulk([
            (int(room_id), event_type, uid, username,
             json.dumps(content or {}, ensure_ascii=False), time.time())
        ])

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
        return await self.add_metrics_bulk([
            (int(room_id), time.time(), online, attention,
             live_status, title, area_name)
        ])

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
        """获取高频关键词（CPU密集计算→线程池，避免阻塞事件循环）"""
        cursor = await self.db.execute(
            """SELECT text FROM messages
               WHERE room_id = ? AND timestamp >= ? AND msg_type = 'danmaku'""",
            (int(room_id), since)
        )
        rows = await cursor.fetchall()

        # jieba 分词已接入，卸载到线程池
        return await asyncio.to_thread(
            _process_keywords, rows, min_len, top_n)

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
        """
        获取弹幕密度时序（用于曲线图）

        ✅ SQL 级 GROUP BY 聚合，免除 Python 双重循环遍历全量数据。
        在大数据量下性能提升数个数量级。
        """
        cursor = await self.db.execute(
            """SELECT (CAST(timestamp AS INT) / ?) * ? AS bucket_start,
                      COUNT(*) AS cnt
               FROM messages
               WHERE room_id = ? AND timestamp >= ? AND msg_type = 'danmaku'
               GROUP BY bucket_start
               ORDER BY bucket_start ASC""",
            (bucket_seconds, bucket_seconds, int(room_id), since)
        )
        rows = await cursor.fetchall()
        return [{"time": r["bucket_start"], "count": r["cnt"]} for r in rows]

    async def get_active_users_density(
        self, room_id: str, since: float = 0,
        bucket_seconds: int = 60
    ) -> dict:
        """
        获取各时段活跃用户数和活跃比

        ✅ SQL 级聚合：活跃用户数、进入人数、平均在线全部交给数据库统计，
        避免 Python 双循环遍历全部行数据。

        返回: {
            "density": [{"time": ts, "active": n, "enter": n, "online": avg, "ratio": r}, ...]
            "total_online_avg": x
        }
        """
        if since == 0:
            since = time.time() - 3600

        # 1. 互动用户（弹幕+礼物+SC+点赞）按时间桶聚合
        cursor = await self.db.execute(
            """SELECT (CAST(timestamp AS INT) / ?) * ? AS bucket_start,
                      COUNT(DISTINCT username) AS active_cnt
               FROM (
                   SELECT username, timestamp FROM messages
                   WHERE room_id = ? AND timestamp >= ? AND username != ''
                   UNION ALL
                   SELECT username, timestamp FROM live_events
                   WHERE room_id = ? AND timestamp >= ?
                   AND event_type IN ('gift', 'super_chat', 'like')
                   AND username != ''
               )
               GROUP BY bucket_start
               ORDER BY bucket_start ASC""",
            (bucket_seconds, bucket_seconds,
             int(room_id), since,
             int(room_id), since)
        )
        active_rows = {r["bucket_start"]: r["active_cnt"]
                       for r in await cursor.fetchall()}

        # 2. 进入人数按时间桶聚合
        cursor = await self.db.execute(
            """SELECT (CAST(timestamp AS INT) / ?) * ? AS bucket_start,
                      COUNT(DISTINCT username) AS enter_cnt
               FROM live_events
               WHERE room_id = ? AND timestamp >= ?
               AND event_type = 'enter' AND username != ''
               GROUP BY bucket_start
               ORDER BY bucket_start ASC""",
            (bucket_seconds, bucket_seconds, int(room_id), since)
        )
        enter_rows = {r["bucket_start"]: r["enter_cnt"]
                      for r in await cursor.fetchall()}

        # 3. 在线人数按时间桶取均值
        cursor = await self.db.execute(
            """SELECT (CAST(timestamp AS INT) / ?) * ? AS bucket_start,
                      AVG(online) AS avg_online
               FROM room_metrics
               WHERE room_id = ? AND timestamp >= ?
               GROUP BY bucket_start
               ORDER BY bucket_start ASC""",
            (bucket_seconds, bucket_seconds, int(room_id), since)
        )
        online_rows = {r["bucket_start"]: round(r["avg_online"])
                       for r in await cursor.fetchall()}

        # 4. 合并所有时间桶
        all_keys = set(active_rows) | set(enter_rows) | set(online_rows)
        if not all_keys:
            return {"density": [], "total_online_avg": 0}

        density = []
        for key in sorted(all_keys):
            active = active_rows.get(key, 0)
            enter = enter_rows.get(key, 0)
            avg_online = online_rows.get(key, 0)
            ratio = round(active / avg_online, 4) if avg_online > 0 else 0
            density.append({
                "time": key,
                "active": active,
                "enter": enter,
                "online": avg_online,
                "ratio": ratio,
            })

        # 5. 全局平均在线
        cursor = await self.db.execute(
            "SELECT AVG(online) FROM room_metrics WHERE room_id = ? AND timestamp >= ?",
            (int(room_id), since)
        )
        row = await cursor.fetchone()
        total_online_avg = round(row[0]) if row and row[0] else 0

        return {
            "density": density,
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
