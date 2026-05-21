# 📺 B站直播弹幕实时分析平台

实时采集 B站直播弹幕、礼物、SC、进入等事件，支持可视化大屏展示。

## 技术栈

| 层级 | 技术 |
|------|------|
| 采集 | Python + bilibili-api-python（封装 aiohttp + WebSocket + Brotli 解压） |
| 存储 | SQLite (零配置，无需 Redis) |
| 后端 | FastAPI + WebSocket 实时推送 |
| 前端 | Vue 3 + ECharts (CDN 加载，无构建步骤) |

## 快速开始

### 1. 安装依赖

```bash
# Python 依赖
pip install fastapi uvicorn aiohttp aiosqlite brotli
```

### 2. 启动服务

```bash
# 从项目根目录
python backend/main.py

# 或指定端口和房间
python backend/main.py --port 8080 --room 1762765173
```

### 3. 打开页面

[http://127.0.0.1:8000](http://127.0.0.1:8000)

## Cookie 配置（必需）

B站从 2024 年起要求 Cookie 认证才能连接 WebSocket。需要导出你的 B站 Cookie：

### 自动导出（推荐）

1. 先启动一个带远程调试端口的浏览器：

**Windows Edge:**
```powershell
& "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" `
  --remote-debugging-port=9222 `
  --user-data-dir="$env:TEMP\bili-debug"
```

**Chrome:**
```bash
google-chrome --remote-debugging-port=9222
```

2. 访问 [https://www.bilibili.com/](https://www.bilibili.com/) 并登录

3. 运行导出工具：
```bash
python backend/export_bilibili_cookie.py --browser --port 9222
```

### 手动导出

1. 打开浏览器, 访问 [https://www.bilibili.com/](https://www.bilibili.com/) 并登录
2. 按 F12 打开开发者工具 → Application → Cookies → bilibili.com
3. 将所有 Cookie 拼成 `key1=value1; key2=value2` 格式

```bash
python backend/export_bilibili_cookie.py --manual
```

Cookie 会保存到项目根目录的 `.bilibili_cookie.txt`，重启服务即可生效。

## 截图

（等你跑起来截图加上 😄）

## 项目结构

```
danmu-analyzer/
├── backend/
│   ├── main.py                 # 启动入口
│   ├── server.py               # FastAPI 服务 + WebSocket 推流
│   ├── collector.py            # B站 WebSocket 采集器
│   ├── storage.py              # SQLite 存储层
│   ├── config.py               # 配置文件
│   ├── export_bilibili_cookie.py  # Cookie 导出工具
│   └── requirements.txt
├── frontend/
│   └── index.html              # Vue3 + ECharts 单页应用
├── data/                       # 数据库存储目录（自动创建）
├── .bilibili_cookie.txt        # Cookie 文件（需自行导出）
├── start.sh
├── docker-compose.yml
└── README.md
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 前端页面 |
| GET | `/api/status` | 服务状态 |
| GET | `/api/stats` | 聚合统计数据 |
| GET | `/api/danmaku` | 弹幕历史 |
| GET | `/api/events` | 事件列表 |
| GET | `/api/metrics` | 房间指标时序 |
| GET | `/api/event-distribution` | 事件分布 |
| POST | `/api/room/{id}/connect` | 切换直播间 |
| WS | `/ws` | 实时数据推流 |

## 自定义配置

编辑 `backend/config.py`：

```python
DEFAULT_ROOM_ID = "1762765173"   # 默认房间
KEYWORDS = ["666", "牛逼", ...]  # 关键词列表
SERVER_PORT = 8000               # 服务端口
```

## 常见问题

**Q: 页面打开后没有数据？**
A: 还没导出 Cookie。B站现在需要登录凭证才能获取弹幕流。参考上面 Cookie 配置部分。

**Q: 不用 Redis 可以吗？**
A: 可以！本项目使用 SQLite 替代 Redis，零配置开箱即用。

**Q: 数据库文件在哪？**
A: `data/live_analytics.db`，会自动创建。

**Q: 支持多房间吗？**
A: 通过 `/api/room/{id}/connect` 切换房间，前端也支持输入房间号切换。

## License

MIT
