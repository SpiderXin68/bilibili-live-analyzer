#!/usr/bin/env python3
"""
B站 Cookie 导出工具

从本地浏览器（Chrome/Edge）导出 B站 Cookie，保存到文件供采集器使用。

用法:
    # 使用默认浏览器自动导出
    python export_bilibili_cookie.py --output .bilibili_cookie.txt

    # 从远程调试端口的 Chrome/Edge 导出
    # 先启动浏览器: chrome.exe --remote-debugging-port=9222
    python export_bilibili_cookie.py --port 9222 --output .bilibili_cookie.txt

    # 手动输入 Cookie 字符串
    python export_bilibili_cookie.py --manual
"""
import argparse
import http.client
import json
import logging
import os
import re
import sys
import time
import urllib.request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cookie-export")

# 需要的关键 Cookie 名称
REQUIRED_COOKIES = ["buvid3", "b_lsid", "_uuid"]
LOGIN_COOKIES = ["SESSDATA", "bili_jct", "DedeUserID"]


def parse_cookie_file(filepath: str) -> dict:
    """从 Netscape Cookie 文件解析（curl -c 格式）"""
    cookies = {}
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7:
                name = parts[5]
                value = parts[6]
                cookies[name] = value
    return cookies


def cookies_to_header(cookies: dict) -> str:
    """将 Cookie 字典转为 HTTP Header 字符串"""
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


def extract_cookies_from_url(url: str) -> dict:
    """从 Chrome DevTools Protocol 获取 Cookie"""
    parsed = urllib.parse.urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
    conn.request(
        "GET", "/json/protocol",
        headers={"Content-Type": "application/json"}
    )
    resp = conn.getresponse()
    if resp.status != 200:
        logger.error(f"连接失败: {resp.status} {resp.reason}")
        return {}

    # 通过 CDP 获取所有 cookies
    # 实际上我们直接通过 http://localhost:PORT/json 获取可用页面
    conn.close()

    conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
    conn.request("GET", "/json")
    resp = conn.getresponse()
    data = json.loads(resp.read())
    conn.close()

    # 找一个 B站页面
    bili_targets = [t for t in data if "bilibili.com" in t.get("url", "")]
    if not bili_targets:
        logger.error("未找到 B站 页面，请先在浏览器中打开 https://www.bilibili.com/")
        return {}

    target = bili_targets[0]
    ws_url = target.get("webSocketDebuggerUrl", "")
    logger.info(f"找到页面: {target.get('title', '')[:40]}...")

    # 通过 WebSocket 执行 JS 获取 cookies
    # 使用简单的 HTTP 请求通过 /json 获取 cookie
    # 实际上我们可以直接读取 document.cookie
    import urllib.parse

    # 通过 CDP 执行 JS 获取 Cookie
    # ws = websocket.WebSocket()
    # ws.connect(ws_url)
    # 但这里为了简单，直接用 HTTP 接口
    # 使用 Chrome DevTools Protocol 的 Network.getAllCookies
    # 通过 WebSocket CDP 获取

    # 简化版：尝试通过 CDP HTTP 接口
    try:
        # 使用 /json/new?{url} 打开新标签页
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
        conn.request(
            "GET",
            f"/json/new?{urllib.parse.quote('https://live.bilibilib.com/')}",
        )
        resp = conn.getresponse()
        new_target = json.loads(resp.read())
        conn.close()

        # 等待页面加载
        time.sleep(2)

        # 读取新页面的 Cookie
        targets_resp = urllib.request.urlopen(f"{url}/json")
        all_targets = json.loads(targets_resp.read())

        # 找到我们的新页面
        for tgt in all_targets:
            if tgt.get("id") == new_target.get("id"):
                logger.info("打开的页面已获取到")

        # 关闭新标签页
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
        conn.request("GET", f"/json/close/{new_target.get('id')}")
        conn.close()

    except Exception as e:
        logger.warning(f"尝试自动获取 Cookie 失败: {e}")

    return {}


def export_cookies_from_browser(port: int = 9222) -> dict:
    """从远程调试端口导出 Cookie"""
    debug_url = f"http://127.0.0.1:{port}"

    try:
        # 检查 DevTools 是否可用
        resp = urllib.request.urlopen(f"{debug_url}/json/version", timeout=5)
        data = json.loads(resp.read())
        browser = data.get("Browser", "Unknown")
        logger.info(f"检测到浏览器: {browser}")
    except Exception as e:
        logger.error(f"无法连接到浏览器调试端口 {port}: {e}")
        logger.info("请先启动浏览器: chromium --remote-debugging-port=9222")
        return {}

    # 获取所有目标页面
    try:
        resp = urllib.request.urlopen(f"{debug_url}/json", timeout=5)
        targets = json.loads(resp.read())
    except Exception as e:
        logger.error(f"获取页面列表失败: {e}")
        return {}

    # 查找 B站页面
    bili_targets = [
        t for t in targets
        if t.get("url", "") and "bilibili.com" in t.get("url", "")
    ]

    if not bili_targets:
        logger.warning("未找到 B站 页面，尝试打开新页面...")
        try:
            import urllib.request
            open_url = f"{debug_url}/json/new?https://live.bilibili.com/"
            resp = urllib.request.urlopen(open_url, timeout=10)
            new_target = json.loads(resp.read())
            logger.info("等待页面加载...")
            time.sleep(5)

            # 重新获取
            resp = urllib.request.urlopen(f"{debug_url}/json", timeout=5)
            targets = json.loads(resp.read())
            bili_targets = [
                t for t in targets
                if t.get("url", "") and "bilibili.com" in t.get("url", "")
            ]

            # 关闭新页面
            try:
                urllib.request.urlopen(
                    f"{debug_url}/json/close/{new_target.get('id')}",
                    timeout=5
                )
            except Exception:
                pass
        except Exception as e:
            logger.error(f"打开页面失败: {e}")

    if not bili_targets:
        logger.error("仍无 B站 页面，请手动在浏览器中登录 https://www.bilibili.com/")
        return {}

    # 尝试通过 CDP 获取 Cookie
    target = bili_targets[0]
    ws_url = target.get("webSocketDebuggerUrl", "")

    if not ws_url:
        logger.error("无法获取 WebSocket Debugger URL")
        return {}

    # 使用 WebSocket 连接 CDP 获取 Cookie
    try:
        import asyncio
        import aiohttp

        async def get_cookies_via_cdp():
            # CDP WebSocket URL
            ws_parsed = ws_url.replace("ws://", "http://")
            # 使用 Fetch API 获取 cookie

            # 简化方法：通过发送 HTTP 请求注入脚本
            # 实际上用 aiohttp websocket

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    # 发送 CDP Runtime.evaluate 命令
                    msg_id = 1

                    # 先获取所有 cookies via document.cookie
                    cmd = json.dumps({
                        "id": msg_id,
                        "method": "Runtime.evaluate",
                        "params": {
                            "expression": "document.cookie",
                            "returnByValue": True,
                        }
                    })
                    await ws.send_str(cmd)

                    resp = await ws.receive(timeout=10)
                    data = json.loads(resp.data)
                    cookie_str = ""
                    if "result" in data and "result" in data["result"]:
                        cookie_str = data["result"]["result"].get("value", "")

                    # 解析 cookies
                    cookies = {}
                    for pair in cookie_str.split(";"):
                        pair = pair.strip()
                        if "=" in pair:
                            name, value = pair.split("=", 1)
                            cookies[name.strip()] = value.strip()

                    # 也尝试获取 localStorage 中的 buvid
                    msg_id = 2
                    cmd = json.dumps({
                        "id": msg_id,
                        "method": "Runtime.evaluate",
                        "params": {
                            "expression": "(function() { "
                            "  try { return localStorage.getItem('buvid3') || ''; } "
                            "  catch(e) { return ''; }"
                            "})()",
                            "returnByValue": True,
                        }
                    })
                    await ws.send_str(cmd)
                    resp = await ws.receive(timeout=5)
                    data = json.loads(resp.data)
                    if "result" in data and "result" in data["result"]:
                        buvid = data["result"]["result"].get("value", "")
                        if buvid and "buvid3" not in cookies:
                            cookies["buvid3"] = buvid

                    return cookies

            return {}

        cookies = asyncio.run(get_cookies_via_cdp())
        return cookies

    except ImportError:
        logger.warning("需要安装 aiohttp: pip install aiohttp")
    except Exception as e:
        logger.warning(f"通过 CDP 获取 Cookie 失败: {e}")
        # 从 URL 中尝试提取
        logger.info("请手动导出 Cookie（见下方说明）")

    return {}


def manual_input() -> dict:
    """手动输入 Cookie 字符串"""
    print("\n" + "=" * 60)
    print("请打开浏览器开发者工具 (F12) → 应用 (Application) → Cookie")
    print("复制 https://www.bilibili.com/ 的所有 Cookie")
    print("粘贴到下方（单行 Cookie 字符串格式）:")
    print("=" * 60)
    print()

    cookie_str = input("Cookie > ").strip()
    if not cookie_str:
        logger.error("未输入 Cookie")
        return {}

    cookies = {}
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" in pair:
            name, value = pair.split("=", 1)
            cookies[name.strip()] = value.strip()

    return cookies


def validate_cookies(cookies: dict) -> bool:
    """验证 Cookie 是否包含必要字段"""
    has_guest = any(k in cookies for k in REQUIRED_COOKIES)
    has_login = any(k in cookies for k in LOGIN_COOKIES)

    if has_login:
        logger.info("✅ 包含登录 Cookie (SESSDATA)")
        return True
    elif has_guest:
        logger.warning("⚠️ 仅包含访客 Cookie，部分 API 可能受限")
        return True
    else:
        logger.error("❌ Cookie 格式无效，缺少必要字段")
        logger.info(f"需要: {', '.join(REQUIRED_COOKIES + LOGIN_COOKIES)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="B站 Cookie 导出工具")
    parser.add_argument("--output", "-o",
                        default=os.path.join(os.path.dirname(__file__),
                                             "..", ".bilibili_cookie.txt"),
                        help="Cookie 输出路径 (默认: .bilibili_cookie.txt)")
    parser.add_argument("--port", "-p", type=int, default=9222,
                        help="浏览器远程调试端口 (默认: 9222)")
    parser.add_argument("--manual", "-m", action="store_true",
                        help="手动输入 Cookie 字符串")
    parser.add_argument("--browser", "-b", action="store_true",
                        help="从浏览器自动导出（需要 Chrome/Edge 远程调试端口）")
    args = parser.parse_args()

    output_path = os.path.abspath(args.output)
    cookies = {}

    if args.manual:
        cookies = manual_input()
    elif args.browser:
        cookies = export_cookies_from_browser(args.port)
    else:
        # 交互式选择
        print("\n选择 Cookie 获取方式:")
        print("  1) 从浏览器自动导出 (需要 Chrome/Edge 远程调试端口)")
        print("  2) 手动输入 Cookie 字符串")
        print("  3) 生成访客 Cookie（受限）")
        choice = input("\n请选择 [1/2/3] (默认 2): ").strip() or "2"

        if choice == "1":
            print("\n确保浏览器已启动并开启远程调试端口:")
            print("  Windows Edge (推荐):")
            print('    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe"')
            print('      --remote-debugging-port=9222')
            print('      --user-data-dir="%TEMP%\\bili-debug"')
            print("  登录 https://www.bilibili.com/ 后按 Enter 继续...")
            input()
            cookies = export_cookies_from_browser(args.port)
        elif choice == "3":
            # 生成访客 Cookie
            import uuid
            cookies = {
                "buvid3": f"XM{uuid.uuid4().hex[:16].upper()}INFOC",
                "buvid4": uuid.uuid4().hex,
            }
            logger.info("生成访客 Cookie")
        else:
            cookies = manual_input()

    if not cookies:
        logger.error("未能获取 Cookie")
        sys.exit(1)

    if not validate_cookies(cookies):
        logger.warning("Cookie 可能不完整，仍将保存")
        if input("继续保存？(y/N): ").lower() != "y":
            sys.exit(1)

    # 写入文件
    header_str = cookies_to_header(cookies)
    with open(output_path, "w") as f:
        f.write(header_str + "\n")

    logger.info(f"✅ Cookie 已保存到: {output_path}")
    logger.info(f"   包含 {len(cookies)} 个 Cookie 字段")
    logger.info(f"   {'✅ 含登录凭证' if any(k in cookies for k in LOGIN_COOKIES) else '⚠️ 仅访客凭证'}")

    # 验证 API 可用性
    print("\n验证 API 可用性...")
    import urllib.request
    req = urllib.request.Request(
        "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo"
        f"?id=1762765173&type=0",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Cookie": header_str,
        }
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        if data.get("code") == 0:
            logger.info("✅ API 验证通过！可以连接 WebSocket")
            token = data.get("data", {}).get("token", "")
            logger.info(f"   Token: {token[:20]}...")
        else:
            logger.warning(f"⚠️ API 返回 code={data.get('code')}: {data.get('message')}")
            logger.info("Cookie 可能已过期，请重新登录导出")
    except Exception as e:
        logger.warning(f"⚠️ API 验证失败: {e}")


if __name__ == "__main__":
    main()
