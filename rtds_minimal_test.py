"""
rtds_minimal_test.py
====================
极简 Polymarket RTDS 诊断。
单连接，订阅 crypto_prices (Binance)，打印所有收到的消息。
用 recv() 显式接收，不用 async for 迭代器。

python3 rtds_minimal_test.py
"""
import asyncio
import json
import time
import sys

try:
    import websockets
except ImportError:
    print("pip install websockets"); sys.exit(1)

URL = "wss://ws-live-data.polymarket.com"

async def main():
    # 尝试两种订阅格式
    subs = [
        # 格式 1: Binance, 无 filters
        {"topic": "crypto_prices", "type": "update"},
        # 格式 2: Chainlink, 无 filters
        {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""},
    ]

    sub_msg = json.dumps({"action": "subscribe", "subscriptions": subs})

    print(f"连接 {URL} …")
    async with websockets.connect(
        URL,
        ping_interval=None,
        ping_timeout=None,
        open_timeout=15,
        max_size=2**20,
    ) as ws:
        print(f"✔ 已连接")
        print(f"→ 发送: {sub_msg}\n")
        await ws.send(sub_msg)

        msg_count = 0
        t0 = time.time()
        last_ping = t0

        while True:
            elapsed = time.time() - t0
            if elapsed > 120:  # 2 分钟够了
                print(f"\n⏱ 2 分钟到，共收 {msg_count} 条消息")
                break

            # 每 5 秒发 PING
            now = time.time()
            if now - last_ping >= 5:
                await ws.send("PING")
                last_ping = now

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                print(f"  [{elapsed:.0f}s] … (2s 无消息, 总收 {msg_count})", flush=True)
                continue

            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")

            if text == "PONG":
                # 不打印每个 PONG，太多了
                continue

            msg_count += 1
            ts_str = f"[{elapsed:.1f}s]"

            if len(text) > 1000:
                # 长消息（backfill），只打头尾
                print(f"{ts_str} msg#{msg_count} ({len(text)} chars): {text[:400]}…{text[-100:]}", flush=True)
            else:
                print(f"{ts_str} msg#{msg_count}: {text}", flush=True)

            # 解析看看结构
            try:
                obj = json.loads(text)
                topic = obj.get("topic", "?")
                mtype = obj.get("type", "?")
                has_payload = "payload" in obj
                has_data = "data" in obj or (isinstance(obj.get("payload"), dict) and "data" in obj["payload"])
                print(f"       → topic={topic} type={mtype} has_payload={has_payload} has_data={has_data}", flush=True)
            except Exception:
                print(f"       → (非 JSON)", flush=True)

    print("\n结束")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n中断")
