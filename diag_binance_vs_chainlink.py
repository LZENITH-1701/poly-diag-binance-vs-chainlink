"""
analysis/diag_binance_vs_chainlink.py
=====================================
只读诊断 —— 同时订 Polymarket RTDS 两个 topic，被动收集数据：
  - crypto_prices              (btcusdt, Binance)
  - crypto_prices_chainlink    (btc/usd, Chainlink Data Streams，Polymarket 结算源)

不改任何生产代码、不触碰 strategy.py / main.py / Postgres。
跑完后输出：
  1. 瞬时价格 basis 分布 (USD signed / USD abs / bps)
  2. 两源 5-min 滚动动量的方向一致率
  3. 两源 5-min 动量差值分布
  4. $60 阈值 cross-divergence 率（**策略交易集分化率**，这是我们真正关心的指标）
  5. 活跃度：每源的 inter-tick gap 分布
  6. 原始 ticks CSV（analysis/diag_out/ticks_*.csv），可以之后离线再分析

用法：
  cd "<poly-s2-final>"
  python3 analysis/diag_binance_vs_chainlink.py --minutes 30
  python3 analysis/diag_binance_vs_chainlink.py --minutes 60

Ctrl+C 提前结束也会保存 + 分析现有数据。
"""
import argparse
import asyncio
import bisect
import csv
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

try:
    import websockets
except ImportError:
    print("缺依赖: pip install websockets", file=sys.stderr)
    sys.exit(1)

# ── 常量 ────────────────────────────────────────────────────────
RTDS_URL                  = "wss://ws-live-data.polymarket.com"
# Polymarket 两个 topic 用不同的 symbol 命名空间：
#   - crypto_prices            → "btcusdt" (Binance 风格, 小写拼接)
#   - crypto_prices_chainlink  → "btc/usd" (Chainlink 风格, 小写带斜杠)
BINANCE_SYMBOL            = "btcusdt"
CHAINLINK_SYMBOL          = "btc/usd"
MOMENTUM_WINDOW_SEC       = 300    # 5 分钟
MOMENTUM_SAMPLE_EVERY_SEC = 15     # 每 15s 采一次动量
THRESHOLD_USD             = 60.0   # 与 strategy.py 的 momentum_threshold_usd 对齐
BASIS_PAIR_WINDOW_SEC     = 1.0    # 配对两源 tick 的最大时间差
PING_EVERY_SEC            = 5      # per Polymarket 文档


# ─────────────────────────────────────────────
# TickCollector —— 一个 topic 一个实例
# ─────────────────────────────────────────────
class TickCollector:
    def __init__(self, topic: str, label: str, symbol: str,
                 sub_type: str, filter_str: str):
        """
        sub_type   : 订阅时的 "type" 字段
        filter_str : 订阅时的 "filters" 字段（已经是最终字符串，不再 json.dumps）
        """
        self.topic      = topic
        self.label      = label
        self.symbol     = symbol
        self.sub_type   = sub_type
        self.filter_str = filter_str
        # (recv_ts_unix, price, msg_type)
        self.ticks: list[tuple[float, float, str]] = []
        self.connected  = False
        self.msg_count  = 0

    async def run(self, stop_event: asyncio.Event):
        # ── 关键：两个 topic 的订阅格式完全不同 ──
        # Binance  : type="update", filters="btcusdt"          (逗号分隔纯字符串)
        # Chainlink: type="*",      filters='{"symbol":"btc/usd"}'  (JSON 字符串)
        sub_payload: dict = {
            "topic": self.topic,
            "type":  self.sub_type,
        }
        if self.filter_str:
            sub_payload["filters"] = self.filter_str

        subscribe_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [sub_payload]
        })

        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    RTDS_URL,
                    origin="https://polymarket.com",
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=10,
                    max_size=2**20,
                ) as ws:
                    await ws.send(subscribe_msg)
                    self.connected = True
                    print(f"[{self.label}] ✔ connected, subscribed to {self.topic}")
                    print(f"[{self.label}]   subscribe_msg = {subscribe_msg}")

                    # 应用层 PING（每 5s，per 文档）
                    async def app_heartbeat():
                        while not stop_event.is_set():
                            try:
                                await ws.send("PING")
                            except Exception:
                                return
                            await asyncio.sleep(PING_EVERY_SEC)

                    async def wait_and_close():
                        await stop_event.wait()
                        try:
                            await ws.close()
                        except Exception:
                            pass

                    hb_task   = asyncio.create_task(app_heartbeat())
                    stop_task = asyncio.create_task(wait_and_close())

                    try:
                        async for raw in ws:
                            if stop_event.is_set():
                                break
                            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                            if text == "PONG":
                                continue
                            self.msg_count += 1
                            if self.msg_count <= 5:
                                print(f"[{self.label}] raw #{self.msg_count}: {text[:600]}", flush=True)
                            self._parse_and_store(text)
                    finally:
                        hb_task.cancel()
                        stop_task.cancel()
            except Exception as e:
                self.connected = False
                if stop_event.is_set():
                    return
                print(f"[{self.label}] ✗ disconnected: {e}  (retry 3s)")
                await asyncio.sleep(3)

    def _parse_and_store(self, raw: str):
        """
        防御式消息解析，兼容多种 payload 结构：
          A)  flat:     {"symbol", "timestamp", "value"}
          B)  wrapped:  {"topic", "type", "payload": {...}}   ← live update
          C)  backfill: {"payload": {"data": [{...}, {...}]}}
        """
        if not raw:
            return
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if not isinstance(msg, dict):
            return

        # 提取 msg_type（顶层 "type" 字段）
        msg_type = msg.get("type") or "update"

        # ── 路径 1: live update ──
        # {"topic":"crypto_prices","type":"update","timestamp":...,"payload":{"symbol":"btcusdt","timestamp":...,"value":...}}
        payload = msg.get("payload")
        if isinstance(payload, dict) and "value" in payload:
            self._store_item(payload, msg_type)
            return

        # ── 路径 2: backfill / subscribe response ──
        # {"payload":{"data":[{...},{...}]}}  或  {"data":[{...},{...}]}
        data = None
        if isinstance(payload, dict):
            data = payload.get("data")
        if data is None:
            data = msg.get("data")

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._store_item(item, msg_type)
            return

        # ── 路径 3: flat ──
        if "value" in msg or "price" in msg:
            self._store_item(msg, msg_type)

    def _store_item(self, item: dict, msg_type: str):
        recv_now = time.time()
        expected_sym = self.symbol.lower()

        sym = item.get("symbol")
        if sym and str(sym).lower() != expected_sym:
            return

        val = item.get("value")
        if val is None:
            val = item.get("price")
        if val is None:
            return

        ts_ms = item.get("timestamp")
        ts = recv_now
        if ts_ms is not None:
            try:
                ts = float(ts_ms) / 1000.0
            except (ValueError, TypeError):
                pass
        try:
            self.ticks.append((ts, float(val), msg_type))
        except (ValueError, TypeError):
            pass


# ─────────────────────────────────────────────
# 分析器
# ─────────────────────────────────────────────
def _pct(sorted_list, p: float):
    if not sorted_list:
        return float("nan")
    n = len(sorted_list)
    i = min(n - 1, max(0, int(p * n)))
    return sorted_list[i]


def analyze(binance: list, chainlink: list):
    print()
    print("=" * 78)
    print("诊断报告")
    print("=" * 78)

    bn_all_updates = sum(1 for _, _, t in binance   if t == "update")
    cl_all_updates = sum(1 for _, _, t in chainlink if t == "update")
    print(f"Binance   收到 ticks: {len(binance):>6}   (update={bn_all_updates})")
    print(f"Chainlink 收到 ticks: {len(chainlink):>6}   (update={cl_all_updates})")

    # 只用 update tick（排除 subscribe 时的 backfill）
    bn = sorted([(ts, v) for ts, v, typ in binance   if typ == "update"])
    cl = sorted([(ts, v) for ts, v, typ in chainlink if typ == "update"])

    if len(bn) < 10 or len(cl) < 10:
        print()
        print("!! 数据太少（任一源 update tick < 10），分析中止")
        # 打印 backfill 信息帮助诊断
        bn_sub = [(ts, v) for ts, v, typ in binance   if typ != "update"]
        cl_sub = [(ts, v) for ts, v, typ in chainlink if typ != "update"]
        print(f"   Binance   backfill ticks: {len(bn_sub)}")
        print(f"   Chainlink backfill ticks: {len(cl_sub)}")
        if bn:
            print(f"   Binance   update 样例: ts={bn[0][0]:.3f} price={bn[0][1]}")
        if cl:
            print(f"   Chainlink update 样例: ts={cl[0][0]:.3f} price={cl[0][1]}")
        return

    dur_bn = bn[-1][0] - bn[0][0]
    dur_cl = cl[-1][0] - cl[0][0]
    print()
    print(f"Binance   活跃时长: {dur_bn:>7.1f}s   平均 {len(bn)/max(dur_bn,0.01):>5.2f} ticks/s")
    print(f"Chainlink 活跃时长: {dur_cl:>7.1f}s   平均 {len(cl)/max(dur_cl,0.01):>5.2f} ticks/s")

    # ─── 活跃度：inter-tick gap ───
    bn_gaps = sorted(bn[i+1][0] - bn[i][0] for i in range(len(bn) - 1))
    cl_gaps = sorted(cl[i+1][0] - cl[i][0] for i in range(len(cl) - 1))
    print()
    print("─── 活跃度 (tick-to-tick 间隔) ───")
    print(f"  Binance    gap  p50={_pct(bn_gaps, .50):.3f}s  p95={_pct(bn_gaps, .95):.3f}s  max={bn_gaps[-1]:.3f}s")
    print(f"  Chainlink  gap  p50={_pct(cl_gaps, .50):.3f}s  p95={_pct(cl_gaps, .95):.3f}s  max={cl_gaps[-1]:.3f}s")

    # ─── 1) 瞬时价格 basis ───
    print()
    print(f"─── 1. 瞬时价格 basis  (Chainlink − Binance, 配对窗口 ±{BASIS_PAIR_WINDOW_SEC}s) ───")
    bn_ts   = [t for t, _ in bn]
    bn_vals = [v for _, v in bn]

    diffs: list[float] = []
    for cts, cv in cl:
        i = bisect.bisect_left(bn_ts, cts)
        best, best_dt = None, 1e9
        for c in (i, i - 1):
            if 0 <= c < len(bn_ts):
                dt = abs(bn_ts[c] - cts)
                if dt < best_dt:
                    best, best_dt = c, dt
        if best is not None and best_dt <= BASIS_PAIR_WINDOW_SEC:
            diffs.append(cv - bn_vals[best])

    if not diffs:
        print("  !! 配对失败（时间不对齐）")
    else:
        signed = sorted(diffs)
        absd   = sorted(abs(x) for x in diffs)
        n      = len(signed)
        mid_bn = sum(bn_vals) / len(bn_vals)
        bps    = sorted(abs(x) / mid_bn * 10000 for x in diffs)
        print(f"  配对样本: {n}")
        print(f"  basis USD  signed : p05={_pct(signed,.05):+8.3f}  p50={_pct(signed,.50):+8.3f}  p95={_pct(signed,.95):+8.3f}  min={signed[0]:+8.3f}  max={signed[-1]:+8.3f}")
        print(f"  basis USD  |abs|  : p50={_pct(absd,.50):8.3f}  p95={_pct(absd,.95):8.3f}  p99={_pct(absd,.99):8.3f}  max={absd[-1]:8.3f}")
        print(f"  basis bps  |abs|  : p50={_pct(bps,.50):8.2f}  p95={_pct(bps,.95):8.2f}  p99={_pct(bps,.99):8.2f}  max={bps[-1]:8.2f}")
        mean_signed = sum(signed) / n
        print(f"  basis 均值 (bias) : {mean_signed:+.3f} USD   (>0 → Chainlink 系统性高于 Binance)")

    # ─── 2) 5-min 滚动动量 ───
    print()
    print(f"─── 2. 5-min 滚动动量  (窗口={MOMENTUM_WINDOW_SEC}s, 每 {MOMENTUM_SAMPLE_EVERY_SEC}s 采样) ───")

    t_start = max(bn[0][0], cl[0][0]) + MOMENTUM_WINDOW_SEC
    t_end   = min(bn[-1][0], cl[-1][0])
    if t_end <= t_start:
        print("  (数据时长不足 5 分钟，无法计算动量)")
        return

    def price_at(ticks: list, t: float):
        ts_list = [x[0] for x in ticks]
        i = bisect.bisect_right(ts_list, t) - 1
        if i < 0:
            return None
        return ticks[i][1]

    mom_pairs: list[tuple[float, float, float]] = []
    t = t_start
    while t <= t_end:
        a = price_at(bn, t)
        b = price_at(bn, t - MOMENTUM_WINDOW_SEC)
        c = price_at(cl, t)
        d = price_at(cl, t - MOMENTUM_WINDOW_SEC)
        if None not in (a, b, c, d):
            mom_pairs.append((t, a - b, c - d))
        t += MOMENTUM_SAMPLE_EVERY_SEC

    if not mom_pairs:
        print("  (动量配对为空)")
        return

    same_sign = sum(
        1 for _, bm, cm in mom_pairs
        if (bm >= 0 and cm >= 0) or (bm < 0 and cm < 0)
    )
    mom_diffs_abs = sorted(abs(bm - cm) for _, bm, cm in mom_pairs)
    n = len(mom_pairs)
    print(f"  动量采样数: {n}")
    print(f"  方向一致率: {same_sign}/{n} = {same_sign/n*100:.2f}%")
    print(f"  |Bn_mom − Cl_mom| USD : "
          f"p50={_pct(mom_diffs_abs,.50):6.2f}  "
          f"p95={_pct(mom_diffs_abs,.95):6.2f}  "
          f"p99={_pct(mom_diffs_abs,.99):6.2f}  "
          f"max={mom_diffs_abs[-1]:6.2f}")

    # ─── 3) 阈值 cross-divergence ───
    print()
    print(f"─── 3. 阈值 ${THRESHOLD_USD:.0f} cross-divergence  ★ 策略交易集分化率 ★ ───")
    bn_pass = [abs(bm) >= THRESHOLD_USD for _, bm, _  in mom_pairs]
    cl_pass = [abs(cm) >= THRESHOLD_USD for _, _,  cm in mom_pairs]
    both_pass = sum(1 for b, c in zip(bn_pass, cl_pass) if     b and     c)
    both_fail = sum(1 for b, c in zip(bn_pass, cl_pass) if not b and not c)
    only_bn   = sum(1 for b, c in zip(bn_pass, cl_pass) if     b and not c)
    only_cl   = sum(1 for b, c in zip(bn_pass, cl_pass) if not b and     c)
    total = len(bn_pass)
    print(f"  两源都过阈值 (都交易)    : {both_pass:>5}/{total} = {both_pass/total*100:6.2f}%")
    print(f"  两源都未过   (都不交易)  : {both_fail:>5}/{total} = {both_fail/total*100:6.2f}%")
    print(f"  仅 Binance   过  (旧会做) : {only_bn:>5}/{total} = {only_bn/total*100:6.2f}%   ← 切换后不再做")
    print(f"  仅 Chainlink 过  (新会做) : {only_cl:>5}/{total} = {only_cl/total*100:6.2f}%   ← 切换后新增做")
    print(f"  合计分化率               : {(only_bn+only_cl):>5}/{total} = {(only_bn+only_cl)/total*100:6.2f}%")

    dir_diverge = sum(
        1 for _, bm, cm in mom_pairs
        if abs(bm) >= THRESHOLD_USD and abs(cm) >= THRESHOLD_USD
        and ((bm > 0) != (cm > 0))
    )
    print(f"  **方向相反且都过阈值**   : {dir_diverge:>5}/{total} = {dir_diverge/total*100:6.2f}%   (最危险场景)")

    # ─── 结论 ───
    print()
    print("─── 如何解读 ───")
    total_div = (only_bn + only_cl) / total * 100
    if total_div < 1.0 and dir_diverge == 0:
        print("  ✅ 分化率 < 1% 且无方向冲突 → Option A（只换价格，保留 Binance 动量）足够安全")
    elif total_div < 5.0:
        print(f"  ⚠️  分化率 {total_div:.1f}% → Option A 可行但会略损失一致性，建议观察实际 PnL")
    else:
        print(f"  ❌ 分化率 {total_div:.1f}% → 必须 Option B（连动量一起切换到 Chainlink）")


# ─────────────────────────────────────────────
# 原始 CSV 落盘
# ─────────────────────────────────────────────
def save_csvs(binance, chainlink, out_dir: Path):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, data in [("binance", binance), ("chainlink", chainlink)]:
        path = out_dir / f"ticks_{name}_{stamp}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fp:
            w = csv.writer(fp)
            w.writerow(["recv_ts_unix", "price", "msg_type"])
            w.writerows(data)
        paths.append(path)
    print()
    print("─── 原始数据 ───")
    for p in paths:
        print(f"  已保存: {p}")

    for p in paths:
        print()
        print(f"─── BEGIN CSV: {p.name} ───")
        try:
            with open(p, "r", encoding="utf-8") as fp:
                sys.stdout.write(fp.read())
        except Exception as e:
            print(f"!! 读取 CSV 失败: {e}")
        sys.stdout.flush()
        print(f"─── END CSV: {p.name} ───")


# ─────────────────────────────────────────────
# 主协程
# ─────────────────────────────────────────────
async def main_async(minutes: int):
    # ★ 核心修复：两个 topic 的订阅格式必须不同 ★
    #
    # Binance (crypto_prices):
    #   type = "update"
    #   filters = "btcusdt"               ← 逗号分隔纯字符串，小写
    #
    # Chainlink (crypto_prices_chainlink):
    #   type = "*"
    #   filters = '{"symbol":"btc/usd"}'  ← JSON 字符串
    #
    bn_col = TickCollector(
        topic="crypto_prices",
        label="Bn",
        symbol=BINANCE_SYMBOL,
        sub_type="update",
        filter_str=BINANCE_SYMBOL,             # "btcusdt" 纯字符串
    )
    cl_col = TickCollector(
        topic="crypto_prices_chainlink",
        label="Cl",
        symbol=CHAINLINK_SYMBOL,
        sub_type="*",
        filter_str=json.dumps({"symbol": CHAINLINK_SYMBOL}),  # '{"symbol":"btc/usd"}'
    )
    stop_event = asyncio.Event()

    async def timer():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=minutes * 60)
        except asyncio.TimeoutError:
            print(f"\n[timer] {minutes} 分钟时限到，收尾…")
            stop_event.set()

    async def progress():
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            bn_updates = sum(1 for _, _, t in bn_col.ticks if t == "update")
            cl_updates = sum(1 for _, _, t in cl_col.ticks if t == "update")
            print(f"  [progress] Bn total={len(bn_col.ticks):>5} live={bn_updates:>5} msgs={bn_col.msg_count:>5} (conn={bn_col.connected})   "
                  f"Cl total={len(cl_col.ticks):>5} live={cl_updates:>5} msgs={cl_col.msg_count:>5} (conn={cl_col.connected})", flush=True)

    loop = asyncio.get_running_loop()

    def on_signal():
        print("\n[sig] 收到中断，保存并分析现有数据…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())

    print(f"收集时长: {minutes} 分钟   (Ctrl+C 可提前停止并保存)")
    print("订阅 Polymarket RTDS:")
    print(f"  - crypto_prices           ({BINANCE_SYMBOL})  type='update', filters='{BINANCE_SYMBOL}'")
    print(f"  - crypto_prices_chainlink ({CHAINLINK_SYMBOL})  type='*', filters='{json.dumps({\"symbol\": CHAINLINK_SYMBOL})}'")
    print()

    await asyncio.gather(
        bn_col.run(stop_event),
        cl_col.run(stop_event),
        timer(),
        progress(),
        return_exceptions=True,
    )

    out_dir = Path(__file__).parent / "diag_out"
    save_csvs(bn_col.ticks, cl_col.ticks, out_dir)
    analyze(bn_col.ticks, cl_col.ticks)


def main():
    ap = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    ap.add_argument("--minutes", type=int, default=30,
                    help="收集时长（分钟），默认 30，推荐 30~60")
    args = ap.parse_args()

    if args.minutes < 6:
        print("!! --minutes 至少 6（动量窗口是 5 分钟）", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(main_async(args.minutes))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
