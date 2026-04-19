"""
analysis/diag_binance_vs_chainlink.py
=====================================
只读诊断 —— 同时订 Polymarket RTDS 两个 topic，被动收集数据：
  - crypto_prices              (BTCUSDT, Binance)
  - crypto_prices_chainlink    (BTCUSDT, Chainlink Data Streams，Polymarket 结算源)

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
SYMBOL                    = "BTCUSDT"
MOMENTUM_WINDOW_SEC       = 300    # 5 分钟
MOMENTUM_SAMPLE_EVERY_SEC = 15     # 每 15s 采一次动量
THRESHOLD_USD             = 60.0   # 与 strategy.py 的 momentum_threshold_usd 对齐
BASIS_PAIR_WINDOW_SEC     = 1.0    # 配对两源 tick 的最大时间差
PING_EVERY_SEC            = 5      # per Polymarket 文档


# ─────────────────────────────────────────────
# TickCollector —— 一个 topic 一个连接
# ─────────────────────────────────────────────
class TickCollector:
    def __init__(self, topic: str, label: str):
        self.topic     = topic
        self.label     = label
        # (recv_ts_unix, price, msg_type)
        self.ticks: list[tuple[float, float, str]] = []
        self.connected = False
        self.msg_count = 0

    async def run(self, stop_event: asyncio.Event):
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic":   self.topic,
                "type":    "update",
                "filters": json.dumps({"symbol": SYMBOL}),
            }]
        })
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    RTDS_URL,
                    ping_interval=20,   # WS 协议级 ping
                    ping_timeout=10,
                    open_timeout=10,
                    max_size=2**20,     # 1 MB per msg，backfill 可能比较大
                ) as ws:
                    await ws.send(subscribe_msg)
                    self.connected = True
                    print(f"[{self.label}] ✔ connected, subscribed to {self.topic}")

                    # 应用层 PING（每 5s，per 文档）
                    async def app_heartbeat():
                        while not stop_event.is_set():
                            try:
                                await ws.send(json.dumps({"type": "ping"}))
                            except Exception:
                                return
                            await asyncio.sleep(PING_EVERY_SEC)

                    # stop_event → 主动关闭 ws
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
                            self.msg_count += 1
                            if self.msg_count <= 3:
                                preview = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                                print(f"[{self.label}] raw #{self.msg_count}: {preview[:500]}", flush=True)
                            self._parse_and_store(raw)
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
        防御式消息解析：Polymarket RTDS 的具体 payload 结构文档没写死，
        这里兼容三种可能形状：
          A)  flat:    {"symbol", "timestamp", "value"}
          B)  wrapped: {"type", "topic", "data": {"symbol", "timestamp", "value"}}
          C)  array:   {"type": "subscribe", "data": [{...}, {...}]}   (backfill)
        """
        try:
            msg = json.loads(raw)
        except Exception:
            return

        if not isinstance(msg, dict):
            return

        msg_type = msg.get("type") or "update"
        data     = msg.get("data")

        items: list = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = [data]
        elif "value" in msg or "price" in msg:
            items = [msg]
        else:
            return

        now = time.time()
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = item.get("symbol")
            if sym and sym != SYMBOL:
                continue
            val = item.get("value")
            if val is None:
                val = item.get("price")
            if val is None:
                continue
            try:
                self.ticks.append((now, float(val), msg_type))
            except (ValueError, TypeError):
                continue


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

    # 只用 update tick（排除 subscribe 时的 backfill，时间戳是 recv 而非实际）
    bn = sorted([(ts, v) for ts, v, typ in binance   if typ == "update"])
    cl = sorted([(ts, v) for ts, v, typ in chainlink if typ == "update"])

    if len(bn) < 10 or len(cl) < 10:
        print()
        print("!! 数据太少（任一源 update tick < 10），分析中止")
        return

    dur_bn = bn[-1][0] - bn[0][0]
    dur_cl = cl[-1][0] - cl[0][0]
    print()
    print(f"Binance   活跃时长: {dur_bn:>7.1f}s   平均 {len(bn)/dur_bn:>5.2f} ticks/s")
    print(f"Chainlink 活跃时长: {dur_cl:>7.1f}s   平均 {len(cl)/dur_cl:>5.2f} ticks/s")

    # ─────────────────────────────────────────────
    # 活跃度：inter-tick gap
    # ─────────────────────────────────────────────
    bn_gaps = sorted(bn[i+1][0] - bn[i][0] for i in range(len(bn) - 1))
    cl_gaps = sorted(cl[i+1][0] - cl[i][0] for i in range(len(cl) - 1))
    print()
    print("─── 活跃度 (tick-to-tick 间隔) ───")
    print(f"  Binance    gap  p50={_pct(bn_gaps, .50):.3f}s  p95={_pct(bn_gaps, .95):.3f}s  max={bn_gaps[-1]:.3f}s")
    print(f"  Chainlink  gap  p50={_pct(cl_gaps, .50):.3f}s  p95={_pct(cl_gaps, .95):.3f}s  max={cl_gaps[-1]:.3f}s")

    # ─────────────────────────────────────────────
    # 1) 瞬时价格 basis
    # ─────────────────────────────────────────────
    print()
    print(f"─── 1. 瞬时价格 basis  (Chainlink − Binance, 配对窗口 ±{BASIS_PAIR_WINDOW_SEC}s) ───")
    bn_ts   = [t for t, _ in bn]
    bn_vals = [v for _, v in bn]

    diffs: list[float] = []   # signed (CL - BN)
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
        # 有向性检查：是否存在持续偏置
        mean_signed = sum(signed) / n
        print(f"  basis 均值 (bias) : {mean_signed:+.3f} USD   (>0 → Chainlink 系统性高于 Binance)")

    # ─────────────────────────────────────────────
    # 2) 5-min 滚动动量
    # ─────────────────────────────────────────────
    print()
    print(f"─── 2. 5-min 滚动动量  (窗口={MOMENTUM_WINDOW_SEC}s, 每 {MOMENTUM_SAMPLE_EVERY_SEC}s 采样) ───")

    t_start = max(bn[0][0], cl[0][0]) + MOMENTUM_WINDOW_SEC
    t_end   = min(bn[-1][0], cl[-1][0])
    if t_end <= t_start:
        print("  (数据时长不足 5 分钟，无法计算动量)")
        return

    def price_at(ticks: list, t: float):
        """返回 ≤ t 的最近一笔价格；找不到返回 None。"""
        ts_list = [x[0] for x in ticks]
        i = bisect.bisect_right(ts_list, t) - 1
        if i < 0:
            return None
        return ticks[i][1]

    mom_pairs: list[tuple[float, float, float]] = []   # (t, bn_mom, cl_mom)
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

    # ─────────────────────────────────────────────
    # 3) 阈值 cross-divergence（策略行为分化）
    # ─────────────────────────────────────────────
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

    # 方向分化（更极端：一侧说 Up 要做，一侧说 Down 要做，虽然罕见）
    dir_diverge = sum(
        1 for _, bm, cm in mom_pairs
        if abs(bm) >= THRESHOLD_USD and abs(cm) >= THRESHOLD_USD
        and ((bm > 0) != (cm > 0))
    )
    print(f"  **方向相反且都过阈值**   : {dir_diverge:>5}/{total} = {dir_diverge/total*100:6.2f}%   (最危险场景)")

    # ─────────────────────────────────────────────
    # 结论提示
    # ─────────────────────────────────────────────
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


# ─────────────────────────────────────────────
# 主协程
# ─────────────────────────────────────────────
async def main_async(minutes: int):
    bn_col = TickCollector("crypto_prices",           "Bn")
    cl_col = TickCollector("crypto_prices_chainlink", "Cl")
    stop_event = asyncio.Event()

    # 时限
    async def timer():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=minutes * 60)
        except asyncio.TimeoutError:
            print(f"\n[timer] {minutes} 分钟时限到，收尾…")
            stop_event.set()

    # 进度条
    async def progress():
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            print(f"  [progress] Bn ticks={len(bn_col.ticks):>5} msgs={bn_col.msg_count:>5} (conn={bn_col.connected})   "
                  f"Cl ticks={len(cl_col.ticks):>5} msgs={cl_col.msg_count:>5} (conn={cl_col.connected})", flush=True)

    # Ctrl+C: asyncio signal handler（比 signal.signal 和 loop 集成得更干净）
    loop = asyncio.get_running_loop()

    def on_signal():
        print("\n[sig] 收到中断，保存并分析现有数据…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_signal)
        except NotImplementedError:
            # Windows 不支持；fallback 到 signal.signal
            signal.signal(sig, lambda *_: stop_event.set())

    print(f"收集时长: {minutes} 分钟   (Ctrl+C 可提前停止并保存)")
    print("订阅 Polymarket RTDS:")
    print(f"  - crypto_prices           ({SYMBOL}, Binance)")
    print(f"  - crypto_prices_chainlink ({SYMBOL}, Chainlink Data Streams)")
    print()

    await asyncio.gather(
        bn_col.run(stop_event),
        cl_col.run(stop_event),
        timer(),
        progress(),
        return_exceptions=True,
    )

    # 落盘 + 分析
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
