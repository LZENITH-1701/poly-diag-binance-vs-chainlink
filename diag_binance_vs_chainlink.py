"""
analysis/diag_binance_vs_chainlink.py
=====================================
只读诊断 —— 同时订 Polymarket RTDS 两个 topic，被动收集数据：
  - crypto_prices              (btcusdt, Binance)
  - crypto_prices_chainlink    (btc/usd, Chainlink Data Streams)

用法：
  python3 diag_binance_vs_chainlink.py --minutes 30

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

try:
    import websockets
except ImportError:
    print("缺依赖: pip install websockets", file=sys.stderr)
    sys.exit(1)

# ── 常量 ──
RTDS_URL                  = "wss://ws-live-data.polymarket.com"
BINANCE_SYMBOL            = "btcusdt"
CHAINLINK_SYMBOL          = "btc/usd"
MOMENTUM_WINDOW_SEC       = 300
MOMENTUM_SAMPLE_EVERY_SEC = 15
THRESHOLD_USD             = 60.0
BASIS_PAIR_WINDOW_SEC     = 1.0
PING_EVERY_SEC            = 5


class TickCollector:
    def __init__(self, label: str, symbol: str, subscribe_payload: dict):
        self.label        = label
        self.symbol       = symbol
        self.sub_payload  = subscribe_payload
        self.ticks: list[tuple[float, float, str]] = []
        self.connected    = False
        self.msg_count    = 0
        self.pong_count   = 0

    async def run(self, stop_event: asyncio.Event):
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [self.sub_payload]
        })

        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    RTDS_URL,
                    ping_interval=None,   # 禁用库层 ping，只用应用层
                    ping_timeout=None,
                    open_timeout=15,
                    max_size=2**20,
                ) as ws:
                    await ws.send(subscribe_msg)
                    self.connected = True
                    print(f"[{self.label}] connected, sent: {subscribe_msg}")

                    async def heartbeat():
                        while not stop_event.is_set():
                            await asyncio.sleep(PING_EVERY_SEC)
                            try:
                                await ws.send("PING")
                            except Exception:
                                return

                    async def stopper():
                        await stop_event.wait()
                        try: await ws.close()
                        except Exception: pass

                    hb = asyncio.create_task(heartbeat())
                    st = asyncio.create_task(stopper())

                    try:
                        async for raw in ws:
                            if stop_event.is_set():
                                break
                            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                            if text == "PONG":
                                self.pong_count += 1
                                continue
                            self.msg_count += 1
                            if self.msg_count <= 8:
                                print(f"[{self.label}] msg#{self.msg_count}: {text[:800]}", flush=True)
                            elif self.msg_count % 200 == 0:
                                print(f"[{self.label}] msg#{self.msg_count} sample: {text[:300]}…", flush=True)
                            self._parse(text)
                    finally:
                        hb.cancel()
                        st.cancel()
            except Exception as e:
                self.connected = False
                if stop_event.is_set():
                    return
                print(f"[{self.label}] disconnected: {e}  (retry 3s)")
                await asyncio.sleep(3)

    def _parse(self, raw: str):
        if not raw:
            return
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if not isinstance(msg, dict):
            return

        msg_type = str(msg.get("type", "unknown"))
        payload  = msg.get("payload")

        # 路径 A: live update
        # {"topic":"…","type":"update","payload":{"symbol":"…","value":…}}
        if isinstance(payload, dict) and "value" in payload:
            self._store(payload, msg_type)
            return

        # 路径 B: backfill batch
        data = None
        if isinstance(payload, dict):
            data = payload.get("data")
        if data is None:
            data = msg.get("data")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._store(item, msg_type)
            return

        # 路径 C: flat
        if "value" in msg or "price" in msg:
            self._store(msg, msg_type)

    def _store(self, item: dict, msg_type: str):
        recv_now = time.time()
        expected = self.symbol.lower()
        sym = item.get("symbol")
        if sym and str(sym).lower() != expected:
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


# ── 分析 ──
def _pct(s, p):
    if not s: return float("nan")
    return s[min(len(s)-1, max(0, int(p*len(s))))]


def analyze(binance, chainlink):
    print("\n" + "="*78)
    print("诊断报告")
    print("="*78)

    def tcnt(ticks):
        d = {}
        for _,_,t in ticks:
            d[t] = d.get(t,0)+1
        return d

    print(f"Binance   总ticks: {len(binance):>6}  分布: {tcnt(binance)}")
    print(f"Chainlink 总ticks: {len(chainlink):>6}  分布: {tcnt(chainlink)}")

    bn_live = [(ts,v) for ts,v,t in binance   if t == "update"]
    cl_live = [(ts,v) for ts,v,t in chainlink if t == "update"]

    if len(bn_live) < 10 or len(cl_live) < 10:
        print(f"\n  ⚠ update tick 不足 (Bn={len(bn_live)}, Cl={len(cl_live)})")
        print("  fallback: 用全部 ticks…")
        bn_live = [(ts,v) for ts,v,_ in binance]
        cl_live = [(ts,v) for ts,v,_ in chainlink]

    if len(bn_live) < 10 or len(cl_live) < 10:
        print("  !! 数据太少，中止"); return

    bn = sorted(bn_live); cl = sorted(cl_live)
    dur_bn = bn[-1][0]-bn[0][0]; dur_cl = cl[-1][0]-cl[0][0]
    print(f"\nBn 时长: {dur_bn:.1f}s  {len(bn)/max(dur_bn,.01):.2f} t/s")
    print(f"Cl 时长: {dur_cl:.1f}s  {len(cl)/max(dur_cl,.01):.2f} t/s")

    bg = sorted(bn[i+1][0]-bn[i][0] for i in range(len(bn)-1))
    cg = sorted(cl[i+1][0]-cl[i][0] for i in range(len(cl)-1))
    print("\n─── 活跃度 ───")
    print(f"  Bn gap p50={_pct(bg,.5):.3f}s p95={_pct(bg,.95):.3f}s max={bg[-1]:.3f}s")
    print(f"  Cl gap p50={_pct(cg,.5):.3f}s p95={_pct(cg,.95):.3f}s max={cg[-1]:.3f}s")

    print(f"\n─── 1. basis (CL−BN, ±{BASIS_PAIR_WINDOW_SEC}s) ───")
    bts=[t for t,_ in bn]; bv=[v for _,v in bn]
    diffs=[]
    for cts,cv in cl:
        i=bisect.bisect_left(bts,cts); best=None; bdt=1e9
        for c in (i,i-1):
            if 0<=c<len(bts):
                dt=abs(bts[c]-cts)
                if dt<bdt: best,bdt=c,dt
        if best is not None and bdt<=BASIS_PAIR_WINDOW_SEC:
            diffs.append(cv-bv[best])

    if not diffs:
        print("  !! 配对失败")
    else:
        sd=sorted(diffs); ab=sorted(abs(x) for x in diffs)
        mid=sum(bv)/len(bv); bp=sorted(abs(x)/mid*10000 for x in diffs)
        print(f"  样本: {len(diffs)}")
        print(f"  signed: p05={_pct(sd,.05):+.3f} p50={_pct(sd,.5):+.3f} p95={_pct(sd,.95):+.3f}")
        print(f"  |abs|:  p50={_pct(ab,.5):.3f} p95={_pct(ab,.95):.3f} max={ab[-1]:.3f}")
        print(f"  bps:    p50={_pct(bp,.5):.2f} p95={_pct(bp,.95):.2f} max={bp[-1]:.2f}")
        print(f"  bias:   {sum(diffs)/len(diffs):+.3f} USD")

    print(f"\n─── 2. 5min动量 ───")
    t0=max(bn[0][0],cl[0][0])+MOMENTUM_WINDOW_SEC
    te=min(bn[-1][0],cl[-1][0])
    if te<=t0: print("  (不足5min)"); return

    def pat(tk,t):
        ts=[x[0] for x in tk]; i=bisect.bisect_right(ts,t)-1
        return tk[i][1] if i>=0 else None

    mps=[]
    t=t0
    while t<=te:
        a,b,c,d=pat(bn,t),pat(bn,t-MOMENTUM_WINDOW_SEC),pat(cl,t),pat(cl,t-MOMENTUM_WINDOW_SEC)
        if None not in (a,b,c,d): mps.append((t,a-b,c-d))
        t+=MOMENTUM_SAMPLE_EVERY_SEC

    if not mps: print("  (空)"); return
    ss=sum(1 for _,bm,cm in mps if (bm>=0)==(cm>=0))
    md=sorted(abs(bm-cm) for _,bm,cm in mps)
    print(f"  采样: {len(mps)}")
    print(f"  方向一致: {ss}/{len(mps)} = {ss/len(mps)*100:.1f}%")
    print(f"  |diff|: p50={_pct(md,.5):.2f} p95={_pct(md,.95):.2f} max={md[-1]:.2f}")

    print(f"\n─── 3. ${THRESHOLD_USD:.0f} 分化 ★ ───")
    bpp=[abs(bm)>=THRESHOLD_USD for _,bm,_ in mps]
    cpp=[abs(cm)>=THRESHOLD_USD for _,_,cm in mps]
    bb=sum(b and c for b,c in zip(bpp,cpp))
    nn=sum(not b and not c for b,c in zip(bpp,cpp))
    ob=sum(b and not c for b,c in zip(bpp,cpp))
    oc=sum(not b and c for b,c in zip(bpp,cpp))
    n=len(bpp)
    print(f"  都做: {bb}/{n}={bb/n*100:.1f}%  都不做: {nn}/{n}={nn/n*100:.1f}%")
    print(f"  仅Bn: {ob}/{n}={ob/n*100:.1f}%  仅Cl: {oc}/{n}={oc/n*100:.1f}%")
    print(f"  分化: {ob+oc}/{n}={(ob+oc)/n*100:.1f}%")
    dd=sum(1 for _,bm,cm in mps if abs(bm)>=THRESHOLD_USD and abs(cm)>=THRESHOLD_USD and (bm>0)!=(cm>0))
    print(f"  方向冲突: {dd}/{n}={dd/n*100:.2f}%")

    td=(ob+oc)/n*100
    print("\n─── 结论 ───")
    if td<1 and dd==0: print("  ✅ 安全切换")
    elif td<5: print(f"  ⚠️  分化{td:.1f}%, 可切换但关注PnL")
    else: print(f"  ❌ 分化{td:.1f}%, 必须连动量一起切")


def save_csvs(binance, chainlink, out_dir):
    stamp=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name,data in [("binance",binance),("chainlink",chainlink)]:
        p=out_dir/f"ticks_{name}_{stamp}.csv"
        with open(p,"w",newline="",encoding="utf-8") as fp:
            w=csv.writer(fp); w.writerow(["tick_ts","price","type"]); w.writerows(data)
        print(f"  saved: {p}")
        print(f"\n─── BEGIN {p.name} ───")
        with open(p,encoding="utf-8") as fp: sys.stdout.write(fp.read())
        print(f"─── END {p.name} ───")


async def main_async(minutes):
    # ★ 完全按 Polymarket 文档格式 ★
    # Binance:   type=update, 不传 filters
    # Chainlink: type=*, filters=""
    bn_sub = {"topic": "crypto_prices", "type": "update"}
    cl_sub = {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}

    bn = TickCollector("Bn", BINANCE_SYMBOL, bn_sub)
    cl = TickCollector("Cl", CHAINLINK_SYMBOL, cl_sub)
    stop = asyncio.Event()

    async def timer():
        try: await asyncio.wait_for(stop.wait(), timeout=minutes*60)
        except asyncio.TimeoutError:
            print(f"\n[timer] {minutes}min done"); stop.set()

    async def progress():
        while not stop.is_set():
            try: await asyncio.wait_for(stop.wait(), timeout=15)
            except asyncio.TimeoutError: pass
            bu=sum(1 for _,_,t in bn.ticks if t=="update")
            cu=sum(1 for _,_,t in cl.ticks if t=="update")
            print(f"  [prog] Bn: tot={len(bn.ticks)} live={bu} msg={bn.msg_count} pong={bn.pong_count}  "
                  f"Cl: tot={len(cl.ticks)} live={cu} msg={cl.msg_count} pong={cl.pong_count}", flush=True)

    loop=asyncio.get_running_loop()
    def onsig():
        print("\n[sig] stopping…"); stop.set()
    for s in (signal.SIGINT,signal.SIGTERM):
        try: loop.add_signal_handler(s,onsig)
        except NotImplementedError: signal.signal(s, lambda *_: stop.set())

    print(f"收集 {minutes}min  (Ctrl+C 提前停)")
    print(f"  Bn: {json.dumps(bn_sub)}")
    print(f"  Cl: {json.dumps(cl_sub)}\n")

    await asyncio.gather(bn.run(stop),cl.run(stop),timer(),progress(),return_exceptions=True)
    out=Path(__file__).parent/"diag_out"
    save_csvs(bn.ticks,cl.ticks,out)
    analyze(bn.ticks,cl.ticks)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--minutes",type=int,default=30)
    a=ap.parse_args()
    if a.minutes<6: print("!! >=6min",file=sys.stderr); sys.exit(1)
    try: asyncio.run(main_async(a.minutes))
    except KeyboardInterrupt: pass

if __name__=="__main__": main()
