"""
DEAD NINJA v2 SMC  -  institutional-style Smart Money paper bot (CoinDCX futures public data)

WHY v1.3 LOST (read from your screenshots: 22 trades, 50% win rate, avg win +4.02 / avg loss -8.53, PF 0.47)
  1. Payoff inverted: partial-book at 1R + breakeven + tight trail cut winners to ~0.5R while losers paid
     the full 1R (+costs). Only 2 of 22 trades reached TP. At 50% win rate you need avg win >= avg loss.
  2. Entries were retail signals (candle patterns, 20-bar breakout, EMA cross) that need late confirmation,
     so the bot chased price. Swing EMA-cross: 6 trades, 17% win, -41.8. Patterns: 46% win, -31.8.
  3. "Quality score" (ADX/volume/RSI) did not predict anything: Q80 trades hit SL, B-grade lost the most.
  4. Contradictory filters: continuation patterns demanded RSI "exhaustion"; most setups died on
     "pullback to EMA21". Logic tab even listed passed filters ("Daily loss ok") as rejections.

WHAT v2 DOES (one setup, institutional logic, mirrored for shorts)
  HTF(1h) structure bias  ->  LTF(15m) liquidity sweep of a swing low/high that closes back inside
  ->  market-structure shift with displacement  ->  limit entry at the fair-value-gap midpoint
  (discount for longs / premium for shorts)  ->  stop behind the sweep  ->  target = untouched external
  liquidity at >= 3R.  No partial booking. Stop moves to +0.25R only after 2R is reached.
  At 3R a 40% win rate gives +0.6R gross expectancy per trade BEFORE costs. Costs are modelled.

HONEST LIMITS
  * Nothing here can promise "99.9%" or any guaranteed win rate. Run `python dead_ninja_v2.py backtest`
    and judge the numbers; then forward-test on paper for weeks. PAPER ONLY - no real orders exist here.
  * Maker/taker fees are ASSUMPTIONS (cfg fee_maker/fee_taker) - set them to your real CoinDCX tier.
Run:  python dead_ninja_v2.py            (dashboard on PORT env, default 10019)
      python dead_ninja_v2.py backtest   (replays the SAME engine on historical candles)
"""
import json, os, sys, time, random, threading, traceback, bisect
import re, csv, io, math
import xml.etree.ElementTree as ET
import email.utils
import urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = "dead_ninja_v2.log"
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def log(msg):
    try:
        if os.path.exists(LOG) and os.path.getsize(LOG) > 1000000:
            os.replace(LOG, LOG + ".1")
        with open(LOG, "a") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + str(msg) + "\n")
    except Exception:
        pass


def notify(msg):
    tok, chat = os.environ.get("DN_TG_TOKEN"), os.environ.get("DN_TG_CHAT")
    if not tok or not chat:
        return

    def go():
        try:
            data = urllib.parse.urlencode(dict(chat_id=chat, text=msg)).encode()
            urllib.request.urlopen(
                "https://api.telegram.org/bot%s/sendMessage" % tok,
                data, timeout=10).read()
        except Exception as e:
            log("telegram failed: %s" % e)
    threading.Thread(target=go, daemon=True).start()



# ============== NEWS ENGINE ==============
NEWS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
    ("Decrypt", "https://decrypt.co/feed"),
]
POS_KW = {"surge": 1, "rally": 1, "soar": 2, "jump": 1, "gain": 1, "bullish": 2,
          "record high": 2, "all-time high": 2, "approve": 1, "approval": 1,
          "etf inflow": 2, "inflow": 1, "adopt": 1, "partnership": 1,
          "upgrade": 1, "launch": 1, "breakout": 1, "rebound": 1, "recover": 1,
          "accumulat": 1, "buy": 1}
NEG_KW = {"crash": 2, "plunge": 2, "dump": 2, "sell-off": 2, "selloff": 2,
          "bearish": 2, "slump": 1, "tumble": 1, "drop": 1, "fall": 1,
          "liquidat": 1, "outflow": 1, "fraud": 2, "collapse": 2, "fear": 1,
          "warning": 1, "reject": 1, "delist": 2, "lawsuit": 1, "probe": 1,
          "hack": 2, "exploit": 2, "stolen": 2, "ban": 1}
RISK_KW = ("hack", "exploit", "stolen", "drained", "rug pull", "delist",
           "sues", "charged", "bankrupt", "insolven", "depeg", "outage")
COIN_NAMES = {
    "BTC": ("bitcoin", "btc"), "ETH": ("ethereum", "ether", "eth"),
    "SOL": ("solana",), "XRP": ("xrp", "ripple"), "BNB": ("bnb", "binance coin"),
    "DOGE": ("dogecoin", "doge"), "ADA": ("cardano",), "LINK": ("chainlink",),
    "AVAX": ("avalanche", "avax"), "SUI": ("sui network",),
    "HYPE": ("hyperliquid",), "ENA": ("ethena",), "WLD": ("worldcoin",),
    "NEAR": ("near protocol",), "ZEC": ("zcash",), "1000PEPE": ("pepe",)}


def news_score(title):
    t = title.lower()
    sc = sum(w for k, w in POS_KW.items() if k in t) - \
        sum(w for k, w in NEG_KW.items() if k in t)
    return max(-3, min(3, sc))


def news_coins(title):
    t = title.lower()
    return [b for b, names in COIN_NAMES.items()
            if any(re.search(r"\b" + re.escape(n) + r"\b", t) for n in names)]


class NewsFeed:
    def __init__(self, cfg):
        self.c, self.items, self.ok = cfg, [], False
        self.err, self.updated = "", 0
        self.lock, self.started = threading.Lock(), False

    def _fetch(self, name, url):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=12).read(2000000)
        root, out = ET.fromstring(raw), []
        for it in root.iter("item"):
            title = (it.findtext("title") or "").strip()
            if not title:
                continue
            ts = time.time()
            try:
                ts = email.utils.parsedate_to_datetime(
                    it.findtext("pubDate")).timestamp()
            except Exception:
                pass
            link = (it.findtext("link") or "").strip()
            out.append(dict(src=name, title=title[:200], t=int(ts * 1000),
                            link=link if link.startswith("http") else "",
                            score=news_score(title), coins=news_coins(title),
                            risk=any(k in title.lower() for k in RISK_KW)))
        return out

    def refresh(self):
        allit, errs = [], []
        for name, url in NEWS_FEEDS:
            try:
                allit += self._fetch(name, url)
            except Exception as e:
                errs.append("%s: %s" % (name, str(e)[:40]))
        with self.lock:
            if allit:
                seen, ded = set(), []
                for x in sorted(allit, key=lambda x: -x["t"]):
                    k = re.sub(r"\W+", "", x["title"].lower())[:60]
                    if k not in seen:
                        seen.add(k)
                        ded.append(x)
                allit = ded
                allit.sort(key=lambda x: -x["t"])
                self.items, self.ok = allit[:120], True
                self.updated = int(time.time() * 1000)
            self.err = "; ".join(errs)[:160]

    def start(self):
        if self.started:
            return
        self.started = True

        def loop():
            while True:
                try:
                    self.refresh()
                except Exception as e:
                    log("news error: %s" % e)
                time.sleep(self.c["news_refresh_sec"])
        threading.Thread(target=loop, daemon=True).start()

    def _win(self, now, hours=None):
        h = hours or self.c["news_window_h"]
        with self.lock:
            return [x for x in self.items if now - x["t"] <= h * 3600000]

    def check(self, base, side, now):
        if not self.ok:
            return True, "News unavailable (neutral)"
        w = self._win(now)
        g = sum(x["score"] for x in w) / len(w) if w else 0.0
        coin = sum(x["score"] for x in w if base in x["coins"])
        risk = [x for x in self._win(now, 6) if base in x["coins"] and x["risk"]]
        if risk:
            return False, "News RISK alert: " + risk[0]["title"][:50]
        cb, gb = self.c["news_coin_block"], self.c["news_global_block"]
        if side == "LONG":
            ok = coin > -cb and g > -gb
        else:
            ok = coin < cb and g < gb
        return ok, "News %s: coin %+d, market %+.1f (%s)" % (
            base, coin, g, "ok" if ok else "against " + side)

    def snapshot(self, now):
        w = self._win(now)
        g = sum(x["score"] for x in w) / len(w) if w else 0.0
        coins = {}
        for b in COIN_NAMES:
            sc = sum(x["score"] for x in w if b in x["coins"])
            n = sum(1 for x in w if b in x["coins"])
            if n:
                coins[b] = dict(score=sc, n=n)
        label = "n/a" if not self.ok else (
            "Bullish" if g > 0.3 else "Bearish" if g < -0.3 else "Neutral")
        with self.lock:
            items = [dict(x) for x in self.items[:30]]
        return dict(ok=self.ok, err=self.err, updated=self.updated, g=g,
                    label=label, n=len(w), items=items, coins=coins)


# ============== CONFIG ==============
PAIRS = ["B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-XRP_USDT", "B-BNB_USDT", "B-DOGE_USDT",
         "B-ADA_USDT", "B-LINK_USDT", "B-AVAX_USDT", "B-SUI_USDT", "B-NEAR_USDT", "B-ENA_USDT", "B-WLD_USDT"]
BTC = "B-BTC_USDT"
IV = 900000           # 15m in ms
HIV = 3600000         # 1h in ms
STATE_FILE = "dead_ninja_v2_state.json"
JOURNAL = "dead_ninja_v2_trades.csv"
BT_FILE = "dead_ninja_v2_backtest.json"

CFG = dict(
    port=int(os.environ.get("PORT", "10019")),
    host=os.environ.get("DN_HOST", "0.0.0.0"),
    token=os.environ.get("DN_TOKEN", ""),
    poll_sec=5, pairs=list(PAIRS),
    # capital / risk
    start_balance=1000.0, risk_pct=0.005, max_open=3, max_same_side=2, max_heat=0.015,
    max_notional_x=2.0, max_gross_x=3.0, max_lev=10, margin_frac=0.20,
    max_daily_loss=0.02, consec_loss_pause=3, pause_hours=8, dd_kill=0.10, dd_kill_hours=24,
    cooldown_candles=8,
    # costs (ASSUMPTIONS - set to your real tier)
    fee_maker=0.0003, fee_taker=0.0006, slippage=0.0003, slip_atr_k=0.04,
    funding_rate=0.0001, funding_every_h=8, max_cost_r=0.20,
    # smart-money detection
    pivot_l=3, sweep_scan=45, sweep_min_atr=0.10, sweep_max_atr=2.5, disp_atr=0.90,
    mss_leg_atr=0.8, mss_max_age=10, fvg_min_atr=0.15, sl_buf_atr=0.30,
    min_sl_atr=0.8, max_sl_atr=3.0, min_rr=3.0, max_rr=8.0, tp_buf_atr=0.15,
    min_bonus=2, kz_start=7, kz_end=17, vol_mult=1.5, btc_filter=True,
    # orders / trade management
    order_ttl_candles=8, fill_through=0.0003, cancel_r=2.0,
    be_at_r=2.0, be_lock_r=0.25, time_stop_candles=64,
    # data / news
    news_enabled=True, news_refresh_sec=600, news_window_h=12, news_coin_block=3, news_global_block=1.0,
    htf_refresh_sec=240, ltf_refresh_sec=20, stale_candles=3,
)
def ema(values, period):
    k, out, prev = 2 / (period + 1), [], None
    for v in values:
        prev = v if prev is None else v * k + prev * (1 - k)
        out.append(prev)
    return out


def rsi(closes, period):
    out, avg_g, avg_l = [50.0], 0.0, 0.0
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        g, l = max(ch, 0), max(-ch, 0)
        if i <= period:
            avg_g += g / period
            avg_l += l / period
        else:
            avg_g = (avg_g * (period - 1) + g) / period
            avg_l = (avg_l * (period - 1) + l) / period
        out.append(100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l))
    return out


def atr(candles, period):
    out, prev_atr = [candles[0]["h"] - candles[0]["l"]], None
    for i in range(1, len(candles)):
        c, pc = candles[i], candles[i - 1]["c"]
        tr = max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc))
        prev_atr = tr if prev_atr is None else (prev_atr * (period - 1) + tr) / period
        out.append(prev_atr)
    return out


def adx(candles, period=14):
    out, tr_s, pdm_s, mdm_s, adx_v = [0.0], 0, 0, 0, 0
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        up, dn = c["h"] - p["h"], p["l"] - c["l"]
        pdm = up if up > dn and up > 0 else 0
        mdm = dn if dn > up and dn > 0 else 0
        tr = max(c["h"] - c["l"], abs(c["h"] - p["c"]), abs(c["l"] - p["c"]))
        tr_s = tr_s - tr_s / period + tr
        pdm_s = pdm_s - pdm_s / period + pdm
        mdm_s = mdm_s - mdm_s / period + mdm
        pdi = 100 * pdm_s / tr_s if tr_s else 0
        mdi = 100 * mdm_s / tr_s if tr_s else 0
        dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) else 0
        adx_v = dx if i == 1 else (adx_v * (period - 1) + dx) / period
        out.append(adx_v)
    return out


def atr_percentile(candles, lookback=100):
    """NEW: is current volatility alive? 0-100. <30 = dead market."""
    if len(candles) < lookback + 5:
        return 50.0
    atrs = atr(candles, 14)
    cur = atrs[-1]
    hist = atrs[-lookback:]
    below = sum(1 for x in hist if x <= cur)
    return below / len(hist) * 100.0


def vol_ratio(candles, lookback=20):
    """NEW: recent volume vs average."""
    if len(candles) < lookback + 2:
        return 1.0
    recent = candles[-1].get("v", 0)
    avg = sum(x.get("v", 0) for x in candles[-lookback - 1:-1]) / lookback
    return recent / avg if avg > 0 else 1.0

class LiveFeed:
    """CoinDCX public data (no API key needed)."""
    live = True

    def _json(self, url):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        for attempt in (1, 2):
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                if attempt == 2 or e.code not in (429, 500, 502, 503, 504):
                    raise
                time.sleep(1.5)

    def pairs(self, cfg):
        if cfg["pairs_override"]:
            return list(cfg["pairs_override"])
        try:
            raw = self._json(
                "https://api.coindcx.com/exchange/v1/derivatives/futures/"
                "data/active_instruments?margin_currency_short_name[]=USDT")
            allp = [x if isinstance(x, str) else x.get("pair", "") for x in raw]
            allp = [x for x in allp if x.startswith("B-") and x.endswith("_USDT")]
        except Exception:
            allp = []
        if not allp:
            allp = FALLBACK
        first = [x for x in FALLBACK if x in allp]
        return first + [x for x in allp if x not in first]

    def get(self, pair, interval):
        raw = self._json(
            "https://public.coindcx.com/market_data/candles/"
            "?pair=%s&interval=%s" % (pair, interval))
        cs = []
        for x in raw:
            t_val = int(x["time"])
            if t_val < 10000000000:  # seconds -> ms
                t_val *= 1000
            cs.append(dict(t=t_val, o=float(x["open"]),
                           h=float(x["high"]), l=float(x["low"]),
                           c=float(x["close"]), v=float(x.get("volume") or 0)))
        cs.sort(key=lambda x: x["t"])
        return cs



# ============== SMART MONEY CORE ==============
def pivots(cs, L=3):
    """Confirmed swing highs/lows as (index, price). A pivot needs L candles on both sides."""
    hi, lo = [], []
    for i in range(L, len(cs) - L):
        h, l = cs[i]["h"], cs[i]["l"]
        if all(h > cs[i - k]["h"] and h >= cs[i + k]["h"] for k in range(1, L + 1)):
            hi.append((i, h))
        if all(l < cs[i - k]["l"] and l <= cs[i + k]["l"] for k in range(1, L + 1)):
            lo.append((i, l))
    return hi, lo


def mirror(cs):
    return [dict(t=x["t"], o=-x["o"], h=-x["l"], l=-x["h"], c=-x["c"], v=x.get("v", 0)) for x in cs]


def htf_bias(hs):
    """BULL = HH+HL and last close above last swing low; BEAR = LH+LL and close below last swing high."""
    if len(hs) < 60:
        return "NA", None
    hi, lo = pivots(hs, 3)
    if len(hi) < 2 or len(lo) < 2:
        return "NEUTRAL", None
    rng = (lo[-1][1], hi[-1][1])
    px = hs[-1]["c"]
    if hi[-1][1] > hi[-2][1] and lo[-1][1] > lo[-2][1] and px > lo[-1][1]:
        return "BULL", rng
    if hi[-1][1] < hi[-2][1] and lo[-1][1] < lo[-2][1] and px < hi[-1][1]:
        return "BEAR", rng
    return "NEUTRAL", rng


STAGES = ["No liquidity sweep", "Sweep done - waiting structure shift", "Shift done - no valid FVG",
          "FVG invalid (tested / not in discount)", "Stop size outside 0.8-3 ATR", "No untouched target >= 3R",
          "SETUP READY"]


def detect_long(cs, hs, rng, c):
    """cs/hs = CLOSED candles. Returns (setup|None, stage_index). Shorts call this on mirrored data."""
    n = len(cs)
    if n < 120 or len(hs) < 60:
        return None, 0
    at = atr(cs, 14)
    a = at[-1]
    if a <= 0:
        return None, 0
    L = c["pivot_l"]
    hi, lo = pivots(cs, L)
    hh, hl = pivots(hs, 3)
    best = 0
    for j in range(n - 2, max(L + 5, n - 1 - c["sweep_scan"]), -1):
        k = cs[j]
        nxt = cs[j + 1] if j + 1 < n else None
        cand = []
        for ix, lv in lo:
            if ix > j - L or j - ix > 90:
                continue
            ext = min(k["l"], nxt["l"]) if nxt and nxt["c"] > lv else k["l"]
            back = k["c"] > lv or (nxt is not None and nxt["c"] > lv)
            depth = lv - ext
            if not (ext < lv and back and c["sweep_min_atr"] * at[j] <= depth <= c["sweep_max_atr"] * at[j]):
                continue
            if ix + 1 < j and min(x["l"] for x in cs[ix + 1:j]) <= lv:
                continue  # level already taken earlier: stale liquidity
            cand.append((ix, lv, ext))
        if not cand:
            continue
        ix, lv, sweep_low = max(cand)
        best = max(best, 1)
        prior = [(i2, p2) for i2, p2 in hi if i2 < j]
        if not prior:
            continue
        mss_lv = prior[-1][1]
        if mss_lv - sweep_low < c["mss_leg_atr"] * at[j]:
            continue
        m = None
        for q in range(j + 1, n):
            if cs[q]["c"] < sweep_low:
                break
            if cs[q]["c"] > mss_lv:
                m = q
                break
        if m is None or n - 1 - m > c["mss_max_age"]:
            continue
        disp = max((cs[q]["c"] - cs[q]["o"]) / at[q] for q in range(j + 1, m + 1))
        if disp < c["disp_atr"]:
            continue
        best = max(best, 2)
        gaps = []
        for q in range(j + 1, min(m + 1, n - 2) + 1):
            g_lo, g_hi = cs[q - 1]["h"], cs[q + 1]["l"]
            if g_hi - g_lo >= c["fvg_min_atr"] * at[q] and cs[q]["c"] > cs[q]["o"]:
                gaps.append((g_hi - g_lo, q, g_lo, g_hi))
        if not gaps:
            continue
        leg_hi = max(x["h"] for x in cs[j:])
        eq = (sweep_low + leg_hi) / 2
        valid = []
        for size, gq, g_lo, g_hi in gaps:
            ce_ = (g_lo + g_hi) / 2
            after = cs[gq + 2:]
            if ce_ <= eq and cs[-1]["c"] > ce_ and not (after and min(x["l"] for x in after) <= ce_):
                valid.append((size, ce_, g_lo, g_hi))
        if not valid:
            best = max(best, 3)
            continue
        _, ce, g_lo, g_hi = max(valid)
        entry = ce
        sl = sweep_low - c["sl_buf_atr"] * a
        dist = entry - sl
        if not (c["min_sl_atr"] * a <= dist <= c["max_sl_atr"] * a):
            best = max(best, 4)
            continue
        pools = []
        for ii, lvl in hi:
            if lvl > entry and all(x["h"] <= lvl for x in cs[ii + 1:]):
                pools.append(lvl)
        for ii, lvl in hh:
            if lvl > entry and all(x["h"] <= lvl for x in hs[ii + 1:]):
                pools.append(lvl)
        tp = None
        for lvl in sorted(set(pools)):
            t_ = lvl - c["tp_buf_atr"] * a
            if (t_ - entry) / dist >= c["min_rr"]:
                if (t_ - entry) / dist <= c["max_rr"]:
                    tp = t_
                break
        if tp is None:
            best = max(best, 5)
            continue
        # ---- bonus confluences (need >= min_bonus) ----
        bonus = []
        hr = (cs[m]["t"] // 3600000) % 24
        if c["kz_start"] <= hr < c["kz_end"]:
            bonus.append("Killzone")
        base = [x.get("v", 0) for x in cs[max(0, j - 20):j]]
        mv = sum(base) / len(base) if base else 0
        if mv > 0 and max(cs[q].get("v", 0) for q in range(j + 1, m + 1)) >= c["vol_mult"] * mv:
            bonus.append("Volume displacement")
        eql = any(i3 != ix and abs(p3 - lv) <= 0.2 * at[j] for i3, p3 in lo)
        hl_lv = any(abs(p3 - lv) <= 0.3 * at[j] for _, p3 in hl)
        if eql or hl_lv:
            bonus.append("Equal lows" if eql else "HTF low swept")
        if rng and entry < (rng[0] + rng[1]) / 2:
            bonus.append("HTF discount")
        return dict(entry=entry, sl=sl, tp=tp, dist=dist, rr=(tp - entry) / dist, level=lv, sweep_low=sweep_low,
                    sweep_t=cs[j]["t"], mss_t=cs[m]["t"], fvg=(g_lo, g_hi), atr=a, bonus=bonus), 6
    return None, best


def detect(cs, hs, bias, rng, c):
    """Direction comes ONLY from HTF structure. Returns (setup|None, stage_index)."""
    if bias == "BULL":
        s, st = detect_long(cs, hs, rng, c)
        if s:
            s["side"] = "LONG"
        return s, st
    if bias == "BEAR":
        mr = (-rng[1], -rng[0]) if rng else None
        s, st = detect_long(mirror(cs), mirror(hs), mr, c)
        if s:
            s.update(side="SHORT", entry=-s["entry"], sl=-s["sl"], tp=-s["tp"], level=-s["level"],
                     sweep_low=-s["sweep_low"], fvg=(-s["fvg"][1], -s["fvg"][0]))
        return s, st
    return None, 0


# ============== PAPER ENGINE ==============
def base_of(pair):
    return pair.replace("B-", "").replace("_USDT", "")


TRADE_COLS = ["id", "pair", "side", "entry_t", "exit_t", "entry", "exit", "sl", "tp", "rr", "qty", "risk_amt",
              "pnl", "r", "fees", "reason", "bonus", "mfe_r", "mae_r", "hold_min"]


def calc_stats(tr, start):
    n = len(tr)
    if not n:
        return dict(n=0)
    rs = [t["r"] for t in tr]
    w = [x for x in rs if x > 0]
    l = [x for x in rs if x <= 0]
    gw = sum(t["pnl"] for t in tr if t["pnl"] > 0)
    gl = -sum(t["pnl"] for t in tr if t["pnl"] <= 0)
    eq = peak = start
    mdd = 0.0
    for t in tr:
        eq += t["pnl"]
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak if peak else 0)
    aw = sum(w) / len(w) if w else 0.0
    al = sum(l) / len(l) if l else 0.0
    be_wr = (abs(al) / (aw + abs(al)) * 100) if (aw + abs(al)) else None

    def grp(key):
        d = {}
        for t in tr:
            k = key(t)
            a = d.setdefault(k, [0, 0.0, 0.0, 0])
            a[0] += 1
            a[1] += t["pnl"]
            a[2] += t["r"]
            a[3] += 1 if t["r"] > 0 else 0
        return d
    hist = {}
    for x in rs:
        b = max(-3, min(5, math.floor(x)))
        hist[b] = hist.get(b, 0) + 1
    return dict(n=n, wins=len(w), wr=len(w) / n * 100, avg_w=aw, avg_l=al, exp=sum(rs) / n,
                pf=(gw / gl) if gl else None, net=sum(t["pnl"] for t in tr), mdd=mdd * 100,
                best=max(rs), worst=min(rs), be_wr=be_wr,
                fees=sum(t.get("fees", 0) for t in tr),
                by_pair=grp(lambda t: base_of(t["pair"])), by_reason=grp(lambda t: t["reason"]),
                by_side=grp(lambda t: t["side"]), by_bonus=grp(lambda t: str(len(str(t.get("bonus", "")).split("+")) if t.get("bonus") else 0)),
                hist=hist)


class Engine:
    def __init__(self, c, feed, live=True, persist=True):
        self.c, self.feed, self.live, self.persist = c, feed, live, persist
        self.lock = threading.RLock()
        self.news = NewsFeed(c) if (live and c["news_enabled"]) else None
        self.cache, self.fails, self.parked = {}, {}, {}
        self.cd, self.scan, self.evalkey, self.evalres = {}, {}, {}, {}
        self.btc, self.err, self.last_ok, self.ferr = "NA", "", 0, ""
        self.last_save, self.last_eq = 0, 0
        self.s = self._load() if persist else self._fresh()

    # ---------- state ----------
    def _fresh(self):
        b = self.c["start_balance"]
        return dict(balance=b, peak=b, positions={}, pending={}, trades=[], eq=[], day=0, day_start=b,
                    pause_until=0, pause_why="", kill_until=0, consec=0, seen=[], events=[],
                    manual_pause=False, started=int(time.time() * 1000))

    def _load(self):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                s = json.load(f)
            base = self._fresh()
            base.update(s)
            return base
        except Exception:
            return self._fresh()

    def save(self, force=False):
        if not self.persist or (not force and time.time() - self.last_save < 10):
            return
        self.last_save = time.time()
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.s, f)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            log("save failed: %s" % e)

    def reset(self):
        with self.lock:
            self.s = self._fresh()
            self.cd.clear()
            self.save(True)

    def ev(self, msg, now=None):
        now = now or int(time.time() * 1000)
        self.s["events"].append(dict(t=now, m=msg))
        del self.s["events"][:-150]
        if self.persist:
            log(msg)

    # ---------- data ----------
    def _get(self, pair, iv, ns, act):
        hit = self.cache.get((pair, iv))
        ttl = self.c["htf_refresh_sec"] if iv == "1h" else (8 if pair in act else self.c["ltf_refresh_sec"])
        if hit and ns - hit[0] < ttl:
            return hit[1]
        if ns < self.parked.get(pair, 0):
            return hit[1] if hit else None
        try:
            cs = self.feed.get(pair, iv)
            if len(cs) < 5:
                raise ValueError("0 candles")
            self.cache[(pair, iv)] = (ns, cs)
            self.fails[pair] = 0
            return cs
        except Exception as e:
            self.fails[pair] = self.fails.get(pair, 0) + 1
            self.ferr = "%s %s: %s" % (base_of(pair), iv, str(e)[:60])
            if self.fails[pair] >= 4:
                self.parked[pair] = ns + 6 * 3600
                self.ev("%s parked 6h (no data)" % base_of(pair))
            return hit[1] if hit else None

    def tick(self):
        now, ns = int(time.time() * 1000), time.time()
        if self.news:
            self.news.start()
        act = set(self.s["positions"]) | set(self.s["pending"])
        jobs = [(p, "15m") for p in self.c["pairs"]] + [(p, "1h") for p in self.c["pairs"]]
        with ThreadPoolExecutor(5) as ex:
            res = list(ex.map(lambda a: self._get(a[0], a[1], ns, act), jobs))
        data = {}
        for (p, iv), cs in zip(jobs, res):
            data.setdefault(p, {})[iv] = cs
        if any(v.get("15m") for v in data.values()):
            self.last_ok = ns
        with self.lock:
            for p in [BTC] + [x for x in self.c["pairs"] if x != BTC]:
                d = data.get(p, {})
                if d.get("15m") and d.get("1h"):
                    self.step(p, d["15m"][-400:], d["1h"][-250:], now)
            self.mark(now)
            self.save()

    # ---------- per-pair logic ----------
    def step(self, pair, ltf, htf, now):
        c, s = self.c, self.s
        closed = [x for x in ltf if x["t"] + IV <= now]
        hcl = [x for x in htf if x["t"] + HIV <= now]
        sc = self.scan.setdefault(pair, {})
        sc.update(price=ltf[-1]["c"], t=ltf[-1]["t"])
        if len(closed) < 130 or len(hcl) < 65:
            sc.update(bias="NA", stage="Warming up", stage_i=-1)
            return
        stale = now - (ltf[-1]["t"] + IV) > c["stale_candles"] * IV
        if pair in s["pending"]:
            self.manage_pending(pair, ltf, now)
        if pair in s["positions"]:
            self.manage_position(pair, ltf, now)
        key = (closed[-1]["t"], hcl[-1]["t"])
        fresh = self.evalkey.get(pair) != key
        if fresh:
            bias, rng = htf_bias(hcl)
            setup, st = detect(closed, hcl, bias, rng, c)
            self.evalkey[pair], self.evalres[pair] = key, (bias, setup, st)
            if pair == BTC:
                self.btc = bias
        bias, setup, st = self.evalres[pair]
        atr_pct = atr(closed[-60:], 14)[-1] / closed[-1]["c"] * 100
        sc.update(bias=bias, stage=STAGES[st], stage_i=st, atr_pct=atr_pct, stale=stale,
                  setup=(dict(side=setup["side"], entry=setup["entry"], sl=setup["sl"], tp=setup["tp"],
                              rr=setup["rr"], bonus=setup["bonus"]) if setup else None))
        if setup and fresh and not stale:
            self.try_place(pair, setup, closed, now)

    def size(self, entry, dist):
        c, bal = self.c, self.s["balance"]
        qty = bal * c["risk_pct"] / dist
        notional = qty * entry
        cap = bal * c["max_notional_x"]
        if notional > cap:
            qty, notional = cap / entry, cap
        lev = min(c["max_lev"], max(1, math.ceil(notional / (bal * c["margin_frac"]))))
        return qty, qty * dist, notional, lev, notional / lev

    def try_place(self, pair, st, closed, now):
        c, s = self.c, self.s
        sid = "%s|%s|%d" % (pair, st["side"], st["sweep_t"])
        sc = self.scan[pair]

        def block(why):
            if sc.get("blocked_id") != sid:
                sc["blocked_id"] = sid
                self.ev("%s %s setup blocked: %s" % (base_of(pair), st["side"], why), now)
            sc["blocked"] = why

        if sid in s["seen"]:
            return
        if s["manual_pause"]:
            return block("Bot paused manually")
        if now < s["pause_until"]:
            return block("Paused: " + s["pause_why"])
        if now < s["kill_until"]:
            return block("Drawdown kill-switch active")
        if s["balance"] < s["day_start"] * (1 - c["max_daily_loss"]):
            return block("Daily loss limit reached")
        if pair in s["positions"] or pair in s["pending"] or now < self.cd.get(pair, 0):
            return
        allo = list(s["positions"].values()) + list(s["pending"].values())
        if len(allo) >= c["max_open"]:
            return block("Max open trades")
        if sum(1 for o in allo if o["side"] == st["side"]) >= c["max_same_side"]:
            return block("Same-side limit")
        if len(st["bonus"]) < c["min_bonus"]:
            return block("Only %d/%d bonus confluences (%s)" % (len(st["bonus"]), c["min_bonus"], ",".join(st["bonus"]) or "none"))
        if c["btc_filter"] and pair != BTC:
            if self.btc == "NA":
                return block("BTC bias unavailable")
            if (st["side"] == "LONG" and self.btc == "BEAR") or (st["side"] == "SHORT" and self.btc == "BULL"):
                return block("Against BTC 1h structure")
        if self.news:
            ok, why = self.news.check(base_of(pair), st["side"], now)
            if not ok:
                return block(why)
        slip = max(c["slippage"], c["slip_atr_k"] * st["atr"] / abs(st["entry"]))
        hold_funding = c["funding_rate"] * (c["time_stop_candles"] * IV / (c["funding_every_h"] * 3600000))
        cost_r = (c["fee_maker"] + c["fee_taker"] + slip + hold_funding) * abs(st["entry"]) / st["dist"]
        if cost_r > c["max_cost_r"]:
            return block("Costs %.2fR > %.2fR (stop too tight)" % (cost_r, c["max_cost_r"]))
        qty, risk, notional, lev, margin = self.size(st["entry"], st["dist"])
        bal = s["balance"]
        if sum(o["risk_amt"] for o in allo) + risk > bal * c["max_heat"]:
            return block("Portfolio heat cap")
        if sum(o["notional"] for o in allo) + notional > bal * c["max_gross_x"]:
            return block("Gross exposure cap")
        s["seen"].append(sid)
        del s["seen"][:-300]
        t0 = closed[-1]["t"]
        s["pending"][pair] = dict(id=sid, pair=pair, side=st["side"], entry=st["entry"], sl=st["sl"], tp=st["tp"],
                                  dist=st["dist"], rr=st["rr"], qty=qty, risk_amt=risk, notional=notional, lev=lev,
                                  margin=margin, slip=slip, placed_t=t0, expires_t=t0 + IV * (1 + c["order_ttl_candles"]),
                                  bonus=st["bonus"], fvg=list(st["fvg"]), placed_ms=now)
        sc["blocked"] = ""
        self.ev("%s %s LIMIT %.6g | SL %.6g | TP %.6g | %.1fR | risk %.2f | %s" % (
            base_of(pair), st["side"], st["entry"], st["sl"], st["tp"], st["rr"], risk, "+".join(st["bonus"])), now)

    def manage_pending(self, pair, ltf, now):
        c, s = self.c, self.s
        o = s["pending"][pair]
        L = o["side"] == "LONG"
        sg = 1 if L else -1
        for x in ltf:
            if x["t"] <= o["placed_t"]:
                continue
            filled = (x["l"] < o["entry"] * (1 - c["fill_through"])) if L else (x["h"] > o["entry"] * (1 + c["fill_through"]))
            if filled:
                del s["pending"][pair]
                return self.open_pos(o, x, now)
            ran = (x["h"] >= o["entry"] + c["cancel_r"] * o["dist"]) if L else (x["l"] <= o["entry"] - c["cancel_r"] * o["dist"])
            if ran:
                del s["pending"][pair]
                return self.ev("%s order cancelled: price left without us" % base_of(pair), now)
        if now >= o["expires_t"]:
            del s["pending"][pair]
            self.ev("%s order expired unfilled" % base_of(pair), now)

    def open_pos(self, o, x, now):
        c, s = self.c, self.s
        fee = o["qty"] * o["entry"] * c["fee_maker"]
        s["balance"] -= fee
        s["positions"][o["pair"]] = dict(o, fill_t=x["t"], done_t=x["t"] - 1, entry_fee=fee, be=False,
                                         mfe=0.0, mae=0.0, r_now=0.0, opened_ms=now)
        self.ev("%s %s FILLED @ %.6g" % (base_of(o["pair"]), o["side"], o["entry"]), now)

    def manage_position(self, pair, ltf, now):
        c, s = self.c, self.s
        p = s["positions"][pair]
        L = p["side"] == "LONG"
        sg = 1 if L else -1
        for x in ltf:
            if x["t"] < p["fill_t"]:
                continue
            closed_x = x["t"] + IV <= now
            if closed_x and x["t"] <= p["done_t"]:
                continue
            fav = ((x["h"] - p["entry"]) if L else (p["entry"] - x["l"])) / p["dist"]
            adv = ((p["entry"] - x["l"]) if L else (x["h"] - p["entry"])) / p["dist"]
            p["mfe"], p["mae"] = max(p["mfe"], fav), max(p["mae"], adv)
            if (x["l"] <= p["sl"]) if L else (x["h"] >= p["sl"]):
                ref = min(p["sl"], x["o"]) if L else max(p["sl"], x["o"])
                return self.close(pair, ref, "BE-STOP" if p["be"] else "SL", True, now)
            if x["t"] != p["fill_t"] and ((x["h"] >= p["tp"]) if L else (x["l"] <= p["tp"])):
                return self.close(pair, p["tp"], "TP", False, now)
            if closed_x:
                if not p["be"] and fav >= c["be_at_r"]:
                    p["sl"] = p["entry"] + sg * c["be_lock_r"] * p["dist"]
                    p["be"] = True
                    self.ev("%s stop moved to +%.2fR" % (base_of(pair), c["be_lock_r"]), now)
                p["done_t"] = x["t"]
        last = ltf[-1]["c"]
        p["r_now"] = (last - p["entry"]) * sg / p["dist"]
        if now - p["fill_t"] >= c["time_stop_candles"] * IV and p["r_now"] < 1.0:
            self.close(pair, last, "TIME", True, now)

    def close(self, pair, price, reason, taker, now):
        c, s = self.c, self.s
        p = s["positions"].pop(pair)
        sg = 1 if p["side"] == "LONG" else -1
        fill = price * (1 - sg * p["slip"]) if taker else price
        gross = (fill - p["entry"]) * p["qty"] * sg
        fee = p["qty"] * fill * (c["fee_taker"] if taker else c["fee_maker"])
        fund = p["qty"] * p["entry"] * c["funding_rate"] * int((now - p["fill_t"]) / (c["funding_every_h"] * 3600000))
        s["balance"] += gross - fee - fund
        net = gross - fee - fund - p["entry_fee"]
        r = net / p["risk_amt"]
        t = dict(id=p["id"], pair=pair, side=p["side"], entry_t=p["fill_t"], exit_t=now, entry=p["entry"], exit=fill,
                 sl=p["sl"], tp=p["tp"], rr=p["rr"], qty=p["qty"], risk_amt=p["risk_amt"], pnl=net, r=r,
                 fees=p["entry_fee"] + fee + fund, reason=reason, bonus="+".join(p["bonus"]),
                 mfe_r=p["mfe"], mae_r=p["mae"], hold_min=(now - p["fill_t"]) / 60000)
        s["trades"].append(t)
        del s["trades"][:-3000]
        s["consec"] = s["consec"] + 1 if net < 0 else 0
        if net < 0:
            self.cd[pair] = now + c["cooldown_candles"] * IV
        if s["consec"] >= c["consec_loss_pause"]:
            s["pause_until"], s["pause_why"], s["consec"] = now + c["pause_hours"] * 3600000, "%d losses in a row" % c["consec_loss_pause"], 0
            self.ev("Paused %dh after %d consecutive losses" % (c["pause_hours"], c["consec_loss_pause"]), now)
        s["eq"].append([now, s["balance"]])
        self.ev("%s %s CLOSED %s %+.2f USDT (%+.2fR)" % (base_of(pair), p["side"], reason, net, r), now)
        notify("%s %s %s %+.2f (%+.2fR)" % (base_of(pair), p["side"], reason, net, r))
        if self.persist:
            self.journal(t)
        self.save(True)

    def journal(self, t):
        try:
            new = not os.path.exists(JOURNAL)
            with open(JOURNAL, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(TRADE_COLS)
                w.writerow([t.get(k, "") for k in TRADE_COLS])
        except Exception as e:
            log("journal failed: %s" % e)

    def unreal(self):
        u = 0.0
        for pair, p in self.s["positions"].items():
            px = self.scan.get(pair, {}).get("price", p["entry"])
            u += (px - p["entry"]) * p["qty"] * (1 if p["side"] == "LONG" else -1)
        return u

    def mark(self, now):
        c, s = self.c, self.s
        day = now // 86400000
        if s["day"] != day:
            s["day"], s["day_start"] = day, s["balance"]
        s["peak"] = max(s["peak"], s["balance"])
        if s["balance"] < s["peak"] * (1 - c["dd_kill"]) and now >= s["kill_until"]:
            s["kill_until"], s["peak"] = now + c["dd_kill_hours"] * 3600000, s["balance"]
            self.ev("KILL-SWITCH: drawdown >= %d%%, no new trades for %dh" % (c["dd_kill"] * 100, c["dd_kill_hours"]), now)
        if now - self.last_eq > 300000:
            self.last_eq = now
            s["eq"].append([now, s["balance"] + self.unreal()])
            del s["eq"][:-3000]

    # ---------- dashboard ----------
    def snapshot(self):
        with self.lock:
            c, s = self.c, self.s
            now = int(time.time() * 1000)
            un = self.unreal()
            eqv = s["balance"] + un
            st = calc_stats(s["trades"], c["start_balance"])
            feed_ok = time.time() - self.last_ok < 90 if self.live else True
            if s["manual_pause"]:
                status = "PAUSED (manual)"
            elif not feed_ok:
                status = "FEED DOWN - no trading"
            elif now < s["kill_until"]:
                status = "KILL-SWITCH (drawdown)"
            elif now < s["pause_until"]:
                status = "PAUSED: " + s["pause_why"]
            elif s["balance"] < s["day_start"] * (1 - c["max_daily_loss"]):
                status = "DAILY LOSS LIMIT"
            else:
                status = "RUNNING"
            pos = []
            for pair, p in s["positions"].items():
                px = self.scan.get(pair, {}).get("price", p["entry"])
                sg = 1 if p["side"] == "LONG" else -1
                pos.append(dict(pair=base_of(pair), side=p["side"], entry=p["entry"], sl=p["sl"], tp=p["tp"], px=px,
                                r=(px - p["entry"]) * sg / p["dist"], pnl=(px - p["entry"]) * p["qty"] * sg,
                                risk=p["risk_amt"], lev=p["lev"], be=p["be"], t=p["fill_t"], rr=p["rr"],
                                bonus=p["bonus"]))
            pen = [dict(pair=base_of(o["pair"]), side=o["side"], entry=o["entry"], sl=o["sl"], tp=o["tp"], rr=o["rr"],
                        risk=o["risk_amt"], exp=o["expires_t"], bonus=o["bonus"],
                        px=self.scan.get(o["pair"], {}).get("price", 0)) for o in s["pending"].values()]
            scan = [dict(pair=base_of(p), **{k: v for k, v in v.items() if k != "blocked_id"})
                    for p, v in self.scan.items()]
            eq = s["eq"][-600:]
            if len(eq) > 240:
                stp = len(eq) / 240.0
                eq = [eq[int(i * stp)] for i in range(240)] + [eq[-1]]
            try:
                with open(BT_FILE, "r", encoding="utf-8") as f:
                    bt = json.load(f)
            except Exception:
                bt = None
            return dict(now=now, live=self.live, status=status, feed_ok=feed_ok, ferr=self.ferr, btc=self.btc,
                        acct=dict(bal=s["balance"], eq=eqv, un=un, start=c["start_balance"],
                                  net=eqv - c["start_balance"], day=eqv - s["day_start"],
                                  heat=sum(p["risk_amt"] for p in list(s["positions"].values()) + list(s["pending"].values()))),
                        stats=st, pos=pos, pen=pen, scan=scan, eq=eq, ev=s["events"][-60:][::-1],
                        trades=[dict(t, pair=base_of(t["pair"])) for t in s["trades"][-150:]][::-1],
                        news=(self.news.snapshot(now) if self.news else None), bt=bt,
                        cfg=dict(risk=c["risk_pct"] * 100, rr=c["min_rr"], bonus=c["min_bonus"], be=c["be_at_r"],
                                 maxopen=c["max_open"], dll=c["max_daily_loss"] * 100, fm=c["fee_maker"] * 100,
                                 ft=c["fee_taker"] * 100))


# ============== DASHBOARD ==============
HTML = r"""<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name=theme-color content="#0a0e17"><title>Dead Ninja v2</title>
<style>
:root{--bg:#0a0e17;--card:#111827;--card2:#0f1623;--line:#1f2937;--tx:#e5e7eb;--mut:#7b8798;--g:#22c55e;--r:#ef4444;--b:#3b82f6;--y:#eab308;--p:#a78bfa}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:13px/1.4 -apple-system,Segoe UI,Roboto,sans-serif;padding:max(10px,env(safe-area-inset-top)) 10px 40px}
.wrap{max-width:760px;margin:0 auto}.top{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px}
h1{font-size:15px;margin:0;letter-spacing:.5px}h1 small{color:var(--mut);font-weight:400;font-size:10px;display:block}
.pill{display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;border:1px solid var(--line);background:var(--card2);white-space:nowrap}
.g{color:var(--g)}.r{color:var(--r)}.m{color:var(--mut)}.y{color:var(--y)}.b{color:var(--b)}
.pg{background:#0d2b1a;border-color:#14532d;color:var(--g)}.pr{background:#2b0d0d;border-color:#7f1d1d;color:var(--r)}.pb{background:#0d1c33;border-color:#1e3a8a;color:var(--b)}.py{background:#2b230d;border-color:#713f12;color:var(--y)}
.hero{background:linear-gradient(135deg,#111827,#0f1a2e);border:1px solid var(--line);border-radius:14px;padding:14px;margin-bottom:10px}
.hero .big{font-size:28px;font-weight:700;letter-spacing:-.5px}.row{display:flex;justify-content:space-between;align-items:flex-end;gap:10px}
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:10px}
.k{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px}.k .l{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.6px}.k .v{font-size:17px;font-weight:700;margin-top:2px}.k .s{color:var(--mut);font-size:10px;margin-top:2px}
.tabs{display:flex;gap:4px;overflow-x:auto;margin:6px 0 10px;border-bottom:1px solid var(--line)}
.tabs button{background:none;border:0;color:var(--mut);padding:8px 10px;font-weight:600;font-size:12px;border-bottom:2px solid transparent;cursor:pointer;white-space:nowrap}
.tabs button.on{color:var(--tx);border-color:var(--b)}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:11px;margin-bottom:10px}
.card h3{margin:0 0 8px;font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.7px;font-weight:600}
.it{padding:9px 0;border-bottom:1px solid var(--line)}.it:last-child{border:0}.it .a{display:flex;justify-content:space-between;gap:8px;align-items:center}
.it .c{color:var(--mut);font-size:11px;margin-top:3px}.bar{height:6px;background:var(--line);border-radius:9px;overflow:hidden;margin-top:5px}.bar i{display:block;height:100%}
.dots{display:flex;gap:3px;margin-top:5px}.dots i{flex:1;height:4px;border-radius:3px;background:var(--line)}.dots i.on{background:var(--b)}.dots i.rd{background:var(--g)}
canvas{width:100%;height:150px;display:block}a{color:var(--b)}
.btn{background:var(--card2);border:1px solid var(--line);color:var(--tx);border-radius:8px;padding:7px 12px;font-size:12px;cursor:pointer}
.warn{background:#2b0d0d;border:1px solid #7f1d1d;color:#fecaca;border-radius:10px;padding:9px;margin-bottom:10px;font-size:12px}
.hist{display:flex;align-items:flex-end;gap:5px;height:80px}.hist div{flex:1;text-align:center;font-size:9px;color:var(--mut)}.hist b{display:block;border-radius:3px 3px 0 0;margin-bottom:2px}
.foot{color:var(--mut);font-size:10px;text-align:center;margin-top:14px}
</style></head><body><div class=wrap>
<div class=top><h1>DEAD NINJA v2 <small>SMART MONEY - sweep > shift > FVG > liquidity - paper only</small></h1><div id=stat></div></div>
<div id=warn></div><div id=hero></div><div class=tabs id=tabs></div><div id=main></div>
<div class=foot>PAPER TRADING ONLY. No real orders. No strategy is guaranteed. Judge it on backtest + live paper numbers.</div></div>
<script>
const K=new URLSearchParams(location.search).get('k')||'',Q=K?('?k='+encodeURIComponent(K)):'';
const TABS=['Overview','Scanner','Trades','Analytics','Log'];let tab=0,S=null;
const $=id=>document.getElementById(id);
const f=(n,d=2)=>n==null||isNaN(n)?'-':Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const px=n=>n==null?'-':(Math.abs(n)<1?f(n,5):Math.abs(n)<100?f(n,3):f(n,2));
const sg=n=>(n>=0?'+':'')+f(n);const cl=n=>n>0?'g':n<0?'r':'m';
const ago=t=>{const s=(Date.now()-t)/1000;return s<90?'now':s<5400?Math.round(s/60)+'m':s<172800?Math.round(s/3600)+'h':Math.round(s/86400)+'d'};
const when=t=>new Date(t).toLocaleString('en-GB',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});
function tabs(){$('tabs').innerHTML=TABS.map((t,i)=>`<button class="${i==tab?'on':''}" onclick="tab=${i};tabs();draw()">${t}</button>`).join('')}
function kpi(l,v,s,c){return `<div class=k><div class=l>${l}</div><div class="v ${c||''}">${v}</div><div class=s>${s||''}</div></div>`}
function chart(el,eq,start){const c=$(el);if(!c)return;const W=c.width=c.clientWidth*2,H=c.height=300;const x=c.getContext('2d');x.clearRect(0,0,W,H);
if(!eq||eq.length<2){x.fillStyle='#7b8798';x.font='24px sans-serif';x.fillText('Equity curve appears after trades',20,150);return}
const v=eq.map(p=>p[1]).concat([start]);let lo=Math.min(...v),hi=Math.max(...v);if(hi==lo){hi+=1;lo-=1}const pd=(hi-lo)*.1;lo-=pd;hi+=pd;
const X=i=>10+i/(eq.length-1)*(W-20),Y=y=>H-10-(y-lo)/(hi-lo)*(H-20);
x.strokeStyle='#374151';x.setLineDash([6,6]);x.beginPath();x.moveTo(0,Y(start));x.lineTo(W,Y(start));x.stroke();x.setLineDash([]);
const up=eq[eq.length-1][1]>=start,col=up?'#22c55e':'#ef4444';x.strokeStyle=col;x.lineWidth=3;x.beginPath();eq.forEach((p,i)=>i?x.lineTo(X(i),Y(p[1])):x.moveTo(X(i),Y(p[1])));x.stroke();
x.lineTo(X(eq.length-1),H);x.lineTo(X(0),H);x.fillStyle=up?'rgba(34,197,94,.10)':'rgba(239,68,68,.10)';x.fill()}
function posCard(p){return `<div class=it><div class=a><b>${p.pair} <span class="pill ${p.side=='LONG'?'pg':'pr'}">${p.side}</span> <span class="pill pb">${p.rr.toFixed(1)}R plan</span>${p.be?' <span class="pill py">stop +0.25R</span>':''}</b><b class="${cl(p.pnl)}">${sg(p.pnl)} (${p.r>=0?'+':''}${p.r.toFixed(2)}R)</b></div>
<div class=c>Entry ${px(p.entry)} - Now ${px(p.px)} - SL ${px(p.sl)} - TP ${px(p.tp)} - risk ${f(p.risk)} - ${p.lev}x - ${ago(p.t)}</div>
<div class=bar><i style="width:${Math.max(0,Math.min(100,(p.r+1)/4*100))}%;background:${p.r>=0?'var(--g)':'var(--r)'}"></i></div></div>`}
function penCard(o){return `<div class=it><div class=a><b>${o.pair} <span class="pill ${o.side=='LONG'?'pg':'pr'}">${o.side}</span> <span class="pill py">LIMIT waiting</span></b><span class=m>expires ${ago(o.exp).replace('now','soon')}</span></div>
<div class=c>Limit ${px(o.entry)} (price ${px(o.px)}) - SL ${px(o.sl)} - TP ${px(o.tp)} - ${o.rr.toFixed(1)}R - ${o.bonus.join(' + ')}</div></div>`}
function ov(){const a=S.acct,t=S.stats,n=t.n||0;let h='';
h+=`<div class=grid>${kpi('Win rate',n?f(t.wr,1)+'%':'-',n?t.wins+'W / '+(n-t.wins)+'L':'no trades yet')}${kpi('Expectancy',n?(t.exp>=0?'+':'')+f(t.exp)+' R':'-','per trade, after costs',n?cl(t.exp):'')}
${kpi('Profit factor',t.pf?f(t.pf):'-','>1.3 wanted',t.pf?(t.pf>=1.3?'g':t.pf<1?'r':'y'):'')}${kpi('Avg win / loss',n?f(t.avg_w)+' / '+f(t.avg_l)+' R':'-',t.be_wr?'break-even win rate '+f(t.be_wr,0)+'%':'')}
${kpi('Max drawdown',n?f(t.mdd,1)+'%':'-','closed trades')}${kpi('Open risk',f(a.heat)+' USDT',f(a.heat/a.bal*100,2)+'% of balance (cap 1.5%)')}</div>`;
h+=`<div class=card><h3>Equity curve</h3><canvas id=cv></canvas></div>`;
h+=`<div class=card><h3>Open positions (${S.pos.length})</h3>${S.pos.length?S.pos.map(posCard).join(''):'<div class=m>No open position. Bot only trades A+ setups, so waiting is normal.</div>'}</div>`;
h+=`<div class=card><h3>Pending limit orders (${S.pen.length})</h3>${S.pen.length?S.pen.map(penCard).join(''):'<div class=m>None</div>'}</div>`;
const b=S.bt;h+=`<div class=card><h3>Backtest proof</h3>${b&&b.n?`<div class=it><div class=a><b>${b.n} trades - win ${f(b.wr,1)}% - expectancy <span class="${cl(b.exp)}">${b.exp>=0?'+':''}${f(b.exp)}R</span> - PF ${b.pf?f(b.pf):'-'}</b></div><div class=c>${b.verdict} - ${b.days}d history - run ${when(b.ts)}</div></div>`:'<div class=m>Not run yet. Run <b>python dead_ninja_v2.py backtest</b> on your host. Do not trust any strategy before this shows a positive expectancy over 30+ trades.</div>'}</div>`;
$('main').innerHTML=h;chart('cv',S.eq,a.start)}
function sc(){const rows=S.scan.slice().sort((a,b)=>b.stage_i-a.stage_i);
let h=`<div class=card><h3>Market structure scanner - BTC 1h: <span class="${S.btc=='BULL'?'g':S.btc=='BEAR'?'r':'m'}">${S.btc}</span></h3>`;
h+=rows.map(r=>{const bc=r.bias=='BULL'?'pg':r.bias=='BEAR'?'pr':'';const i=r.stage_i;
return `<div class=it><div class=a><b>${r.pair} <span class="pill ${bc}">${r.bias}</span></b><span>${px(r.price)} <span class=m>ATR ${r.atr_pct?r.atr_pct.toFixed(2):'-'}%</span></span></div>
<div class=c>${r.stale?'<span class=r>stale data</span> - ':''}${r.stage}${r.setup?` - <b class="${r.setup.side=='LONG'?'g':'r'}">${r.setup.side} ${r.setup.rr.toFixed(1)}R</b> [${r.setup.bonus.join('+')||'no bonus'}]`:''}${r.blocked?` - <span class=y>blocked: ${r.blocked}</span>`:''}</div>
<div class=dots>${[0,1,2,3,4,5].map(k=>`<i class="${i>=6?'rd':(i>k?'on':'')}"></i>`).join('')}</div></div>`}).join('');
$('main').innerHTML=h+`</div><div class=card><h3>How to read</h3><div class=m>Dots = sweep, structure shift, FVG, valid zone, stop size, 3R target. Green = all done and order placed. Directional bias comes only from the 1h structure; the bot never trades against it or against BTC.</div></div>`}
function tr(){let h=`<div class=card><h3>Trade history <a href="/api/trades.csv${Q}" style="float:right">CSV</a></h3>`;
h+=S.trades.length?S.trades.map(t=>`<div class=it><div class=a><b>${t.pair} <span class="pill ${t.side=='LONG'?'pg':'pr'}">${t.side}</span> <span class="pill">${t.reason}</span></b><b class="${cl(t.pnl)}">${sg(t.pnl)} (${t.r>=0?'+':''}${t.r.toFixed(2)}R)</b></div>
<div class=c>${px(t.entry)} > ${px(t.exit)} - plan ${t.rr.toFixed(1)}R - peak +${t.mfe_r.toFixed(1)}R / dip -${t.mae_r.toFixed(1)}R - fees ${f(t.fees)} - ${Math.round(t.hold_min)}m - ${when(t.exit_t)}<br>${t.bonus}</div></div>`).join(''):'<div class=m>No closed trades yet.</div>';
$('main').innerHTML=h+'</div>'}
function bars(title,obj,mode){const e=Object.entries(obj||{}).sort((a,b)=>b[1][2]-a[1][2]);if(!e.length)return '';const mx=Math.max(...e.map(x=>Math.abs(x[1][2])),.01);
return `<div class=card><h3>${title}</h3>${e.map(([k,v])=>`<div class=it><div class=a><span>${k} <span class=m>${v[0]} trades, ${Math.round(v[3]/v[0]*100)}% win</span></span><b class="${cl(v[2])}">${v[2]>=0?'+':''}${v[2].toFixed(2)}R</b></div><div class=bar><i style="width:${Math.abs(v[2])/mx*100}%;background:${v[2]>=0?'var(--g)':'var(--r)'}"></i></div></div>`).join('')}</div>`}
function an(){const t=S.stats;if(!t.n){$('main').innerHTML='<div class=card><div class=m>Analytics appear after the first closed trade.</div></div>';return}
const hs=t.hist,ks=[-3,-2,-1,0,1,2,3,4,5],mx=Math.max(...ks.map(k=>hs[k]||0),1);
let h=`<div class=grid>${kpi('Net P&L',sg(t.net)+' USDT','fees paid '+f(t.fees),cl(t.net))}${kpi('Best / worst',f(t.best)+' / '+f(t.worst)+' R','single trade')}</div>`;
h+=`<div class=card><h3>R distribution (what each trade returned)</h3><div class=hist>${ks.map(k=>`<div><b style="height:${(hs[k]||0)/mx*60}px;background:${k<0?'var(--r)':k==0?'var(--mut)':'var(--g)'}"></b>${k<=-3?'<=-3':k>=5?'5+':k}R<br>${hs[k]||0}</div>`).join('')}</div></div>`;
h+=bars('By pair',t.by_pair)+bars('By exit reason',t.by_reason)+bars('By side',t.by_side)+bars('By number of confluences',t.by_bonus);
const nw=S.news;h+=nw?`<div class=card><h3>News filter (${nw.label}, ${nw.n} items)</h3>${nw.items.slice(0,6).map(x=>`<div class=it><div class=c>${x.src} - ${ago(x.t)} - ${x.title}</div></div>`).join('')}</div>`:'';
$('main').innerHTML=h}
function lg(){let h=`<div class=card><h3>Activity log</h3>${S.ev.map(e=>`<div class=it><div class=c><span class=m>${when(e.t)}</span> ${e.m}</div></div>`).join('')||'<div class=m>Nothing yet</div>'}</div>`;
h+=`<div class=card><h3>Controls</h3><button class=btn onclick="ctl('pause')">Pause</button> <button class=btn onclick="ctl('resume')">Resume</button> <button class=btn onclick="if(confirm('Reset paper account to 1000 USDT? Deletes history.'))ctl('reset')">Reset account</button>
<div class="m" style="margin-top:8px">Risk ${S.cfg.risk}%/trade - min ${S.cfg.rr}R - stop to +0.25R after ${S.cfg.be}R - max ${S.cfg.maxopen} open - daily loss stop ${S.cfg.dll}% - fees maker ${S.cfg.fm}% / taker ${S.cfg.ft}% (assumed)</div></div>`;$('main').innerHTML=h}
function draw(){if(!S)return;[ov,sc,tr,an,lg][tab]()}
async function ctl(a){await fetch('/api/'+(a=='reset'?'reset':'ctl')+Q+(Q?'&':'?')+'a='+a+'&confirm=YES');load()}
async function load(){try{const r=await fetch('/api/state'+Q);S=await r.json();const a=S.acct;
$('stat').innerHTML=`<span class="pill ${S.status=='RUNNING'?'pg':'pr'}">${S.status}</span> <span class="pill ${S.feed_ok?'pg':'pr'}">${S.feed_ok?'LIVE DATA':'NO DATA'}</span>`;
$('warn').innerHTML=S.feed_ok?'':`<div class=warn>Live data not reachable: ${S.ferr||'waiting'} . The bot will NOT trade without live data.</div>`;
$('hero').innerHTML=`<div class=hero><div class=row><div><div class=m style="font-size:10px;text-transform:uppercase;letter-spacing:.6px">Equity (paper)</div><div class=big>${f(a.eq)} <span style="font-size:13px" class=m>USDT</span></div></div>
<div style="text-align:right"><div class="${cl(a.net)}" style="font-weight:700">${sg(a.net)} (${sg(a.net/a.start*100)}%)</div><div class=m style="font-size:11px">Today ${sg(a.day)} - unreal ${sg(a.un)}</div></div></div></div>`;
draw()}catch(e){$('stat').innerHTML='<span class="pill pr">OFFLINE</span>'}}
tabs();load();setInterval(load,6000);
</script></body></html>"""


# ============== SERVER ==============
class Handler(BaseHTTPRequestHandler):
    engine = None

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        tok = CFG["token"]
        if tok and q.get("k", [""])[0] != tok:
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Forbidden")
            return
        e, p = self.engine, u.path
        if p == "/api/state":
            body, ct = json.dumps(e.snapshot()).encode(), "application/json"
        elif p == "/api/trades.csv":
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(TRADE_COLS)
            with e.lock:
                for t in e.s["trades"]:
                    w.writerow([t.get(k, "") for k in TRADE_COLS])
            body, ct = buf.getvalue().encode(), "text/csv; charset=utf-8"
        elif p == "/api/ctl":
            a = q.get("a", [""])[0]
            with e.lock:
                if a in ("pause", "resume"):
                    e.s["manual_pause"] = (a == "pause")
                    e.ev("Bot %s by user" % ("paused" if a == "pause" else "resumed"))
                    e.save(True)
            body, ct = b"{}", "application/json"
        elif p == "/api/reset":
            if q.get("confirm", [""])[0] == "YES":
                e.reset()
                e.ev("Paper account reset")
            body, ct = b"{}", "application/json"
        else:
            body, ct = HTML.encode(), "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def bot_loop(engine):
    while True:
        try:
            engine.tick()
        except Exception as ex:
            engine.err = "Engine error: %s" % ex
            log(traceback.format_exc())
        time.sleep(CFG["poll_sec"])


# ============== BACKTEST (same Engine, replayed on history) ==============
def _rows(raw):
    out = []
    for x in raw:
        t = int(x["time"])
        out.append(dict(t=t * 1000 if t < 10 ** 10 else t, o=float(x["open"]), h=float(x["high"]),
                        l=float(x["low"]), c=float(x["close"]), v=float(x.get("volume") or 0)))
    return out


def fetch_hist(feed, pair, iv, want):
    got, end = {}, None
    for _ in range(15):
        url = "https://public.coindcx.com/market_data/candles/?pair=%s&interval=%s&limit=1000" % (pair, iv)
        if end:
            url += "&endTime=%d" % end
        try:
            rows = _rows(feed._json(url))
        except Exception:
            break
        new = [r for r in rows if r["t"] not in got]
        if not new:
            break
        for r in new:
            got[r["t"]] = r
        if len(got) >= want:
            break
        end = min(got) - 1
    return sorted(got.values(), key=lambda r: r["t"])


def run_replay(eng, data, start_i=130):
    times = {p: [x["t"] for x in d[0]] for p, d in data.items()}
    htimes = {p: [x["t"] for x in d[1]] for p, d in data.items()}
    idx = {p: {t: i for i, t in enumerate(ts)} for p, ts in times.items()}
    order = ([BTC] if BTC in data else []) + [p for p in data if p != BTC]
    for ts in sorted({t for p in data for t in times[p][start_i:]}):
        now = ts + IV
        for p in order:
            i = idx[p].get(ts)
            if i is None or i < start_i:
                continue
            l, h = data[p]
            hn = bisect.bisect_right(htimes[p], now - HIV)
            if hn >= 65:
                eng.step(p, l[max(0, i - 399):i + 1], h[max(0, hn - 250):hn], now)
        eng.mark(now)


def backtest(days=30):
    feed, c = LiveFeed(), dict(CFG)
    c["news_enabled"] = False
    data = {}
    for p in c["pairs"]:
        try:
            l = fetch_hist(feed, p, "15m", days * 96 + 200)
            h = fetch_hist(feed, p, "1h", days * 24 + 120)
        except Exception as e:
            print("skip", p, e)
            continue
        if len(l) >= 200 and len(h) >= 80:
            data[p] = (l, h)
            print("%-12s 15m=%d 1h=%d (%.1f days)" % (p, len(l), len(h), len(l) / 96.0))
    if BTC not in data:
        print("BTC history not available - cannot backtest (check internet / CoinDCX access).")
        return
    eng = Engine(c, None, live=False, persist=False)
    t0 = time.time()
    run_replay(eng, data)
    tr = eng.s["trades"]
    st = calc_stats(tr, c["start_balance"])
    span = max(len(d[0]) for d in data.values()) / 96.0
    print("\n=== BACKTEST (%.0f days of history, %d pairs, %.0fs) ===" % (span, len(data), time.time() - t0))
    if not st["n"]:
        verdict = "No trades produced - filters are strict; extend history."
        print(verdict)
        res = dict(n=0, verdict=verdict)
    else:
        verdict = ("TOO FEW TRADES (%d) - not proven" % st["n"]) if st["n"] < 30 else (
            "Positive expectancy in sample - forward-test on paper before anything else"
            if st["exp"] > 0.15 and (st["pf"] or 0) > 1.3 else "NO EDGE SHOWN - do not trade this live")
        print("trades %d | win %.1f%% | avg win %.2fR | avg loss %.2fR | expectancy %+.3fR | PF %s | maxDD %.1f%% | net %+.2f USDT"
              % (st["n"], st["wr"], st["avg_w"], st["avg_l"], st["exp"], ("%.2f" % st["pf"]) if st["pf"] else "-", st["mdd"], st["net"]))
        print("break-even win rate at this payoff: %.0f%%" % (st["be_wr"] or 0))
        for k, v in sorted(st["by_reason"].items()):
            print("  exit %-8s n=%-3d  %+.2fR" % (k, v[0], v[2]))
        print("VERDICT:", verdict)
        res = dict(n=st["n"], wr=st["wr"], exp=st["exp"], pf=st["pf"], days=round(span), verdict=verdict,
                   ts=int(time.time() * 1000))
        with open("dead_ninja_v2_backtest_trades.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(TRADE_COLS)
            for t in tr:
                w.writerow([t.get(k, "") for k in TRADE_COLS])
    with open(BT_FILE, "w", encoding="utf-8") as f:
        json.dump(res, f)
    print("Saved summary -> %s (shown on dashboard Overview)" % BT_FILE)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        return backtest(int(sys.argv[2]) if len(sys.argv) > 2 else 30)
    feed = LiveFeed()
    try:
        feed.get(BTC, "15m")
        print("[OK] CoinDCX live data connected")
    except Exception as e:
        print("[!] CoinDCX not reachable now (%s). Dashboard starts; bot will NOT trade until live data returns." % e)
    eng = Engine(CFG, feed, True, True)
    Handler.engine = eng
    threading.Thread(target=bot_loop, args=(eng,), daemon=True).start()
    port = CFG["port"]
    while True:
        try:
            srv = ThreadingHTTPServer((CFG["host"], port), Handler)
            break
        except OSError:
            port += 1
    print("\n=== DEAD NINJA v2 SMC dashboard ===\nOpen: http://127.0.0.1:%d%s\n" % (port, ("/?k=" + CFG["token"]) if CFG["token"] else ""))
    log("started v2")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nShutdown.")


if __name__ == "__main__":
    main()
