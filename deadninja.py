"""
DEAD NINJA v1.3 PRO  (paper trading, CoinDCX futures data)
 - Live data, 20 pairs, 15m + 1h, BREAKOUT + PATTERN (13 candle patterns) + EMA
 - News engine (RSS keyword sentiment) used as a trade filter
 - Realistic limit-retest entries (pending orders, fill only if price returns)
 - Funding-fee model, permanent trade journal (CSV), filter-rejection analytics
 - Dashboard: Overview / Signals (Expert + Live) / Trades / Logic / News / Scanner
Paper only. No real orders. Not financial advice.
"""
import json, os, sys, time, random, threading, traceback, struct, zlib
import re, csv, io, math
import xml.etree.ElementTree as ET
import email.utils
import urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = "dead_ninja_v10.log"


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

# ============== UNIVERSE ==============
LIQUID = ["B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-XRP_USDT",
          "B-BNB_USDT", "B-DOGE_USDT", "B-HYPE_USDT", "B-ENA_USDT",
          "B-WLD_USDT", "B-NEAR_USDT", "B-ZEC_USDT", "B-1000PEPE_USDT",
          "B-ADA_USDT", "B-LINK_USDT", "B-AVAX_USDT", "B-SUI_USDT"]
# Chhode gaye (risky): IRYS, BIRB, AKE (pump coins), XAU, XAG, CL, BZ (commodity)

STABLES = ("USDC", "FDUSD", "TUSD", "DAI", "USDE", "BUSD",
           "USDP", "PYUSD", "USDS", "EURI", "USD1")

CFG = dict(
    # --- infrastructure ---
    poll_sec=5,
    port=8000,
    host=os.environ.get("DN_HOST", "127.0.0.1"),
    token=os.environ.get("DN_TOKEN", ""),
    max_pairs=20,
    pairs_override=list(LIQUID),

    # --- capital ---
    start_balance=1000.0,
    risk_per_trade=0.008,
    max_open=3,
    max_same_side=2,
    max_heat=0.025,
    max_total_margin=0.45,
    mode_slots=dict(intraday=2, swing=2),

    # --- safety ---
    max_daily_loss=0.02,
    max_dd_kill=0.15,
    cooldown_candles=8,

    # --- cost gate ---
    fee=0.0006,
    slippage=0.0002,
    max_cost_r=0.25,   # now includes funding cost
    maker_fee=0.0003,  # ASSUMPTION - check your CoinDCX futures maker fee
    slip_atr_k=0.04,   # dynamic slippage = max(slippage, 0.04*ATR/price, spread/2)
    spread_enabled=True,
    max_spread_bps=8,
    max_gross_x=3.0,   # total notional <= 3x balance
    cluster_heat=0.016,  # same cluster + same side total risk cap
    strat_window=20, strat_kill_n=15, strat_kill_r=0.0, strat_kill_hours=48,
    weekend_atr_min=40,
    htf_soft=True, htf_soft_mult=0.5,  # 1h conflict -> half size instead of reject
    pattern_limit_entry=False, pattern_retrace=0.4,
    fill_revalidate=True,
    pattern_sl_min_atr=2.0,  # was 0.2: tiny stops made cost 0.5-0.9R
    pattern_adx_min=20,
    pattern_pullback_atr=1.0,
    funding_rate=0.0001,
    funding_every_h=8,
    news_enabled=True,
    news_refresh_sec=600,
    news_window_h=12,
    news_coin_block=3,
    news_global_block=1.0,
    journal_file="dead_ninja_trades.csv",

    # --- quality gates ---
    min_quality_score=65,   # was 70 (strict). Paper-test value, not proven
    min_atr_percentile=30,
    adx_min=28,
    adx_strong=35,
    rsi_max=68, rsi_min=32,
    vol_mult=1.3,

    # --- regime ---
    regime_pair="B-BTC_USDT",
    regime_filter=True,
    htf_filter=True,
    btc_chop_adx=20,

    # --- stops / targets ---
    sl_atr_mult=2.0,
    sl_min_atr=1.0,
    sl_max_atr=2.5,
    tp_r=2.5,
    pb_r=1.0,
    pb_pct=0.4,
    trail_atr=1.2,
    trail_after_r=1.0,

    # --- entry ---
    limit_entry=True,
    entry_offset_atr=0.3,
    entry_timeout_candles=2,

    # --- modes ---
    modes=["intraday", "swing"],
    strategies=["BREAKOUT", "PATTERN", "EMA", "REVERSAL"],
    rev_rsi_long=38, rev_rsi_short=62,   # exhaustion needed for counter-trend entries
    rev_extreme_atr=0.5,   # pattern must form at the N-bar low/high (within 0.5 ATR)
    rev_vol_mult=1.0, rev_tp_r=1.5, rev_risk_mult=0.5,
    rev_max_open=1, rev_min_quality=65,

    # --- breakout ---
    brk_n=20,
    brk_vol_mult=1.3,
    brk_max_ext=1.2,
    body_min=0.55,
    brk_rsi_max=68, brk_rsi_min=32,

    # --- leverage ---
    max_lev_major=8, max_lev_large=6, max_lev_alt=4,
    min_lev=2,
    liq_buffer=3.0,
    mmr=0.005,
    lev_vol_k=4.0,
    max_margin_pct=0.20,

    allow_sim_fallback=False,
    sim_ticks_per_candle=6,
    reset=False,
)

MODES = {
    "intraday": dict(
        interval="15m", trend_ema=100, adx_min=25, brk_n=20,
        sl_atr_mult=2.0, tp_r=2.5, pb_r=1.0, trail_atr=1.2,
        risk_per_trade=0.008, hold_h=16,
        fast_ema=9, slow_ema=21,  # <--- YEH ADD KIYA GAYA HAI
    ),
    "swing": dict(
        interval="1h", trend_ema=100, adx_min=28, brk_n=24,
        sl_atr_mult=2.5, tp_r=3.0, pb_r=1.5, trail_atr=1.8,
        risk_per_trade=0.008, hold_h=96,
        fast_ema=9, slow_ema=21,  # <--- YEH ADD KIYA GAYA HAI
    ),
}

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

class SimFeed:
    """Fake feed (only if CoinDCX is unreachable)."""
    live = False

    def __init__(self, c):
        self.rnd = random.Random()
        self.n = c["sim_ticks_per_candle"]
        self.st, self.px, self.dr = {}, {}, {}
        self.names = FALLBACK[:6]

    def pairs(self, cfg):
        return self.names

    def _init(self, pair, iv):
        ms = IVMS[iv]
        self.px.setdefault(pair, self.rnd.uniform(0.5, 60000))
        self.dr.setdefault(pair, 0.0)
        t0 = int(time.time() * 1000) - 300 * ms
        p, drift, cs = 100.0, 0.0, []
        for k in range(300):
            if self.rnd.random() < 0.04:
                drift = self.rnd.uniform(-0.0008, 0.0008)
            o = p
            p = o * (1 + drift + self.rnd.gauss(0, 0.003))
            cs.append(dict(
                t=t0 + k * ms, o=o, c=p,
                v=self.rnd.uniform(50, 150),
                h=max(o, p) * (1 + abs(self.rnd.gauss(0, 0.0015))),
                l=min(o, p) * (1 - abs(self.rnd.gauss(0, 0.0015)))))
        f = self.px[pair] / p
        for x in cs:
            for k in ("o", "h", "l", "c"):
                x[k] *= f
        q = self.px[pair]
        self.st[(pair, iv)] = dict(
            cs=cs, ticks=0, ms=ms,
            cur=dict(t=t0 + 300 * ms, o=q, h=q, l=q, c=q))

    def get(self, pair, iv):
        if (pair, iv) not in self.st:
            self._init(pair, iv)
        s = self.st[(pair, iv)]
        if self.rnd.random() < 0.03:
            self.dr[pair] = self.rnd.uniform(-0.0012, 0.0012)
        self.px[pair] *= 1 + self.dr[pair] + self.rnd.gauss(0, 0.0015)
        p, cur = self.px[pair], s["cur"]
        cur["c"] = p
        cur["h"] = max(cur["h"], p)
        cur["l"] = min(cur["l"], p)
        s["ticks"] += 1
        if s["ticks"] >= self.n:
            s["ticks"] = 0
            mult = 2.5 if self.rnd.random() < 0.15 else 1
            s["cs"].append(dict(
                cur, v=self.rnd.uniform(50, 150) * mult,
                h=cur["h"] * (1 + abs(self.rnd.gauss(0, 0.001))),
                l=cur["l"] * (1 - abs(self.rnd.gauss(0, 0.001)))))
            s["cur"] = dict(t=cur["t"] + s["ms"], o=p, h=p, l=p, c=p)
        return [dict(x) for x in s["cs"][-400:]] + [dict(s["cur"])]

def candle_patterns(cs, i):
    c, p, q = cs[i], cs[i - 1], cs[i - 2]
    rng = c["h"] - c["l"]
    if rng <= 0:
        return []
    body = lambda k: abs(k["c"] - k["o"])
    b = body(c)
    up = c["h"] - max(c["o"], c["c"])
    lo = min(c["o"], c["c"]) - c["l"]
    bull = c["c"] > c["o"]
    bear = c["c"] < c["o"]
    out = []
    if bull and p["c"] < p["o"] and c["o"] <= p["c"] and c["c"] >= p["o"] and b > body(p):
        out.append(("Bullish Engulfing", "LONG"))
    if bear and p["c"] > p["o"] and c["o"] >= p["c"] and c["c"] <= p["o"] and b > body(p):
        out.append(("Bearish Engulfing", "SHORT"))
    if lo >= 0.6 * rng and b <= 0.35 * rng and up <= 0.15 * rng:
        out.append(("Hammer", "LONG"))
    if up >= 0.6 * rng and b <= 0.35 * rng and lo <= 0.15 * rng:
        out.append(("Shooting Star", "SHORT"))
    qb, pb = body(q), body(p)
    if qb > 0 and pb <= 0.4 * qb:
        if q["c"] < q["o"] and bull and c["c"] > (q["o"] + q["c"]) / 2:
            out.append(("Morning Star", "LONG"))
        if q["c"] > q["o"] and bear and c["c"] < (q["o"] + q["c"]) / 2:
            out.append(("Evening Star", "SHORT"))
    bp = body(p)
    if bull and p["c"] < p["o"] and bp > 0 and c["o"] > p["c"] and c["c"] < p["o"] and b < 0.6 * bp:
        out.append(("Bullish Harami", "LONG"))
    if bear and p["c"] > p["o"] and bp > 0 and c["o"] < p["c"] and c["c"] > p["o"] and b < 0.6 * bp:
        out.append(("Bearish Harami", "SHORT"))
    if bull and p["c"] < p["o"] and c["o"] < p["c"] and (p["o"] + p["c"]) / 2 < c["c"] < p["o"]:
        out.append(("Piercing Line", "LONG"))
    if bear and p["c"] > p["o"] and c["o"] > p["c"] and p["o"] < c["c"] < (p["o"] + p["c"]) / 2:
        out.append(("Dark Cloud Cover", "SHORT"))
    if bull and p["c"] < p["o"] and abs(c["l"] - p["l"]) <= 0.1 * rng:
        out.append(("Tweezer Bottom", "LONG"))
    if bear and p["c"] > p["o"] and abs(c["h"] - p["h"]) <= 0.1 * rng:
        out.append(("Tweezer Top", "SHORT"))
    if bull and p["c"] > p["o"] and q["c"] > q["o"] and c["c"] > p["c"] > q["c"] and c["o"] > p["o"] > q["o"]:
        out.append(("Three White Soldiers", "LONG"))
    if bear and p["c"] < p["o"] and q["c"] < q["o"] and c["c"] < p["c"] < q["c"] and c["o"] < p["o"] < q["o"]:
        out.append(("Three Black Crows", "SHORT"))
    if b >= 0.85 * rng:
        out.append(("Bullish Marubozu" if bull else "Bearish Marubozu",
                    "LONG" if bull else "SHORT"))
    return out


def build_ctx(cs, c):
    cl = [x["c"] for x in cs]
    i = len(cl) - 1
    X = dict(
        cs=cs, i=i, cl=cl,
        f=ema(cl, c["fast_ema"]),
        s=ema(cl, c["slow_ema"]),
        tr=ema(cl, c["trend_ema"]),
        rs=rsi(cl, 14),
        at=atr(cs, 14),
        ad=adx(cs))
    X["avgv"] = sum(x.get("v", 0) for x in cs[-21:-1]) / 20
    X["atr_pct"] = atr_percentile(cs)
    X["vr"] = vol_ratio(cs)
    X["ind"] = dict(
        ema_f=X["f"][i], ema_s=X["s"][i], trend=X["tr"][i],
        adx=X["ad"][i], rsi=X["rs"][i], atr=X["at"][i],
        atr_pct=X["atr_pct"], vol_ratio=X["vr"], close=cl[i])
    X["pats"] = candle_patterns(cs, i)
    n, a = c["brk_n"], X["at"][i]
    hh = max(x["h"] for x in cs[i - n:i])
    ll = min(x["l"] for x in cs[i - n:i])
    X["hh"], X["ll"] = hh, ll
    X["near"] = ("Near %d-high" % n) if 0 <= hh - cl[i] <= 0.5 * a else \
                ("Near %d-low" % n) if 0 <= cl[i] - ll <= 0.5 * a else ""
    return X

def score_reversal(X, cand, c):
    """Counter-trend quality 0-100: exhaustion + extreme + climax volume + pattern + candle."""
    ind, k, L = X["ind"], X["cs"][X["i"]], cand["side"] == "LONG"
    score, rs = 0, []
    r = ind["rsi"]
    for lim, pts in ((25, 25), (30, 20), (35, 15), (40, 8)):
        if (r <= lim) if L else (r >= 100 - lim):
            score += pts
            rs.append("RSI %.0f exhausted (+%d)" % (r, pts))
            break
    prox = cand.get("prox", 9)
    pts = 20 if prox <= 0 else 14 if prox <= 0.3 else 8 if prox <= 0.6 else 0
    score += pts
    rs.append("At extreme, %.2f ATR (+%d)" % (max(prox, 0), pts))
    vr = ind["vol_ratio"]
    pts = 20 if vr >= 2 else 15 if vr >= 1.5 else 8 if vr >= 1.2 else 0
    score += pts
    rs.append("Volume %.1fx (+%d)" % (vr, pts))
    n = cand["label"]
    pts = 15 if any(w in n for w in ("Engulfing", "Star", "Piercing", "Dark Cloud", "Soldiers", "Crows")) \
        else 12 if n in ("Hammer", "Shooting Star") else 10 if "Tweezer" in n else 6
    score += pts
    rs.append("Pattern %s (+%d)" % (n, pts))
    rng = k["h"] - k["l"]
    if rng > 0:
        b = abs(k["c"] - k["o"]) / rng * 100
        pts = 10 if b >= 55 else 5 if b >= 40 else 0
        score += pts
        rs.append("Candle body %.0f%% (+%d)" % (b, pts))
    ap = ind["atr_pct"]
    pts = 10 if ap >= 50 else 5 if ap >= 30 else 0
    score += pts
    rs.append("ATR pct %.0f%% (+%d)" % (ap, pts))
    if X.get("avgv", 0) <= 0:
        score = score * 100.0 / 80.0
        rs.append("No volume data: scaled x1.25")
    return int(score), rs


def score_setup(X, cand, c):
    if cand["strat"] == "REVERSAL":
        return score_reversal(X, cand, c)
    """
    Quality score 0-100. Higher = better. <70 = skip.
    Components:
      - ADX strength (0-25)
      - Volume confirmation (0-20)
      - Trend alignment (0-20)
      - ATR percentile / volatility (0-15)
      - RSI position (0-10)
      - Candle body strength (0-10)
    """
    ind = X["ind"]
    score = 0
    reasons = []

    # 1. Trend strength (25)
    ad = ind["adx"]
    if ad >= 40:
        score += 25
        reasons.append("ADX %.1f very strong (+25)" % ad)
    elif ad >= 35:
        score += 20
        reasons.append("ADX %.1f strong (+20)" % ad)
    elif ad >= 28:
        score += 15
        reasons.append("ADX %.1f ok (+15)" % ad)
    else:
        reasons.append("ADX %.1f weak (+0)" % ad)

    # 2. Volume (20)
    vr = ind["vol_ratio"]
    if vr >= 2.0:
        score += 20
        reasons.append("Vol %.1fx surge (+20)" % vr)
    elif vr >= 1.5:
        score += 15
        reasons.append("Vol %.1fx good (+15)" % vr)
    elif vr >= 1.3:
        score += 8
        reasons.append("Vol %.1fx ok (+8)" % vr)
    else:
        reasons.append("Vol %.1fx low (+0)" % vr)

    # 3. Trend alignment (20)
    px = ind["close"]
    tr = ind["trend"]
    if cand["side"] == "LONG" and px > tr * 1.01:
        score += 20
        reasons.append("Price 1% above EMA (+20)")
    elif cand["side"] == "SHORT" and px < tr * 0.99:
        score += 20
        reasons.append("Price 1% below EMA (+20)")
    elif (cand["side"] == "LONG" and px > tr) or \
         (cand["side"] == "SHORT" and px < tr):
        score += 12
        reasons.append("Price vs EMA ok (+12)")
    else:
        reasons.append("Price vs EMA weak (+0)")

    # 4. Volatility (15)
    ap = ind["atr_pct"]
    if ap >= 60:
        score += 15
        reasons.append("ATR pct %.0f%% active (+15)" % ap)
    elif ap >= 40:
        score += 10
        reasons.append("ATR pct %.0f%% ok (+10)" % ap)
    elif ap >= 30:
        score += 5
        reasons.append("ATR pct %.0f%% marginal (+5)" % ap)
    else:
        reasons.append("ATR pct %.0f%% dead (+0)" % ap)

    # 5. RSI (10)
    r = ind["rsi"]
    if cand["side"] == "LONG":
        if 40 <= r <= 65:
            score += 10
            reasons.append("RSI %.0f sweet (+10)" % r)
        elif 35 <= r <= 68:
            score += 5
            reasons.append("RSI %.0f ok (+5)" % r)
    else:
        if 35 <= r <= 60:
            score += 10
            reasons.append("RSI %.0f sweet (+10)" % r)
        elif 32 <= r <= 65:
            score += 5
            reasons.append("RSI %.0f ok (+5)" % r)

    # 6. Candle body (10)
    k = X["cs"][X["i"]]
    rng = k["h"] - k["l"]
    if rng > 0:
        body_pct = abs(k["c"] - k["o"]) / rng * 100
        if body_pct >= 70:
            score += 10
            reasons.append("Body %.0f%% strong (+10)" % body_pct)
        elif body_pct >= 55:
            score += 5
            reasons.append("Body %.0f%% ok (+5)" % body_pct)

    if X.get("avgv", 0) <= 0:
        score = score * 100.0 / 80.0
        reasons.append("No volume data: score scaled x1.25")
    return int(score), reasons


def find_setups(X, c):
    cs, i, cl = X["cs"], X["i"], X["cl"]
    a, k, px = X["at"][i], cs[X["i"]], cl[i]
    f, s, tr, rs, ad = X["f"], X["s"], X["tr"], X["rs"], X["ad"]
    ck = lambda ok, t: dict(ok=bool(ok), t=t)
    rng, out = k["h"] - k["l"], []

    # ---- BREAKOUT ----
    if "BREAKOUT" in c["strategies"] and i > c["brk_n"] + 2 and rng > 0:
        n = c["brk_n"]
        hh, ll = X["hh"], X["ll"]
        for side, hit in (("LONG", px > hh), ("SHORT", px < ll)):
            if not hit:
                continue
            L = side == "LONG"
            lvl = hh if L else ll
            
            # --- LIMIT ENTRY LOGIC ---
            entry_px = px
            if c.get("limit_entry"):
                entry_px = lvl - (c["entry_offset_atr"] * a) if L else lvl + (c["entry_offset_atr"] * a)
                # Agar limit price close se bahut door hai, toh normal close use karo
                if (L and entry_px < k["l"]) or (not L and entry_px > k["h"]):
                    entry_px = px
            
            pos = (entry_px - k["l"]) / rng if L else (k["h"] - entry_px) / rng
            body = abs(k["c"] - k["o"])
            ch = [
                ck(True, "BREAKOUT %s %.4f" % ("above" if L else "below", lvl)),
                ck(body >= c["body_min"] * rng and pos >= 0.7,
                   "Strong close: body %.0f%%, top 30%%" % (body / rng * 100)),
                ck(rng >= a, "Range %.1f x ATR (need >= 1.0)" % (rng / a)),
                ck(abs(px - lvl) <= c["brk_max_ext"] * a,
                   "Not chasing: %.1f ATR (max %.1f)" % (abs(px - lvl) / a, c["brk_max_ext"])),
                ck(px > tr[i] if L else px < tr[i], "Trend aligned"),
                ck(ad[i] >= c["adx_min"], "ADX %.1f >= %d" % (ad[i], c["adx_min"])),
                ck(rs[i] < c["brk_rsi_max"] if L else rs[i] > c["brk_rsi_min"],
                   "RSI %.1f not exhausted" % rs[i]),
            ]
            if X["avgv"] > 0:
                v = k.get("v", 0) / X["avgv"]
                ch.append(ck(v >= c["brk_vol_mult"], "Vol %.1fx (need %.1f)" % (v, c["brk_vol_mult"])))
            
            sl = (min(k["l"], lvl) - 0.2 * a) if L else (max(k["h"], lvl) + 0.2 * a)
            out.append(dict(side=side, strat="BREAKOUT", sl=sl, entry_px=entry_px, checks=ch,
                            label="Breakout %d-bar %s" % (n, "high" if L else "low")))

    # ---- PATTERN ----
    if "PATTERN" in c["strategies"] and i >= 3:
        for name, side in X["pats"]:
            L = side == "LONG"
            if (px > tr[i]) != L:
                if "REVERSAL" in c["strategies"] and "Marubozu" not in name:
                    sp = 3 if ("Star" in name or "Soldiers" in name or "Crows" in name) else \
                        2 if any(w in name for w in ("Engulfing", "Harami", "Piercing", "Dark Cloud", "Tweezer")) else 1
                    zn = cs[i - sp + 1:i + 1]
                    ex = min(x["l"] for x in zn) if L else max(x["h"] for x in zn)
                    prox = ((ex - X["ll"]) if L else (X["hh"] - ex)) / a
                    ch = [
                        ck(True, "Reversal: %s (counter-trend %s)" % (name, side)),
                        ck(rs[i] <= c["rev_rsi_long"] if L else rs[i] >= c["rev_rsi_short"],
                           "RSI %.1f exhausted (need %s%d)" % (
                               rs[i], "<=" if L else ">=",
                               c["rev_rsi_long"] if L else c["rev_rsi_short"])),
                        ck(prox <= c["rev_extreme_atr"],
                           "At %d-bar %s (%.2f ATR away, max %.1f)" % (
                               c["brk_n"], "low" if L else "high", max(prox, 0), c["rev_extreme_atr"])),
                        ck((k["c"] > k["o"]) if L else (k["c"] < k["o"]),
                           "Confirmation candle closes %s" % ("up" if L else "down")),
                        ck(0.6 * a <= rng <= 3 * a, "Size %.1f x ATR" % (rng / a)),
                    ]
                    if X["avgv"] > 0:
                        v = k.get("v", 0) / X["avgv"]
                        ch.append(ck(v >= c["rev_vol_mult"], "Climax volume %.1fx (need %.1f)" % (v, c["rev_vol_mult"])))
                    out.append(dict(side=side, strat="REVERSAL", label=name, entry_px=px,
                                    checks=ch, prox=prox, tp_r=c["rev_tp_r"],
                                    risk_mult=c["rev_risk_mult"],
                                    sl=(ex - 0.2 * a) if L else (ex + 0.2 * a)))
                continue
            span = 3 if ("Star" in name or "Soldiers" in name or "Crows" in name) else \
                2 if any(w in name for w in ("Engulfing", "Harami", "Piercing", "Dark Cloud", "Tweezer")) else 1
            zone = cs[i - span + 1:i + 1]
            ext = min(x["l"] for x in zone) if L else max(x["h"] for x in zone)
            ch = [
                ck(True, "Pattern: %s" % name),
                ck(True, "With trend"),
                ck(ad[i] >= c.get("pattern_adx_min", c["adx_min"]),
                   "ADX %.1f (pattern min %d)" % (ad[i], c.get("pattern_adx_min", c["adx_min"]))),
                ck((k["l"] <= s[i] + c.get("pattern_pullback_atr", 0.5) * a and px >= s[i]) if L else
                   (k["h"] >= s[i] - c.get("pattern_pullback_atr", 0.5) * a and px <= s[i]),
                   "Pullback to EMA%d" % c["slow_ema"]),
                ck(rs[i] < 65 if L else rs[i] > 35, "RSI %.1f ok" % rs[i]),
                ck(0.6 * a <= rng <= 3 * a, "Size %.1f x ATR" % (rng / a)),
            ]
            out.append(dict(side=side, strat="PATTERN", label=name, entry_px=px, checks=ch,
                            sl=(ext - 0.2 * a) if L else (ext + 0.2 * a)))

    # ---- EMA CROSSOVER ----
    if "EMA" in c["strategies"] and i > 2:
        F, S = c["fast_ema"], c["slow_ema"]
        for side, hit in (
                ("LONG", f[i - 1] <= s[i - 1] and f[i] > s[i]),
                ("SHORT", f[i - 1] >= s[i - 1] and f[i] < s[i])):
            if not hit:
                continue
            L = side == "LONG"
            ch = [
                ck(True, "EMA%d crossed %s EMA%d" % (F, "above" if L else "below", S)),
                ck(px > tr[i] if L else px < tr[i], "Trend aligned"),
                ck(ad[i] >= c["adx_min"], "ADX %.1f" % ad[i]),
                ck(rs[i] < c["rsi_max"] if L else rs[i] > c["rsi_min"], "RSI %.1f ok" % rs[i]),
            ]
            out.append(dict(side=side, strat="EMA", label="EMA%d/%d cross" % (F, S),
                            entry_px=px, checks=ch, sl=None))
    return out

FALLBACK = LIQUID  # SimFeed aur LiveFeed ke liye zaroori hai

def plan_stop(cand, price, ind, c):
    a = ind["atr"]
    if cand.get("sl") is not None:
        dist = abs(price - cand["sl"])
        mn = c.get("pattern_sl_min_atr", 2.0) * a
        if dist < mn:
            dist = mn
        return dist, "Pattern SL (min %.1f ATR)" % c.get("pattern_sl_min_atr", 2.0)
    
    dist = c.get("sl_atr_mult", 2.0) * a
    min_dist = c.get("sl_min_atr", 1.0) * a
    max_dist = c.get("sl_max_atr", 2.5) * a
    
    if dist < min_dist: dist = min_dist
    if dist > max_dist: dist = max_dist
    
    return dist, "ATR-based SL (%.1fx)" % (dist / a)

def auto_leverage(pair, price, dist, atr, c):
    base = base_of(pair)
    if base in ("BTC", "ETH"):
        max_lev = c.get("max_lev_major", 8)
        lwhy = "Major coin"
    elif base in ("SOL", "BNB", "XRP", "ADA", "DOGE"):
        max_lev = c.get("max_lev_large", 6)
        lwhy = "Large cap"
    else:
        max_lev = c.get("max_lev_alt", 4)
        lwhy = "Altcoin (lower lev)"

    vol_factor = 1.0
    if atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        if atr_pct > 3.0:
            vol_factor = 0.5
            lwhy += ", high vol"
        elif atr_pct > 1.5:
            vol_factor = 0.75
            lwhy += ", med vol"

    lev = int(max_lev * vol_factor)
    if lev < c.get("min_lev", 2):
        lev = c.get("min_lev", 2)
    if dist and price:
        capl = int(1.0 / (c.get("liq_buffer", 3.0) * dist / price + c.get("mmr", 0.005)))
        if capl < lev:
            lev, lwhy = capl, lwhy + ", liq-buffer cap"

    return lev, lwhy


ORDER = ["intraday", "swing"]
def base_of(pair):
    return pair.replace("B-", "").replace("_USDT", "")
IVMS = {"1m": 60000, "5m": 300000, "15m": 900000, "30m": 1800000,
        "1h": 3600000, "2h": 7200000, "4h": 14400000}

CLUSTER = {"BTC": "CORE", "ETH": "CORE", "BNB": "CORE"}   # everything else = ALT

_NUM = re.compile(r"[-+]?\d[\d.,]*%?")
TRADE_COLS = ["id", "pair", "side", "mode0", "strat", "setup", "quality",
              "entry_t", "exit_t", "entry", "exit", "qty", "lev", "pnl", "r",
              "fees", "exit_reason", "exit_why"]


class Engine:
    def __init__(self, cfg, feed, live):
        self.c, self.feed, self.live = cfg, feed, live
        self.mc = {m: dict(cfg, **MODES[m]) for m in ORDER if m in cfg["modes"]}
        # ... aage ka code ...

        self.mc = {m: dict(cfg, **MODES[m]) for m in ORDER if m in cfg["modes"]}
        self.base = min(self.mc, key=lambda m: IVMS[self.mc[m]["interval"]])
        self.lock = threading.Lock()
        self.file = "dead_ninja_v10_state_%s.json" % ("live" if live else "sim")
        self.s = dict(
            balance=cfg["start_balance"], trades=[], equity=[], decisions=[],
            peak=cfg["start_balance"], max_dd=0.0, next_id=1, positions={},
            day=None, day_start=cfg["start_balance"],
            paused_day=None, kill_until=0)
        self.pairs, self.reserve, self.dead = [], [], {}
        self.prices = {}
        self.inited = False
        self.nxt, self.last_closed, self.scan = {}, {}, {}
        self.fail, self.skip_until = {}, {}
        self.regime, self.tdir, self.cool = {}, {}, {}
        self.status, self.err, self.updated, self.dirty = "starting", "", 0, False
        self.signals, self.spread_cache = {}, {}
        self.news = NewsFeed(cfg) if cfg.get("news_enabled") else None
        if not cfg["reset"] and os.path.exists(self.file):
            try:
                old = json.load(open(self.file))
                if "positions" in old:
                    old["positions"] = {
                        k: v for k, v in old["positions"].items() if "mode" in v}
                    self.s.update(old)
            except Exception as e:
                log("state load failed: %s" % e)

    def save(self):
        try:
            tmp = self.file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.s, f)
            os.replace(tmp, self.file)
        except Exception as e:
            log("state save failed: %s" % e)

    def mark_equity(self):
        s = self.s
        s["equity"].append([int(time.time() * 1000), round(s["balance"], 4)])
        del s["equity"][:-600]
        s["peak"] = max(s["peak"], s["balance"])
        s["max_dd"] = max(s["max_dd"], (s["peak"] - s["balance"]) / s["peak"])

    def fetch(self, item):
        key, iv = item
        try:
            cs = self.feed.get(key[0], iv)
            if len(cs) <= 3:
                raise ValueError("only %d candles" % len(cs))
            return key, cs, ""
        except Exception as e:
            return key, None, (str(e) or e.__class__.__name__)[:60]

    def kill(self, pair, why, now):
        self.dead[pair] = dict(why=why, until=now + 6 * 3600000)
        removed = pair in self.pairs
        if removed:
            self.pairs.remove(pair)
        if removed and self.reserve:
            self.pairs.append(self.reserve.pop(0))
        for k in [k for k in self.scan if k.startswith(pair + "|")]:
            del self.scan[k]
        log("pair parked 6h: %s (%s)" % (pair, why))

    def tick(self):
        self.s.setdefault("pending", {})
        self.s.setdefault("rej", dict(total=0, first={}, all={}))
        self.s.setdefault("strat_off", {})
        self.s.setdefault("strat_reset", {})
        if self.news:
            self.news.start()
        if not self.inited:
            allp = [p for p in self.feed.pairs(self.c)
                    if base_of(p) not in STABLES]
            self.pairs = allp[:self.c["max_pairs"]]
            self.reserve = allp[self.c["max_pairs"]:]
            self.inited = True
        now = int(time.time() * 1000)
        for pr in [p for p, d in self.dead.items() if now >= d["until"]]:
            del self.dead[pr]
            target = self.pairs if len(self.pairs) < self.c["max_pairs"] else self.reserve
            target.append(pr)
            for m in self.mc:
                self.fail[(pr, m)] = 0
                self.skip_until.pop((pr, m), None)
        held = self.s["positions"]
        urgent, rest = [], []
        for pair in list(self.pairs) + [p for p in held if p not in self.pairs]:
            for m, mc in self.mc.items():
                key = (pair, m)
                isu = (pair in held or pair in self.s["pending"]) and m == self.base
                if not isu and now < self.skip_until.get(key, 0):
                    continue
                if isu or not self.live or now >= self.nxt.get(key, 0):
                    (urgent if isu else rest).append((key, mc["interval"]))
        with self.lock:
            self.updated, self.status, self.err = now, "ok", ""
        for batch in (urgent, rest):
            if not batch:
                continue
            with ThreadPoolExecutor(max_workers=6) as ex:
                res = list(ex.map(self.fetch, batch))
            with self.lock:
                self.process(res, int(time.time() * 1000))
        with self.lock:
            if self.dirty:
                self.mark_equity()
                self.save()
                self.dirty = False

    def process(self, res, now):
        if res and all(cs is None for _, cs, _w in res) and not self.last_closed:
            self.status, self.err = "feed_error", "No data (%s)" % res[0][2]
        for key, cs, why in res:
            pair, m = key
            held = self.s["positions"]
            if cs is None:
                n = self.fail[key] = self.fail.get(key, 0) + 1
                self.scan.setdefault(
                    "%s|%s" % key, dict(pair=pair, mode=m))["status"] = (
                        "No data: " + why)
                if pair not in held:
                    self.skip_until[key] = now + min(1800, 30 * 2 ** min(n, 6)) * 1000
                    if n >= 8:
                        self.fail[key] = 0
                        self.kill(pair, why, now)
                continue
            self.fail[key] = 0
            iv = self.mc[m]["interval"]
            closed, price = cs[:-1], cs[-1]["c"]
            if m == self.base:
                self.prices[pair] = price
                if self.live and closed[-1]["t"] < now - 4 * IVMS[iv]:
                    self.status, self.err = "stale", "Delayed: " + pair
                    if pair not in held:
                        continue
                if pair in held:
                    self.manage(pair, price, cs, now)
                elif pair in self.s.get("pending", {}):
                    self.check_pending(pair, price, cs, now)
            if self.last_closed.get(key) != closed[-1]["t"]:
                first = key not in self.last_closed
                self.last_closed[key] = closed[-1]["t"]
                self.nxt[key] = closed[-1]["t"] + 2 * IVMS[iv] + 3000
                self.evaluate(pair, m, closed, cs[-1],
                              allow_trade=not first, now=now)
                self.dirty = True

    def event(self, p, txt):
        p["events"].append([int(time.time() * 1000), txt])
        del p["events"][:-40]
# NOTE: This continues the Engine class body (already indented inside class)

    def manage(self, pair, price, cs, now):
        """Wick-based SL/TP. Stop first. Trailing from 1R. No time-limit cut."""
        c, p = self.c, self.s["positions"][pair]
        L = p["side"] == "LONG"
        sg = 1 if L else -1
        slip = p.get("slip", c["slippage"])
        if p.get("seen_t") is None:
            p["seen_t"] = cs[-1]["t"]
        new = [x for x in cs[:-1] if x["t"] > p["seen_t"]]
        hi = max([price] + [x["h"] for x in new])
        lo = min([price] + [x["l"] for x in new])
        if new:
            p["seen_t"] = new[-1]["t"]
        p["last"] = price
        fr, fh = c.get("funding_rate", 0), c.get("funding_every_h", 8)
        if fr and fh:
            per = int((now - p["entry_t"]) / (fh * 3600000))
            if per > p.get("fund_n", 0):
                chg = (per - p.get("fund_n", 0)) * p["qty"] * price * fr
                self.s["balance"] -= chg
                p["realized"] -= chg
                p["fees_paid"] += chg
                p["fund_n"] = per
                self.event(p, "FUNDING fee -%.3f" % chg)
                self.dirty = True
        p["ext"] = max(p["ext"], hi) if L else min(p["ext"], lo)
        r_now = (price - p["entry"]) * sg / p["dist"]
        p["r_now"] = r_now

        # Stop-loss (wick)
        if (lo <= p["sl"]) if L else (hi >= p["sl"]):
            ref = min(price, p["sl"]) if L else max(price, p["sl"])
            fill = ref * (1 - sg * slip)
            if (fill <= p["liq"]) if L else (fill >= p["liq"]):
                return self.close(pair, p["liq"], "LIQ",
                                  "LIQUIDATED at %.4f" % p["liq"])
            if p["pb"]:
                return self.close(pair, fill, "TSL",
                                  "Trail SL %.4f hit -> %.4f" % (p["sl"], fill))
            return self.close(pair, fill, "SL",
                              "SL %.4f hit -> %.4f" % (p["sl"], fill))

        # Partial book
        lvl = p["entry"] + sg * p["pb_r"] * p["dist"]
        if not p["pb"] and ((hi >= lvl) if L else (lo <= lvl)):
            q = p["qty"] * p["pb_pct"]
            gross = (lvl - p["entry"]) * q * sg
            fee = q * lvl * c["fee"]
            self.s["balance"] += gross - fee
            p["realized"] += gross - fee
            p["fees_paid"] += fee
            p["qty"] -= q
            p["pb"], p["pb_price"] = True, lvl
            p["sl"] = p["entry"] * (1 + sg * 2 * c["fee"])
            p["sl_logged"] = p["sl"]
            p["seen_t"] = cs[-1]["t"]
            self.event(p, "PARTIAL BOOK %d%% @ %.4f (+%.2f, %.1fR)" % (
                p["pb_pct"] * 100, lvl, gross - fee, p["pb_r"]))
            self.event(p, "SL -> breakeven %.4f" % p["sl"])
            self.dirty = True

        # Final TP
        if (hi >= p["tp"]) if L else (lo <= p["tp"]):
            return self.close(pair, p["tp"], "TP",
                              "Target %.4f hit (%.1fR)" % (
                                  p["tp"], p["tp_r"]))

        # Trailing after 1R
        if p["pb"] or r_now >= c["trail_after_r"]:
            nsl = p["ext"] - sg * p["trail_atr"] * p["atr0"]
            if (nsl > p["sl"]) if L else (nsl < p["sl"]):
                p["sl"] = nsl
                p["seen_t"] = cs[-1]["t"]
                if abs(nsl - p["sl_logged"]) >= 0.25 * p["dist"]:
                    p["sl_logged"] = nsl
                    self.event(p, "TRAIL SL %.4f (%.1f ATR, peak %.4f)" % (
                        nsl, p["trail_atr"], p["ext"]))

    def close(self, pair, price, reason, why):
        s, p = self.s, self.s["positions"].pop(pair)
        sg = 1 if p["side"] == "LONG" else -1
        gross = (price - p["entry"]) * p["qty"] * sg
        fee = p["qty"] * price * self.c["fee"]
        s["balance"] += gross - fee
        net = p["realized"] + gross - fee - p["entry_fee"]
        now = int(time.time() * 1000)
        self.event(p, "EXIT %s @ %.4f" % (reason, price))
        if reason in ("SL", "LIQ"):
            iv = self.mc[p["mode0"]]["interval"] if p["mode0"] in self.mc else "15m"
            self.cool[pair] = now + self.c["cooldown_candles"] * IVMS[iv]
        s["trades"].append(dict(
            p, qty=p["qty0"], exit=price, exit_t=now, exit_reason=reason,
            exit_why=why, pnl=net,
            fees=p["entry_fee"] + p["fees_paid"] + fee,
            r=net / p["risk_amt"], pnl_pct=net / p["margin"] * 100))
        del s["trades"][:-2000]
        self.dirty = True
        notify("%s %s %s: %s %+.2f (%+.2fR)" % (
            base_of(pair), p["side"], p["mode0"], reason, net,
            net / p["risk_amt"]))
        log("CLOSE %s %s %s %+.2f (%.2fR)" % (
            pair, p["side"], reason, net, net / p["risk_amt"]))
        self.journal(s["trades"][-1])

    def spread_bps(self, pair, now):
        if not self.live or not self.c.get("spread_enabled"):
            return None
        hit = self.spread_cache.get(pair)
        if hit and now - hit[0] < 60000:
            return hit[1]
        v = None
        try:
            d = self.feed._json(
                "https://public.coindcx.com/market_data/orderbook?pair=%s" % pair)
            bb = max(float(k) for k in d["bids"])
            ba = min(float(k) for k in d["asks"])
            v = (ba - bb) / ((ba + bb) / 2) * 1e4
        except Exception:
            v = None
        self.spread_cache[pair] = (now, v)
        return v

    def slip(self, pair, price, atr, now):
        sp = self.spread_bps(pair, now)
        return max(self.c["slippage"],
                   self.c["slip_atr_k"] * atr / price if price else 0,
                   (sp / 2e4) if sp else 0)

    @staticmethod
    def skey(strat, label):
        return strat + (":" + label if strat in ("PATTERN", "REVERSAL") else "")

    def strat_ok(self, key, now):
        s, c = self.s, self.c
        off, rst = s.setdefault("strat_off", {}), s.setdefault("strat_reset", {})
        if key in off:
            if off[key] > now:
                return False, "Strategy %s paused (neg. expectancy), %dh left" % (
                    key, (off[key] - now) // 3600000)
            del off[key]
            rst[key] = now
        since = rst.get(key, 0)
        tr = [t for t in s["trades"]
              if self.skey(t.get("strat", ""), t.get("setup", "")) == key
              and t.get("exit_t", 0) > since][-c["strat_window"]:]
        if len(tr) >= c["strat_kill_n"]:
            avg = sum(t["r"] for t in tr) / len(tr)
            if avg < c["strat_kill_r"]:
                off[key] = now + c["strat_kill_hours"] * 3600000
                log("STRATEGY PAUSED %s avgR %.2f over %d" % (key, avg, len(tr)))
                notify("Strategy paused: %s avgR %.2f" % (key, avg))
                return False, "Strategy %s paused: avg %.2fR over %d" % (key, avg, len(tr))
            return True, "Strategy %s expectancy ok (%+.2fR, %d)" % (key, avg, len(tr))
        return True, "Strategy %s: %d/%d trades (learning)" % (key, len(tr), c["strat_kill_n"])

    def revalidate(self, pair, o, cs, now):
        c = self.mc[self.base]
        want = "UP" if o["side"] == "LONG" else "DOWN"
        closed = cs[:-1]
        if len(closed) >= c["trend_ema"] + 30:
            ind = build_ctx(closed, c)["ind"]
            if ("UP" if ind["close"] > ind["trend"] else "DOWN") != want:
                return "trend flipped"
            if ind["adx"] < max(15, c["adx_min"] - 8):
                return "ADX collapsed %.1f" % ind["adx"]
        if self.c.get("htf_filter") and not self.c.get("htf_soft") \
                and o["mode"] == "intraday" and "swing" in self.mc:
            hd = self.tdir.get((pair, "swing"))
            if hd and hd != want:
                return "higher-TF flipped"
        reg = self.regime.get(o["mode"])
        if self.c.get("regime_filter") and pair != self.c["regime_pair"] \
                and reg and reg["trend"] != want:
            return "BTC regime flipped"
        if self.news:
            ok, txt = self.news.check(base_of(pair), o["side"], now)
            if not ok:
                return txt
        return None

    def journal(self, t):
        try:
            fn = self.c["journal_file"]
            new = not os.path.exists(fn)
            with open(fn, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(TRADE_COLS)
                w.writerow([t.get(k, "") for k in TRADE_COLS])
        except Exception as e:
            log("journal failed: %s" % e)

    def note(self, pair, mode, cand, checks, bad, q, price, dist, c, count=True):
        sg = 1 if cand["side"] == "LONG" else -1
        rej = self.s.setdefault("rej", dict(total=0, first={}, all={}))
        if count:
            rej["total"] += 1
        norm = lambda t: re.sub(r"\s+", " ", _NUM.sub("#", t))[:46]
        if bad and count:
            k = norm(bad[0]["t"])
            rej["first"][k] = rej["first"].get(k, 0) + 1
            for b in bad:
                k = norm(b["t"])
                rej["all"][k] = rej["all"].get(k, 0) + 1
            for d in (rej["first"], rej["all"]):
                while len(d) > 60:
                    del d[min(d, key=d.get)]
        self.signals[(pair, mode)] = dict(
            pair=pair, mode=mode, side=cand["side"], strat=cand["strat"],
            label=cand["label"], q=q, fails=len(bad), total=len(checks),
            first_fail=bad[0]["t"] if bad else "",
            entry=price, sl=price - sg * dist,
            tp=price + sg * dist * cand.get("tp_r", c["tp_r"]), rr=cand.get("tp_r", c["tp_r"]),
            t=int(time.time() * 1000))

    def place(self, pair, cand, price, ind, checks, candle_t, q, qr, c, now):
        a = ind["atr"]
        ep = cand.get("entry_px", price)
        lim = (cand["strat"] == "BREAKOUT" and c.get("limit_entry")) or \
            (cand["strat"] == "PATTERN" and c.get("pattern_limit_entry"))
        if lim and abs(ep - price) > 0.1 * a:
            self.s.setdefault("pending", {})[pair] = dict(
                pair=pair, cand=cand, entry_px=ep, ind=ind, checks=checks,
                cur_t=candle_t, q=q, qr=qr, mode=cand["mode"],
                side=cand["side"], created=now,
                expire=now + c["entry_timeout_candles"] * IVMS[c["interval"]])
            self.dirty = True
            log("PENDING %s %s limit %.5f" % (pair, cand["side"], ep))
            notify("PENDING %s %s limit %.5f (Q=%d)" % (
                base_of(pair), cand["side"], ep, q))
            return "pending"
        self.open(pair, cand, price, ind, checks, candle_t, q, qr)
        return "open"

    def check_pending(self, pair, price, cs, now):
        s = self.s
        o = s["pending"][pair]
        L = o["side"] == "LONG"
        ep, sl = o["entry_px"], o["cand"].get("sl")
        win = [x for x in cs if x["t"] >= o["cur_t"]]
        lo = min([x["l"] for x in win] + [price])
        hi = max([x["h"] for x in win] + [price])
        dead = None
        if now >= o["expire"]:
            dead = "expired (no retest)"
        elif sl is not None and ((price <= sl) if L else (price >= sl)):
            dead = "invalidated (price beyond stop)"
        elif pair in s["positions"] or len(s["positions"]) >= self.c["max_open"]:
            dead = "no free slot"
        elif self.c.get("fill_revalidate", True):
            why = self.revalidate(pair, o, cs, now)
            if why:
                dead = "revalidation failed: " + why
        if dead:
            del s["pending"][pair]
            self.dirty = True
            log("PENDING cancelled %s: %s" % (pair, dead))
            return
        if (lo <= ep) if L else (hi >= ep):
            del s["pending"][pair]
            self.open(pair, o["cand"], ep, o["ind"], o["checks"],
                      o["cur_t"], o["q"], o["qr"], maker=True)

    def size(self, price, dist, lev, c):
        bal = self.s["balance"]
        qty = bal * c["risk_per_trade"] / dist
        cap = bal * c["max_margin_pct"] * lev / price
        capped = qty > cap
        qty = min(qty, cap)
        return qty, qty * dist, qty * price / lev, capped
# NOTE: Continues Engine class (same indentation)

    def evaluate(self, pair, mode, closed, cur, allow_trade, now):
        c, s = self.mc[mode], self.s
        sc = self.scan.setdefault("%s|%s" % (pair, mode),
                                   dict(pair=pair, mode=mode))
        self.signals.pop((pair, mode), None)
        if base_of(pair) in STABLES:
            sc.update(status="Skipped: stablecoin")
            return
        day = closed[-1]["t"] // 86400000
        if s["day"] != day:
            s["day"], s["day_start"] = day, s["balance"]
        if s["balance"] < s["day_start"] * (1 - c["max_daily_loss"]):
            s["paused_day"] = day
        if s["balance"] < s["peak"] * (1 - c["max_dd_kill"]) and \
                now >= s.get("kill_until", 0):
            s["kill_until"] = now + 86400000
            s["peak"] = s["balance"]
            log("KILL-SWITCH: dd, paused 24h")
            notify("KILL-SWITCH: dd limit hit")
        if len(closed) < c["trend_ema"] + 30:
            sc.update(status="Warming up")
            return
        X = build_ctx(closed, c)
        ind = X["ind"]
        d = "UP" if ind["close"] > ind["trend"] else "DOWN"
        self.tdir[(pair, mode)] = d
        if pair == c["regime_pair"]:
            self.regime[mode] = dict(trend=d, adx=ind["adx"])
        sc.update(ind, bias=d, t=closed[-1]["t"],
                  pat=", ".join(n for n, _ in X["pats"]),
                  near=X["near"],
                  status="In trade" if pair in s["positions"] else "Watching")
        order = [m for m in ORDER if m in self.mc]
        hi_m = order[order.index(mode) + 1] if order.index(mode) + 1 < len(order) else None

        for cand in find_setups(X, c):
            cand["mode"] = mode
            want = "UP" if cand["side"] == "LONG" else "DOWN"
            price = cur["c"]
            if cand["strat"] == "PATTERN" and self.c.get("pattern_limit_entry"):
                kk = X["cs"][X["i"]]
                sgn = 1 if cand["side"] == "LONG" else -1
                cand["entry_px"] = kk["c"] - sgn * self.c["pattern_retrace"] * (kk["h"] - kk["l"])
            plan_px = price
            if cand["strat"] in ("BREAKOUT", "PATTERN") and \
                    abs(cand.get("entry_px", price) - price) > 0.1 * ind["atr"] and (
                        (cand["strat"] == "BREAKOUT" and c.get("limit_entry")) or
                        (cand["strat"] == "PATTERN" and c.get("pattern_limit_entry"))):
                plan_px = cand["entry_px"]
            dist, _ = plan_stop(cand, plan_px, ind, c)
            lev, lwhy = auto_leverage(pair, plan_px, dist, ind["atr"], c)
            qty, risk, margin, _cap = self.size(plan_px, dist, lev, c)
            pos = list(s["positions"].values())
            margin_used = sum(p["margin"] for p in pos)
            heat = sum(p["risk_amt"] for p in pos)
            in_mode = sum(1 for p in pos if p["mode"] == mode)
            same = sum(1 for p in pos if p["side"] == cand["side"])
            lim = c["mode_slots"].get(mode, self.c["max_open"])
            slp = self.slip(pair, plan_px, ind["atr"], now)
            fund_r = (c["hold_h"] / c["funding_every_h"]) * c["funding_rate"] * plan_px / dist
            cost_r = (2 * self.c["fee"] + 2 * slp) / (dist / plan_px) + fund_r
            qscore, qreasons = score_setup(X, cand, c)

            extra = [
                dict(ok=s["paused_day"] != day,
                     t="Daily loss (%.0f%%) ok" % (c["max_daily_loss"] * 100)),
                dict(ok=now >= s.get("kill_until", 0),
                     t="DD kill-switch (%.0f%%) ok" % (c["max_dd_kill"] * 100)),
                dict(ok=pair not in s["positions"], t="No open in pair"),
                dict(ok=pair not in s.get("pending", {}), t="No pending order in pair"),
                dict(ok=now >= self.cool.get(pair, 0),
                     t="No cooldown (%d candles)" % c["cooldown_candles"]),
                dict(ok=len(pos) < c["max_open"],
                     t="Active %d/%d" % (len(pos), c["max_open"])),
                dict(ok=in_mode < lim,
                     t="%s %d/%d" % (mode.upper(), in_mode, lim)),
                dict(ok=same < c["max_same_side"],
                     t="Same-side %d/%d" % (same, c["max_same_side"])),
                dict(ok=heat + risk <= s["balance"] * c["max_heat"],
                     t="Heat %.2f%% + %.2f%% <= %.1f%%" % (
                         heat / s["balance"] * 100,
                         risk / s["balance"] * 100,
                         c["max_heat"] * 100)),
                dict(ok=margin_used + margin <= s["balance"] * c["max_total_margin"],
                     t="Margin <= %.0f%%" % (c["max_total_margin"] * 100)),
                dict(ok=cost_r <= c["max_cost_r"],
                     t="Cost %.2fR (max %.2fR)" % (cost_r, c["max_cost_r"])),
                dict(ok=qscore >= (c["rev_min_quality"] if cand["strat"] == "REVERSAL" else c["min_quality_score"]),
                     t="Quality %d/100 (min %d)" % (
                         qscore, c["rev_min_quality"] if cand["strat"] == "REVERSAL" else c["min_quality_score"])),
                dict(ok=ind["atr_pct"] >= c["min_atr_percentile"],
                     t="ATR pct %.0f%% (min %d)" % (
                         ind["atr_pct"], c["min_atr_percentile"])),
                dict(ok=lev >= c["min_lev"],
                     t="Lev %dx (min %dx): %s" % (
                         lev, c["min_lev"], lwhy)),
            ]
            sk = self.skey(cand["strat"], cand["label"])
            sok, stxt = self.strat_ok(sk, now)
            extra.append(dict(ok=sok, t=stxt))
            cl_name = CLUSTER.get(base_of(pair), "ALT")
            cl_heat = sum(p["risk_amt"] for p in pos
                          if p["side"] == cand["side"]
                          and CLUSTER.get(base_of(p["pair"]), "ALT") == cl_name)
            extra.append(dict(ok=cl_heat + risk <= s["balance"] * self.c["cluster_heat"],
                              t="Cluster %s %s heat %.2f%%+%.2f%% <= %.1f%%" % (
                                  cl_name, cand["side"], cl_heat / s["balance"] * 100,
                                  risk / s["balance"] * 100, self.c["cluster_heat"] * 100)))
            gross = sum(p["notional"] for p in pos) + qty * plan_px
            extra.append(dict(ok=gross <= s["balance"] * self.c["max_gross_x"],
                              t="Gross exposure %.2fx <= %.1fx" % (
                                  gross / s["balance"], self.c["max_gross_x"])))
            sp = self.spread_bps(pair, now)
            extra.append(dict(ok=sp is None or sp <= self.c["max_spread_bps"],
                              t=("Spread %.1f bps (max %d)" % (sp, self.c["max_spread_bps"]))
                              if sp is not None else "Spread unknown (neutral)"))
            if time.gmtime(closed[-1]["t"] / 1000).tm_wday >= 5:
                extra.append(dict(ok=ind["atr_pct"] >= self.c["weekend_atr_min"],
                                  t="Weekend: ATR pct %.0f%% (min %d)" % (
                                      ind["atr_pct"], self.c["weekend_atr_min"])))
            if cand["strat"] == "REVERSAL":
                nrev = sum(1 for p in pos if p.get("strat") == "REVERSAL")
                extra.append(dict(ok=nrev < self.c["rev_max_open"],
                                  t="Counter-trend open %d/%d" % (nrev, self.c["rev_max_open"])))
            if c["regime_filter"] and pair != c["regime_pair"] and cand["strat"] != "REVERSAL":
                reg = self.regime.get(mode)
                if reg:
                    reg_ok = (reg["trend"] == want and
                              reg["adx"] >= c["btc_chop_adx"])
                    extra.append(dict(ok=reg_ok,
                        t="BTC %s ADX %.0f (need %s + ADX>=%d)" % (
                            reg["trend"], reg["adx"], want,
                            c["btc_chop_adx"])))
                else:
                    extra.append(dict(ok=False, t="BTC regime unknown"))
            if c["htf_filter"] and hi_m and cand["strat"] != "REVERSAL":
                hd = self.tdir.get((pair, hi_m))
                if hd == want or not self.c.get("htf_soft"):
                    extra.append(dict(ok=hd == want,
                        t="Higher-TF %s (need %s)" % (hd or "?", want)))
                else:
                    cand["risk_mult"] = self.c["htf_soft_mult"]
                    extra.append(dict(ok=True,
                        t="Higher-TF %s vs %s: soft, size x%.1f" % (
                            hd or "?", want, self.c["htf_soft_mult"])))
            if self.news:
                nok, ntxt = self.news.check(base_of(pair), cand["side"], now)
                extra.append(dict(ok=nok, t=ntxt))
            extra.append(dict(ok=allow_trade, t="Bot warmed up"))

            checks = cand["checks"] + extra
            bad = [x for x in checks if not x["ok"]]
            self.note(pair, mode, cand, checks, bad, qscore, plan_px, dist, c, count=allow_trade)
            if bad:
                action = "SKIP"
                reason = "%s rejected: %s" % (cand["label"], bad[0]["t"])
                sc["status"] = "Rejected: " + bad[0]["t"][:34]
            else:
                action = cand["side"]
                reason = "%s: %s (Q=%d)" % (
                    cand["strat"], cand["label"], qscore)
                res = self.place(pair, cand, price, ind, checks, cur["t"], qscore, qreasons, c, now)
                sc["status"] = "In trade" if res == "open" else "Pending limit"
            dd = s["decisions"]
            dd.append(dict(
                pair=pair, mode=mode, t=closed[-1]["t"],
                price=ind["close"], ind=ind, action=action,
                reason=reason, strat=cand["strat"],
                quality=qscore, checks=checks))
            del dd[:-80]
            if not bad:
                break

    def open(self, pair, cand, price, ind, checks, candle_t, qscore, qreasons, maker=False):
        mode = cand["mode"]
        c, s = self.mc[mode], self.s
        side, a = cand["side"], ind["atr"]
        sg = 1 if side == "LONG" else -1
        dist, how = plan_stop(cand, price, ind, c)
        slip = self.slip(pair, price, a, int(time.time() * 1000))
        entry = price if maker else price * (1 + sg * slip)
        lev, lwhy = auto_leverage(pair, entry, dist, a, c)
        qty, risk_amt, margin, capped = self.size(entry, dist, lev, c)
        rm = cand.get("risk_mult", 1.0)
        qty, risk_amt, margin = qty * rm, risk_amt * rm, margin * rm
        sl = entry - sg * dist
        if cand.get("tp_r"):
            c = dict(c, tp_r=cand["tp_r"])
        tp = entry + sg * dist * c["tp_r"]
        liq = entry * (1 - sg * (1 / lev - c["mmr"]))
        notional, fee = qty * entry, qty * entry * (self.c["maker_fee"] if maker else self.c["fee"])
        s["balance"] -= fee
        sl_roi = dist / entry * lev * 100
        reasons = [
            "Mode: %s (%s, hold %gh)" % (
                mode.upper(), c["interval"], c["hold_h"]),
            "Setup: %s - %s" % (cand["strat"], cand["label"]),
            "Quality score: %d/100" % qscore,
        ] + [r for r in qreasons[:6]]
        reasons.append("Risk %.2f USDT (%.2f%% of bal)%s. %s" % (
            risk_amt, risk_amt / s["balance"] * 100,
            " capped" if capped else "", how))
        reasons.append("Lev %dx: %s" % (lev, lwhy))
        if maker:
            reasons.append("Filled via limit retest (maker, no slippage)")
        reasons.append(
            "Liq %.4f (%.1f%%), stop %.2f%%. SL -%.0f%% ROI, TP +%.0f%% ROI" % (
                liq, abs(liq - entry) / entry * 100,
                dist / entry * 100, sl_roi, sl_roi * c["tp_r"]))
        reasons.append(
            "Entry %.4f (slip %.2f%%), qty %.5f, notional %.2f, margin %.2f" % (
                entry, (0 if maker else slip) * 100, qty, notional, margin))
        reasons.append(
            "Plan: SL %.4f | +%.1fR book %d%% & BE | trail %.1f ATR | TP %.4f (%.1fR)" % (
                sl, c["pb_r"], c["pb_pct"] * 100,
                c["trail_atr"], tp, c["tp_r"]))
        now = int(time.time() * 1000)
        s["positions"][pair] = dict(
            id=s["next_id"], pair=pair, side=side, strat=cand["strat"],
            setup=cand["label"], mode=mode, mode0=mode,
            entry=entry, entry_t=now, hold_ms=c["hold_h"] * 3600000,
            candle_t=candle_t, qty=qty, qty0=qty,
            notional=notional, margin=margin, lev=lev, liq=liq,
            sl_roi=sl_roi, tp_roi=sl_roi * c["tp_r"],
            tp_r=c["tp_r"], pb_r=c["pb_r"], pb_pct=c["pb_pct"],
            trail_atr=c["trail_atr"], sl=sl, sl0=sl, tp=tp, dist=dist,
            atr0=a, risk_amt=risk_amt, entry_fee=fee, fees_paid=0.0,
            pb=False, pb_price=None, realized=0.0, ext=entry, last=entry,
            r_now=0.0, fund_n=0, slip=slip, sl_logged=sl, seen_t=None, quality=qscore,
            reasons=reasons, ind=ind,
            events=[[now, "OPEN %s @ %.4f %dx (%s, %s, Q=%d)" % (
                side, entry, lev, mode.upper(), cand["label"], qscore)]])
        s["next_id"] += 1
        self.dirty = True
        notify("OPEN %s %s %s @ %.4f Q=%d" % (
            base_of(pair), side, mode, entry, qscore))
        log("OPEN %s %s %s @ %.4f Q=%d" % (
            pair, side, cand["label"], entry, qscore))

    def strat_stats(self):
        out = {}
        for t in self.s["trades"]:
            q = t.get("quality", 0)
            qgrade = "A" if q >= 85 else "B" if q >= 70 else "C"
            for k in (t.get("strat", "EMA"),
                      "mode:" + t.get("mode0", "intraday"),
                      "quality:" + qgrade):
                o = out.setdefault(k, dict(n=0, w=0, net=0.0))
                o["n"] += 1
                o["w"] += 1 if t["r"] > 0.05 else 0
                o["net"] += t["pnl"]
        return out
# NOTE: Still inside Engine class

    def snapshot(self):
        with self.lock:
            s, c = self.s, self.c
            t = s["trades"]
            wins = [x for x in t if x["r"] > 0.05]
            loss = [x for x in t if x["r"] < -0.05]
            n = len(t)
            gp = sum(x["pnl"] for x in t if x["pnl"] > 0)
            gl = -sum(x["pnl"] for x in t if x["pnl"] <= 0)
            pos = []
            for p in s["positions"].values():
                q = dict(p)
                sg = 1 if p["side"] == "LONG" else -1
                q["upnl"] = (p["last"] - p["entry"]) * p["qty"] * sg + p["realized"]
                pos.append(q)
            sc = []
            for v in self.scan.values():
                if "adx" in v or "status" in v:
                    r = dict(v)
                    r["price"] = self.prices.get(v["pair"])
                    sc.append(r)
            sc.sort(key=lambda x: -x.get("adx", 0))
            now_ms = int(time.time() * 1000)
            sigs = []
            for x in sorted(self.signals.values(),
                            key=lambda x: (x["fails"], -x["q"]))[:30]:
                gr = "EXPERT" if x["fails"] == 0 else (
                    "NEAR" if x["fails"] <= 1 and x["q"] >= 60 else "WATCH")
                sigs.append(dict(x, grade=gr))
            pend = [dict(pair=o["pair"], side=o["side"], entry=o["entry_px"],
                         q=o["q"], strat=o["cand"]["strat"], expire=o["expire"],
                         label=o["cand"]["label"])
                    for o in s.get("pending", {}).values()]
            rj = s.get("rej", dict(total=0, first={}, all={}))
            rejv = dict(
                total=rj["total"],
                first=sorted(rj["first"].items(), key=lambda kv: -kv[1])[:12],
                all=sorted(rj["all"].items(), key=lambda kv: -kv[1])[:12])
            newsv = self.news.snapshot(now_ms) if self.news else dict(
                ok=False, err="disabled", updated=0, g=0, label="off",
                n=0, items=[], coins={})
            paused = [[k, v] for k, v in s.get("strat_off", {}).items() if v > now_ms]
            why = {}
            for pr, d in self.dead.items():
                why.setdefault(d["why"] + " (retry 6h)", []).append(base_of(pr))
            return dict(
                cfg=dict(
                    interval="+".join(m["interval"] for m in self.mc.values()),
                    lev="auto-lev", start=c["start_balance"],
                    npairs=len(self.pairs), max_open=c["max_open"],
                    skipped=len(self.dead),
                    strat="v1.3 PRO %s | %s | Q>=%d | max %d | cost<=%.2fR" % (
                        "+".join(m.upper() for m in self.mc),
                        "+".join(c["strategies"]),
                        c["min_quality_score"],
                        c["max_open"], c["max_cost_r"])),
                mode="LIVE" if self.live else "SIM",
                status=self.status, err=self.err, updated=self.updated,
                balance=s["balance"], positions=pos,
                upnl=sum(p["upnl"] for p in pos),
                stats=dict(
                    trades=n, wins=len(wins), losses=len(loss),
                    be=n - len(wins) - len(loss),
                    win_rate=(len(wins) / (len(wins) + len(loss)) * 100
                              if (wins or loss) else None),
                    tp=sum(1 for x in t if x["exit_reason"] == "TP"),
                    sl=sum(1 for x in t if x["exit_reason"] == "SL"),
                    tsl=sum(1 for x in t if x["exit_reason"] == "TSL"),
                    pb=sum(1 for x in t if x.get("pb")),
                    pf=gp / gl if gl else None,
                    net=s["balance"] - c["start_balance"],
                    net_pct=(s["balance"] / c["start_balance"] - 1) * 100,
                    max_dd=s["max_dd"] * 100,
                    avg_win=(sum(x["pnl"] for x in wins) / len(wins)
                             if wins else 0),
                    avg_loss=(sum(x["pnl"] for x in loss) / len(loss)
                              if loss else 0),
                    avg_r=sum(x["r"] for x in t) / n if n else 0,
                    avg_cost_r=(sum(x["fees"] / x["risk_amt"] for x in t) / n
                                if n else 0),
                    avg_quality=(sum(x.get("quality", 0) for x in t) / n
                                 if n else 0)),
                equity=s["equity"], scan=sc[:100],
                bystrat=self.strat_stats(),
                issues=[[w, len(v), ", ".join(v[:8])]
                        for w, v in why.items()],
                signals=sigs, pending=pend, rej=rejv, news=newsv, paused=paused,
                trades=list(reversed(t[-100:])),
                decisions=list(reversed(s["decisions"][-40:])))
# NOTE: Engine class is CLOSED. These are module-level functions.

_ICONS = {}


def make_icon(n):
    if n not in _ICONS:
        c, rows = n / 2, []
        for y in range(n):
            row = bytearray([0])
            for x in range(n):
                dx, dy = x - c, y - c
                if (dx * dx + dy * dy) ** 0.5 < n * 0.38:
                    tri = (-n * 0.2 < dy < n * 0.2 and
                           abs(dx) < (dy + n * 0.2) * 0.55)
                    row += bytes((34, 197, 94) if tri else (59, 130, 246))
                else:
                    row += bytes((10, 14, 22))
            rows.append(bytes(row))

        def ch(t, d):
            return (struct.pack(">I", len(d)) + t + d +
                    struct.pack(">I", zlib.crc32(t + d) & 0xffffffff))
        _ICONS[n] = (b"\x89PNG\r\n\x1a\n" +
                     ch(b"IHDR", struct.pack(">IIBBBBB", n, n, 8, 2, 0, 0, 0)) +
                     ch(b"IDAT", zlib.compress(b"".join(rows))) +
                     ch(b"IEND", b""))
    return _ICONS[n]


MANIFEST = json.dumps(dict(
    name="DEAD NINJA PYTHON v0.9",
    short_name="Dead Ninja",
    start_url="/", scope="/", display="standalone",
    background_color="#0a0e16", theme_color="#0a0e16",
    icons=[dict(src="/icon-192.png", sizes="192x192",
                type="image/png", purpose="any maskable"),
           dict(src="/icon-512.png", sizes="512x512",
                type="image/png", purpose="any maskable")]))

SW = ("self.addEventListener('install',()=>self.skipWaiting());"
      "self.addEventListener('fetch',e=>e.respondWith(fetch(e.request)));")
HTML = r"""<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>DEAD NINJA v1.3 PRO</title>
<link rel=manifest href=/manifest.json>
<meta name=theme-color content=#0B0E14>
<style>
:root{--bg:#0B0E14;--c1:#171B26;--c2:#1E2433;--bd:#2A3243;--tx:#FFFFFF;--mu:#848E9C;--g:#0ECB81;--r:#F6465D;--b:#1E4BFF;--y:#FCD535;--blue:#2B6BFF}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--tx);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding-bottom:40px}
.w{max-width:760px;margin:auto;padding:0 16px}
header{display:flex;justify-content:space-between;align-items:center;padding:16px 0 12px}
.brand b{letter-spacing:0.5px;font-size:16px;color:var(--tx)}
.brand small{display:block;color:var(--mu);font-size:11px}
.pill{padding:4px 12px;border-radius:20px;font-size:11px;font-weight:700;letter-spacing:0.5px}
.LIVE{background:rgba(14,203,129,0.15);color:var(--g)}
.SIM{background:rgba(252,213,53,0.15);color:var(--y)}
.bar{display:flex;justify-content:space-between;background:var(--c1);border:1px solid var(--bd);border-radius:16px;padding:16px;margin:6px 0 12px;box-shadow:0 4px 12px rgba(0,0,0,0.1)}
.big{font-size:28px;font-weight:700;color:var(--tx)}
.mu{color:var(--mu)}
.pos{color:var(--g)!important}
.neg{color:var(--r)!important}
.warn{background:rgba(246,70,93,0.1);border:1px solid rgba(246,70,93,0.3);color:var(--r);padding:10px 14px;border-radius:12px;margin-bottom:12px;font-size:12px;display:flex;align-items:center;gap:8px}
.tabs{display:flex;overflow-x:auto;gap:6px;margin:12px 0;position:sticky;top:0;background:var(--bg);padding:8px 0;z-index:5;border-bottom:1px solid var(--bd)}
.tabs button{flex:0 0 auto;white-space:nowrap;background:transparent;border:none;color:var(--mu);padding:10px 14px;border-radius:8px;font-weight:600;font-size:13px;transition:0.2s}
.tabs button.on{background:var(--c2);color:var(--tx)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.card{background:var(--c1);border:1px solid var(--bd);border-radius:16px;padding:16px;margin-bottom:12px}
.k{background:var(--c1);border:1px solid var(--bd);border-radius:14px;padding:12px 14px}
.k small{color:var(--mu);display:block;font-size:12px;margin-bottom:4px}
.k b{font-size:18px;font-weight:700}
.prog{height:4px;background:var(--c2);border-radius:4px;margin-top:8px;overflow:hidden}
.prog i{display:block;height:100%;background:var(--b);border-radius:4px}
h3{margin:0 0 12px;font-size:14px;font-weight:600;color:var(--tx)}
.badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:700}
.LONG{background:rgba(14,203,129,0.15);color:var(--g)}
.SHORT{background:rgba(246,70,93,0.15);color:var(--r)}
.SKIP,.WAIT{background:var(--c2);color:var(--mu)}
.TP{background:rgba(14,203,129,0.15);color:var(--g)}
.SL{background:rgba(246,70,93,0.15);color:var(--r)}
.kv{display:grid;grid-template-columns:1fr 1fr;gap:8px 16px;margin:12px 0}
.kv span{color:var(--mu);font-size:12px}
.kv b{float:right;font-weight:600;font-size:13px}
details{background:var(--c1);border:1px solid var(--bd);border-radius:14px;margin-bottom:10px;overflow:hidden}
summary{padding:14px;cursor:pointer;list-style:none;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
summary::-webkit-details-marker{display:none}
.sp{flex:1}
.body{padding:0 14px 14px;border-top:1px solid var(--bd);background:var(--c2)}
ul{margin:8px 0 0;padding-left:0;list-style:none;font-size:13px}
li{padding:4px 0;color:var(--tx)}
li:before{content:"\2713  ";color:var(--g);font-weight:800}
li.x:before{content:"\2717  ";color:var(--r)}
.empty{color:var(--mu);text-align:center;padding:20px;font-size:13px}
canvas{width:100%;height:160px;display:block}
.ft{color:var(--mu);font-size:11px;text-align:center;margin-top:24px;padding-bottom:20px}
.tb{display:grid;grid-template-columns:1.1fr 1.3fr .6fr .6fr 2fr;gap:8px;font-size:12px;align-items:center}
.tb b{color:var(--mu);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px}
.pc{border:1px solid var(--bd);border-radius:14px;padding:14px;margin-bottom:10px;background:var(--c1)}
.pc details{background:var(--c2);margin:10px 0 0;border:none}
.rng{position:relative;height:6px;border-radius:4px;margin:24px 0 8px;background:var(--c2)}
.rng i{position:absolute;top:-4px;width:2px;height:14px;background:#fff;border-radius:2px}
.rng u{position:absolute;top:-4px;width:2px;height:14px;background:var(--y)}
.rng em{position:absolute;top:-4px;width:2px;height:14px;background:var(--r)}
.EXPERT{background:rgba(252,213,53,.18);color:var(--y)}
.NEAR{background:rgba(43,107,255,.18);color:#7AA2FF}
.WATCH{background:var(--c2);color:var(--mu)}
.dl{display:block;text-align:center;padding:10px;margin-bottom:10px;border:1px solid var(--bd);border-radius:12px;color:var(--y);text-decoration:none;font-size:13px}
.bar2{height:6px;background:var(--c2);border-radius:4px;margin:3px 0 8px}.bar2 i{display:block;height:100%;background:var(--r);border-radius:4px}
[hidden]{display:none!important}
.tag{font-size:11px;color:#A1B0CC;background:var(--c2);border-radius:4px;padding:2px 6px}
</style></head><body><div class=w>
<header><div class=brand><b>&#9698; DEAD NINJA</b><small id=strat></small></div>
<span id=mode class=pill>...</span></header>
<div class=bar>
<div><small class=mu id=pair style="font-size:12px"></small><div class=big id=price>--</div></div>
<div style="text-align:right"><small class=mu>Equity (paper)</small>
<div id=eq style="font-size:20px;font-weight:700;color:var(--tx)"></div>
<small class=mu id=upd></small></div></div>
<div id=warn class=warn hidden></div>
<nav class=tabs>
<button class=on data-t=ov>Overview</button>
<button data-t=sg>Signals</button>
<button data-t=tr>Trades</button>
<button data-t=nw>News</button>
<button data-t=lg>Logic</button>
<button data-t=sc>Scanner</button>
</nav>
<section id=ov><div class=grid id=kpi></div><div style="height:10px"></div>
<div class=card><h3>Open positions</h3><div id=pos></div></div>
<div class=card><h3>Strategy performance</h3><div id=bs></div></div>
<div class=card><h3>Equity curve</h3><canvas id=cv></canvas></div>
<div id=iss></div></section>
<section id=sg hidden></section>
<section id=nw hidden></section>
<section id=tr hidden></section>
<section id=lg hidden></section>
<section id=sc hidden></section>
<div class=ft>PAPER TRADING ONLY - no real orders. Not financial advice. News = keyword scoring.</div>
</div><script>
const $=i=>document.getElementById(i);
const f=(n,d=2)=>n==null?'-':Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const sg=(n,d=2)=>(n>0?'+':'')+f(n,d);
const cl=n=>n>0?'pos':n<0?'neg':'';
const tm=ms=>new Date(ms).toLocaleString('en-GB',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});
const dur=ms=>{let m=Math.round(ms/60000);return m<60?m+'m':Math.floor(m/60)+'h '+m%60+'m'};
let last=null,sig='',open=new Set();
document.addEventListener('toggle',e=>{const k=e.target.dataset&&e.target.dataset.k;if(k){e.target.open?open.add(k):open.delete(k)}},true);
document.querySelectorAll('.tabs button').forEach(b=>b.onclick=()=>{
document.querySelectorAll('.tabs button').forEach(x=>x.classList.toggle('on',x==b));
['ov','sg','tr','nw','lg','sc'].forEach(t=>$(t).hidden=t!=b.dataset.t);if(b.dataset.t=='ov')draw()});
const nm=p=>p.replace('B-','').replace('_USDT','');
const pd=n=>n<1?5:n<100?3:2;
const li=a=>a.map(r=>'<li>'+r+'</li>').join('');
function kpi(l,v,c,x){return `<div class=k><small>${l}</small><b class="${c||''}">${v}</b>${x||''}</div>`}
function render(d){const s=d.stats;last=d;
$('strat').textContent=d.cfg.strat;
$('mode').textContent=d.mode=='LIVE'?'LIVE DATA':'SIMULATED';
$('mode').className='pill '+d.mode;
$('pair').textContent='Open PnL - '+d.cfg.npairs+' pairs'+(d.cfg.skipped?' ('+d.cfg.skipped+' parked)':'')+' - '+d.cfg.interval;
const pr=$('price');pr.textContent=sg(d.upnl);pr.className='big '+cl(d.upnl);
$('eq').textContent=f(d.balance+d.upnl)+' USDT';
$('upd').textContent=d.updated?'updated '+new Date(d.updated).toLocaleTimeString():'';
const w=$('warn');w.hidden=!d.err&&d.mode=='LIVE';
w.textContent=d.err||(d.mode=='SIM'?'SIMULATED prices - CoinDCX unreachable':'');
$('kpi').innerHTML=
kpi('Balance',f(d.balance))+
kpi('Net PnL',sg(s.net)+' ('+sg(s.net_pct)+'%)',cl(s.net))+
kpi('Expectancy',sg(s.avg_r)+' R',cl(s.avg_r))+
kpi('Win rate',s.win_rate==null?'--':f(s.win_rate,1)+'%','',
`<div class=prog><i style="width:${s.win_rate||0}%"></i></div>`)+
kpi('Profit factor',s.pf==null?'--':f(s.pf),s.pf>=1?'pos':s.pf?'neg':'')+
kpi('Cost/trade',f(s.avg_cost_r)+' R')+
kpi('Avg Quality',f(s.avg_quality,0)+'/100')+
kpi('Target (TP)',s.tp,'pos')+
kpi('Stop (SL)',s.sl,'neg')+
kpi('Partial (PB)',s.pb,'pos')+
kpi('Trail (TSL)',s.tsl)+
kpi('Active',d.positions.length+'/'+d.cfg.max_open,d.positions.length>=d.cfg.max_open?'neg':'pos')+
kpi('Trades',s.trades+' ('+s.wins+'W/'+s.losses+'L/'+s.be+'BE)')+
kpi('Max DD',f(s.max_dd,1)+'%',s.max_dd>10?'neg':'')+
kpi('Avg win/loss',sg(s.avg_win)+' / '+sg(s.avg_loss))+
kpi('News mood',d.news.label,d.news.g>0.3?'pos':d.news.g<-0.3?'neg':'')+
kpi('Pending limits',d.pending.length);
$('pos').innerHTML=d.positions.length?d.positions.map(posHTML).join(''):'<div class=empty>No open position.</div>';
$('sc').innerHTML=scHTML(d);
$('bs').innerHTML=bsHTML(d);
$('iss').innerHTML=issHTML(d);
$('sg').innerHTML=sgHTML(d);$('nw').innerHTML=nwHTML(d);
const ns=JSON.stringify([d.trades.length,d.trades[0]&&d.trades[0].exit_t,d.decisions.length]);
if(ns!=sig){sig=ns;
$('tr').innerHTML=dlLink()+(d.trades.length?d.trades.map(tradeHTML).join(''):'<div class=empty>No closed trades yet.</div>');
$('lg').innerHTML=d.decisions.length?d.decisions.map(decHTML).join(''):'<div class=empty>No setups yet.</div>'}
draw()}
function bsHTML(d){const e=Object.entries(d.bystrat||{});
return e.length?'<div class=tb style="grid-template-columns:1.4fr .6fr .8fr 1fr"><b>Strategy</b><b>Trades</b><b>Win %</b><b>Net PnL</b>'+e.map(([k,o])=>`<span>${k.replace('mode:','Mode: ').replace('quality:','Q:')}</span><span>${o.n}</span><span>${f(o.w/o.n*100,0)}%</span><span class="${cl(o.net)}">${sg(o.net)}</span>`).join('')+'</div>':'<div class=empty>Appears after first closed trade.</div>'}
function posHTML(p){const q=pd(p.entry);
const den=p.tp-p.sl0;
const pct=Math.max(0,Math.min(100,(p.last-p.sl0)/den*100));
const ep=(p.entry-p.sl0)/den*100;
const cs=Math.max(0,Math.min(100,(p.sl-p.sl0)/den*100));
const k='p'+p.id;
return `<div class=pc><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:4px">
<b style="font-size:15px">${nm(p.pair)}</b>
<span class="badge ${p.side}">${p.side}</span>
<span class=tag>${p.setup||p.strat}</span>
<span class=tag>${(p.mode0&&p.mode0!=p.mode?p.mode0+'->'+p.mode:p.mode).toUpperCase()}</span>
<span class=tag>Q${p.quality||0}</span>
<span class=mu style="font-size:11px">#${p.id} ${tm(p.entry_t)}</span>
${p.pb?'<span class="badge TP">PB</span>':''}
<span class=sp></span>
<b class="${cl(p.upnl)}" style="font-size:18px">${sg(p.upnl)}</b></div>
<div class=rng><u style="left:${ep}%"></u><em style="left:${cs}%"></em>
<i style="left:calc(${pct}% - 1px)"></i></div>
<div style="display:flex;justify-content:space-between;font-size:11px">
<span class=neg>SL ${f(p.sl,q)}</span>
<span class=mu>Entry ${f(p.entry,q)}</span>
<span class=pos>TP ${f(p.tp,q)}</span></div>
<div class=kv>
<div><span>Now</span><b>${f(p.last,q)} (${sg(p.r_now)}R)</b></div>
<div><span>Qty</span><b>${f(p.qty,4)}</b></div>
<div><span>Risk</span><b>${f(p.risk_amt)}</b></div>
<div><span>Margin</span><b>${f(p.margin)}</b></div>
<div><span>Lev</span><b>${p.lev}x</b></div>
<div><span>Held</span><b>${dur(Date.now()-p.entry_t)}/${dur(p.hold_ms)}</b></div>
<div><span>Liq</span><b>${f(p.liq,q)}</b></div>
<div><span>ROI</span><b><span class=neg>-${f(p.sl_roi,0)}%</span>/<span class=pos>+${f(p.tp_roi,0)}%</span></b></div>
</div>
<details data-k=${k} ${open.has(k)?'open':''}>
<summary style="font-size:12px;padding:10px;background:var(--c2);border-radius:8px">Entry logic + events</summary>
<div class=body><h3>Entry</h3><ul>${li(p.reasons)}</ul>
<h3 style="margin-top:10px">Events</h3><ul>${p.events.map(e=>'<li>'+tm(e[0])+' - '+e[1]+'</li>').join('')}</ul></div>
</details></div>`}
function tradeHTML(t){const k='t'+t.id;const q=pd(t.entry);
const R={TP:'TARGET',SL:'SL HIT',TSL:'TRAIL',LIQ:'LIQUIDATED',TIME:'TIME'};
return `<details data-k=${k} ${open.has(k)?'open':''}>
<summary><b style="font-size:14px">${nm(t.pair)}</b>
<span class="badge ${t.side}">${t.side}</span>
<span class=tag>${t.mode0?t.mode0+' - ':''}${t.setup||t.strat||'EMA'}</span>
<span class=tag>Q${t.quality||0}</span>
<span class="badge ${t.pnl>0?'TP':'SL'}">${R[t.exit_reason]||t.exit_reason}</span>
${t.pb?'<span class="badge TP">PB</span>':''}
<span class=sp></span>
<b class="${cl(t.pnl)}">${sg(t.pnl)}</b>
<small class=mu>${tm(t.exit_t)}</small></summary>
<div class=body><div class=kv>
<div><span>Entry</span><b>${f(t.entry,q)}</b></div>
<div><span>Exit</span><b>${f(t.exit,q)}</b></div>
<div><span>SL0</span><b>${f(t.sl0,q)}</b></div>
<div><span>TP</span><b>${f(t.tp,q)}</b></div>
<div><span>Qty</span><b>${f(t.qty,4)}</b></div>
<div><span>Notional</span><b>${f(t.notional)}</b></div>
<div><span>Lev</span><b>${t.lev?t.lev+'x':'-'}</b></div>
<div><span>Fees</span><b>${f(t.fees)}</b></div>
<div><span>R</span><b class="${cl(t.r)}">${sg(t.r)}R</b></div>
<div><span>ROI</span><b class="${cl(t.pnl_pct)}">${sg(t.pnl_pct)}%</b></div>
<div><span>Duration</span><b>${dur(t.exit_t-t.entry_t)}</b></div></div>
<h3>Entry logic</h3><ul>${li(t.reasons)}</ul>
<h3 style="margin-top:10px">Timeline</h3><ul>${t.events.map(e=>'<li>'+tm(e[0])+' - '+e[1]+'</li>').join('')}</ul>
<h3 style="margin-top:10px">Exit reason</h3><ul><li class="${t.pnl>0?'':'x'}">${t.exit_why}</li></ul></div>
</details>`}
function decHTML(x){const k='d'+x.pair+x.t;
return `<details data-k=${k} ${open.has(k)?'open':''}>
<summary><b>${nm(x.pair)}</b>
<span class="badge ${x.action}">${x.action}</span>
<span class=tag>${(x.mode||'')+' '+(x.strat||'')}</span>
<span class=tag>Q${x.quality||0}</span>
<span style="flex:1;font-size:12px;color:var(--mu)">${x.reason.replace(/^(LONG|SHORT) /,'')}</span>
<small class=mu>${tm(x.t)}</small></summary>
<div class=body><ul>${x.checks.map(c=>`<li class="${c.ok?'':'x'}">${c.t}</li>`).join('')}</ul></div>
</details>`}
function issHTML(d){return d.issues&&d.issues.length?'<div class=card><h3>Parked pairs</h3>'+d.issues.map(i=>`<div style="font-size:12px;margin-bottom:8px"><b>${i[1]}x</b> ${i[0]}<br><span class=mu>${i[2]}</span></div>`).join('')+'</div>':''}
function scHTML(d){return issHTML(d)+'<div class=card><h3>Scanner - '+d.scan.length+' rows</h3><div class=tb><b>Pair</b><b>Price</b><b>ADX</b><b>RSI</b><b>Status</b>'+d.scan.map(x=>`<span>${nm(x.pair)} <small style="color:var(--mu)">${x.mode||''}</small></span><span>${x.price?f(x.price,pd(x.price)):'-'}</span><span class="${x.adx>=25?'pos':''}">${x.adx!=null?f(x.adx,1):'-'}</span><span>${x.rsi!=null?f(x.rsi,0):'-'}</span><small style="color:var(--mu)">${x.bias?x.bias+' - ':''}${x.status||''}${x.pat?' | '+x.pat:''}</small>`).join('')+'</div></div>'}

const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ago=ms=>{const m=Math.round((Date.now()-ms)/60000);return m<60?m+'m ago':m<1440?Math.floor(m/60)+'h ago':Math.floor(m/1440)+'d ago'};
function dlLink(){return '<a class=dl href="/api/trades.csv'+location.search+'">Download full trade history (CSV)</a>'}
function sigRow(x){const q=pd(x.entry);
return `<div class=pc><div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
<b style="font-size:15px">${esc(nm(x.pair))}</b><span class="badge ${x.side}">${x.side}</span>
<span class="badge ${x.grade}">${x.grade}</span><span class=tag>${esc(x.label)}</span>
<span class=tag>${esc(x.mode)}</span><span class=tag>Q${x.q}</span><span class=sp></span>
<small class=mu>${x.total-x.fails}/${x.total} filters</small></div>
<div style="display:flex;justify-content:space-between;font-size:12px;margin-top:8px">
<span class=neg>SL ${f(x.sl,q)}</span><span class=mu>Entry ${f(x.entry,q)}</span><span class=pos>TP ${f(x.tp,q)} (${x.rr}R)</span></div>
${x.fails?`<div class=mu style="font-size:12px;margin-top:6px">Blocked: ${esc(x.first_fail)}${x.fails>1?' (+'+(x.fails-1)+' more)':''}</div>`:'<div class=pos style="font-size:12px;margin-top:6px">All filters passed</div>'}</div>`}
function sgHTML(d){const g=d.signals||[],ex=g.filter(x=>x.grade=='EXPERT'),rest=g.filter(x=>x.grade!='EXPERT');
let h='<div class=card><h3>Expert trades (all filters passed)</h3>'+(ex.length?ex.map(sigRow).join(''):'<div class=empty>No A+ setup right now. Bot waits for every filter.</div>')+'</div>';
if((d.paused||[]).length)h+='<div class=card><h3>Auto-paused strategies (negative expectancy)</h3>'+d.paused.map(p=>`<div style="font-size:13px">${esc(p[0])} <small class=mu>resumes in ${dur(Math.max(0,p[1]-Date.now()))}</small></div>`).join('')+'</div>';
if(d.pending.length)h+='<div class=card><h3>Pending limit orders</h3>'+d.pending.map(o=>`<div style="font-size:13px;margin-bottom:8px"><b>${esc(nm(o.pair))}</b> <span class="badge ${o.side}">${o.side}</span> limit ${f(o.entry,pd(o.entry))} <span class=tag>${esc(o.label)}</span> <small class=mu>expires in ${dur(Math.max(0,o.expire-Date.now()))}</small></div>`).join('')+'</div>';
h+='<div class=card><h3>Live market signals</h3>'+(rest.length?rest.map(sigRow).join(''):'<div class=empty>No breakout or candle pattern on the last closed candles.</div>')+'</div>';
const r=d.rej,mx=r.first.length?r.first[0][1]:1;
h+='<div class=card><h3>Which filter blocks most ('+r.total+' setups seen)</h3>'+(r.first.length?r.first.map(([k,v])=>`<div style="font-size:12px">${esc(k)} <b style="float:right">${v}</b></div><div class=bar2><i style="width:${v/mx*100}%"></i></div>`).join(''):'<div class=empty>Collecting data...</div>')+'</div>';
return h}
function nwHTML(d){const n=d.news;
let h=`<div class=card><h3>News mood: <span class="${n.g>0.3?'pos':n.g<-0.3?'neg':''}">${esc(n.label)}</span> <small class=mu>(${f(n.g,2)} avg, ${n.n} headlines, last window)</small></h3>
<div class=mu style="font-size:12px">${n.ok?'Updated '+ago(n.updated):'News feed not reachable yet'}${n.err?' | '+esc(n.err):''}<br>Keyword scoring is crude. Used only to block trades against strong news, never to open one.</div></div>`;
const cs=Object.entries(n.coins||{});
if(cs.length)h+='<div class=card><h3>Per-coin sentiment</h3><div style="display:flex;flex-wrap:wrap;gap:6px">'+cs.map(([b,o])=>`<span class="badge ${o.score>0?'LONG':o.score<0?'SHORT':'WATCH'}">${esc(b)} ${o.score>0?'+':''}${o.score} (${o.n})</span>`).join('')+'</div></div>';
h+='<div class=card><h3>Latest headlines</h3>'+(n.items.length?n.items.map(x=>`<div style="margin-bottom:12px;font-size:13px"><span class="badge ${x.score>0?'LONG':x.score<0?'SHORT':'WATCH'}">${x.score>0?'+':''}${x.score}</span> ${x.risk?'<span class="badge SHORT">RISK</span> ':''}${x.link?`<a href="${esc(x.link)}" target=_blank rel=noopener style="color:var(--tx)">${esc(x.title)}</a>`:esc(x.title)}<br><small class=mu>${esc(x.src)} - ${ago(x.t)} ${x.coins.map(c=>'<span class=tag>'+esc(c)+'</span>').join(' ')}</small></div>`).join(''):'<div class=empty>No headlines yet.</div>')+'</div>';
return h}
function draw(){const c=$('cv');if(!last||$('ov').hidden)return;
const r=devicePixelRatio||1,W=c.clientWidth,H=c.clientHeight;
c.width=W*r;c.height=H*r;const g=c.getContext('2d');g.scale(r,r);
let e=last.equity.map(x=>x[1]);e.push(last.balance+last.upnl);
if(e.length<2){g.fillStyle='#848E9C';g.font='12px sans-serif';g.fillText('Equity curve after a few candles',10,H/2);return}
const mn=Math.min(...e,last.cfg.start),mx=Math.max(...e,last.cfg.start),pad=(mx-mn)*.15||1;
const y=v=>H-8-(v-mn+pad)/(mx-mn+2*pad)*(H-16);
const x=i=>i/(e.length-1)*(W-4)+2;
g.strokeStyle='#2A3243';g.setLineDash([4,4]);g.lineWidth=1;
g.beginPath();g.moveTo(0,y(last.cfg.start));g.lineTo(W,y(last.cfg.start));g.stroke();
g.setLineDash([]);
const up=e[e.length-1]>=last.cfg.start,col=up?'#0ECB81':'#F6465D';
const gr=g.createLinearGradient(0,0,0,H);gr.addColorStop(0,col+'33');gr.addColorStop(1,col+'00');
g.beginPath();e.forEach((v,i)=>i?g.lineTo(x(i),y(v)):g.moveTo(x(i),y(v)));
g.strokeStyle=col;g.lineWidth=2;g.stroke();
g.lineTo(x(e.length-1),H);g.lineTo(x(0),H);g.fillStyle=gr;g.fill()}
async function poll(){try{const r=await fetch('/api/state'+location.search);render(await r.json())}
catch(e){$('warn').hidden=false;$('warn').textContent='Connection lost...'}}
poll();setInterval(poll,3000);
if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js').catch(()=>{});
addEventListener('resize',draw);
</script></body></html>"""

class Handler(BaseHTTPRequestHandler):
    engine = None

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        p = u.path
        tok = CFG["token"]
        public = p.startswith(("/manifest.json", "/sw.js", "/icon-"))
        if tok and not public and \
                urllib.parse.parse_qs(u.query).get("k", [""])[0] != tok:
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Forbidden")
            return
        if p.startswith("/api/trades.csv"):
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(TRADE_COLS)
            with self.engine.lock:
                for t in self.engine.s["trades"]:
                    w.writerow([t.get(k, "") for k in TRADE_COLS])
            body, ctype = buf.getvalue().encode(), "text/csv; charset=utf-8"
        elif p.startswith("/api/state"):
            body, ctype = json.dumps(self.engine.snapshot()).encode(), "application/json"
        elif p.startswith("/manifest.json"):
            body, ctype = MANIFEST.encode(), "application/manifest+json"
        elif p.startswith("/sw.js"):
            body, ctype = SW.encode(), "application/javascript"
        elif p.startswith("/icon-"):
            body = make_icon(512 if "512" in p else 192)
            ctype = "image/png"
        else:
            body, ctype = HTML.encode(), "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def bot_loop(engine):
    while True:
        try:
            engine.tick()
        except Exception as e:
            engine.err = "Engine error: %s" % e
            log(traceback.format_exc())
        time.sleep(CFG["poll_sec"])


def main():
    live, feed = True, LiveFeed()
    try:
        feed.get("B-BTC_USDT", "15m")
        print("[OK] Connected to CoinDCX | modes=%s | %s" % (
            "+".join(CFG["modes"]), "+".join(CFG["strategies"])))
    except Exception as e:
        print("[!] CoinDCX not reachable:", e)
        if not CFG["allow_sim_fallback"]:
            sys.exit(1)
        print("[!] Using SIMULATED feed (demo only)")
        feed, live = SimFeed(CFG), False
    eng = Engine(CFG, feed, live)
    Handler.engine = eng
    threading.Thread(target=bot_loop, args=(eng,), daemon=True).start()
    port = CFG["port"]
    while True:
        try:
            srv = ThreadingHTTPServer((CFG["host"], port), Handler)
            break
        except OSError:
            port += 1
    url = "http://127.0.0.1:%d%s" % (
        port, ("/?k=" + CFG["token"]) if CFG["token"] else "")
    print("\n=== DEAD NINJA v1.3 PRO dashboard ===")
    print("Open: %s" % url)
    print("(logs: %s)\n" % LOG)
    log("started v1.3 live=%s" % live)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nShutdown.")


if __name__ == "__main__":
    main()

#
# CHANGELOG v0.8 -> v0.9
# 1. Scalp mode removed (fees/slippage ate 1R). Cost gate now 0.12R (was 0.25R).
# 2. Quality scoring (0-100) added. Only trades with Q >= 70 execute.
# 3. ATR percentile filter (>= 30%) — no dead-market trades.
# 4. BTC regime check now also checks ADX >= 20 (no trades in chop).
# 5. Trailing SL starts at 1R even before PB (locks profit earlier).
# 6. Time-limit exits removed — trailing SL handles stale trades.
# 7. ADX min 28 (was 25), RSI band 32-68 (was 30-70), body min 0.55 (was 0.50).
# 8. Max open 3 (was 5), max same-side 2 (was 3), risk 0.8% (was 1%), heat 2.5% (was 4%).
# 9. Leverage tiers tighter: 8/6/4 (was 10/7/5). Liq buffer 3.0 (was 2.5).
# 10. Partial book 40% at 1R (was 50%). Trail 1.2 ATR (was 1.5).
# 11. Daily loss hard stop 2% (was 3%), drawdown kill-switch 15% (was 12%, but only peak-to-now).
# 12. Dashboard: Q-grade badge, avg quality stat, split Part 12a-d for mobile copy-paste.
# CHANGELOG v0.9 -> v1.3 PRO
# 1. Cost gate 0.12R -> 0.22R (0.12R blocked BTC/ETH almost always).
# 2. Pattern ADX min 20 + pullback within 1.0 ATR (old rules contradicted each other).
# 3. +9 candle patterns (Harami, Piercing, Dark Cloud, Tweezer, Soldiers, Crows, Marubozu).
# 4. News engine (RSS) as a filter + dashboard News tab.
# 5. Breakout limit-retest entries are now real pending orders (fill only if price returns).
# 6. Funding-fee model; permanent CSV trade journal; CSV download in dashboard.
# 7. Signals tab: Expert (all filters pass) + Live signals + filter-rejection analytics.
# 8. Volume-missing safe scoring.

# CHANGELOG v1.0 -> v1.1 (from gap review, each claim verified against code)
# 1. Strategy auto-pause: avg R of last 20 trades < 0 after 15 trades -> paused 48h.
# 2. Dynamic slippage (ATR + live spread), maker/taker fee split, spread filter.
# 3. Funding cost included in cost gate (cost_r).
# 4. Cluster heat cap + gross exposure cap (3x balance).
# 5. Pending orders re-validated every tick (trend, ADX, higher-TF, BTC regime, news).
# 6. Optional pattern limit-retest entry (pattern_limit_entry).
# 7. Liquidation buffer now enforced in leverage (was configured but unused).
# 8. Soft higher-TF filter (half size), weekend volatility filter.
# 9. kill() rotation bug fixed; analytics skip warmup candle; plan price used for cost/size.
# 10. Paper-test profile: quality 65 (was 70), intraday ADX 25 (was 28).

# v1.2: pattern stop min 0.2 ATR -> 2.0 ATR (cost gate was rejecting 10 of 16 setups at 0.46-0.86R);
#       empty red banner hidden (CSS [hidden] was overridden).

# v1.3: REVERSAL strategy = counter-trend entries (both sides whatever the market does):
#   candle reversal pattern + RSI exhaustion + at 20-bar extreme + confirmation close + climax volume.
#   Half risk, TP 1.5R, max 1 open at a time, skips BTC-regime/HTF trend filters, own quality score.
