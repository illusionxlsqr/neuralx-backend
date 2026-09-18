from fastapi import FastAPI, APIRouter, HTTPException, Header, Depends
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import re
import json
import asyncio
import logging
from pathlib import Path
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import uuid

try:
    from emergentintegrations.llm.chat import LlmChat, UserMessage, TextDelta, StreamDone
except ImportError:
    LlmChat = None
from BinaryOptionsToolsV2 import PocketOptionAsync

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
client = AsyncIOMotorClient(mongo_url, serverSelectionTimeoutMS=2000)
db = client[os.environ.get('DB_NAME', 'neuralx')]

EMERGENT_LLM_KEY = os.environ.get('EMERGENT_LLM_KEY')
AI_MODEL = ("openai", "gpt-5.4")

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Multi-Session Management
# ---------------------------------------------------------------------------

def get_user_id(x_user_id: str = Header(...)):
    if not x_user_id or len(x_user_id) > 100:
        raise HTTPException(400, 'X-User-Id header required')
    return x_user_id

SESSIONS = {}

def get_session(user_id: str):
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "PO": {
                "status": "disconnected",
                "error": "",
                "demo": None,
                "uid": None,
                "balance": None,
                "start_balance": None,
                "running": False,
                "amount": 1.0,
                "take_profit": None,
                "stop_loss": None,
                "duration": 60,
                "fast_exit": False,
                "current_trade": None,
                "trades": [],
                "reasoning": [],
                "assets": [],
                "wins": 0, "losses": 0, "draws": 0,
                "last_action_ts": 0.0,
                "last_balance_ts": 0.0,
                "last_wait_ts": 0.0,
                "asset_cooldown": {},
                "loss_streak": 0,
                "global_pause_until": 0.0,
            },
            "CLIENT": {"c": None},
            "_client_lock": asyncio.Lock(),
            "_trade_lock": asyncio.Lock(),
            "engine_task": None,
        }
    return SESSIONS[user_id]

def S_VARS(user_id):
    sess = get_session(user_id)
    return sess["PO"], sess["CLIENT"], sess["_client_lock"], sess["_trade_lock"]


CRYPTO_KEYWORDS = ["btc", "bitcoin", "eth", "ethereum", "sol", "solana", "bnb", "xrp",
                   "doge", "ada", "ltc", "link", "avax", "dot", "matic", "trx", "ton", "crypto"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def add_reasoning(user_id, text, symbol, kind="info"):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    PO["reasoning"].insert(0, {"id": str(uuid.uuid4()), "ts": now_iso(),
                               "symbol": symbol, "text": text, "kind": kind})
    PO["reasoning"] = PO["reasoning"][:40]


def parse_ssid(ssid: str):
    """Extract isDemo / uid from the full 42[\"auth\",{...}] string."""
    demo, uid = None, None
    try:
        m = re.search(r'\{.*\}', ssid)
        if m:
            data = json.loads(m.group(0))
            demo = bool(data.get("isDemo", 1)) if data.get("isDemo") is not None else None
            uid = data.get("uid")
    except Exception:
        pass
    return demo, uid


async def connect_client(user_id, ssid: str):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    async with _client_lock:
        prev_client = CLIENT["c"]
        had_session = prev_client is not None and PO["status"] == "connected"
        PO["status"] = "connecting"
        PO["error"] = ""
        demo, uid = parse_ssid(ssid)
        try:
            c = PocketOptionAsync(ssid)
            try:
                await c.connect()
            except Exception:
                pass
            connected = False
            for _ in range(24):
                try:
                    if await c.is_connected():
                        connected = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)
            bal = await asyncio.wait_for(c.balance(), timeout=15)
            try:
                demo = await c.is_demo()
            except Exception:
                pass
            CLIENT["c"] = c
            PO.update({
                "status": "connected", "demo": demo, "uid": uid,
                "balance": float(bal), "start_balance": float(bal), "error": "",
            })
            PO["last_balance_ts"] = datetime.now(timezone.utc).timestamp()
            await db.po_config.update_one(
                {"_id": user_id},
                {"$set": {"ssid": ssid}},
                upsert=True,
            )
            add_reasoning(user_id, f"Connesso a Pocket Option ({'DEMO' if demo else 'REALE'}) — saldo ${bal:,.2f}", "SYS", "info")
            await discover_assets(user_id)
            return True
        except Exception as e:
            logger.error(f"PO connect failed: {e}")
            if had_session:
                # a bad reconnect must not kill an already-working session
                CLIENT["c"] = prev_client
                PO["status"] = "connected"
                PO["error"] = ""
                add_reasoning(user_id, f"Riconnessione ignorata (SSID non valida): sessione precedente mantenuta.", "SYS", "info")
                return False
            PO["status"] = "error"
            PO["error"] = str(e)[:300]
            CLIENT["c"] = None
            add_reasoning(user_id, f"Connessione fallita: {str(e)[:120]}", "SYS", "loss")
            return False


async def discover_assets(user_id):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    c = CLIENT["c"]
    if not c:
        return
    try:
        assets = await c.active_assets()
        crypto = []
        for a in assets:
            sym = (a.get("symbol") or "").lower()
            atype = (a.get("asset_type") or "").lower()
            name = (a.get("name") or "").lower()
            is_crypto = atype == "cryptocurrency" or any(k in sym or k in name for k in CRYPTO_KEYWORDS)
            if is_crypto and a.get("is_active", True):
                crypto.append({
                    "symbol": a.get("symbol"),
                    "name": a.get("name") or a.get("symbol"),
                    "payout": a.get("payout") or 0,
                    "is_otc": a.get("is_otc", False),
                })
        crypto.sort(key=lambda x: x["payout"], reverse=True)
        PO["assets"] = crypto[:20]
        if crypto:
            add_reasoning(user_id, f"Auto-detect: {len(crypto)} crypto disponibili. Top: {crypto[0]['symbol']} (payout {crypto[0]['payout']}%)", "SYS", "info")
    except Exception as e:
        logger.warning(f"discover_assets failed: {e}")


def sma(vals, n):
    if len(vals) < n:
        return sum(vals) / len(vals) if vals else 0
    return sum(vals[-n:]) / n


def fmt_timer(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds % 60 == 0:
        return f"{seconds // 60}min"
    return f"{seconds // 60}min {seconds % 60}s"


def ema(vals, n):
    if not vals:
        return 0
    k = 2 / (n + 1)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e


def rsi(vals, n=14):
    if len(vals) < n + 1:
        return 50.0
    gains = losses = 0.0
    for i in range(-n, 0):
        ch = vals[i] - vals[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses -= ch
    if losses == 0:
        return 100.0
    rs = (gains / n) / (losses / n)
    return 100 - 100 / (1 + rs)


def stdev(vals):
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    return (sum((x - m) ** 2 for x in vals) / n) ** 0.5


async def analyze_and_pick(user_id):
    """Trader engine multi-timeframe con filtri stringenti."""
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    c = CLIENT["c"]
    if not c or not PO["assets"]:
        await discover_assets(user_id)
        if not PO["assets"]:
            return None
    now_ts = datetime.now(timezone.utc).timestamp()

    # only analyze the top payout assets in parallel to keep every tick fast
    candidates = [a for a in PO["assets"] if (a.get("payout") or 0) >= 60][:6]

    async def fetch(asset):
        sym = asset["symbol"]
        cd = PO["asset_cooldown"].get(sym, 0)
        if cd > now_ts:
            return (asset, None, None)
        try:
            candles, htf = await asyncio.gather(
                asyncio.wait_for(c.get_candles(sym, 60, 80), timeout=12),
                asyncio.wait_for(c.get_candles(sym, 300, 50), timeout=12),
            )
        except Exception as e:
            logger.warning(f"candles {sym} failed: {e}")
            return (asset, None, None)
        return (asset, candles, htf)

    results = await asyncio.gather(*[fetch(a) for a in candidates])

    best = None
    reject_reasons = []  # per il log "studio in corso"
    for asset, candles, htf in results:
        sym = asset["symbol"]
        cd = PO["asset_cooldown"].get(sym, 0)
        if cd > now_ts:
            reject_reasons.append(f"{sym}: in cooldown")
            continue
        if candles is None or htf is None:
            reject_reasons.append(f"{sym}: candele non disponibili")
            continue
        closes = [float(x.get("close")) for x in candles if x.get("close") is not None]
        opens = [float(x.get("open")) for x in candles if x.get("open") is not None]
        highs = [float(x.get("high")) for x in candles if x.get("high") is not None]
        lows = [float(x.get("low")) for x in candles if x.get("low") is not None]
        htf_closes = [float(x.get("close")) for x in htf if x.get("close") is not None]
        if len(closes) < 40 or len(htf_closes) < 30:
            continue

        # ---- HTF trend maestro
        htf_ema50 = ema(htf_closes[-50:], 50) if len(htf_closes) >= 50 else ema(htf_closes, min(30, len(htf_closes)))
        htf_ema20 = ema(htf_closes[-30:], 20)
        htf_bias = 0
        if htf_ema20 > htf_ema50 and htf_closes[-1] > htf_ema50:
            htf_bias = 1     # uptrend
        elif htf_ema20 < htf_ema50 and htf_closes[-1] < htf_ema50:
            htf_bias = -1    # downtrend
        # NB: se HTF neutro NON blocchiamo più — richiede solo più confluenza sotto

        # ---- entry TF indicators
        e9 = ema(closes[-40:], 9)
        e21 = ema(closes[-40:], 21)
        macd_now = ema(closes[-40:], 12) - ema(closes[-40:], 26)
        macd_prev = ema(closes[-41:-1], 12) - ema(closes[-41:-1], 26) if len(closes) >= 41 else macd_now
        r = rsi(closes, 14)
        roc = (closes[-1] - closes[-6]) / closes[-6] * 100 if closes[-6] else 0.0
        mid = sma(closes[-20:], 20)
        sd = stdev(closes[-20:])
        last = closes[-1]
        op = opens[-1]
        hi = highs[-1] if highs else last
        lo = lows[-1] if lows else last

        # volatility filter (soglia bassa: evita solo mercati completamente flat)
        rel_vol = (sd / mid * 100) if mid else 0.0
        if rel_vol < 0.008:
            reject_reasons.append(f"{sym}: volatilità troppo bassa ({rel_vol:.3f}%)")
            continue

        # zona morta RSI stretta
        if 49.5 <= r <= 50.5:
            reject_reasons.append(f"{sym}: RSI zona morta {r:.1f}")
            continue

        # candela di indecisione: solo casi estremi
        body = abs(last - op)
        rng = max(1e-9, hi - lo)
        if body / rng < 0.10:
            reject_reasons.append(f"{sym}: candela indecisa")
            continue

        # ---- Scoring simmetrico bull/bear
        bull = 0
        bear = 0
        reasons_bull = []
        reasons_bear = []
        if e9 > e21: bull += 1; reasons_bull.append("EMA9>EMA21")
        elif e9 < e21: bear += 1; reasons_bear.append("EMA9<EMA21")
        if macd_now > 0 and macd_now >= macd_prev: bull += 1; reasons_bull.append("MACD su")
        elif macd_now < 0 and macd_now <= macd_prev: bear += 1; reasons_bear.append("MACD giù")
        if r > 52: bull += 1; reasons_bull.append(f"RSI {r:.0f}↑")
        elif r < 48: bear += 1; reasons_bear.append(f"RSI {r:.0f}↓")
        if roc > 0.02: bull += 1; reasons_bull.append(f"ROC {roc:+.2f}%")
        elif roc < -0.02: bear += 1; reasons_bear.append(f"ROC {roc:+.2f}%")
        if last > op: bull += 1; reasons_bull.append("candela verde")
        elif last < op: bear += 1; reasons_bear.append("candela rossa")

        # scegli lato dominante
        if bull > bear:
            side, score, reasons = "call", bull, reasons_bull
        elif bear > bull:
            side, score, reasons = "put", bear, reasons_bear
        else:
            reject_reasons.append(f"{sym}: bull/bear pari {bull}-{bear}")
            continue

        # HTF come modificatore: allineato → soglia 2, neutro → 3, contro → skip
        if (htf_bias == 1 and side == "put") or (htf_bias == -1 and side == "call"):
            reject_reasons.append(f"{sym}: {side} contro HTF")
            continue
        need = 2 if htf_bias != 0 else 3
        if score < need:
            reject_reasons.append(f"{sym}: {score}/5 {side} (serve {need})")
            continue
        bias_label = "HTF↑" if htf_bias == 1 else ("HTF↓" if htf_bias == -1 else "HTF neutro")

        # duration in base a volatilità e forza segnale (in secondi o minuti)
        if PO.get("fast_exit"):
            duration = 15 if score >= 5 else 30
        elif rel_vol >= 0.15 or abs(roc) >= 0.2:
            duration = 30 if score >= 5 else 60
        elif rel_vol >= 0.08:
            duration = 60
        else:
            duration = 120

        # priority score: forza segnale + payout + volatilità utile
        prio = score * 1000 + (asset.get("payout") or 0) + min(int(rel_vol * 10), 30)
        if best is None or prio > best["score"]:
            action = "CALL (compra)" if side == "call" else "PUT (vende)"
            best = {
                "symbol": sym, "side": side, "payout": asset.get("payout"), "price": last,
                "duration": duration, "score": prio, "strength": score,
                "reason": (
                    f"{bias_label} confermato · {action} · confluenza {score}/5 · "
                    f"{' | '.join(reasons)} · vol {rel_vol:.2f}% · payout {asset.get('payout')}% · "
                    f"timer {fmt_timer(duration)}"
                ),
            }
    if best is None and reject_reasons:
        # attach info so the caller can log why every candidate was rejected
        return {"_reject": reject_reasons[:3]}
    return best


async def settle_trade(user_id, trade_id, asset, side, amount, duration):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    c = CLIENT["c"]
    try:
        result = await c.check_win(trade_id, duration + 20)
        outcome = (result.get("result") or "unknown").lower()
        profit = float(result.get("profit") or 0)
    except Exception as e:
        outcome, profit = "unknown", 0.0
        logger.warning(f"check_win failed: {e}")
    if outcome == "win":
        PO["wins"] += 1
        PO["loss_streak"] = 0
        kind = "win"
    elif outcome == "loss":
        PO["losses"] += 1
        PO["loss_streak"] += 1
        kind = "loss"
        # cooldown per l'asset appena perso (90s)
        cool_until = datetime.now(timezone.utc).timestamp() + 90
        PO["asset_cooldown"][asset] = cool_until
        # pausa globale dopo 2 loss consecutivi
        if PO["loss_streak"] >= 2:
            PO["global_pause_until"] = datetime.now(timezone.utc).timestamp() + 60
            add_reasoning(user_id, "2 loss di fila: pausa 1 min per ristudiare il mercato.", "SYS", "info")
    elif outcome == "draw":
        PO["draws"] += 1
        kind = "info"
    else:
        kind = "info"
    trade = {
        "id": str(trade_id), "asset": asset, "side": side, "amount": amount,
        "duration": duration, "result": outcome, "profit": round(profit, 2),
        "opened_at": PO["current_trade"]["opened_at"] if PO["current_trade"] else now_iso(),
        "closed_at": now_iso(),
    }
    PO["trades"].insert(0, trade)
    PO["trades"] = PO["trades"][:60]
    add_reasoning(
        user_id,
        f"Risultato {asset} {side.upper()}: {outcome.upper()} — {'+' if profit >= 0 else ''}${profit:,.2f}",
        asset, kind,
    )
    await db.po_trades.insert_one({**trade, "user_id": user_id, "_p": now_iso()})
    PO["current_trade"] = None
    await refresh_balance(user_id, force=True)
    check_targets(user_id)


async def refresh_balance(user_id, force=False):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    c = CLIENT["c"]
    if not c:
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    if not force and now_ts - PO["last_balance_ts"] < 6:
        return
    try:
        bal = float(await asyncio.wait_for(c.balance(), timeout=12))
        PO["balance"] = bal
        PO["last_balance_ts"] = now_ts
    except Exception as e:
        logger.warning(f"balance refresh failed: {e}")


def check_targets(user_id):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    bal = PO["balance"]
    if bal is None or not PO["running"]:
        return
    if PO["take_profit"] is not None and bal >= PO["take_profit"]:
        PO["running"] = False
        add_reasoning(user_id, f"🎯 TARGET RAGGIUNTO: saldo ${bal:,.2f} ≥ ${PO['take_profit']:,.2f}. Bot fermato.", "SYS", "win")
    elif PO["stop_loss"] is not None and bal <= PO["stop_loss"]:
        PO["running"] = False
        add_reasoning(user_id, f"🛑 STOP-LOSS: saldo ${bal:,.2f} ≤ ${PO['stop_loss']:,.2f}. Bot fermato.", "SYS", "loss")


async def monitor_open_trade(user_id):
    """Track the live price of the running trade so the UI can show if it is winning."""
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    ct = PO["current_trade"]
    c = CLIENT["c"]
    if not ct or not c:
        return
    try:
        candles = await asyncio.wait_for(c.get_candles(ct["asset"], 5, 3), timeout=8)
        closes = [float(x.get("close")) for x in candles if x.get("close") is not None]
        if not closes:
            return
        price = closes[-1]
        ct["live_price"] = price
        if ct.get("entry_price"):
            ct["winning"] = price > ct["entry_price"] if ct["side"] == "call" else price < ct["entry_price"]
    except Exception as e:
        logger.warning(f"monitor failed: {e}")


async def engine_tick(user_id):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    if PO["status"] != "connected" or CLIENT["c"] is None:
        return
    await refresh_balance(user_id)
    check_targets(user_id)
    if PO["current_trade"] is not None:
        await monitor_open_trade(user_id)
        return
    if not PO["running"]:
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    if PO["global_pause_until"] > now_ts:
        if now_ts - PO["last_wait_ts"] > 25:
            PO["last_wait_ts"] = now_ts
            secs = int(PO["global_pause_until"] - now_ts)
            add_reasoning(user_id, f"In pausa dopo 2 loss consecutivi ({secs}s rimanenti). Il bot ristudia.", "SYS", "info")
        return
    if now_ts - PO["last_action_ts"] < 5:
        return
    async with _trade_lock:
        if PO["current_trade"] is not None:
            return
        pick = await analyze_and_pick(user_id)
        if not pick or pick.get("_reject") is not None:
            if now_ts - PO["last_wait_ts"] > 12:
                PO["last_wait_ts"] = now_ts
                detail = ""
                if isinstance(pick, dict) and pick.get("_reject"):
                    detail = " · " + " ; ".join(pick["_reject"])
                add_reasoning(user_id, f"Studio in corso: nessun setup di qualità{detail}. Aspetto occasione.", "SYS", "info")
            return
        amount = float(PO["amount"])
        duration = int(pick.get("duration", 60))
        c = CLIENT["c"]
        try:
            fn = c.buy if pick["side"] == "call" else c.sell
            res = await asyncio.wait_for(fn(pick["symbol"], amount, duration), timeout=20)
            trade_id = res[0] if isinstance(res, (list, tuple)) else res
        except Exception as e:
            add_reasoning(user_id, f"Ordine {pick['symbol']} fallito: {str(e)[:100]}", pick["symbol"], "loss")
            logger.error(f"order failed: {e}")
            return
        PO["current_trade"] = {
            "id": str(trade_id), "asset": pick["symbol"], "side": pick["side"],
            "amount": amount, "duration": duration, "opened_at": now_iso(),
            "expiry_ts": now_ts + duration, "reason": pick["reason"], "payout": pick["payout"],
            "entry_price": pick["price"], "live_price": pick["price"], "winning": None,
        }
        PO["last_action_ts"] = now_ts
        add_reasoning(
            user_id,
            f"{'BUY/CALL' if pick['side'] == 'call' else 'SELL/PUT'} {pick['symbol']} ${amount} — timer {fmt_timer(duration)} — {pick['reason']} (payout {pick['payout']}%)",
            pick["symbol"], "call" if pick["side"] == "call" else "put",
        )
        asyncio.create_task(settle_trade(user_id, trade_id, pick["symbol"], pick["side"], amount, duration))


async def engine_loop(user_id):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    # try to restore a saved session
    cfg = await db.po_config.find_one({"_id": user_id})
    if cfg:
        PO["amount"] = cfg.get("amount", 1.0)
        PO["take_profit"] = cfg.get("take_profit")
        PO["stop_loss"] = cfg.get("stop_loss")
        PO["fast_exit"] = cfg.get("fast_exit", False)
        if cfg.get("ssid"):
            asyncio.create_task(connect_client(user_id, cfg["ssid"]))
    while True:
        try:
            await engine_tick(user_id)
        except Exception as e:
            logger.error(f"engine_tick error: {e}")
        await asyncio.sleep(3)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def state_payload(user_id):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    total = PO["wins"] + PO["losses"]
    win_rate = (PO["wins"] / total * 100) if total else 0
    session_pnl = None
    if PO["balance"] is not None and PO["start_balance"] is not None:
        session_pnl = round(PO["balance"] - PO["start_balance"], 2)
    ct = None
    if PO["current_trade"]:
        ct = {k: PO["current_trade"].get(k) for k in
              ["id", "asset", "side", "amount", "duration", "opened_at", "expiry_ts", "reason", "payout",
               "entry_price", "live_price", "winning"]}
        ct["duration_label"] = fmt_timer(PO["current_trade"].get("duration", 0))
    return {
        "status": PO["status"],
        "connected": PO["status"] == "connected",
        "error": PO["error"],
        "demo": PO["demo"],
        "uid": PO["uid"],
        "balance": PO["balance"],
        "start_balance": PO["start_balance"],
        "session_pnl": session_pnl,
        "running": PO["running"],
        "amount": PO["amount"],
        "take_profit": PO["take_profit"],
        "stop_loss": PO["stop_loss"],
        "duration": PO["duration"],
        "duration_mode": "auto",
        "fast_exit": PO["fast_exit"],
        "wins": PO["wins"], "losses": PO["losses"], "draws": PO["draws"],
        "trades_count": PO["wins"] + PO["losses"] + PO["draws"],
        "win_rate": round(win_rate, 1),
        "current_trade": ct,
        "trades": PO["trades"][:30],
        "reasoning": PO["reasoning"][:30],
        "assets": PO["assets"],
        "server_ts": now_iso(),
    }


@api_router.get("/")
async def root():
    return {"message": "NEURAL-X Pocket Option engine online"}


@api_router.get("/po/state")
async def get_state(user_id: str = Depends(get_user_id)):
    return state_payload(user_id)


class ConnectBody(BaseModel):
    ssid: str


@api_router.post("/po/connect")
async def po_connect(body: ConnectBody, user_id: str = Depends(get_user_id)):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    ok = await connect_client(user_id, body.ssid.strip())
    if not ok:
        raise HTTPException(400, PO["error"] or "connessione fallita")
    return state_payload(user_id)


@api_router.post("/po/disconnect")
async def po_disconnect(user_id: str = Depends(get_user_id)):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    PO["running"] = False
    c = CLIENT["c"]
    if c:
        try:
            await c.disconnect()
        except Exception:
            pass
    CLIENT["c"] = None
    PO["status"] = "disconnected"
    add_reasoning(user_id, "Disconnesso da Pocket Option (SSID conservata per il prossimo login)", "SYS", "info")
    return state_payload(user_id)


class ForgetBody(BaseModel):
    confirm: bool = False


@api_router.post("/po/forget-ssid")
async def po_forget_ssid(body: ForgetBody, user_id: str = Depends(get_user_id)):
    """Cancella completamente la SSID dal database (azione distruttiva volontaria)."""
    if not body.confirm:
        raise HTTPException(400, "conferma richiesta")
    await db.po_config.update_one({"_id": user_id}, {"$unset": {"ssid": ""}})
    return {"ok": True}


class AutoLoginBody(BaseModel):
    email: str
    password: str


async def _capture_ssid(email: str, password: str, timeout_s: int = 120):
    """Apre un browser headless, fa login su Pocket Option e cattura il frame WS
    che inizia con 42["auth",...]. La password NON viene salvata da nessuna parte."""
    from playwright.async_api import async_playwright
    captured = {"ssid": None, "error": None}

    async with async_playwright() as p:
        exec_path = "/root/bin/chromium" if os.path.exists("/root/bin/chromium") else "/usr/bin/google-chrome"
        browser = await p.chromium.launch(
            headless=True,
            executable_path=exec_path,
            args=[
                "--no-sandbox", "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--lang=it-IT",
            ],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="it-IT",
            timezone_id="Europe/Rome",
            extra_http_headers={"Accept-Language": "it-IT,it;q=0.9,en;q=0.8"},
        )
        # stealth-lite: nascondi navigator.webdriver
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['it-IT','it','en-US','en']});
            window.chrome = {runtime: {}};
        """)
        page = await context.new_page()

        def on_ws(ws):
            def on_frame(payload):
                try:
                    s = payload if isinstance(payload, str) else payload.decode("utf-8", "ignore")
                except Exception:
                    return
                if s.startswith('42["auth"') and captured["ssid"] is None:
                    captured["ssid"] = s
            ws.on("framesent", on_frame)
            ws.on("framereceived", on_frame)

        page.on("websocket", on_ws)

        try:
            await page.goto("https://po.trade/en/login/", wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            await browser.close()
            captured["error"] = f"impossibile aprire il sito: {str(e)[:120]}"
            return captured

        # attendi che Cloudflare si risolva da solo (fino a 25s)
        for _ in range(25):
            await asyncio.sleep(1)
            try:
                title = (await page.title()).lower()
            except Exception:
                title = ""
            if "just a moment" not in title and "attendere" not in title:
                break

        # ora prova a fare login
        try:
            await page.wait_for_selector('input[type="email"], input[name="email"]', timeout=20000)
            await page.fill('input[type="email"], input[name="email"]', email)
            await page.fill('input[type="password"], input[name="password"]', password)
            # piccolo delay umano
            await page.wait_for_timeout(600)
            await page.click('button[type="submit"], form button')
        except Exception as e:
            captured["error"] = f"form di login non raggiungibile: {str(e)[:150]}"
            await browser.close()
            return captured

        # aspetta il messaggio auth
        deadline = asyncio.get_event_loop().time() + timeout_s
        captcha_hits = 0
        while asyncio.get_event_loop().time() < deadline and captured["ssid"] is None:
            await asyncio.sleep(1)
            try:
                body_txt = (await page.content()).lower()
            except Exception:
                body_txt = ""
            if any(k in body_txt for k in ["recaptcha", "hcaptcha", "please complete the security check", "verify you are human"]):
                captcha_hits += 1
                if captcha_hits >= 5:
                    captured["error"] = "Pocket Option chiede un captcha umano: il login automatico è bloccato"
                    break
            else:
                captcha_hits = 0

        await browser.close()
        if not captured["ssid"] and not captured["error"]:
            captured["error"] = "timeout: SSID non catturata (Pocket Option non ha aperto la sessione WebSocket)"
        return captured


@api_router.post("/po/auto-login")
async def po_auto_login(body: AutoLoginBody, user_id: str = Depends(get_user_id)):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    """One-shot: fa login su Pocket Option con email+password, cattura la SSID,
    la usa per connettere il bot e la SCARTA (password mai memorizzata)."""
    if not body.email or not body.password:
        raise HTTPException(400, "email e password richieste")
    try:
        result = await asyncio.wait_for(_capture_ssid(body.email, body.password), timeout=120)
    except asyncio.TimeoutError:
        raise HTTPException(408, "timeout durante il login automatico")
    except Exception as e:
        raise HTTPException(500, f"errore login automatico: {str(e)[:200]}")

    if result.get("error") or not result.get("ssid"):
        raise HTTPException(400, result.get("error") or "SSID non catturata")

    ssid = result["ssid"]
    ok = await connect_client(user_id, ssid)
    if not ok:
        raise HTTPException(400, PO["error"] or "connessione fallita dopo login")
    return state_payload(user_id)


@api_router.get("/po/ssid")
async def po_get_ssid(user_id: str = Depends(get_user_id)):
    """Restituisce la SSID salvata (per copiarla da mobile)."""
    cfg = await db.po_config.find_one({"_id": user_id})
    ssid = cfg.get("ssid") if cfg else None
    if not ssid:
        raise HTTPException(404, "nessuna SSID salvata")
    return {"ssid": ssid}


class SettingsBody(BaseModel):
    amount: Optional[float] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    duration: Optional[int] = None
    fast_exit: Optional[bool] = None


@api_router.post("/po/settings")
async def po_settings(body: SettingsBody, user_id: str = Depends(get_user_id)):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    if body.amount is not None:
        if body.amount <= 0:
            raise HTTPException(400, "importo non valido")
        PO["amount"] = float(body.amount)
    if body.take_profit is not None:
        PO["take_profit"] = float(body.take_profit) if body.take_profit > 0 else None
    if body.stop_loss is not None:
        PO["stop_loss"] = float(body.stop_loss) if body.stop_loss > 0 else None
    if body.duration is not None and body.duration in (30, 60, 120, 180, 300):
        PO["duration"] = int(body.duration)
    if body.fast_exit is not None:
        PO["fast_exit"] = bool(body.fast_exit)
    await db.po_config.update_one(
        {"_id": user_id},
        {"$set": {"amount": PO["amount"], "take_profit": PO["take_profit"],
                  "stop_loss": PO["stop_loss"], "fast_exit": PO["fast_exit"]}},
        upsert=True,
    )
    return state_payload(user_id)


class ToggleBody(BaseModel):
    running: bool


@api_router.post("/po/toggle")
async def po_toggle(body: ToggleBody, user_id: str = Depends(get_user_id)):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    if body.running and PO["status"] != "connected":
        raise HTTPException(400, "connettiti prima con la SSID")
    if body.running:
        bal = PO["balance"]
        # guard against stale/illogical targets that would stop the bot instantly
        if bal is not None:
            if PO["take_profit"] is not None and PO["take_profit"] <= bal:
                add_reasoning(user_id, f"Take-profit ${PO['take_profit']:,.2f} ≤ saldo attuale ${bal:,.2f}: ignorato (deve essere superiore al saldo).", "SYS", "info")
                PO["take_profit"] = None
            if PO["stop_loss"] is not None and PO["stop_loss"] >= bal:
                add_reasoning(user_id, f"Stop-loss ${PO['stop_loss']:,.2f} ≥ saldo attuale ${bal:,.2f}: ignorato (deve essere inferiore al saldo).", "SYS", "info")
                PO["stop_loss"] = None
            await db.po_config.update_one(
                {"_id": user_id},
                {"$set": {"take_profit": PO["take_profit"], "stop_loss": PO["stop_loss"]}},
                upsert=True,
            )
        PO["start_balance"] = bal
        PO["running"] = True
        PO["loss_streak"] = 0
        PO["global_pause_until"] = 0.0
        PO["asset_cooldown"] = {}
        add_reasoning(user_id, "▶ Bot AVVIATO — analisi multi-timeframe (HTF 5m + entry 1m) con filtri di qualità attivi.", "SYS", "info")
    else:
        PO["running"] = False
        add_reasoning(user_id, "⏸ Bot FERMATO", "SYS", "info")
    return state_payload(user_id)


# ------------------------------- AI Chat -----------------------------------

class ChatBody(BaseModel):
    session_id: str
    message: str


def build_system_prompt(user_id):
    PO, CLIENT, _client_lock, _trade_lock = S_VARS(user_id)
    s = state_payload(user_id)
    assets = ", ".join(a["symbol"] for a in s["assets"][:6]) or "nessuno"
    ct = "nessuna"
    if s["current_trade"]:
        ct = f"{s['current_trade']['asset']} {s['current_trade']['side'].upper()} ${s['current_trade']['amount']}"
    return (
        "Sei NEURAL-X, l'assistente AI di un bot di trading automatico collegato a Pocket Option (po.trade). "
        "Sei diretto, sincero e parli la lingua dell'utente (italiano). "
        "Spieghi in modo chiaro perché il bot ha aperto un trade, come funziona la strategia (analisi MULTI-TIMEFRAME: prima definisce il trend maestro su TF 5m con EMA20/EMA50, poi cerca conferma sull'entry TF 1m con confluenza ≥4/5 tra EMA9/21, MACD, RSI, ROC e candela; entra SOLO se la volatilità è utile, non c'è zona morta RSI e la candela non è di indecisione; cooldown 3 min sull'asset dopo un loss e pausa globale 2 min dopo 2 loss consecutivi), "
        "e rispondi a qualsiasi domanda sul trading. IMPORTANTE: sii responsabile — le opzioni binarie sono ad ALTO RISCHIO, "
        "nessuno può garantire vincite, e con account reale si possono perdere soldi veri. Non promettere mai profitti sicuri. "
        f"\n\nCONTESTO LIVE — Stato: {s['status']} | Account: {'DEMO' if s['demo'] else 'REALE' if s['demo'] is False else 'n/d'} | "
        f"Saldo: {s['balance']} | P&L sessione: {s['session_pnl']} | Vinte: {s['wins']} Perse: {s['losses']} | "
        f"Win rate: {s['win_rate']}% | Bot attivo: {s['running']} | Importo/trade: ${s['amount']} | "
        f"Stop a: TP={s['take_profit']} SL={s['stop_loss']} | Trade in corso: {ct} | Crypto rilevate: {assets}."
    )


@api_router.post("/chat")
async def chat(body: ChatBody, user_id: str = Depends(get_user_id)):
    await db.chat_messages.insert_one({"user_id": user_id, "session_id": body.session_id, "role": "user",
                                       "content": body.message, "ts": now_iso()})
    if LlmChat is None:
        reply = "Assistente NEURAL-X attivo. Trading automatico operativo."
        await db.chat_messages.insert_one({"user_id": user_id, "session_id": body.session_id, "role": "assistant",
                                           "content": reply, "ts": now_iso()})
        async def fallback_gen():
            yield f"data: {json.dumps({'delta': reply})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        return StreamingResponse(fallback_gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    chat_client = LlmChat(api_key=EMERGENT_LLM_KEY, session_id=body.session_id,
                          system_message=build_system_prompt(user_id)).with_model(*AI_MODEL)

    async def event_gen():
        full = ""
        try:
            async for ev in chat_client.stream_message(UserMessage(text=body.message)):
                if isinstance(ev, TextDelta):
                    full += ev.content
                    yield f"data: {json.dumps({'delta': ev.content})}\n\n"
                elif isinstance(ev, StreamDone):
                    break
        except Exception as e:
            logger.error(f"chat error: {e}")
            yield f"data: {json.dumps({'delta': ' [chat AI non disponibile: aggiungi credito alla Universal Key]'})}\n\n"
        await db.chat_messages.insert_one({"user_id": user_id, "session_id": body.session_id, "role": "assistant",
                                           "content": full, "ts": now_iso()})
        yield f"data: {json.dumps({'done': True})}\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@api_router.get("/chat/history/{session_id}")
async def chat_history(session_id: str, user_id: str = Depends(get_user_id)):
    msgs = await db.chat_messages.find({"user_id": user_id, "session_id": session_id}, {"_id": 0}).sort("ts", 1).to_list(200)
    return {"messages": msgs}


app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup():
    try:
        configs = await db.po_config.find({"ssid": {"$exists": True}}).to_list(1000)
        for cfg in configs:
            user_id = cfg["_id"]
            sess = get_session(user_id)
            if not sess.get("engine_task"):
                sess["engine_task"] = asyncio.create_task(engine_loop(user_id))
    except Exception as e:
        logger.warning(f"Startup DB load skipped (database not reachable): {e}")


@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

