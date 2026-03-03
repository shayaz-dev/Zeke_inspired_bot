# ╔══════════════════════════════════════════════════════════════════╗
# ║   Zeke-inspired Liquidation + OI Reset Alert Bot                ║
# ║   Targets: Buy during high long liquidations + OI reset         ║
# ║           (downside cascade exhaustion)                         ║
# ║           Sell during high short liquidations + OI reset        ║
# ║           (upside squeeze exhaustion)                           ║
# ║   Runs forever — deploy on VPS / Replit / Railway / etc.        ║
# ╚══════════════════════════════════════════════════════════════════╝

import sys
import os
import json
import time
import threading
import logging
import collections
from datetime import datetime, timezone
from typing import Optional
import requests
import websocket
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# Force unbuffered output for Railway logs
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.info("=== Zeke Liq Bot starting up ===")

# ────────────────────────────────────────────────
#  CONFIGURATION — loaded from environment variables
# ────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("CHAT_ID")

logging.info(f"TELEGRAM_TOKEN set: {bool(TELEGRAM_BOT_TOKEN)}")
logging.info(f"CHAT_ID set: {bool(TELEGRAM_CHAT_ID)}")

if not TELEGRAM_BOT_TOKEN:
    logging.error("ERROR: TELEGRAM_TOKEN not set!")
    sys.exit(1)

if not TELEGRAM_CHAT_ID:
    logging.error("ERROR: CHAT_ID not set!")
    sys.exit(1)

logging.info(f"Config OK — Chat ID: {TELEGRAM_CHAT_ID}")

SYMBOL             = os.environ.get("SYMBOL", "BTCUSDT")
CONTRACT_TYPE      = "linear"

# Liquidation thresholds
LIQ_USD_THRESHOLD_LONG  = int(os.environ.get("LIQ_THRESHOLD_LONG",  "80000000"))
LIQ_USD_THRESHOLD_SHORT = int(os.environ.get("LIQ_THRESHOLD_SHORT", "60000000"))

OI_DROP_PCT_THRESHOLD   = float(os.environ.get("OI_DROP_PCT", "12.0"))

LOOKBACK_MINUTES_FOR_LIQ = 15
OI_POLL_INTERVAL_SEC     = 300
ALERT_COOLDOWN_SEC       = 600
WS_RECONNECT_DELAY_SEC   = 10
WS_MAX_RECONNECT_DELAY_SEC = 120

logging.info(
    f"Config: SYMBOL={SYMBOL} | "
    f"LONG threshold=${LIQ_USD_THRESHOLD_LONG:,} | "
    f"SHORT threshold=${LIQ_USD_THRESHOLD_SHORT:,} | "
    f"OI drop={OI_DROP_PCT_THRESHOLD}%"
)

# ────────────────────────────────────────────────
#  GLOBALS & STATE
# ────────────────────────────────────────────────

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN, threaded=False)
bot_start_time = time.time()

liq_events_lock = threading.Lock()
liq_events: collections.deque = collections.deque()

oi_lock = threading.Lock()
last_oi_value: Optional[float] = None
last_oi_time:  Optional[float] = None

alert_lock = threading.Lock()
last_alert_time: dict[str, float] = {"LONG": 0.0, "SHORT": 0.0}

_ws_reconnect_delay = WS_RECONNECT_DELAY_SEC


# ────────────────────────────────────────────────
#  HELPERS
# ────────────────────────────────────────────────

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def is_on_cooldown(direction: str) -> bool:
    with alert_lock:
        elapsed = time.time() - last_alert_time.get(direction, 0.0)
        return elapsed < ALERT_COOLDOWN_SEC


def mark_alert_sent(direction: str) -> None:
    with alert_lock:
        last_alert_time[direction] = time.time()


def send_alert(direction: str, liq_usd: float, oi_drop_pct: float) -> None:
    if is_on_cooldown(direction):
        logging.info(f"Alert for {direction} suppressed — cooldown active")
        return

    emoji = "📉🟢" if direction == "LONG" else "📈🔴"
    action = "BUY signal — target bounce" if direction == "LONG" else "SELL signal — target reversal"
    title = f"{emoji} {direction} LIQ CASCADE + OI RESET"

    text = (
        f"*{title}*\n\n"
        f"• Symbol: *{SYMBOL}*\n"
        f"• Liquidations \\({LOOKBACK_MINUTES_FOR_LIQ} min\\): *${liq_usd:,.0f}*\n"
        f"• OI drop: *{oi_drop_pct:.1f}%*\n"
        f"• Time: `{now_utc_str()}`\n\n"
        f"→ Scale limit orders — {action}"
    )

    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton(
            "TradingView 📊",
            url=f"https://www.tradingview.com/chart/?symbol=BINANCE:{SYMBOL}P",
        ),
        InlineKeyboardButton(
            "Coinglass Liqs 🔥",
            url="https://www.coinglass.com/LiquidationData",
        ),
    )

    try:
        bot.send_message(
            TELEGRAM_CHAT_ID,
            text,
            parse_mode="MarkdownV2",
            disable_web_page_preview=True,
            reply_markup=markup,
        )
        mark_alert_sent(direction)
        logging.info(f"✅ Alert sent → {direction} | liq=${liq_usd:,.0f} | OI drop={oi_drop_pct:.1f}%")
    except Exception as e:
        logging.error(f"Telegram send failed: {e}")


def cleanup_old_liqs() -> None:
    cutoff = time.time() - LOOKBACK_MINUTES_FOR_LIQ * 60
    with liq_events_lock:
        while liq_events and liq_events[0][0] < cutoff:
            liq_events.popleft()


def record_liq(side: str, usd_value: float) -> None:
    with liq_events_lock:
        liq_events.append((time.time(), side, usd_value))


def calculate_liq_volume(side_filter: str) -> float:
    cleanup_old_liqs()
    with liq_events_lock:
        return sum(usd for _, side, usd in liq_events if side == side_filter)


def get_current_oi() -> Optional[float]:
    try:
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL}"
        r = requests.get(url, timeout=6)
        r.raise_for_status()
        return float(r.json()["openInterest"])
    except Exception as e:
        logging.warning(f"Failed to fetch OI: {e}")
        return None


# ────────────────────────────────────────────────
#  OI DROP LOGIC
# ────────────────────────────────────────────────

def get_oi_drop_percentage() -> Optional[float]:
    global last_oi_value, last_oi_time

    current_oi = get_current_oi()
    if current_oi is None:
        return None

    now = time.time()

    with oi_lock:
        if last_oi_value is None or last_oi_time is None:
            last_oi_value = current_oi
            last_oi_time  = now
            return 0.0

        minutes_diff = (now - last_oi_time) / 60
        if minutes_diff < 3:
            return None

        if current_oi >= last_oi_value:
            last_oi_value = current_oi
            last_oi_time  = now
            return 0.0

        drop_pct = (last_oi_value - current_oi) / last_oi_value * 100

        if drop_pct > 2.0:
            last_oi_value = current_oi
            last_oi_time  = now

        return drop_pct


# ────────────────────────────────────────────────
#  SIGNAL LOGIC
# ────────────────────────────────────────────────

def check_for_signal() -> None:
    long_liq_usd = calculate_liq_volume("LONG")
    if long_liq_usd >= LIQ_USD_THRESHOLD_LONG:
        oi_drop = get_oi_drop_percentage()
        if oi_drop is not None and oi_drop >= OI_DROP_PCT_THRESHOLD:
            send_alert("LONG", long_liq_usd, oi_drop)

    short_liq_usd = calculate_liq_volume("SHORT")
    if short_liq_usd >= LIQ_USD_THRESHOLD_SHORT:
        oi_drop = get_oi_drop_percentage()
        if oi_drop is not None and oi_drop >= OI_DROP_PCT_THRESHOLD:
            send_alert("SHORT", short_liq_usd, oi_drop)


# ────────────────────────────────────────────────
#  WEBSOCKET — Liquidation stream
# ────────────────────────────────────────────────

def _parse_force_order(order: dict) -> None:
    side      = order.get("S", "")
    price     = float(order.get("ap") or order.get("p") or 0)
    qty       = float(order.get("q", 0))
    usd_value = price * qty

    if usd_value <= 0:
        return

    liq_side = "SHORT" if side == "BUY" else "LONG"
    record_liq(liq_side, usd_value)
    logging.debug(f"Liq recorded: {liq_side} ${usd_value:,.0f}")
    check_for_signal()


def on_liq_message(ws, message: str) -> None:
    try:
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                if item.get("e") == "forceOrder":
                    _parse_force_order(item["o"])
        elif isinstance(data, dict):
            if data.get("e") == "forceOrder":
                _parse_force_order(data["o"])
            elif "data" in data and isinstance(data["data"], dict):
                inner = data["data"]
                if inner.get("e") == "forceOrder":
                    _parse_force_order(inner["o"])
    except Exception as e:
        logging.error(f"WS message parse error: {e}")


def on_error(ws, error) -> None:
    logging.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg) -> None:
    global _ws_reconnect_delay
    logging.warning(
        f"WebSocket closed (code={close_status_code}) — "
        f"reconnecting in {_ws_reconnect_delay}s…"
    )
    time.sleep(_ws_reconnect_delay)
    _ws_reconnect_delay = min(_ws_reconnect_delay * 2, WS_MAX_RECONNECT_DELAY_SEC)
    start_liquidation_websocket()


def on_open(ws) -> None:
    global _ws_reconnect_delay
    logging.info("WebSocket connected — listening for liquidations")
    _ws_reconnect_delay = WS_RECONNECT_DELAY_SEC


def start_liquidation_websocket() -> None:
    ws_url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_liq_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)


# ────────────────────────────────────────────────
#  OI POLLER
# ────────────────────────────────────────────────

def oi_poller() -> None:
    global last_oi_value, last_oi_time

    while True:
        try:
            current = get_current_oi()
            if current is not None:
                now = time.time()
                with oi_lock:
                    if last_oi_value is not None and current < last_oi_value:
                        drop_pct = (last_oi_value - current) / last_oi_value * 100
                        if drop_pct >= OI_DROP_PCT_THRESHOLD:
                            logging.info(
                                f"📊 OI poller: significant drop — "
                                f"{drop_pct:.1f}% "
                                f"(was {last_oi_value:,.0f}, now {current:,.0f})"
                            )
                    last_oi_value = current
                    last_oi_time  = now
        except Exception as e:
            logging.error(f"OI poller error: {e}")

        time.sleep(OI_POLL_INTERVAL_SEC)


# ────────────────────────────────────────────────
#  HEARTBEAT
# ────────────────────────────────────────────────

def heartbeat() -> None:
    while True:
        time.sleep(60)
        cleanup_old_liqs()
        long_sum  = calculate_liq_volume("LONG")
        short_sum = calculate_liq_volume("SHORT")
        with oi_lock:
            oi_snap = last_oi_value
        logging.info(
            f"[heartbeat] LONG liqs=${long_sum:>14,.0f}  "
            f"SHORT liqs=${short_sum:>14,.0f}  "
            f"OI={oi_snap:,.0f}" if oi_snap else
            f"[heartbeat] LONG liqs=${long_sum:>14,.0f}  "
            f"SHORT liqs=${short_sum:>14,.0f}  OI=n/a"
        )


# ────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────

if __name__ == "__main__":
    logging.info(
        f"Starting Zeke Liquidation Bot ▸ {SYMBOL} ▸ "
        f"LONG threshold: ${LIQ_USD_THRESHOLD_LONG:,}  "
        f"SHORT threshold: ${LIQ_USD_THRESHOLD_SHORT:,}  "
        f"OI drop: {OI_DROP_PCT_THRESHOLD}%"
    )

    initial_oi = get_current_oi()
    if initial_oi:
        with oi_lock:
            last_oi_value = initial_oi
            last_oi_time  = time.time()
        logging.info(f"Initial OI baseline: {initial_oi:,.2f}")
    else:
        logging.warning("Could not fetch initial OI — will set on first poll")

    threading.Thread(target=start_liquidation_websocket, daemon=True, name="ws-liq").start()
    threading.Thread(target=oi_poller,                   daemon=True, name="oi-poll").start()
    threading.Thread(target=heartbeat,                   daemon=True, name="heartbeat").start()

    logging.info("Bot running — waiting for signals…")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.info("Shutdown requested — exiting.")
