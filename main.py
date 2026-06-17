import asyncio
import contextlib
import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import gspread
import pandas as pd
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from fastapi import FastAPI, HTTPException

APP_VERSION = "1.3.0-option-underlying-metrics"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("loss-position-screener")

app = FastAPI(title="Loss Position Screener", version=APP_VERSION)

# Columns A:AA intentionally remain compatible with the earlier Manager tab.
# New option/metric metadata is appended at AB:AE.
HEADERS = [
    "symbol",                  # A - actual Alpaca position symbol; downstream bots should act on this
    "side",                    # B
    "asset_class",             # C
    "qty",                     # D
    "avg_entry",               # E
    "current_price",           # F
    "market_value",            # G
    "unrealized_pl",           # H
    "unrealized_pct",          # I
    "daily_close",             # J - metric symbol daily close
    "sma_200",                 # K - metric symbol SMA 200
    "sma_50",                  # L - metric symbol SMA 50
    "pos_52w",                 # M - metric symbol 52-week position
    "dollar_vol_m",            # N - metric symbol dollar volume, millions
    "atr14_pct",               # O - metric symbol ATR14 / metric price
    "current_gt_sma200",       # P
    "sma50_gt_sma200",         # Q
    "first_seen_red_at",       # R
    "days_red",                # S
    "refreshed_at",            # T
    "entry_score_now",         # U - Google Sheets formula
    "loss_health_score",       # V - Google Sheets formula
    "action",                  # W - Google Sheets formula: HOLD/WATCH/REDUCE/EXIT
    "reduce_pct",              # X - Google Sheets formula
    "reason",                  # Y - Google Sheets formula
    "cooldown_days",           # Z - Google Sheets formula
    "data_status",             # AA
    "metric_symbol",           # AB - symbol used for market data. For options, this is the underlying.
    "option_type",             # AC - CALL/PUT/blank
    "option_expiration",       # AD
    "option_strike",           # AE
]

MANAGER_TAB_NAME = os.getenv("MANAGER_TAB_NAME", "Manager")
HISTORY_CALENDAR_DAYS = int(os.getenv("HISTORY_CALENDAR_DAYS", "420"))
BAR_BATCH_SIZE = int(os.getenv("BAR_BATCH_SIZE", "50"))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# Perpetual Railway loop settings.
# Default: run one Manager refresh, wait 5 minutes after it finishes, then run the next.
LOOP_ENABLED = os.getenv("LOOP_ENABLED", "true").strip().lower() not in {"false", "0", "no", "off"}
MIN_LOOP_INTERVAL_SECONDS = max(30, int(os.getenv("MIN_LOOP_INTERVAL_SECONDS", "60")))
MANAGER_LOOP_INTERVAL_SECONDS = max(
    MIN_LOOP_INTERVAL_SECONDS,
    int(os.getenv("MANAGER_LOOP_INTERVAL_SECONDS", os.getenv("LOOP_INTERVAL_SECONDS", "300"))),
)
MANAGER_LOOP_INITIAL_DELAY_SECONDS = max(0, int(os.getenv("MANAGER_LOOP_INITIAL_DELAY_SECONDS", "15")))

_refresh_lock = threading.Lock()
_loop_task: Optional[asyncio.Task] = None
_last_refresh_started_at: Optional[str] = None
_last_refresh_finished_at: Optional[str] = None
_last_refresh_result: Optional[Dict[str, Any]] = None
_last_refresh_error: Optional[str] = None
_last_cycle_seconds: Optional[float] = None


# -----------------------------
# Generic helpers
# -----------------------------

def as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if hasattr(value, "value"):
        value = value.value
    return str(value)


def as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return default


def rounded(value: Optional[float], places: int = 4) -> Any:
    if value is None:
        return ""
    try:
        return round(float(value), places)
    except (ValueError, TypeError):
        return ""


def get_field(obj: Any, name: str, default: Any = None) -> Any:
    if hasattr(obj, name):
        return getattr(obj, name)
    if isinstance(obj, dict):
        return obj.get(name, default)
    return default


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def bool_or_blank(value: Optional[bool]) -> Any:
    if value is None:
        return ""
    return bool(value)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# -----------------------------
# Position/option symbol helpers
# -----------------------------

# Alpaca option positions commonly appear as OCC-like compact symbols:
#   CNI270115C00115000 -> underlying=CNI, expiration=2027-01-15, type=CALL, strike=115.0
#   CYTK280121C00075000 -> underlying=CYTK, expiration=2028-01-21, type=CALL, strike=75.0
# This regex is intentionally non-greedy for the underlying so the date is captured reliably.
OPTION_SYMBOL_RE = re.compile(r"^(.+?)(\d{6})([CP])(\d{8})$")


def parse_option_symbol(symbol: str) -> Dict[str, Any]:
    raw = (symbol or "").strip().upper()
    match = OPTION_SYMBOL_RE.match(raw)
    if not match:
        return {
            "is_option_symbol": False,
            "underlying": "",
            "option_type": "",
            "expiration": "",
            "strike": None,
        }

    underlying, yymmdd, cp, strike_raw = match.groups()
    try:
        yy = int(yymmdd[0:2])
        year = 2000 + yy
        month = int(yymmdd[2:4])
        day = int(yymmdd[4:6])
        expiration = date(year, month, day).isoformat()
    except ValueError:
        expiration = ""

    try:
        strike = int(strike_raw) / 1000.0
    except ValueError:
        strike = None

    return {
        "is_option_symbol": True,
        "underlying": underlying,
        "option_type": "CALL" if cp == "C" else "PUT",
        "expiration": expiration,
        "strike": strike,
    }


def position_metric_info(symbol: str, asset_class: str) -> Dict[str, Any]:
    symbol = (symbol or "").strip().upper()
    asset_class = (asset_class or "").strip().lower()

    parsed = parse_option_symbol(symbol)
    if asset_class == "us_option" or parsed["is_option_symbol"]:
        metric_symbol = parsed["underlying"] if parsed["is_option_symbol"] else ""
        return {
            "metric_symbol": metric_symbol,
            "option_type": parsed["option_type"],
            "option_expiration": parsed["expiration"],
            "option_strike": parsed["strike"],
            "is_supported_for_metrics": bool(metric_symbol),
        }

    if asset_class == "us_equity" or not asset_class:
        return {
            "metric_symbol": symbol,
            "option_type": "",
            "option_expiration": "",
            "option_strike": None,
            "is_supported_for_metrics": bool(symbol),
        }

    return {
        "metric_symbol": "",
        "option_type": "",
        "option_expiration": "",
        "option_strike": None,
        "is_supported_for_metrics": False,
    }


# -----------------------------
# Environment / client helpers
# -----------------------------

def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def alpaca_trading_client() -> TradingClient:
    key = require_env("ALPACA_API_KEY")
    secret = require_env("ALPACA_SECRET_KEY")
    paper = os.getenv("ALPACA_PAPER", "true").strip().lower() not in {"false", "0", "no"}
    return TradingClient(key, secret, paper=paper)


def alpaca_data_client() -> StockHistoricalDataClient:
    key = require_env("ALPACA_API_KEY")
    secret = require_env("ALPACA_SECRET_KEY")
    return StockHistoricalDataClient(key, secret)


def get_data_feed() -> DataFeed:
    raw = os.getenv("ALPACA_DATA_FEED", "iex").strip().upper()
    try:
        return DataFeed[raw]
    except KeyError:
        logger.warning("Unknown ALPACA_DATA_FEED=%s; falling back to IEX", raw)
        return DataFeed.IEX


def gspread_client() -> gspread.Client:
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_json:
        try:
            info = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from exc
        return gspread.service_account_from_dict(info)

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    return gspread.service_account(filename=credentials_path)


# -----------------------------
# Google Sheets helpers
# -----------------------------

def open_or_create_manager_tab(gc: gspread.Client) -> gspread.Worksheet:
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Missing required environment variable: GOOGLE_SHEET_ID")

    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        return spreadsheet.worksheet(MANAGER_TAB_NAME)
    except gspread.WorksheetNotFound:
        logger.info("Creating worksheet tab: %s", MANAGER_TAB_NAME)
        return spreadsheet.add_worksheet(title=MANAGER_TAB_NAME, rows=100, cols=len(HEADERS))


def read_existing_first_seen(ws: gspread.Worksheet) -> Dict[str, str]:
    """Preserve first_seen_red_at across full refreshes of the Manager tab."""
    try:
        values = ws.get_all_values()
    except Exception:
        logger.exception("Could not read existing Manager values; first_seen_red_at will reset")
        return {}

    if not values or len(values) < 2:
        return {}

    headers = values[0]
    try:
        symbol_idx = headers.index("symbol")
        first_seen_idx = headers.index("first_seen_red_at")
    except ValueError:
        return {}

    result: Dict[str, str] = {}
    for row in values[1:]:
        if len(row) <= max(symbol_idx, first_seen_idx):
            continue
        symbol = row[symbol_idx].strip().upper()
        first_seen = row[first_seen_idx].strip()
        if symbol and first_seen:
            result[symbol] = first_seen
    return result


def days_since_iso_date(iso_text: str) -> int:
    if not iso_text:
        return 0
    try:
        parsed = datetime.fromisoformat(iso_text.replace("Z", "+00:00"))
        return max(0, (datetime.now(timezone.utc).date() - parsed.date()).days)
    except ValueError:
        try:
            parsed_date = date.fromisoformat(iso_text[:10])
            return max(0, (datetime.now(timezone.utc).date() - parsed_date).days)
        except ValueError:
            return 0


def row_formulas(row_number: int) -> Dict[str, str]:
    """Formulas are intentionally on the Manager tab only; the Screener tab is never touched."""
    # Applies to long equities and long calls. Puts and other instruments are left as WATCH.
    applicable = f'OR($C{row_number}="us_equity",AND($C{row_number}="us_option",$AC{row_number}="CALL"))'

    entry_score_now = (
        f'=IFERROR(IF(OR($B{row_number}<>"long",$AA{row_number}<>"OK",NOT({applicable})),"",'
        f'40*$P{row_number}+20*$Q{row_number}+30*$M{row_number}+10*MIN(1,$N{row_number}/10)),"")'
    )

    loss_health_score = (
        f'=IFERROR(IF(OR($B{row_number}<>"long",$AA{row_number}<>"OK",NOT({applicable})),"",'
        f'MAX(0,MIN(100,'
        f'35*$P{row_number}+'
        f'15*$Q{row_number}+'
        f'20*$M{row_number}+'
        f'20*MIN(1,$N{row_number}/10)+'
        f'10*($I{row_number}>-0.03)-'
        f'10*($I{row_number}<=-0.05)-'
        f'20*($I{row_number}<=-0.08)-'
        f'10*($S{row_number}>=10)-'
        f'20*($S{row_number}>=20)'
        f'))),"")'
    )

    action = (
        f'=IFERROR(IF($B{row_number}<>"long","WATCH",'
        f'IF($AA{row_number}<>"OK","WATCH",'
        f'IF(NOT({applicable}),"WATCH",'
        f'IF(AND($I{row_number}<0,$P{row_number}=FALSE),"EXIT",'
        f'IF($V{row_number}<50,"EXIT",'
        f'IF($V{row_number}<75,"REDUCE",'
        f'IF($V{row_number}<90,"WATCH","HOLD"))))))),"")'
    )

    reduce_pct = (
        f'=IFERROR(IF($W{row_number}="EXIT",100,'
        f'IF($W{row_number}="REDUCE",IF($V{row_number}<60,50,25),0)),"")'
    )

    reason = (
        f'=IFERROR(IF($B{row_number}<>"long","Short position: long-only formula not applied",'
        f'IF($AA{row_number}<>"OK","Data unavailable",'
        f'IF(NOT({applicable}),"Unsupported position type for this formula",'
        f'IF(AND($I{row_number}<0,$P{row_number}=FALSE),'
        f'IF($C{row_number}="us_option","Underlying lost SMA 200","Lost SMA 200"),'
        f'IF($V{row_number}<50,"Loss health score below 50",'
        f'IF($V{row_number}<75,"Deteriorating negative position",'
        f'IF($V{row_number}<90,"Negative but structurally alive","Healthy pullback"))))))),"")'
    )

    cooldown_days = (
        f'=IFERROR(IF($W{row_number}="EXIT",IF($I{row_number}<=-0.08,10,5),'
        f'IF($W{row_number}="REDUCE",3,0)),"")'
    )

    return {
        "entry_score_now": entry_score_now,
        "loss_health_score": loss_health_score,
        "action": action,
        "reduce_pct": reduce_pct,
        "reason": reason,
        "cooldown_days": cooldown_days,
    }


def write_manager_tab(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    ws.clear()
    row_count = max(len(rows) + 20, 100)
    ws.resize(rows=row_count, cols=len(HEADERS))
    ws.update(values=rows, range_name="A1", value_input_option="USER_ENTERED")

    try:
        last_col = "AE"
        ws.freeze(rows=1)
        ws.format(f"A1:{last_col}1", {"textFormat": {"bold": True}})
    except Exception:
        logger.info("Skipping optional worksheet formatting", exc_info=True)


# -----------------------------
# Alpaca / market data helpers
# -----------------------------

def get_red_positions(trading: TradingClient) -> List[Any]:
    positions = trading.get_all_positions()
    red_positions = []

    for pos in positions:
        unrealized_pl = as_float(get_field(pos, "unrealized_pl"), 0.0)
        unrealized_pct = as_float(get_field(pos, "unrealized_plpc"), None)

        if unrealized_pct is None:
            avg_entry = as_float(get_field(pos, "avg_entry_price"), None)
            current = as_float(get_field(pos, "current_price"), None)
            if avg_entry and current:
                unrealized_pct = (current - avg_entry) / avg_entry

        is_red = (unrealized_pl is not None and unrealized_pl < 0) or (
            unrealized_pct is not None and unrealized_pct < 0
        )
        if is_red:
            red_positions.append(pos)

    return red_positions


def fetch_daily_bars(symbols: List[str]) -> pd.DataFrame:
    clean_symbols = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    if not clean_symbols:
        return pd.DataFrame()

    data_client = alpaca_data_client()
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=HISTORY_CALENDAR_DAYS)
    feed = get_data_feed()

    frames: List[pd.DataFrame] = []
    for batch in chunks(clean_symbols, BAR_BATCH_SIZE):
        logger.info("Fetching daily bars for metric symbols: %s", batch)
        request = StockBarsRequest(
            symbol_or_symbols=batch,
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            feed=feed,
            adjustment=Adjustment.RAW,
        )
        try:
            barset = data_client.get_stock_bars(request)
            df = barset.df
            if df is not None and not df.empty:
                frames.append(df.reset_index())
        except Exception:
            # If one symbol in a batch is bad, Alpaca rejects the whole request. Try one-by-one so one
            # invalid metric symbol does not starve the rest of the Manager tab.
            logger.exception("Failed to fetch daily bars for batch; retrying one-by-one: %s", batch)
            for symbol in batch:
                single_request = StockBarsRequest(
                    symbol_or_symbols=[symbol],
                    timeframe=TimeFrame.Day,
                    start=start,
                    end=end,
                    feed=feed,
                    adjustment=Adjustment.RAW,
                )
                try:
                    single_barset = data_client.get_stock_bars(single_request)
                    single_df = single_barset.df
                    if single_df is not None and not single_df.empty:
                        frames.append(single_df.reset_index())
                except Exception:
                    logger.exception("Failed to fetch daily bars for metric symbol: %s", symbol)

    if not frames:
        return pd.DataFrame()

    all_bars = pd.concat(frames, ignore_index=True)
    if "timestamp" in all_bars.columns:
        all_bars = all_bars.sort_values(["symbol", "timestamp"])
    return all_bars


def compute_metrics(symbol: str, bars_df: pd.DataFrame, current_price: Optional[float]) -> Dict[str, Any]:
    if not symbol:
        return {"data_status": "NO_METRIC_SYMBOL"}
    if bars_df.empty or "symbol" not in bars_df.columns:
        return {"data_status": "NO_BARS"}

    df = bars_df[bars_df["symbol"].str.upper() == symbol.upper()].copy()
    if df.empty:
        return {"data_status": "NO_BARS"}

    for col in ["close", "high", "low", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close", "high", "low", "volume"])
    if df.empty:
        return {"data_status": "BAD_BARS"}

    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")

    close_series = df["close"]
    latest_close = float(close_series.iloc[-1])
    effective_price = current_price if current_price is not None and current_price > 0 else latest_close

    sma_50 = float(close_series.tail(min(50, len(close_series))).mean())
    sma_200 = float(close_series.tail(min(200, len(close_series))).mean())

    lookback_52w = df.tail(min(252, len(df)))
    low_52w = float(lookback_52w["low"].min())
    high_52w = float(lookback_52w["high"].max())
    if high_52w > low_52w:
        pos_52w = max(0.0, min(1.0, (effective_price - low_52w) / (high_52w - low_52w)))
    else:
        pos_52w = 0.5

    latest_volume = float(df["volume"].iloc[-1])
    dollar_vol_m = (latest_close * latest_volume) / 1_000_000

    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_14 = float(true_range.tail(min(14, len(true_range))).mean())
    atr14_pct = atr_14 / effective_price if effective_price and effective_price > 0 else None

    return {
        "daily_close": latest_close,
        "sma_200": sma_200,
        "sma_50": sma_50,
        "pos_52w": pos_52w,
        "dollar_vol_m": dollar_vol_m,
        "atr14_pct": atr14_pct,
        "current_gt_sma200": effective_price > sma_200 if sma_200 else None,
        "sma50_gt_sma200": sma_50 > sma_200 if sma_50 and sma_200 else None,
        "data_status": "OK",
    }


# -----------------------------
# Main refresh routine
# -----------------------------

def build_manager_rows() -> Dict[str, Any]:
    trading = alpaca_trading_client()
    gc = gspread_client()
    ws = open_or_create_manager_tab(gc)
    existing_first_seen = read_existing_first_seen(ws)

    red_positions = get_red_positions(trading)

    position_infos: Dict[str, Dict[str, Any]] = {}
    metric_symbols: Set[str] = set()
    for pos in red_positions:
        symbol = as_str(get_field(pos, "symbol")).upper()
        asset_class = as_str(get_field(pos, "asset_class"), "").lower()
        info = position_metric_info(symbol, asset_class)
        position_infos[symbol] = info
        if info["is_supported_for_metrics"] and info["metric_symbol"]:
            metric_symbols.add(info["metric_symbol"])

    symbols = sorted(position_infos.keys())
    bars_df = fetch_daily_bars(sorted(metric_symbols))

    now_iso = utc_now_iso()
    today_first_seen = datetime.now(timezone.utc).date().isoformat()
    output_rows: List[List[Any]] = [HEADERS]

    for index, pos in enumerate(sorted(red_positions, key=lambda p: as_str(get_field(p, "symbol")).upper()), start=2):
        symbol = as_str(get_field(pos, "symbol")).upper()
        side = as_str(get_field(pos, "side"), "").lower()
        asset_class = as_str(get_field(pos, "asset_class"), "").lower()
        qty = as_float(get_field(pos, "qty"), 0.0)
        avg_entry = as_float(get_field(pos, "avg_entry_price"), None)
        current_price = as_float(get_field(pos, "current_price"), None)
        market_value = as_float(get_field(pos, "market_value"), None)
        unrealized_pl = as_float(get_field(pos, "unrealized_pl"), None)
        unrealized_pct = as_float(get_field(pos, "unrealized_plpc"), None)

        if unrealized_pct is None and avg_entry and current_price:
            unrealized_pct = (current_price - avg_entry) / avg_entry

        info = position_infos.get(symbol) or position_metric_info(symbol, asset_class)
        metric_symbol = info["metric_symbol"]
        option_type = info["option_type"]
        option_expiration = info["option_expiration"]
        option_strike = info["option_strike"]

        # For options, the Manager metrics are based on the underlying's latest daily close.
        # Do not pass the option premium as current_price into an underlying 52-week calculation.
        metric_current_price = current_price if asset_class == "us_equity" else None
        metrics = compute_metrics(metric_symbol, bars_df, metric_current_price)
        first_seen = existing_first_seen.get(symbol, today_first_seen)
        formulas = row_formulas(index)

        row = [
            symbol,
            side,
            asset_class,
            rounded(qty, 6),
            rounded(avg_entry, 4),
            rounded(current_price, 4),
            rounded(market_value, 2),
            rounded(unrealized_pl, 2),
            rounded(unrealized_pct, 6),
            rounded(metrics.get("daily_close"), 4),
            rounded(metrics.get("sma_200"), 4),
            rounded(metrics.get("sma_50"), 4),
            rounded(metrics.get("pos_52w"), 4),
            rounded(metrics.get("dollar_vol_m"), 2),
            rounded(metrics.get("atr14_pct"), 6),
            bool_or_blank(metrics.get("current_gt_sma200")),
            bool_or_blank(metrics.get("sma50_gt_sma200")),
            first_seen,
            days_since_iso_date(first_seen),
            now_iso,
            formulas["entry_score_now"],
            formulas["loss_health_score"],
            formulas["action"],
            formulas["reduce_pct"],
            formulas["reason"],
            formulas["cooldown_days"],
            metrics.get("data_status", "UNKNOWN"),
            metric_symbol,
            option_type,
            option_expiration,
            rounded(option_strike, 3),
        ]
        output_rows.append(row)

    write_manager_tab(ws, output_rows)

    return {
        "status": "ok",
        "app_version": APP_VERSION,
        "manager_tab": MANAGER_TAB_NAME,
        "red_positions": len(red_positions),
        "symbols": symbols,
        "metric_symbols": sorted(metric_symbols),
        "written_rows_including_header": len(output_rows),
        "refreshed_at": now_iso,
    }


def run_manager_refresh(source: str = "manual") -> Dict[str, Any]:
    global _last_refresh_started_at, _last_refresh_finished_at, _last_refresh_result, _last_refresh_error, _last_cycle_seconds

    acquired = _refresh_lock.acquire(blocking=False)
    if not acquired:
        return {
            "status": "busy",
            "message": "Manager refresh already running; skipped overlapping request.",
            "source": source,
            "last_refresh_started_at": _last_refresh_started_at,
        }

    started = time.monotonic()
    _last_refresh_started_at = utc_now_iso()
    _last_refresh_error = None
    try:
        logger.info("Starting Manager refresh cycle from %s", source)
        result = build_manager_rows()
        _last_refresh_result = result
        _last_refresh_finished_at = utc_now_iso()
        _last_cycle_seconds = round(time.monotonic() - started, 2)
        result["source"] = source
        result["cycle_seconds"] = _last_cycle_seconds
        result["loop_enabled"] = LOOP_ENABLED
        result["next_cycle_after_seconds"] = MANAGER_LOOP_INTERVAL_SECONDS if source == "loop" else None
        logger.info(
            "Finished Manager refresh from %s: red_positions=%s symbols=%s metric_symbols=%s",
            source,
            result.get("red_positions"),
            result.get("symbols"),
            result.get("metric_symbols"),
        )
        return result
    except Exception as exc:
        _last_refresh_finished_at = utc_now_iso()
        _last_cycle_seconds = round(time.monotonic() - started, 2)
        _last_refresh_error = str(exc)
        logger.exception("Manager refresh failed from %s", source)
        raise
    finally:
        _refresh_lock.release()


async def manager_loop() -> None:
    if MANAGER_LOOP_INITIAL_DELAY_SECONDS:
        logger.info("Manager loop initial delay: %s seconds", MANAGER_LOOP_INITIAL_DELAY_SECONDS)
        await asyncio.sleep(MANAGER_LOOP_INITIAL_DELAY_SECONDS)

    while True:
        cycle_started = time.monotonic()
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, run_manager_refresh, "loop")
        except asyncio.CancelledError:
            logger.info("Manager loop cancelled")
            raise
        except Exception:
            logger.exception("Manager loop cycle failed; continuing after throttle interval")

        cycle_seconds = time.monotonic() - cycle_started
        logger.info(
            "Manager loop sleeping for %s seconds after %.2f-second cycle",
            MANAGER_LOOP_INTERVAL_SECONDS,
            cycle_seconds,
        )
        await asyncio.sleep(MANAGER_LOOP_INTERVAL_SECONDS)


@app.on_event("startup")
async def start_manager_loop() -> None:
    global _loop_task
    logger.warning(
        "Loaded Loss Position Screener version=%s loop_enabled=%s interval=%s initial_delay=%s manager_tab=%s",
        APP_VERSION,
        LOOP_ENABLED,
        MANAGER_LOOP_INTERVAL_SECONDS,
        MANAGER_LOOP_INITIAL_DELAY_SECONDS,
        MANAGER_TAB_NAME,
    )
    if LOOP_ENABLED:
        logger.info(
            "Starting perpetual Manager loop: interval=%s seconds, minimum_interval=%s seconds",
            MANAGER_LOOP_INTERVAL_SECONDS,
            MIN_LOOP_INTERVAL_SECONDS,
        )
        _loop_task = asyncio.create_task(manager_loop())
    else:
        logger.info("Perpetual Manager loop disabled; use /run for manual refreshes")


@app.on_event("shutdown")
async def stop_manager_loop() -> None:
    global _loop_task
    if _loop_task is not None:
        _loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _loop_task
        _loop_task = None


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "service": "loss-position-screener",
        "status": "running",
        "app_version": APP_VERSION,
        "manager_tab": MANAGER_TAB_NAME,
        "loop_enabled": LOOP_ENABLED,
        "loop_interval_seconds": MANAGER_LOOP_INTERVAL_SECONDS,
        "minimum_loop_interval_seconds": MIN_LOOP_INTERVAL_SECONDS,
        "loop_initial_delay_seconds": MANAGER_LOOP_INITIAL_DELAY_SECONDS,
        "run_endpoint": "/run",
        "status_endpoint": "/status",
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "app_version": APP_VERSION, "loop_enabled": LOOP_ENABLED}


@app.get("/status")
def status() -> Dict[str, Any]:
    return {
        "status": "ok",
        "app_version": APP_VERSION,
        "manager_tab": MANAGER_TAB_NAME,
        "loop_enabled": LOOP_ENABLED,
        "loop_interval_seconds": MANAGER_LOOP_INTERVAL_SECONDS,
        "loop_initial_delay_seconds": MANAGER_LOOP_INITIAL_DELAY_SECONDS,
        "refresh_in_progress": _refresh_lock.locked(),
        "last_refresh_started_at": _last_refresh_started_at,
        "last_refresh_finished_at": _last_refresh_finished_at,
        "last_cycle_seconds": _last_cycle_seconds,
        "last_refresh_result": _last_refresh_result,
        "last_refresh_error": _last_refresh_error,
    }


@app.api_route("/run", methods=["GET", "POST"])
def run() -> Dict[str, Any]:
    try:
        return run_manager_refresh("manual")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
