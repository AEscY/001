import os
import hmac
import base64
import hashlib
import websocket
import json
import requests
import time
import threading
from datetime import datetime, timedelta, timezone
from collections import deque
from flask import Flask
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== 版本信息 ====================
VERSION = "3.5.0"  # 增加乖离率扣分机制，避免追高接盘

# ==================== 配置区 ====================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    print("⚠️ 警告: TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未设置")

BTC_SYMBOL = "BTC-USDT"
DEFAULT_ALT_SYMBOLS = ["ETH-USDT", "SOL-USDT", "BNB-USDT", "ADA-USDT", "DOGE-USDT", "XRP-USDT"]

# ==================== 评分权重（6个核心因子） ====================
WEIGHT_TREND = 35       # 自身趋势
WEIGHT_DIVERGENCE = 25  # 背离差
WEIGHT_VOLUME = 15      # 成交量
WEIGHT_RSI = 10         # RSI
WEIGHT_FUNDING = 5      # 资金费率
WEIGHT_BIAS = 10        # 乖离率（新增，避免追高）

SCORE_THRESHOLD_HIGH = 70
SCORE_THRESHOLD_MEDIUM = 50

# ==================== 乖离率阈值 ====================
BIAS_SAFE = 3        # 低于此值加分
BIAS_WARN = 5        # 超过此值开始扣分
BIAS_DANGER = 8      # 超过此值扣大分

# ==================== 信号冷却配置 ====================
COOLDOWN_HOURS = 24

# ==================== 基础参数 ====================
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
VOLUME_THRESHOLD = 1000000
ALERT_COOLDOWN = 120
SUMMARY_MIN_DIFF = 0.3

# ==================== 风控参数 ====================
RISK_PER_TRADE = 0.02
ACCOUNT_BALANCE = 0
DEFAULT_LEVERAGE = 3
ATR_STOP_MULTIPLIER = 1.5
ATR_TAKE_MULTIPLIER = 2.5

# ==================== 验证配置 ====================
VERIFY_MINUTES = 15
VERIFY_PRICE_CHANGE_PCT = 0.8
PENDING_SIGNALS = []
PENDING_LOCK = threading.Lock()
VERIFY_STATS = {"total": 0, "success": 0, "failed": 0, "expired": 0}
SIGNAL_HISTORY = []
STATS_LOCK = threading.Lock()

# ==================== 自动过滤 ====================
AUTO_FILTER_ENABLED = True
MAX_COINS = 200
FILTER_INTERVAL = 1800

# ==================== 波动扫描 ====================
VOLATILITY_SCAN_ENABLED = True
VOLATILITY_THRESHOLD = 3.0
VOLATILITY_SCAN_INTERVAL = 60

# ==================== 全局状态 ====================
alt_symbols = set(DEFAULT_ALT_SYMBOLS)
price_data = {BTC_SYMBOL: {"price": 0, "change": 0, "volume": 0}}
for sym in alt_symbols:
    price_data[sym] = {"price": 0, "change": 0, "volume": 0}

diff_history = {}
last_alert_time = {}
ws_instance = None
ws_lock = threading.Lock()
restart_flag = False

auto_refresh_enabled = False
auto_refresh_interval = 300
auto_refresh_timer = None
price_candle_cache = {}

usdt_balance_cache = {"balance": 0, "timestamp": 0, "valid": False}
BALANCE_CACHE_TTL = 60

current_leverage = DEFAULT_LEVERAGE
MIN_SCORE_THRESHOLD = 50

# ==================== 菜单键盘 ====================
def get_main_keyboard():
    buttons = [
        [KeyboardButton("/status"), KeyboardButton("/summary")],
        [KeyboardButton("/autorefresh on"), KeyboardButton("/autorefresh off")],
        [KeyboardButton("/addcoin"), KeyboardButton("/addtop")],
        [KeyboardButton("/removecoin"), KeyboardButton("/clear")],
        [KeyboardButton("/setthreshold"), KeyboardButton("/setleverage")],
        [KeyboardButton("/debug"), KeyboardButton("/refreshbalance")],
        [KeyboardButton("/help")]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)

# ==================== 推送函数 ====================
def send_telegram(msg, parse_mode=None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"⚠️ Token或ChatID未配置，无法发送: {msg[:50]}...")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"推送失败: {e}")

# ==================== OKX API 签名工具 ====================
def generate_sign(timestamp, method, request_path, body, secret_key):
    message = timestamp + method + request_path + body
    mac = hmac.new(
        bytes(secret_key, encoding='utf8'),
        bytes(message, encoding='utf-8'),
        hashlib.sha256
    )
    return base64.b64encode(mac.digest()).decode('utf-8')

def get_usdt_balance(force_refresh=False):
    global usdt_balance_cache
    api_key = os.environ.get("OKX_API_KEY")
    secret_key = os.environ.get("OKX_SECRET_KEY")
    passphrase = os.environ.get("OKX_PASSPHRASE")
    if not api_key or not secret_key or not passphrase:
        return ACCOUNT_BALANCE
    now = time.time()
    if not force_refresh and usdt_balance_cache["valid"] and (now - usdt_balance_cache["timestamp"]) < BALANCE_CACHE_TTL:
        return usdt_balance_cache["balance"]
    try:
        dt = datetime.now(timezone.utc)
        timestamp = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        method = "GET"
        request_path = "/api/v5/account/balance?ccy=USDT"
        url = "https://www.okx.com" + request_path
        sign = generate_sign(timestamp, method, request_path, "", secret_key)
        headers = {
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
        }
        resp = requests.get(url, headers=headers, timeout=5)
        data = resp.json()
        if data.get("code") == "0":
            for detail in data["data"][0]["details"]:
                if detail["ccy"] == "USDT":
                    balance = float(detail.get("availBal", 0))
                    usdt_balance_cache["balance"] = balance
                    usdt_balance_cache["timestamp"] = now
                    usdt_balance_cache["valid"] = True
                    return balance
            return 0.0
        else:
            print(f"⚠️ API 返回错误: {data.get('msg', '未知错误')}")
            return ACCOUNT_BALANCE
    except Exception as e:
        print(f"⚠️ 获取USDT余额异常: {e}")
        return ACCOUNT_BALANCE

def refresh_balance_cache():
    global usdt_balance_cache
    usdt_balance_cache["valid"] = False
    return get_usdt_balance(force_refresh=True)

# ==================== 核心工具函数 ====================
def get_swap_symbols():
    try:
        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data["code"] != "0":
            return set()
        symbols = set()
        for inst in data["data"]:
            if inst["settleCcy"] == "USDT":
                base = "-".join(inst["instId"].split("-")[:2])
                symbols.add(base)
        return symbols
    except Exception as e:
        print(f"获取合约列表失败: {e}")
        return set()

def calculate_rsi(symbol, interval="15m", limit=50):
    try:
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar={interval}&limit={limit}"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        if data["code"] != "0":
            return 50
        candles = data["data"]
        closes = [float(c[4]) for c in candles]
        if len(closes) < 20:
            return 50
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(diff if diff > 0 else 0)
            losses.append(abs(diff) if diff < 0 else 0)
        avg_gain = sum(gains[-14:]) / 14 if len(gains) >= 14 else sum(gains) / len(gains)
        avg_loss = sum(losses[-14:]) / 14 if len(losses) >= 14 else sum(losses) / len(losses)
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    except:
        return 50

def get_funding_rate(symbol):
    try:
        swap_symbol = symbol.replace("-USDT", "-USDT-SWAP")
        url = f"https://www.okx.com/api/v5/public/funding-rate?instId={swap_symbol}"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        if data["code"] == "0" and data["data"]:
            return float(data["data"][0]["fundingRate"])
        return 0.0
    except:
        return 0.0

def get_ema(symbol, period=50):
    try:
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar=4H&limit=100"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        if data["code"] != "0":
            return 0
        candles = data["data"]
        closes = [float(c[4]) for c in candles]
        if len(closes) < period:
            return 0
        ema = sum(closes[-period:]) / period
        return ema
    except:
        return 0

def get_atr_percent(symbol, period=14):
    try:
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar=1h&limit=50"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        if data["code"] != "0":
            return 0.5
        candles = data["data"]
        if len(candles) < period:
            return 0.5
        tr_list = []
        for i in range(1, len(candles)):
            high = float(candles[i][2])
            low = float(candles[i][3])
            prev_close = float(candles[i-1][4])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
        atr = sum(tr_list[-period:]) / period
        return atr / float(candles[-1][4]) * 100
    except:
        return 0.5

def calculate_position(current_price, atr_pct, score, account_balance=None, leverage=None):
    if account_balance is None:
        account_balance = get_usdt_balance()
    if leverage is None:
        leverage = current_leverage
    if atr_pct <= 0:
        atr_pct = 0.5
    stop_loss_pct = atr_pct * ATR_STOP_MULTIPLIER
    risk_amount = account_balance * RISK_PER_TRADE
    adjusted_risk = risk_amount * (0.5 + (score / 100) * 0.5)
    contract_size = (adjusted_risk * leverage) / (current_price * (stop_loss_pct / 100))
    return max(1, int(contract_size))

def get_session_score():
    hour = datetime.now(timezone.utc).hour
    if 12 <= hour <= 18:
        return 5
    elif 22 <= hour or hour <= 2:
        return 3
    elif 6 <= hour <= 10:
        return -5
    return 0

# ==================== 乖离率计算（v3.5 核心新增） ====================
def get_bias_score(current_price, ema50, signal_type):
    """
    计算乖离率评分，避免追高/追低
    返回 (分数, 偏离百分比, 描述)
    """
    if ema50 == 0:
        return 0, 0, "无EMA数据"
    bias = (current_price - ema50) / ema50 * 100
    
    if signal_type == "LONG":
        if bias < BIAS_SAFE:
            return WEIGHT_BIAS, bias, f"低位启动 ({bias:+.1f}%)"
        elif bias < BIAS_WARN:
            return WEIGHT_BIAS // 2, bias, f"正常范围 ({bias:+.1f}%)"
        elif bias < BIAS_DANGER:
            return -8, bias, f"偏高 ({bias:+.1f}%) 追高风险"
        else:
            return -15, bias, f"严重追高 ({bias:+.1f}%) 接盘风险"
    else:  # SHORT
        if bias > -BIAS_SAFE:
            return WEIGHT_BIAS, bias, f"低位启动 ({bias:+.1f}%)"
        elif bias > -BIAS_WARN:
            return WEIGHT_BIAS // 2, bias, f"正常范围 ({bias:+.1f}%)"
        elif bias > -BIAS_DANGER:
            return -8, bias, f"偏低 ({bias:+.1f}%) 追空风险"
        else:
            return -15, bias, f"严重追空 ({bias:+.1f}%) 接盘风险"

# ==================== 趋势判定 ====================
def get_coin_trend(symbol, current_price):
    ema20 = get_ema(symbol, 20)
    ema50 = get_ema(symbol, 50)
    ema100 = get_ema(symbol, 100)
    if ema20 == 0 or ema50 == 0 or ema100 == 0:
        return "NEUTRAL", 0, 0
    # 多头排列
    if ema20 > ema50 > ema100:
        ema_state = "UP"
    elif ema20 < ema50 < ema100:
        ema_state = "DOWN"
    else:
        ema_state = "NEUTRAL"
    # 价格相对EMA50的位置
    price_vs_ema50 = current_price / ema50
    if price_vs_ema50 > 1.02:
        price_state = "UP"
    elif price_vs_ema50 < 0.98:
        price_state = "DOWN"
    else:
        price_state = "NEUTRAL"
    # 综合判定
    if ema_state == "UP" and (price_state == "UP" or price_state == "NEUTRAL"):
        return "UP", WEIGHT_TREND, ema50
    elif ema_state == "DOWN" and (price_state == "DOWN" or price_state == "NEUTRAL"):
        return "DOWN", WEIGHT_TREND, ema50
    else:
        if price_state == "UP":
            return "UP", WEIGHT_TREND // 2, ema50
        elif price_state == "DOWN":
            return "DOWN", WEIGHT_TREND // 2, ema50
        else:
            return "NEUTRAL", 0, ema50

# ==================== 背离差计算 ====================
def get_divergence_score(alt_change, btc_change):
    diff = alt_change - btc_change
    score = min(WEIGHT_DIVERGENCE, max(0, abs(diff) * 8))
    return score, diff

# ==================== 评分引擎（6个核心因子） ====================
def analyze_signal(symbol, current_price, volume, alt_change, btc_change):
    # ---- 1. 趋势判定 ----
    trend_dir, trend_score, ema50 = get_coin_trend(symbol, current_price)
    if trend_dir == "NEUTRAL":
        return None, 0, "趋势不明朗"

    # ---- 2. 背离差 ----
    div_score, diff = get_divergence_score(alt_change, btc_change)
    if div_score < 5:
        return None, 0, "背离差不足"

    # 确定信号方向
    if (trend_dir == "UP" and diff > 0) or (trend_dir == "DOWN" and diff < 0):
        signal_type = "LONG" if trend_dir == "UP" else "SHORT"
        direction_bonus = 10
    else:
        signal_type = "LONG" if diff > 0 else "SHORT"
        direction_bonus = -10

    # ---- 3. 成交量 ----
    vol_score = WEIGHT_VOLUME if volume > VOLUME_THRESHOLD else 0

    # ---- 4. RSI ----
    rsi = calculate_rsi(symbol)
    if signal_type == "LONG" and rsi < RSI_OVERSOLD:
        rsi_score = WEIGHT_RSI
    elif signal_type == "SHORT" and rsi > RSI_OVERBOUGHT:
        rsi_score = WEIGHT_RSI
    elif signal_type == "LONG" and rsi > RSI_OVERBOUGHT:
        rsi_score = -WEIGHT_RSI // 2
    elif signal_type == "SHORT" and rsi < RSI_OVERSOLD:
        rsi_score = -WEIGHT_RSI // 2
    else:
        rsi_score = 0

    # ---- 5. 资金费率 ----
    funding = get_funding_rate(symbol)
    if signal_type == "LONG":
        if funding < -0.005:
            funding_score = WEIGHT_FUNDING
        elif funding > 0.01:
            funding_score = -WEIGHT_FUNDING
        else:
            funding_score = 0
    else:
        if funding > 0.005:
            funding_score = WEIGHT_FUNDING
        elif funding < -0.01:
            funding_score = -WEIGHT_FUNDING
        else:
            funding_score = 0

    # ---- 6. 乖离率（v3.5 核心新增） ----
    bias_score, bias_pct, bias_desc = get_bias_score(current_price, ema50, signal_type)
    
    # ---- 综合评分 ----
    base_score = trend_score + div_score + vol_score + rsi_score + funding_score + bias_score + direction_bonus
    session_score = get_session_score()
    total_score = max(0, min(100, base_score + session_score))

    # 构造详情
    details = [
        f"趋势: {trend_dir} ({trend_score}分)",
        f"背离差: {diff:+.2f}% ({div_score:.0f}分)",
        f"成交量: {'达标' if vol_score>0 else '一般'} ({vol_score}分)",
        f"RSI: {rsi:.0f} ({rsi_score:+d}分)",
        f"费率: {funding*100:.3f}% ({funding_score:+d}分)",
        f"乖离率: {bias_pct:+.1f}% ({bias_score:+d}分) {bias_desc}"
    ]
    if direction_bonus != 0:
        details.append(f"方向一致性: {direction_bonus:+d}分")
    details = " | ".join(details)

    return signal_type, total_score, details

# ==================== 信号冷却检查 ====================
def check_cooldown(symbol, signal_type):
    now = time.time()
    if symbol in last_alert_time:
        last = last_alert_time[symbol]
        if last["direction"] == signal_type and (now - last["timestamp"]) < COOLDOWN_HOURS * 3600:
            return True
    return False

def update_cooldown(symbol, signal_type):
    last_alert_time[symbol] = {"timestamp": time.time(), "direction": signal_type}

# ---- Part 1 结束，请继续复制 Part 2 ----

# ==================== 核心检测 ====================
def check_divergence():
    btc = price_data[BTC_SYMBOL]
    btc_change = btc["change"]
    btc_price = btc["price"]
    alerts = []
    now = time.time()

    swap_symbols = get_swap_symbols()

    for sym in list(alt_symbols):
        alt = price_data.get(sym)
        if not alt or alt["price"] == 0:
            continue
        alt_change = alt["change"]
        alt_price = alt["price"]
        volume = alt.get("volume", 0)

        signal_type, score, details = analyze_signal(sym, alt_price, volume, alt_change, btc_change)
        if not signal_type:
            continue

        if check_cooldown(sym, signal_type):
            continue

        if score >= SCORE_THRESHOLD_HIGH:
            level = "高置信度"
            emoji = "🟢" if signal_type == "LONG" else "🔴"
        elif score >= SCORE_THRESHOLD_MEDIUM:
            level = "中等置信度"
            emoji = "🟡" if signal_type == "LONG" else "🟠"
        else:
            continue

        update_cooldown(sym, signal_type)

        action = "做多" if signal_type == "LONG" else "做空"
        inst_id = sym
        okx_url = f"https://www.okx.com/zh-hans/trade-swap/{inst_id.lower()}" if inst_id in swap_symbols else f"https://www.okx.com/zh-hans/markets/spot/{inst_id.lower()}"

        trend_dir, _, ema50 = get_coin_trend(sym, alt_price)
        trend_emoji = {"UP": "🟢", "DOWN": "🔴", "NEUTRAL": "⚪"}.get(trend_dir, "⚪")
        trend_label = {"UP": "上升", "DOWN": "下降", "NEUTRAL": "横盘"}.get(trend_dir, "横盘")

        atr_pct = get_atr_percent(sym)
        if signal_type == "LONG":
            sl = alt_price * (1 - atr_pct * ATR_STOP_MULTIPLIER / 100)
            tp = alt_price * (1 + atr_pct * ATR_TAKE_MULTIPLIER / 100)
            rr = (tp - alt_price) / (alt_price - sl) if (alt_price - sl) > 0 else 0
        else:
            sl = alt_price * (1 + atr_pct * ATR_STOP_MULTIPLIER / 100)
            tp = alt_price * (1 - atr_pct * ATR_TAKE_MULTIPLIER / 100)
            rr = (alt_price - tp) / (sl - alt_price) if (sl - alt_price) > 0 else 0

        position_size = calculate_position(alt_price, atr_pct, score)

        alert_text = (
            f"{emoji} 【{level}·{action}】[{sym}]({okx_url})\n"
            f"评分: {score:.1f}/100\n"
            f"趋势: {trend_emoji} {trend_label}\n"
            f"价格: ${alt_price:.4f}\n"
            f"📊 {details}\n"
            f"止损: ${sl:.4f} | 止盈: ${tp:.4f} | 盈亏比: {rr:.1f}:1\n"
            f"建议仓位: {position_size} 张 (杠杆{current_leverage}x)"
        )

        alerts.append(alert_text)

    if alerts:
        header = f"📊 BTC: ${btc_price:.2f} | 24h: {btc_change:+.2f}%\n" + "="*30 + "\n"
        full_msg = header + "\n\n".join(alerts)
        send_telegram(full_msg, parse_mode='Markdown')

        with PENDING_LOCK:
            for a in alerts:
                import re
                match = re.search(r'【.*?】([A-Z]+-[A-Z]+)', a)
                if match:
                    sym = match.group(1)
                    price_match = re.search(r'价格: \$([\d.]+)', a)
                    PENDING_SIGNALS.append({
                        "symbol": sym,
                        "signal_type": "LONG" if "做多" in a else "SHORT",
                        "price": float(price_match.group(1)) if price_match else 0,
                        "timestamp": time.time(),
                        "verified": False,
                        "status": "pending"
                    })

# ==================== 验证循环 ====================
def verify_loop():
    while True:
        time.sleep(60)
        now = time.time()
        with PENDING_LOCK:
            to_remove = []
            for i, signal in enumerate(PENDING_SIGNALS):
                if signal["verified"]:
                    continue
                elapsed = (now - signal["timestamp"]) / 60
                if elapsed >= VERIFY_MINUTES:
                    sym = signal["symbol"]
                    current_data = price_data.get(sym)
                    if current_data is None or current_data["price"] == 0:
                        try:
                            url = f"https://www.okx.com/api/v5/market/ticker?instId={sym}"
                            resp = requests.get(url, timeout=5)
                            data = resp.json()
                            if data["code"] == "0" and data["data"]:
                                current_price = float(data["data"][0]["last"])
                            else:
                                current_price = None
                        except:
                            current_price = None
                    else:
                        current_price = current_data["price"]

                    if current_price is not None:
                        success = False
                        change_pct = 0.0
                        if signal["signal_type"] == "LONG":
                            change_pct = (current_price - signal["price"]) / signal["price"] * 100
                            if change_pct > VERIFY_PRICE_CHANGE_PCT:
                                success = True
                        else:
                            change_pct = (signal["price"] - current_price) / signal["price"] * 100
                            if change_pct > VERIFY_PRICE_CHANGE_PCT:
                                success = True

                        action = "做多" if signal["signal_type"] == "LONG" else "做空"
                        if success:
                            signal["status"] = "success"
                            msg = (f"✅ 上一单【{action}】{signal['symbol']} 验证成功！\n"
                                   f"推送价: ${signal['price']:.4f} → 现价: ${current_price:.4f}\n"
                                   f"变动: {change_pct:+.2f}%")
                            send_telegram(msg)
                            with STATS_LOCK:
                                VERIFY_STATS["total"] += 1
                                VERIFY_STATS["success"] += 1
                        else:
                            signal["status"] = "failed"
                            with STATS_LOCK:
                                VERIFY_STATS["total"] += 1
                                VERIFY_STATS["failed"] += 1
                    else:
                        signal["status"] = "expired"
                        with STATS_LOCK:
                            VERIFY_STATS["total"] += 1
                            VERIFY_STATS["expired"] += 1

                    signal["verified"] = True
                    to_remove.append(i)

            for idx in sorted(to_remove, reverse=True):
                PENDING_SIGNALS.pop(idx)

# ==================== WebSocket ====================
def restart_websocket():
    global ws_instance, restart_flag
    with ws_lock:
        restart_flag = True
        if ws_instance:
            ws_instance.close()
    time.sleep(1)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if "data" not in data:
            return
        for item in data["data"]:
            inst_id = item.get("instId", "")
            if inst_id not in price_data:
                continue
            last = float(item.get("last", 0))
            open24h = float(item.get("open24h", 0))
            if open24h != 0:
                change = (last - open24h) / open24h * 100
            else:
                change = 0.0
            volume = float(item.get("volCcy24h", 0))
            price_data[inst_id]["price"] = last
            price_data[inst_id]["change"] = change
            price_data[inst_id]["volume"] = volume
        check_divergence()
    except Exception as e:
        print(f"解析错误: {e}")

def on_error(ws, error):
    print(f"⚠️ WebSocket错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 连接断开，5秒后重连...")
    time.sleep(5)
    start_ws()

def on_open(ws):
    global ws_instance, restart_flag
    with ws_lock:
        ws_instance = ws
        restart_flag = False
    all_symbols = [BTC_SYMBOL] + list(alt_symbols)
    print(f"✅ WebSocket已连接，监控 {len(all_symbols)} 个币种")
    batch_size = 50
    total_batches = (len(all_symbols) + batch_size - 1) // batch_size
    for i in range(0, len(all_symbols), batch_size):
        batch = all_symbols[i:i+batch_size]
        args = [{"channel": "tickers", "instId": sym} for sym in batch]
        sub_msg = {"op": "subscribe", "args": args}
        ws.send(json.dumps(sub_msg))
        print(f"📨 已发送订阅批次 {i//batch_size + 1}/{total_batches}，共 {len(batch)} 个币种")
        time.sleep(0.5)

def start_ws():
    while True:
        ws = websocket.WebSocketApp(
            "wss://ws.okx.com:8443/ws/v5/public",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
        time.sleep(5)

# ==================== 币种管理 ====================
def add_symbol(symbol):
    if symbol == BTC_SYMBOL or symbol in alt_symbols:
        return False
    alt_symbols.add(symbol)
    price_data[symbol] = {"price": 0, "change": 0, "volume": 0}
    restart_websocket()
    return True

def remove_symbol(symbol):
    if symbol == BTC_SYMBOL or symbol not in alt_symbols:
        return False
    alt_symbols.remove(symbol)
    price_data.pop(symbol, None)
    restart_websocket()
    return True

def clear_alts():
    if not alt_symbols:
        return False
    for sym in list(alt_symbols):
        alt_symbols.remove(sym)
        price_data.pop(sym, None)
    restart_websocket()
    return True

def add_top_n(n):
    try:
        swap_symbols = get_swap_symbols()
        if not swap_symbols:
            print("⚠️ 未获取到合约列表，将使用全部现货")
        resp = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SPOT", timeout=10)
        data = resp.json()
        if data["code"] != "0":
            return []
        tickers = data["data"]
        usdt_tickers = []
        for t in tickers:
            inst_id = t["instId"]
            if inst_id.endswith("-USDT") and inst_id != BTC_SYMBOL:
                if swap_symbols and inst_id not in swap_symbols:
                    continue
                usdt_tickers.append(t)
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("volCcy24h", 0)), reverse=True)
        top = [t["instId"] for t in sorted_tickers[:n]]
        added = []
        for sym in top:
            if sym not in alt_symbols:
                alt_symbols.add(sym)
                price_data[sym] = {"price": 0, "change": 0, "volume": 0}
                added.append(sym)
        if added:
            restart_websocket()
        return added
    except Exception as e:
        print(f"获取交易量排行失败: {e}")
        return []

def auto_scan_new_coins():
    known_symbols = set(alt_symbols)
    while True:
        try:
            swap_symbols = get_swap_symbols()
            resp = requests.get("https://www.okx.com/api/v5/public/instruments?instType=SPOT", timeout=10)
            data = resp.json()
            if data["code"] == "0":
                current_usdt = [inst["instId"] for inst in data["data"] if inst["quoteCcy"] == "USDT" and inst["instId"] != BTC_SYMBOL]
                new_coins = []
                for sym in current_usdt:
                    if sym not in known_symbols and (not swap_symbols or sym in swap_symbols):
                        new_coins.append(sym)
                if new_coins:
                    for sym in new_coins:
                        if sym not in alt_symbols:
                            alt_symbols.add(sym)
                            price_data[sym] = {"price": 0, "change": 0, "volume": 0}
                    known_symbols.update(new_coins)
                    restart_websocket()
                    send_telegram(f"🆕 自动发现新合约币种并已添加监控: {', '.join(new_coins)}")
        except Exception as e:
            print(f"自动扫描出错: {e}")
        time.sleep(3600)

def auto_filter_coins():
    global alt_symbols, price_data
    while True:
        time.sleep(FILTER_INTERVAL)
        if not AUTO_FILTER_ENABLED:
            continue
        try:
            swap_symbols = get_swap_symbols()
            if not swap_symbols:
                print("⚠️ 未获取到合约列表，跳过本次过滤")
                continue
            resp = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SPOT", timeout=10)
            data = resp.json()
            if data["code"] != "0":
                continue
            tickers = data["data"]
            usdt_tickers = []
            for t in tickers:
                inst_id = t["instId"]
                if inst_id.endswith("-USDT") and inst_id != BTC_SYMBOL:
                    if inst_id in swap_symbols:
                        usdt_tickers.append(t)
            sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get("volCcy24h", 0)), reverse=True)
            top_symbols = [t["instId"] for t in sorted_tickers[:MAX_COINS]]
            current_set = set(alt_symbols)
            target_set = set(top_symbols)
            to_add = target_set - current_set
            to_remove = current_set - target_set - {BTC_SYMBOL}
            if to_add or to_remove:
                for sym in to_remove:
                    alt_symbols.remove(sym)
                    price_data.pop(sym, None)
                for sym in to_add:
                    alt_symbols.add(sym)
                    price_data[sym] = {"price": 0, "change": 0, "volume": 0}
                restart_websocket()
                msg = (
                    f"🔄 自动过滤已更新监控列表\n"
                    f"保留成交额前 {MAX_COINS} 的合约币种\n"
                    f"添加: {', '.join(list(to_add)[:5])}" + ("..." if len(to_add)>5 else "") + "\n"
                    f"移除: {', '.join(list(to_remove)[:5])}" + ("..." if len(to_remove)>5 else "")
                )
                send_telegram(msg)
        except Exception as e:
            print(f"自动过滤出错: {e}")

# ==================== 波动扫描 ====================
def volatility_scanner():
    cache = {}
    while True:
        time.sleep(VOLATILITY_SCAN_INTERVAL)
        if not VOLATILITY_SCAN_ENABLED:
            continue
        try:
            resp = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SPOT", timeout=10)
            data = resp.json()
            if data["code"] != "0":
                continue
            alerts = []
            for item in data["data"]:
                symbol = item["instId"]
                if not symbol.endswith("-USDT") or symbol == BTC_SYMBOL:
                    continue
                current_price = float(item["last"])
                if symbol in alt_symbols:
                    continue
                if symbol in cache:
                    prev_price = cache[symbol]
                    price_change_pct = (current_price - prev_price) / prev_price * 100
                    if abs(price_change_pct) >= VOLATILITY_THRESHOLD:
                        alerts.append({
                            'symbol': symbol,
                            'price': current_price,
                            'change': price_change_pct,
                            'type': '🚀 暴涨' if price_change_pct > 0 else '💥 暴跌'
                        })
                        if symbol not in alt_symbols:
                            alt_symbols.add(symbol)
                            price_data[symbol] = {"price": current_price, "change": 0, "volume": 0}
                cache[symbol] = current_price
            if alerts:
                msg = "🚨 **突发异动警报（小币种）**\n"
                for a in alerts[:5]:
                    msg += f"{a['symbol']} {a['type']} {a['change']:+.2f}% | 现价: ${a['price']:.4f}\n"
                msg += "\n✅ 已自动加入监控列表，将持续追踪。"
                send_telegram(msg)
                restart_websocket()
        except Exception as e:
            print(f"波动扫描出错: {e}")

# ==================== 汇总 ====================
def generate_summary():
    btc = price_data[BTC_SYMBOL]
    btc_change = btc["change"]
    btc_price = btc["price"]
    items = []
    for sym in list(alt_symbols):
        alt = price_data.get(sym)
        if not alt or alt["price"] == 0:
            continue
        diff = alt["change"] - btc_change
        items.append({"symbol": sym, "price": alt["price"], "change": alt["change"], "diff": diff})
    items.sort(key=lambda x: x["diff"], reverse=True)
    long_candidates = [i for i in items if i["diff"] > SUMMARY_MIN_DIFF]
    short_candidates = [i for i in items if i["diff"] < -SUMMARY_MIN_DIFF]
    neutral = [i for i in items if abs(i["diff"]) <= SUMMARY_MIN_DIFF]
    header = f"📊 BTC: ${btc_price:.2f} | 24h: {btc_change:+.2f}%\n监控: {len(items)}个山寨币\n" + "="*30 + "\n"
    lines = []
    if long_candidates:
        lines.append("🟢 做多候选（强于BTC）:")
        for i in long_candidates[:20]:
            lines.append(f"  {i['symbol']} +{i['change']:.2f}% (背离 +{i['diff']:.2f}%)")
        if len(long_candidates) > 20:
            lines.append(f"  ... 还有 {len(long_candidates)-20} 个")
    if short_candidates:
        lines.append("\n🔴 做空候选（弱于BTC）:")
        for i in short_candidates[:20]:
            lines.append(f"  {i['symbol']} {i['change']:.2f}% (背离 {i['diff']:.2f}%)")
        if len(short_candidates) > 20:
            lines.append(f"  ... 还有 {len(short_candidates)-20} 个")
    if neutral:
        lines.append(f"\n⚪ 中性（背离≤±{SUMMARY_MIN_DIFF}%）: {len(neutral)} 个")
    return header + "\n".join(lines)

# ==================== 自动刷新 ====================
def auto_refresh_job():
    global auto_refresh_timer
    if auto_refresh_enabled:
        summary = generate_summary()
        send_telegram(summary)
        auto_refresh_timer = threading.Timer(auto_refresh_interval, auto_refresh_job)
        auto_refresh_timer.daemon = True
        auto_refresh_timer.start()

def start_auto_refresh():
    global auto_refresh_enabled, auto_refresh_timer
    if auto_refresh_timer:
        auto_refresh_timer.cancel()
    auto_refresh_enabled = True
    auto_refresh_timer = threading.Timer(auto_refresh_interval, auto_refresh_job)
    auto_refresh_timer.daemon = True
    auto_refresh_timer.start()

def stop_auto_refresh():
    global auto_refresh_enabled, auto_refresh_timer
    auto_refresh_enabled = False
    if auto_refresh_timer:
        auto_refresh_timer.cancel()
        auto_refresh_timer = None

# ==================== Telegram 命令 ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    await update.message.reply_text(
        f"🤖 **合约胜率增强Bot v{VERSION}**\n"
        "核心逻辑：趋势 + 背离 + 成交量 + RSI + 费率 + 乖离率\n"
        "⚡ 乖离率自动扣分，避免追高接盘\n\n"
        "📊 **命令**：\n"
        "/status – 状态 & 参数\n"
        "/summary – 强弱汇总\n"
        "/autorefresh on/off – 自动刷新\n"
        "/addcoin / addtop – 管理币种\n"
        "/setthreshold <0~100> – 设置评分推送阈值\n"
        "/setleverage <倍数> – 设置杠杆\n"
        "/debug – BTC数据\n"
        "/refreshbalance – 强制刷新余额\n"
        "/help – 此帮助",
        reply_markup=get_main_keyboard(),
        parse_mode='Markdown'
    )

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    count = len(alt_symbols)
    status_auto = "🟢 开启" if auto_refresh_enabled else "🔴 关闭"
    interval_min = auto_refresh_interval // 60
    pending_count = len(PENDING_SIGNALS)
    with STATS_LOCK:
        total = VERIFY_STATS["total"]
        success = VERIFY_STATS["success"]
        failed = VERIFY_STATS["failed"]
        expired = VERIFY_STATS["expired"]
        success_rate = f"{success/total*100:.1f}%" if total > 0 else "N/A"
    balance = get_usdt_balance()
    msg = (
        f"📋 监控: {count} 个山寨币（仅合约）\n"
        f"自动刷新: {status_auto}"
        f"{f' (间隔 {interval_min} 分钟)' if auto_refresh_enabled else ''}\n"
        f"待验证信号: {pending_count} 个\n"
        f"💰 USDT余额: ${balance:,.2f}\n"
        f"⚡ 当前杠杆: {current_leverage}x\n"
        f"🎯 评分推送阈值: {MIN_SCORE_THRESHOLD}\n"
        f"📊 近24小时验证统计:\n"
        f"  总数: {total} | ✅成功: {success} | ❌失败: {failed} | ⏳失效: {expired}\n"
        f"  成功率: {success_rate}\n"
        f"⏰ 信号冷却: {COOLDOWN_HOURS}小时 (同币同向)\n"
        f"📈 乖离率扣分: 偏离EMA50 > {BIAS_DANGER}% 扣大分"
    )
    msg += "\n\n列表: " + ", ".join(list(alt_symbols)[:15])
    if count > 15:
        msg += f" ... 共{count}个"
    await update.message.reply_text(msg, reply_markup=get_main_keyboard(), parse_mode='Markdown')

async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    msg = generate_summary()
    if len(msg) > 4000:
        for i in range(0, len(msg), 4000):
            await update.message.reply_text(msg[i:i+4000], reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text(msg, reply_markup=get_main_keyboard())

async def autorefresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global auto_refresh_interval
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text("用法: /autorefresh on/off/分钟数", reply_markup=get_main_keyboard())
        return
    arg = context.args[0].lower()
    if arg == "on":
        if not auto_refresh_enabled:
            start_auto_refresh()
            await update.message.reply_text(f"✅ 自动刷新已开启，间隔 {auto_refresh_interval//60} 分钟", reply_markup=get_main_keyboard())
        else:
            await update.message.reply_text("ℹ️ 已开启", reply_markup=get_main_keyboard())
    elif arg == "off":
        if auto_refresh_enabled:
            stop_auto_refresh()
            await update.message.reply_text("🔴 已关闭", reply_markup=get_main_keyboard())
        else:
            await update.message.reply_text("ℹ️ 已关闭", reply_markup=get_main_keyboard())
    elif arg.isdigit():
        minutes = int(arg)
        if minutes < 1 or minutes > 60:
            await update.message.reply_text("⚠️ 间隔 1~60 分钟", reply_markup=get_main_keyboard())
            return
        auto_refresh_interval = minutes * 60
        if auto_refresh_enabled:
            stop_auto_refresh()
            start_auto_refresh()
        else:
            start_auto_refresh()
        await update.message.reply_text(f"✅ 间隔设为{minutes}分钟，已开启", reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text("参数错误", reply_markup=get_main_keyboard())

async def addcoin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text("用法: /addcoin PEPE-USDT", reply_markup=get_main_keyboard())
        return
    sym = context.args[0].upper()
    if not sym.endswith("-USDT"):
        await update.message.reply_text("格式错误，需为 币种-USDT", reply_markup=get_main_keyboard())
        return
    if add_symbol(sym):
        await update.message.reply_text(f"✅ 已添加 {sym}", reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text(f"⚠️ {sym} 已存在或无效", reply_markup=get_main_keyboard())

async def addtop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    n = 200
    if context.args and context.args[0].isdigit():
        n = int(context.args[0])
    if n > 300:
        await update.message.reply_text("⚠️ 数量过大，建议≤300", reply_markup=get_main_keyboard())
        n = 300
    await update.message.reply_text(f"⏳ 获取成交额前{n}的合约币种...", reply_markup=get_main_keyboard())
    added = add_top_n(n)
    if added:
        await update.message.reply_text(f"✅ 添加 {len(added)} 个合约币种: {', '.join(added[:10])}" + ("..." if len(added)>10 else ""), reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text("⚠️ 无新币种", reply_markup=get_main_keyboard())

async def removecoin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text("用法: /removecoin ETH-USDT", reply_markup=get_main_keyboard())
        return
    sym = context.args[0].upper()
    if remove_symbol(sym):
        await update.message.reply_text(f"✅ 已移除 {sym}", reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text(f"⚠️ {sym} 不存在或为主币", reply_markup=get_main_keyboard())

async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if clear_alts():
        await update.message.reply_text("✅ 已清空所有山寨币，仅保留BTC", reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text("⚠️ 列表已空", reply_markup=get_main_keyboard())

async def setthreshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MIN_SCORE_THRESHOLD
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text(
            f"📊 当前评分推送阈值: {MIN_SCORE_THRESHOLD}\n"
            "用法: /setthreshold <数值>（如 /setthreshold 60）\n"
            "范围: 0~100，低于阈值不推送信号",
            reply_markup=get_main_keyboard()
        )
        return
    try:
        val = int(context.args[0])
        if val < 0 or val > 100:
            await update.message.reply_text("⚠️ 阈值范围应在 0~100 之间", reply_markup=get_main_keyboard())
            return
        MIN_SCORE_THRESHOLD = val
        await update.message.reply_text(
            f"✅ 评分推送阈值已更新为 {MIN_SCORE_THRESHOLD}\n"
            f"（评分 >= {MIN_SCORE_THRESHOLD} 的信号才会推送）",
            reply_markup=get_main_keyboard()
        )
    except ValueError:
        await update.message.reply_text("⚠️ 请输入有效整数，如 /setthreshold 60", reply_markup=get_main_keyboard())

async def setleverage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_leverage
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text(
            f"📊 当前杠杆: {current_leverage}x\n"
            "用法: /setleverage <倍数>（如 /setleverage 5）",
            reply_markup=get_main_keyboard()
        )
        return
    try:
        lev = int(context.args[0])
        if lev < 1 or lev > 100:
            await update.message.reply_text("⚠️ 杠杆范围 1~100", reply_markup=get_main_keyboard())
            return
        current_leverage = lev
        await update.message.reply_text(f"✅ 杠杆已设置为 {current_leverage}x", reply_markup=get_main_keyboard())
    except ValueError:
        await update.message.reply_text("⚠️ 请输入有效整数", reply_markup=get_main_keyboard())

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    btc = price_data.get(BTC_SYMBOL)
    if btc:
        balance = get_usdt_balance()
        msg = (f"🔍 **BTC 当前数据**\n"
               f"价格: ${btc['price']:.2f}\n"
               f"24h涨跌幅: {btc['change']:.2f}%\n"
               f"成交额: ${btc['volume']:.0f}\n"
               f"监控币种总数: {len(alt_symbols)}\n"
               f"💰 USDT余额: ${balance:,.2f}\n"
               f"⚡ 当前杠杆: {current_leverage}x\n"
               f"🎯 评分推送阈值: {MIN_SCORE_THRESHOLD}\n"
               f"⏰ 信号冷却: {COOLDOWN_HOURS}小时\n"
               f"📈 乖离率扣分阈值: >{BIAS_DANGER}% 扣大分")
    else:
        msg = "❌ 未获取到BTC数据"
    await update.message.reply_text(msg, reply_markup=get_main_keyboard(), parse_mode='Markdown')

async def refreshbalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    await update.message.reply_text("🔄 正在刷新 USDT 余额...", reply_markup=get_main_keyboard())
    balance = refresh_balance_cache()
    await update.message.reply_text(f"✅ 余额已刷新: ${balance:,.2f}", reply_markup=get_main_keyboard())

# ==================== Telegram Bot ====================
def run_telegram_bot():
    try:
        print("🤖 正在尝试启动 Telegram Bot...")
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            print("❌ Token 或 Chat ID 为空，无法启动 Bot")
            return
        app = Application.builder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help))
        app.add_handler(CommandHandler("status", status))
        app.add_handler(CommandHandler("summary", summary))
        app.add_handler(CommandHandler("autorefresh", autorefresh))
        app.add_handler(CommandHandler("addcoin", addcoin))
        app.add_handler(CommandHandler("addtop", addtop))
        app.add_handler(CommandHandler("removecoin", removecoin))
        app.add_handler(CommandHandler("clear", clear))
        app.add_handler(CommandHandler("setthreshold", setthreshold))
        app.add_handler(CommandHandler("setleverage", setleverage))
        app.add_handler(CommandHandler("debug", debug))
        app.add_handler(CommandHandler("refreshbalance", refreshbalance))
        print("🤖 Telegram Bot 正在运行")
        app.run_polling(drop_pending_updates=True)
    except Exception as e:
        print(f"❌ Telegram Bot 启动失败: {e}")
        import traceback
        traceback.print_exc()

# ==================== Flask ====================
flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "Monitoring running", 200

def run_http():
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 HTTP 心跳服务正在启动，端口: {port}...")
    flask_app.run(host='0.0.0.0', port=port, debug=False)

# ==================== 启动通知 ====================
def send_startup_notification():
    time.sleep(8)
    balance = get_usdt_balance()
    msg = (
        f"🚀 **Bot 已重新启动！**\n"
        f"版本: {VERSION}\n"
        f"启动时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"监控币种: {len(alt_symbols)} 个合约币种\n"
        f"💰 USDT余额: ${balance:,.2f}\n"
        f"⚡ 当前杠杆: {current_leverage}x\n"
        f"🎯 评分推送阈值: {MIN_SCORE_THRESHOLD}\n"
        f"⏰ 信号冷却: {COOLDOWN_HOURS}小时\n"
        f"📈 乖离率扣分: 偏离EMA50 > {BIAS_DANGER}% 扣大分\n"
        f"使用 /status 查看详情"
    )
    send_telegram(msg)

# ==================== 主程序 ====================
if __name__ == "__main__":
    print(f"🚀 合约胜率增强版 v{VERSION} 启动于 {datetime.now(timezone.utc)}")
    print(f"初始监控: {BTC_SYMBOL} + {', '.join(DEFAULT_ALT_SYMBOLS)}")
    print(f"默认杠杆: {DEFAULT_LEVERAGE}x")
    print(f"自动过滤: 保留前 {MAX_COINS} 名")
    print(f"评分推送阈值: {MIN_SCORE_THRESHOLD}")
    print(f"信号冷却: {COOLDOWN_HOURS}小时")
    print(f"乖离率扣分阈值: >{BIAS_DANGER}% 扣大分")

    threading.Thread(target=verify_loop, daemon=True).start()
    threading.Thread(target=auto_scan_new_coins, daemon=True).start()
    threading.Thread(target=auto_filter_coins, daemon=True).start()
    threading.Thread(target=volatility_scanner, daemon=True).start()
    threading.Thread(target=start_ws, daemon=True).start()

    http_thread = threading.Thread(target=run_http, daemon=True)
    http_thread.start()

    threading.Thread(target=send_startup_notification, daemon=True).start()

    print("🤖 正在主线程启动 Telegram Bot...")
    run_telegram_bot()