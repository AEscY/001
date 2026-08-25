import os
import websocket
import json
import requests
import time
import threading
from datetime import datetime
from flask import Flask
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== 版本信息 ====================
VERSION = "1.6.1"  # 修复：分批订阅 WebSocket，避免断开

# ==================== 配置区 ====================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ 请设置环境变量 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID")

BTC_SYMBOL = "BTC-USDT"
DEFAULT_ALT_SYMBOLS = ["ETH-USDT", "SOL-USDT", "BNB-USDT", "ADA-USDT", "DOGE-USDT", "XRP-USDT"]

# 基础参数（Z-Score 阈值替代固定背离差）
BTC_TREND_THRESHOLD = 0.3      # BTC 波动 >0.3% 才启用趋势模式
ZSCORE_LONG_THRESHOLD = 1.8    # Z-Score > 1.8 视为强势（做多）
ZSCORE_SHORT_THRESHOLD = -1.8  # Z-Score < -1.8 视为弱势（做空）
MIN_ZSCORE_SAMPLES = 20        # 最少样本数才启用 Z-Score，否则回退固定阈值
FALLBACK_LONG = 0.6            # 样本不足时的固定做多阈值
FALLBACK_SHORT = -0.6          # 样本不足时的固定做空阈值

# RSI / 费率 / 成交量参数（可调）
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
VOLUME_THRESHOLD = 1000000

ALERT_COOLDOWN = 120
SUMMARY_MIN_DIFF = 0.3

# ==================== 验证配置 ====================
VERIFY_MINUTES = 15
VERIFY_PRICE_CHANGE_PCT = 0.8
PENDING_SIGNALS = []
PENDING_LOCK = threading.Lock()
VERIFY_STATS = {"total": 0, "success": 0, "failed": 0, "expired": 0}
STATS_LOCK = threading.Lock()

# ==================== 自动过滤配置 ====================
AUTO_FILTER_ENABLED = True
MAX_COINS = 200
FILTER_INTERVAL = 1800

# ==================== 波动扫描配置 ====================
VOLATILITY_SCAN_ENABLED = True
VOLATILITY_THRESHOLD = 3.0
VOLATILITY_SCAN_INTERVAL = 60

# ==================== 独立行情监控配置 ====================
INDEPENDENT_MODE_ENABLED = True
INDEPENDENT_THRESHOLD = 3.0
INDEPENDENT_LOOKBACK = 15

# ==================== 全局状态 ====================
alt_symbols = set(DEFAULT_ALT_SYMBOLS)
price_data = {BTC_SYMBOL: {"price": 0, "change": 0, "volume": 0}}
for sym in alt_symbols:
    price_data[sym] = {"price": 0, "change": 0, "volume": 0}

# ---- 历史背离差存储（用于 Z-Score） ----
diff_history = {}  # {symbol: [diff1, diff2, ...]} 最多保留 100 个

last_alert_time = {}
ws_instance = None
ws_lock = threading.Lock()
restart_flag = False

auto_refresh_enabled = False
auto_refresh_interval = 300
auto_refresh_timer = None

# ==================== 菜单键盘 ====================
def get_main_keyboard():
    buttons = [
        [KeyboardButton("/status"), KeyboardButton("/summary")],
        [KeyboardButton("/autorefresh on"), KeyboardButton("/autorefresh off")],
        [KeyboardButton("/addcoin"), KeyboardButton("/addtop")],
        [KeyboardButton("/removecoin"), KeyboardButton("/clear")],
        [KeyboardButton("/setdiff"), KeyboardButton("/setrsi"), KeyboardButton("/setvol")],
        [KeyboardButton("/setvolatility"), KeyboardButton("/debug"), KeyboardButton("/help")]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)

# ==================== 推送函数（支持Markdown） ====================
def send_telegram(msg, parse_mode=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"推送失败: {e}")

# ==================== 1. 获取合约列表 ====================
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

# ==================== 2. RSI 计算 ====================
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

# ==================== 3. 资金费率 ====================
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

# ==================== 4. 技术指标辅助 ====================
def get_ema(symbol, period=50):
    """获取 EMA50 (4小时K线)"""
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
    """获取 ATR 百分比（用于盈亏比）"""
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

# ==================== 5. Z-Score 计算 ====================
def update_diff_history(symbol, current_diff, max_len=100):
    """更新某个币种的历史背离差列表"""
    if symbol not in diff_history:
        diff_history[symbol] = []
    history = diff_history[symbol]
    history.append(current_diff)
    if len(history) > max_len:
        history.pop(0)

def get_zscore(symbol, current_diff):
    """计算当前背离差的 Z-Score，若样本不足返回 None"""
    history = diff_history.get(symbol, [])
    if len(history) < MIN_ZSCORE_SAMPLES:
        return None
    # 计算均值和标准差
    mean = sum(history) / len(history)
    variance = sum((x - mean) ** 2 for x in history) / len(history)
    std = variance ** 0.5
    if std == 0:
        return 0
    return (current_diff - mean) / std

# ==================== 6. 多因子评分（核心升级版） ====================
def analyze_signal(symbol, diff, btc_change, alt_change, volume, current_price, use_independent=False):
    """
    新评分逻辑：
    - 趋势过滤（EMA50）加减分
    - Z-Score 统计显著性加分
    - RSI、资金费率、成交量因子
    - 盈亏比计算
    """
    if use_independent:
        if alt_change > 0:
            signal_type = "LONG"
        elif alt_change < 0:
            signal_type = "SHORT"
        else:
            return None, 0, ""
    else:
        # 趋势模式：使用 Z-Score 判定
        if abs(btc_change) < BTC_TREND_THRESHOLD:
            return None, 0, ""  # BTC 横盘，由独立模式处理
        
        zscore = get_zscore(symbol, diff)
        if zscore is None:
            # 样本不足，回退固定阈值
            if btc_change > 0 and diff > FALLBACK_LONG:
                signal_type = "LONG"
            elif btc_change < 0 and diff < FALLBACK_SHORT:
                signal_type = "SHORT"
            else:
                return None, 0, ""
        else:
            # Z-Score 判定
            if zscore > ZSCORE_LONG_THRESHOLD:
                signal_type = "LONG"
            elif zscore < ZSCORE_SHORT_THRESHOLD:
                signal_type = "SHORT"
            else:
                return None, 0, ""
    
    # ---- 开始评分 ----
    details = []
    score = 0
    
    # 基础分：根据背离差或独立波动
    if use_independent:
        base_score = min(50, 30 + abs(alt_change) * 8)
        details.append(f"独立波动 {alt_change:+.2f}% (基础分{base_score:.0f})")
    else:
        base_score = min(50, 30 + abs(diff) * 15)
        details.append(f"背离差 {diff:+.2f}% (基础分{base_score:.0f})")
    score += base_score
    
    # ---- 因子1: 趋势过滤（EMA50） ----
    ema50 = get_ema(symbol)
    if ema50 > 0:
        if signal_type == "LONG":
            if current_price > ema50 * 1.02:
                score += 15
                details.append(f"站上EMA50 (+15)")
            elif current_price < ema50 * 0.98:
                score -= 20
                details.append(f"跌破EMA50 (-20)")
            else:
                details.append(f"EMA50附近")
        else:  # SHORT
            if current_price < ema50 * 0.98:
                score += 15
                details.append(f"跌破EMA50 (+15)")
            elif current_price > ema50 * 1.02:
                score -= 20
                details.append(f"站上EMA50 (-20)")
            else:
                details.append(f"EMA50附近")
    
    # ---- 因子2: RSI ----
    rsi = calculate_rsi(symbol)
    if signal_type == "LONG":
        if rsi > RSI_OVERBOUGHT:
            score -= 30
            details.append(f"RSI={rsi:.0f}超买 (-30)")
        elif rsi < RSI_OVERSOLD:
            score += 20
            details.append(f"RSI={rsi:.0f}低位反弹 (+20)")
        else:
            details.append(f"RSI={rsi:.0f}中性")
    else:
        if rsi < RSI_OVERSOLD:
            score -= 30
            details.append(f"RSI={rsi:.0f}超卖 (-30)")
        elif rsi > RSI_OVERBOUGHT:
            score += 20
            details.append(f"RSI={rsi:.0f}高位回落 (+20)")
        else:
            details.append(f"RSI={rsi:.0f}中性")
    
    # ---- 因子3: 资金费率 ----
    funding = get_funding_rate(symbol)
    if signal_type == "LONG":
        if funding > 0.01:
            score -= 20
            details.append(f"费率{funding*100:.3f}%过高 (-20)")
        elif funding < -0.005:
            score += 15
            details.append(f"费率{funding*100:.3f}%空头拥挤 (+15)")
        else:
            details.append(f"费率{funding*100:.3f}%中性")
    else:
        if funding < -0.01:
            score -= 20
            details.append(f"费率{funding*100:.3f}%过低 (-20)")
        elif funding > 0.005:
            score += 15
            details.append(f"费率{funding*100:.3f}%多头拥挤 (+15)")
        else:
            details.append(f"费率{funding*100:.3f}%中性")
    
    # ---- 因子4: 成交量 ----
    if volume > VOLUME_THRESHOLD:
        score += 10
        details.append(f"成交额${volume/1000000:.1f}M (+10)")
    else:
        details.append(f"成交额${volume/1000000:.1f}M (一般)")
    
    # ---- 因子5: Z-Score 显著性加分（仅趋势模式） ----
    if not use_independent and abs(btc_change) >= BTC_TREND_THRESHOLD:
        z = get_zscore(symbol, diff)
        if z is not None:
            if z > 2.5:
                score += 15
                details.append(f"Z-Score={z:.2f} 极强 (+15)")
            elif z > 2.0:
                score += 10
                details.append(f"Z-Score={z:.2f} 强 (+10)")
            elif z > 1.8:
                score += 5
                details.append(f"Z-Score={z:.2f} (+5)")
            elif z < -2.5:
                score += 15
                details.append(f"Z-Score={z:.2f} 极弱 (+15)")
            elif z < -2.0:
                score += 10
                details.append(f"Z-Score={z:.2f} 弱 (+10)")
            elif z < -1.8:
                score += 5
                details.append(f"Z-Score={z:.2f} (+5)")
    
    # ---- 盈亏比计算 ----
    atr_pct = get_atr_percent(symbol)
    if signal_type == "LONG":
        sl = current_price * (1 - atr_pct * 1.5 / 100)
        tp = current_price * (1 + atr_pct * 2.5 / 100)
        rr = (tp - current_price) / (current_price - sl) if (current_price - sl) > 0 else 0
        details.append(f"止损 {sl:.4f} 止盈 {tp:.4f} 盈亏比 {rr:.1f}:1")
    else:
        sl = current_price * (1 + atr_pct * 1.5 / 100)
        tp = current_price * (1 - atr_pct * 2.5 / 100)
        rr = (current_price - tp) / (sl - current_price) if (sl - current_price) > 0 else 0
        details.append(f"止损 {sl:.4f} 止盈 {tp:.4f} 盈亏比 {rr:.1f}:1")
    
    final_score = max(0, min(100, score))
    return signal_type, final_score, " | ".join(details)

# ==================== 7. 背离检测（集成 Z-Score 历史） ====================
def check_divergence():
    btc = price_data[BTC_SYMBOL]
    btc_change = btc["change"]
    btc_price = btc["price"]
    alerts = []
    now = time.time()

    for sym in list(alt_symbols):
        alt = price_data.get(sym)
        if not alt or alt["price"] == 0:
            continue
        alt_change = alt["change"]
        alt_price = alt["price"]
        diff = alt_change - btc_change
        volume = alt.get("volume", 0)

        # 更新历史背离差
        update_diff_history(sym, diff)

        if sym in last_alert_time and (now - last_alert_time[sym]) < ALERT_COOLDOWN:
            continue

        if abs(btc_change) > BTC_TREND_THRESHOLD:
            signal_type, score, details = analyze_signal(sym, diff, btc_change, alt_change, volume, alt_price, use_independent=False)
        else:
            continue

        if signal_type and score >= 50:
            last_alert_time[sym] = now
            emoji = "🟢" if signal_type == "LONG" else "🔴"
            action = "做多" if signal_type == "LONG" else "做空"
            inst_id = sym
            okx_url = f"https://www.okx.com/zh-hans/markets/swap/{inst_id.lower()}"
            alert_text = (
                f"{emoji} 【{action}】[{sym}]({okx_url}) | 评分: {score:.2f}/100\n"
                f"背离差: {diff:+.2f}% | 价格: ${alt_price:.4f}\n"
                f"📊 {details}"
            )
            alerts.append({
                "symbol": sym,
                "signal_type": signal_type,
                "price": alt_price,
                "score": score,
                "text": alert_text
            })

    if alerts:
        header = f"📊 BTC: ${btc_price:.2f} | 24h: {btc_change:+.2f}%\n" + "="*30 + "\n"
        full_msg = header + "\n\n".join([a["text"] for a in alerts])
        send_telegram(full_msg, parse_mode='Markdown')

        with PENDING_LOCK:
            for a in alerts:
                PENDING_SIGNALS.append({
                    "symbol": a["symbol"],
                    "signal_type": a["signal_type"],
                    "price": a["price"],
                    "timestamp": time.time(),
                    "verified": False,
                    "status": "pending"
                })

# ==================== 8. 验证循环 ====================
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

# ==================== 9. WebSocket（分批订阅修复） ====================
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
    
    # 复制当前监控列表，避免订阅过程中被修改
    all_symbols = [BTC_SYMBOL] + list(alt_symbols)
    print(f"✅ WebSocket已连接，监控 {len(all_symbols)} 个币种")
    
    # 分批订阅，每批最多 50 个
    batch_size = 50
    total_batches = (len(all_symbols) + batch_size - 1) // batch_size
    
    for i in range(0, len(all_symbols), batch_size):
        batch = all_symbols[i:i+batch_size]
        args = [{"channel": "tickers", "instId": sym} for sym in batch]
        sub_msg = {"op": "subscribe", "args": args}
        ws.send(json.dumps(sub_msg))
        print(f"📨 已发送订阅批次 {i//batch_size + 1}/{total_batches}，共 {len(batch)} 个币种")
        time.sleep(0.5)  # 避免速率限制

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
        time.sleep(5)  # 重连间隔 5 秒

# ==================== 10. 币种管理 ====================
def add_symbol(symbol):
    if symbol == BTC_SYMBOL or symbol in alt_symbols:
        return False
    alt_symbols.add(symbol)
    price_data[symbol] = {"price": 0, "change": 0, "volume": 0}
    diff_history[symbol] = []  # 初始化历史记录
    restart_websocket()
    return True

def remove_symbol(symbol):
    if symbol == BTC_SYMBOL or symbol not in alt_symbols:
        return False
    alt_symbols.remove(symbol)
    price_data.pop(symbol, None)
    diff_history.pop(symbol, None)
    restart_websocket()
    return True

def clear_alts():
    if not alt_symbols:
        return False
    for sym in list(alt_symbols):
        alt_symbols.remove(sym)
        price_data.pop(sym, None)
        diff_history.pop(sym, None)
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
                diff_history[sym] = []
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
                            diff_history[sym] = []
                    known_symbols.update(new_coins)
                    restart_websocket()
                    send_telegram(f"🆕 自动发现新合约币种并已添加监控: {', '.join(new_coins)}")
        except Exception as e:
            print(f"自动扫描出错: {e}")
        time.sleep(3600)

# ==================== 11. 自动过滤 ====================
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
                    diff_history.pop(sym, None)
                for sym in to_add:
                    alt_symbols.add(sym)
                    price_data[sym] = {"price": 0, "change": 0, "volume": 0}
                    diff_history[sym] = []

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

# ==================== 12. 波动扫描 ====================
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
                            diff_history[symbol] = []
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

# ==================== 13. 独立行情监控 ====================
def independent_scanner():
    cache = {}
    while True:
        time.sleep(60)
        if not INDEPENDENT_MODE_ENABLED:
            continue
        
        btc = price_data.get(BTC_SYMBOL)
        if not btc or abs(btc["change"]) > BTC_TREND_THRESHOLD:
            continue
        
        try:
            resp = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SPOT", timeout=10)
            data = resp.json()
            if data["code"] != "0":
                continue
            
            now = time.time()
            alerts = []
            for item in data["data"]:
                symbol = item["instId"]
                if not symbol.endswith("-USDT") or symbol == BTC_SYMBOL:
                    continue
                
                current_price = float(item["last"])
                if symbol not in cache:
                    cache[symbol] = {"price": current_price, "time": now, "processed": False}
                    continue
                
                prev = cache[symbol]
                elapsed = (now - prev["time"]) / 60
                if elapsed > INDEPENDENT_LOOKBACK + 2:
                    cache[symbol] = {"price": current_price, "time": now, "processed": False}
                    continue
                if prev.get("processed", False):
                    continue
                
                price_change = (current_price - prev["price"]) / prev["price"] * 100
                if abs(price_change) >= INDEPENDENT_THRESHOLD:
                    # 独立模式直接调用 analyze_signal
                    signal_type, score, details = analyze_signal(
                        symbol, diff=0, btc_change=0, alt_change=price_change,
                        volume=float(item.get("volCcy24h", 0)),
                        current_price=current_price,
                        use_independent=True
                    )
                    
                    if signal_type and score >= 50:
                        emoji = "🟢" if signal_type == "LONG" else "🔴"
                        action = "做多" if signal_type == "LONG" else "做空"
                        inst_id = symbol
                        okx_url = f"https://www.okx.com/zh-hans/markets/swap/{inst_id.lower()}"
                        alerts.append(
                            f"{emoji} 【独立信号·{action}】[{symbol}]({okx_url})\n"
                            f"15分钟 {price_change:+.2f}% | 现价: ${current_price:.4f}\n"
                            f"评分: {score:.2f}/100\n"
                            f"📊 {details}"
                        )
                        if symbol not in alt_symbols:
                            alt_symbols.add(symbol)
                            price_data[symbol] = {"price": current_price, "change": 0, "volume": 0}
                            diff_history[symbol] = []
                    
                    cache[symbol]["processed"] = True
                    cache[symbol]["price"] = current_price
                    cache[symbol]["time"] = now
                else:
                    cache[symbol]["price"] = current_price
                    cache[symbol]["time"] = now
            
            if alerts:
                header = f"📊 BTC 横盘（24h: {btc['change']:+.2f}%），独立行情监控\n" + "="*30 + "\n"
                send_telegram(header + "\n\n".join(alerts[:5]), parse_mode='Markdown')
                if any("已自动加入" in a for a in alerts):
                    restart_websocket()
        except Exception as e:
            print(f"独立行情监控出错: {e}")

# ==================== 14. 汇总 ====================
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

# ==================== 15. 自动刷新 ====================
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

# ==================== 16. Telegram命令 ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    await update.message.reply_text(
        f"🤖 **合约胜率增强Bot v{VERSION}**\n"
        "点击下方按钮快速操作。\n\n"
        "📊 **核心升级**：Z-Score 统计背离（自动适应市场）\n"
        "📈 每个信号自带盈亏比，评分 ≥70 视为优质信号\n\n"
        "⚙️ **命令**：\n"
        "/status – 查看状态 & 参数\n"
        "/summary – 强弱汇总\n"
        "/autorefresh on/off – 自动刷新\n"
        "/addcoin / addtop – 管理币种\n"
        "/setdiff – 调整 Z-Score 阈值\n"
        "/setvolatility – 波动扫描阈值\n"
        "/debug – BTC数据\n"
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

    filter_status = "🟢 开启" if AUTO_FILTER_ENABLED else "🔴 关闭"
    volatility_status = "🟢 开启" if VOLATILITY_SCAN_ENABLED else "🔴 关闭"
    independent_status = "🟢 开启" if INDEPENDENT_MODE_ENABLED else "🔴 关闭"
    msg = (
        f"📋 监控: {count} 个山寨币（仅合约）\n"
        f"自动刷新: {status_auto}"
        f"{f' (间隔 {interval_min} 分钟)' if auto_refresh_enabled else ''}\n"
        f"自动过滤: {filter_status} (保留前 {MAX_COINS} 名)\n"
        f"波动扫描: {volatility_status} (阈值 {VOLATILITY_THRESHOLD}%)\n"
        f"独立行情: {independent_status} (阈值 {INDEPENDENT_THRESHOLD}%)\n"
        f"待验证信号: {pending_count} 个\n"
        f"📊 近24小时验证统计:\n"
        f"  总数: {total} | ✅成功: {success} | ❌失败: {failed} | ⏳失效: {expired}\n"
        f"  成功率: {success_rate}\n\n"
        f"⚙️ **当前评分参数**：\n"
        f"Z-Score 阈值: 做多 {ZSCORE_LONG_THRESHOLD} | 做空 {ZSCORE_SHORT_THRESHOLD}\n"
        f"固定回退阈值: {FALLBACK_LONG}% / {FALLBACK_SHORT}%\n"
        f"RSI超买/超卖: {RSI_OVERBOUGHT}/{RSI_OVERSOLD}\n"
        f"成交量阈值: {VOLUME_THRESHOLD/1000000:.1f}M"
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

async def setdiff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ZSCORE_LONG_THRESHOLD, ZSCORE_SHORT_THRESHOLD
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text(
            f"📊 当前 Z-Score 阈值: 做多 {ZSCORE_LONG_THRESHOLD} | 做空 {ZSCORE_SHORT_THRESHOLD}\n"
            "用法: /setdiff <做多阈值> <做空阈值>\n"
            "示例: /setdiff 1.5 -1.5（更敏感） | /setdiff 2.0 -2.0（更严格）",
            reply_markup=get_main_keyboard()
        )
        return
    try:
        long_val = float(context.args[0])
        short_val = float(context.args[1]) if len(context.args) > 1 else -long_val
        if long_val < 0.5 or long_val > 4.0 or short_val > -0.5 or short_val < -4.0:
            await update.message.reply_text("⚠️ 阈值范围应在 0.5~4.0 和 -0.5~-4.0", reply_markup=get_main_keyboard())
            return
        ZSCORE_LONG_THRESHOLD = long_val
        ZSCORE_SHORT_THRESHOLD = short_val
        await update.message.reply_text(
            f"✅ Z-Score 阈值已更新\n"
            f"做多阈值: {ZSCORE_LONG_THRESHOLD} | 做空阈值: {ZSCORE_SHORT_THRESHOLD}\n"
            f"（数值越大，信号越严格）",
            reply_markup=get_main_keyboard()
        )
    except ValueError:
        await update.message.reply_text("⚠️ 请输入有效数字，如 /setdiff 1.8 -1.8", reply_markup=get_main_keyboard())

async def setvolatility(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global VOLATILITY_THRESHOLD
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    if not context.args:
        await update.message.reply_text(
            f"📊 当前波动扫描阈值: {VOLATILITY_THRESHOLD}%\n"
            "用法: /setvolatility <数值>（如 /setvolatility 3.5）",
            reply_markup=get_main_keyboard()
        )
        return
    try:
        new_threshold = float(context.args[0])
        if new_threshold < 0.5 or new_threshold > 20:
            await update.message.reply_text("⚠️ 阈值范围应在 0.5% ~ 20% 之间", reply_markup=get_main_keyboard())
            return
        VOLATILITY_THRESHOLD = new_threshold
        await update.message.reply_text(
            f"✅ 波动扫描阈值已更新为 {VOLATILITY_THRESHOLD}%",
            reply_markup=get_main_keyboard()
        )
    except ValueError:
        await update.message.reply_text("⚠️ 请输入有效的数字，如 /setvolatility 3.5", reply_markup=get_main_keyboard())

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != int(TELEGRAM_CHAT_ID):
        return
    btc = price_data.get(BTC_SYMBOL)
    if btc:
        msg = (f"🔍 **BTC 当前数据**\n"
               f"价格: ${btc['price']:.2f}\n"
               f"24h涨跌幅: {btc['change']:.2f}%\n"
               f"成交额: ${btc['volume']:.0f}\n"
               f"监控币种总数: {len(alt_symbols)}\n"
               f"历史样本数: {sum(len(v) for v in diff_history.values())}")
    else:
        msg = "❌ 未获取到BTC数据"
    await update.message.reply_text(msg, reply_markup=get_main_keyboard(), parse_mode='Markdown')

# ==================== 17. Telegram Bot ====================
def run_telegram_bot():
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
    app.add_handler(CommandHandler("setdiff", setdiff))
    app.add_handler(CommandHandler("setvolatility", setvolatility))
    app.add_handler(CommandHandler("debug", debug))
    print("🤖 Telegram Bot 正在运行 (主线程)")
    app.run_polling()

# ==================== 18. Flask心跳 ====================
flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "Monitoring running", 200

def run_http():
    flask_app.run(host='0.0.0.0', port=10000)

# ==================== 19. 启动通知 ====================
def send_startup_notification():
    time.sleep(8)
    msg = (
        f"🚀 **Bot 已重新启动！**\n"
        f"版本: {VERSION}\n"
        f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"监控币种: {len(alt_symbols)} 个合约币种\n"
        f"Z-Score 阈值: 多 {ZSCORE_LONG_THRESHOLD} / 空 {ZSCORE_SHORT_THRESHOLD}\n"
        f"使用 /status 查看详情"
    )
    send_telegram(msg)

# ==================== 20. 主程序 ====================
if __name__ == "__main__":
    print(f"🚀 合约胜率增强版 v{VERSION} 启动于 {datetime.now()}")
    print(f"初始监控: {BTC_SYMBOL} + {', '.join(DEFAULT_ALT_SYMBOLS)}")
    print(f"Z-Score 阈值: 多 {ZSCORE_LONG_THRESHOLD} / 空 {ZSCORE_SHORT_THRESHOLD}")
    print(f"自动过滤: 保留前 {MAX_COINS} 名")

    # 启动所有后台线程
    threading.Thread(target=verify_loop, daemon=True).start()
    threading.Thread(target=auto_scan_new_coins, daemon=True).start()
    threading.Thread(target=auto_filter_coins, daemon=True).start()
    threading.Thread(target=volatility_scanner, daemon=True).start()
    threading.Thread(target=independent_scanner, daemon=True).start()
    threading.Thread(target=start_ws, daemon=True).start()

    # Flask 子线程
    http_thread = threading.Thread(target=run_http, daemon=True)
    http_thread.start()
    print("🌐 HTTP 心跳服务已启动 (子线程)")

    # 发送启动通知
    threading.Thread(target=send_startup_notification, daemon=True).start()

    # 主线程运行 Telegram Bot
    print("🤖 正在主线程启动 Telegram Bot...")
    run_telegram_bot()