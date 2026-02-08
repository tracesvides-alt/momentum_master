"""
Daily Market Tweet Generator
sakaさんのトーンで米国株マーケット速報を生成
Discord Webhookにも投稿対応
"""
import pandas as pd
from datetime import datetime
import random
import os
import requests
import yfinance as yf
import json

# Import sector definitions from market_logic
from market_logic import SECTOR_DEFINITIONS, TICKER_TO_SECTOR, SECTOR_JP_MAP

# Major indices to track
MAJOR_INDICES = {
    "^DJI": "ダウ30",
    "^GSPC": "S&P500", 
    "^NDX": "ナス100",
    "^RUT": "ラッセル2000",
    "BTC-USD": "BTC",
    "GC=F": "金"
}

# Discord Webhook URL (環境変数から取得)
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '')


def get_major_indices():
    """Fetch 1-day returns for major indices"""
    results = []
    
    for ticker, jp_name in MAJOR_INDICES.items():
        try:
            data = yf.Ticker(ticker)
            hist = data.history(period="5d")
            if len(hist) >= 2:
                prev_close = hist['Close'].iloc[-2]
                last_close = hist['Close'].iloc[-1]
                pct_change = ((last_close - prev_close) / prev_close) * 100
                results.append((jp_name, pct_change))
            else:
                results.append((jp_name, 0.0))
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            results.append((jp_name, 0.0))
    
    return results

def load_cache():
    """Load the momentum cache"""
    df = pd.read_csv('data/momentum_cache.csv')
    return df

def get_top_movers(df, n=5):
    """Get top 5 gainers and losers by 1d return"""
    # Sort by 1d return
    sorted_df = df.sort_values('1d', ascending=False)
    
    # Top gainers
    gainers = sorted_df.head(n)[['Ticker', '1d']].values.tolist()
    
    # Top losers
    losers = sorted_df.tail(n)[['Ticker', '1d']].values.tolist()
    losers = losers[::-1]  # Reverse to show worst first
    
    return gainers, losers

def get_sector_performance(df):
    """Calculate sector performance based on average 1d returns (using Japanese names)"""
    sector_stats = []
    
    for sector_name, tickers in SECTOR_DEFINITIONS.items():
        df_sec = df[df['Ticker'].isin(tickers)]
        if df_sec.empty:
            continue
        
        # Get Japanese name
        jp_name = SECTOR_JP_MAP.get(sector_name, sector_name)
        avg_ret = df_sec['1d'].mean()
        
        sector_stats.append({
            'name': jp_name,
            'avg': avg_ret
        })
    
    # Sort by average return
    sector_stats.sort(key=lambda x: x['avg'], reverse=True)
    
    # Convert to Series for compatibility
    sector_perf = pd.Series(
        {s['name']: s['avg'] for s in sector_stats}
    )
    
    return sector_perf


def generate_comment(gainers, losers, sector_perf, indices=None):
    """Generate a 3-5 line market comment focused on overall sentiment and outlook in saka's style"""
    
    # Calculate market strength
    avg_gain = sum([g[1] for g in gainers]) / len(gainers) if gainers else 0
    avg_loss = sum([l[1] for l in losers]) / len(losers) if losers else 0
    overall_strength = avg_gain + avg_loss  # positive = strong, negative = weak
    
    # Analyze sector performance
    strong_sectors = sector_perf[sector_perf > 2]  # >2% sectors
    weak_sectors = sector_perf[sector_perf < -2]  # <-2% sectors
    num_strong = len(strong_sectors)
    num_weak = len(weak_sectors)
    
    # Extract major index trends if available
    index_trend = None
    if indices and len(indices) >= 3:
        # Check if major indices are aligned (all up or all down)
        sp500_ret = indices[1][1] if len(indices) > 1 else 0
        nasdaq_ret = indices[2][1] if len(indices) > 2 else 0
        
        if sp500_ret > 1 and nasdaq_ret > 1:
            index_trend = "strong"
        elif sp500_ret < -1 and nasdaq_ret < -1:
            index_trend = "weak"
        else:
            index_trend = "mixed"
    
    lines = []
    
    # Line 1: Overall market sentiment (ALWAYS add)
    if overall_strength > 3:
        openers = [
            "今日は全体的にいい感じの相場でしたね〜✨",
            "まあまあ調子良かったんじゃないでしょうか(*^^*)",
            "なかなか強い地合いでした！",
        ]
    elif overall_strength > 0:
        openers = [
            "上げ下げ色々ありますけど、トータルではプラス圏ですかね",
            "まちまちな感じでしたけど、ちょい上げって感じ",
            "そこそこ堅調だった気がします",
        ]
    elif overall_strength > -3:
        openers = [
            "うーん、ちょっと厳しめの日でしたね💦",
            "今日はイマイチだったかな…",
            "なかなか厳しい1日でしたな(´・ω・`)",
        ]
    else:
        openers = [
            "今日は辛すぎる…(´；ω；｀)",
            "全体的に弱かったですね…厳しい",
            "下げがキツめの日でした💀",
        ]
    lines.append(random.choice(openers))
    
    # Line 2: Index/sector breadth analysis (ALWAYS add at least something)
    if index_trend == "strong":
        breadths = [
            f"主要指数が揃って上げてますし、セクターも{num_strong}個がプラス圏",
            f"指数が全部プラスで、{num_strong}セクターが上げてる感じ",
            f"指数もセクターも広く買われてますね。{num_strong}セクターが強かった",
        ]
    elif index_trend == "weak":
        breadths = [
            f"指数が全体的に弱くて、{num_weak}セクターが売られてる状況",
            f"主要指数が揃って下げ。{num_weak}セクターがマイナス圏です",
            f"指数もセクターも全体的に軟調でしたね…{num_weak}セクター下落",
        ]
    elif num_strong > num_weak and num_strong > 0:
        breadths = [
            f"セクター別で見ると{num_strong}個がプラス。まずまず広がってる感じ",
            f"{num_strong}セクターが上げてるので、裾野は広いかな",
            "セクターも全体的に堅調でしたね",
        ]
    elif num_weak > num_strong and num_weak > 0:
        breadths = [
            f"{num_weak}セクターが売られてて、ちょっと広めに下げてますね",
            f"弱いセクターが{num_weak}個もあるので、なかなか厳しい",
            "セクター全体的に弱めでした",
        ]
    else:
        # Fallback: general market breadth comment
        breadths = [
            "セクターはまちまちって感じですかね",
            "銘柄によって強弱がハッキリ分かれてますね",
        ]
    lines.append(random.choice(breadths))
    
    # Line 3: Market mood/background speculation (ALWAYS add)
    if overall_strength > 3:
        moods = [
            "リスクオンな雰囲気が出てきたって感じ",
            "買い意欲が戻ってきてる気がします",
            "地合い改善してきてるかもですね",
        ]
    elif overall_strength > 0:
        moods = [
            "様子見ムードはありつつも、下値は固めって感じかな",
            "慎重ながらも買いが入ってきてる印象",
            "じわじわ上げてる感じで悪くないですね",
        ]
    elif overall_strength > -3:
        moods = [
            "ちょっとリスクオフ気味かもですね",
            "警戒感が出てきちゃってるな〜って感じ",
            "様子見ムードが強まってる感じ",
        ]
    else:
        moods = [
            "完全にリスクオフモード入ってますね…",
            "売りが強すぎて厳しい展開",
            "全体的に弱気ムードが漂ってますね",
        ]
    lines.append(random.choice(moods))
    
    # Line 4: Forward-looking outlook (ALWAYS add)
    if overall_strength > 3:
        outlooks = [
            "このまま上昇トレンド継続してくれたら嬉しいんですけどね😎",
            "明日も続くようなら流れ変わってきたかも⤴︎⤴︎",
            "この勢いで週末も期待したいところ！",
        ]
    elif overall_strength > 0:
        outlooks = [
            "明日次第ですかね。上抜けるか、また戻されるか…",
            "このまま上に抜けていってほしいもの👀",
            "明日の動き次第で流れが決まりそう",
        ]
    elif overall_strength > -3:
        outlooks = [
            "明日は反発してくれないと厳しいですね💦",
            "そろそろ下げ止まってほしいんですけど…",
            "ここから切り返せるかどうかって感じ",
        ]
    else:
        outlooks = [
            "明日こそは反発頼みます…(´；ω；｀)",
            "下げすぎやろと思いつつ耐える展開💦",
            "早く底打ちしてくれないかなー",
        ]
    lines.append(random.choice(outlooks))
    
    # Return 3-4 lines (always at least 4 lines now)
    return "\n".join(lines)


def format_tweet(gainers, losers, sector_perf, indices=None):
    """Format the final tweet"""
    today = datetime.now().strftime("%m/%d")
    
    # Format major indices
    index_lines = []
    if indices:
        for name, ret in indices:
            index_lines.append(f"{name} {ret:+.1f}%")
    
    # Format gainers
    gainer_lines = []
    for ticker, ret in gainers:
        gainer_lines.append(f"${ticker} {ret:+.1f}%")
    
    # Format losers
    loser_lines = []
    for ticker, ret in losers:
        loser_lines.append(f"${ticker} {ret:+.1f}%")
    
    # Top 5 sectors up
    top_sectors = sector_perf.head(5)
    top_sector_lines = []
    for sector, ret in top_sectors.items():
        # Use full sector name (already in Japanese + English format)
        top_sector_lines.append(f"{sector} {ret:+.1f}%")
    
    # Bottom 5 sectors
    bottom_sectors = sector_perf.tail(5)[::-1]
    bottom_sector_lines = []
    for sector, ret in bottom_sectors.items():
        # Use full sector name (already in Japanese + English format)
        bottom_sector_lines.append(f"{sector} {ret:+.1f}%")
    
    # Generate comment
    comment = generate_comment(gainers, losers, sector_perf, indices)
    
    # Build indices section
    indices_section = ""
    if index_lines:
        indices_section = f"""📈 主要指数
{index_lines[0]} | {index_lines[1]} | {index_lines[2]}
{index_lines[3]} | {index_lines[4]} | {index_lines[5]}

"""
    
    # Build tweet
    tweet = f"""📊 {today} 米国株マーケット速報

{indices_section}🔥 爆上げTOP5
{' | '.join(gainer_lines[:3])}
{' | '.join(gainer_lines[3:5])}

💀 下落TOP5
{' | '.join(loser_lines[:3])}
{' | '.join(loser_lines[3:5])}

📦 セクター上位5
🚀 {top_sector_lines[0]}
{top_sector_lines[1]}
{top_sector_lines[2]}
{top_sector_lines[3]}
{top_sector_lines[4]}

📦 セクター下位5
💨 {bottom_sector_lines[0]}
{bottom_sector_lines[1]}
{bottom_sector_lines[2]}
{bottom_sector_lines[3]}
{bottom_sector_lines[4]}

{comment}

🚀Momentum Master
https://momentummaster.streamlit.app/

#米国株"""
    
    return tweet


def post_to_discord(tweet_text):
    """Post the tweet to Discord via webhook"""
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ Discord Webhook URL not set!")
        print("環境変数 DISCORD_WEBHOOK_URL を設定してください")
        return False
    
    # Discord Embed for rich formatting
    embed = {
        "title": "📊 米国株マーケット速報",
        "description": tweet_text,
        "color": 0x1DA1F2,  # Twitter blue
        "footer": {
            "text": "Xにコピペ用 | Momentum Master"
        },
        "timestamp": datetime.utcnow().isoformat()
    }
    
    payload = {
        "embeds": [embed],
        "content": "**新しいマーケット速報が生成されました！**\n以下のテキストをXにコピペしてください 👇"
    }
    
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print("✅ Discord投稿成功！")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Discord投稿失敗: {e}")
        return False


def load_watchlist():
    """Load watchlist from watchlist.json"""
    try:
        with open('watchlist.json', 'r') as f:
            data = json.load(f)
            return data.get('watchlist', [])
    except FileNotFoundError:
        print("⚠️ watchlist.json not found, skipping watchlist analysis")
        return []
    except Exception as e:
        print(f"⚠️ Error loading watchlist: {e}")
        return []


def get_stock_analysis(ticker):
    """Get detailed analysis for a single stock"""
    try:
        # Import market_logic for analysis
        import market_logic
        
        # Get historical data and analysis
        df_hist, summary = market_logic.analyze_stock_history(ticker)
        
        if df_hist is None:
            return None
            
        # Get 1-day and 5-day returns from recent history
        if len(df_hist) >= 2:
            prev_close = df_hist['Close'].iloc[-2]
            curr_close = df_hist['Close'].iloc[-1]
            day_return = ((curr_close - prev_close) / prev_close) * 100
        else:
            day_return = 0
            
        if len(df_hist) >= 6:
            week_ago_close = df_hist['Close'].iloc[-6]
            week_return = ((curr_close - week_ago_close) / week_ago_close) * 100
        else:
            week_return = 0
        
        return {
            'ticker': ticker,
            'price': summary.get('price', 0),
            'status': summary.get('status', 'N/A'),
            'action': summary.get('action', ''),
            'rsi': summary.get('rsi', 0),
            'macd': summary.get('macd', 0),
            'chandelier': summary.get('chandelier', 0),
            'day_return': day_return,
            'week_return': week_return
        }
    except Exception as e:
        print(f"⚠️ Error analyzing {ticker}: {e}")
        return None


def format_watchlist_tweet(watchlist_analyses):
    """Format watchlist analysis tweet"""
    if not watchlist_analyses:
        return None
    
    today = datetime.now().strftime("%m/%d")
    
    lines = [f"📋 ウォッチリスト分析 ({today})"]
    lines.append("")
    
    for data in watchlist_analyses:
        ticker = data['ticker']
        price = data['price']
        status = data['status']
        day_ret = data['day_return']
        rsi = data['rsi']
        macd_signal = "Bullish" if data['macd'] > 0 else "Bearish"
        chandelier = data['chandelier']
        
        # Emoji based on status
        status_emoji = "🟢" if status == "BUY" else "🔴" if status == "SELL" else "🟡"
        
        lines.append(f"{status_emoji} ${ticker} | {status}")
        lines.append(f"  現在値: ${price:.2f} ({day_ret:+.2f}%)")
        lines.append(f"  RSI: {rsi:.0f} | MACD: {macd_signal}")
        lines.append(f"  損切: ${chandelier:.2f}")
        lines.append("")
    
    lines.append("🚀Momentum Master")
    lines.append("https://momentummaster.streamlit.app/")
    
    return "\n".join(lines)


def format_weekly_summary(watchlist_analyses):
    """Format weekly summary tweet (Saturday only)"""
    if not watchlist_analyses:
        return None
    
    today = datetime.now().strftime("%m/%d")
    
    # Sort by weekly return
    sorted_data = sorted(watchlist_analyses, key=lambda x: x['week_return'], reverse=True)
    
    lines = [f"📊 週間サマリー ({today})"]
    lines.append("")
    
    for data in sorted_data:
        ticker = data['ticker']
        week_ret = data['week_return']
        emoji = "🔥" if week_ret > 5 else "📈" if week_ret > 0 else "📉" if week_ret > -5 else "💀"
        lines.append(f"{emoji} ${ticker}: {week_ret:+.2f}%")
    
    lines.append("")
    lines.append("🚀Momentum Master")
    lines.append("https://momentummaster.streamlit.app/")
    
    return "\n".join(lines)


def get_signal_stocks_from_history():
    """Get stocks with active signals using history cache, matching Streamlit app workflow"""
    import pickle
    import market_logic
    
    # Load history cache
    history_cache_path = 'data/history_cache.pkl'
    try:
        with open(history_cache_path, 'rb') as f:
            history_dict = pickle.load(f)
    except FileNotFoundError:
        print(f"⚠️ History cache not found: {history_cache_path}")
        return {}
    
    # Get today's signals using market_logic function (same as Streamlit app)
    try:
        daily_signals = market_logic.get_todays_signals(history_dict)
    except Exception as e:
        print(f"⚠️ Error getting signals: {e}")
        return {}
    
    return daily_signals


def format_signal_alert_message(daily_signals, max_per_type=4):
    """Format signal alert message in watchlist style with detailed metrics
    
    Args:
        daily_signals: Dict with Buy_Breakout, Buy_Reversal, Buy_Reentry, Sell lists
        max_per_type: Maximum number of signals to show per type (default: 4 for Discord limit)
    """
    import market_logic
    
    if not daily_signals:
        return None
    
    # Extract and sort signal lists by score (if available)
    def sort_by_score(signal_list):
        """Sort signals by score in descending order"""
        # Check if signals have 'score' or similar field
        if signal_list and isinstance(signal_list[0], dict):
            # Try various score fields
            if 'Score' in signal_list[0]:
                return sorted(signal_list, key=lambda x: x.get('Score', 0), reverse=True)
            elif 'BullScore' in signal_list[0]:
                return sorted(signal_list, key=lambda x: x.get('BullScore', 0), reverse=True)
            elif 'RVOL' in signal_list[0]:
                # Use RVOL as proxy for strength
                return sorted(signal_list, key=lambda x: x.get('RVOL', 0), reverse=True)
        return signal_list
    
    buy_breakout = sort_by_score(daily_signals.get('Buy_Breakout', []))
    buy_reversal = sort_by_score(daily_signals.get('Buy_Reversal', []))
    buy_reentry = sort_by_score(daily_signals.get('Buy_Reentry', []))
    sells = sort_by_score(daily_signals.get('Sell', []))
    
    total_count = len(buy_breakout) + len(buy_reversal) + len(buy_reentry) + len(sells)
    if total_count == 0:
        return None
    
    today = datetime.now().strftime("%m/%d")
    lines = [f"📊 {today} 本日の売買シグナル速報\n"]
    
    # Bilingual signal type labels
    SIGNAL_TYPE_LABELS = {
        'Breakout': '🚀 ブレイクアウト [Breakout]',
        'Reversal': '🎣 リバーサル [Reversal]',
        'Reentry': '🔄 リエントリー [Reentry]',
        'Sell': '👋 売りシグナル [Sell]'
    }
    
    # Summary counts
    signal_counts = []
    if len(buy_breakout) > 0:
        signal_counts.append(f"🟢 Breakout ({len(buy_breakout)})")
    if len(buy_reversal) > 0:
        signal_counts.append(f"🟢 Reversal ({len(buy_reversal)})")
    if len(buy_reentry) > 0:
        signal_counts.append(f"🟢 Reentry ({len(buy_reentry)})")
    if len(sells) > 0:
        signal_counts.append(f"🔴 Sell ({len(sells)})")
    
    lines.append(" | ".join(signal_counts) + "\n")
    
    # Helper function to format signal section (watchlist style)
    def format_signal_section(signal_list, signal_type, max_items):
        if not signal_list:
            return []
        
        # Limit to max_items (top N by score)
        limited_list = signal_list[:max_items]
        has_more = len(signal_list) > max_items
        
        section_lines = [f"\n**{SIGNAL_TYPE_LABELS[signal_type]}** ({len(signal_list)}銘柄)"]
        
        # Get reason translation map
        reason_map = getattr(market_logic, 'REASON_JP_MAP', {})
        
        for stock in limited_list:
            ticker = stock['Ticker']
            price = stock.get('Price', 0)
            reason = stock.get('Reason', '')
            reason_jp = reason_map.get(reason, reason)
            
            # Calculate price change if available
            one_day_change = stock.get('1d', 0)
            if one_day_change != 0:
                price_display = f"${price:.2f} ({one_day_change:+.2f}%)"
            else:
                price_display = f"${price:.2f}"
            
            # MACD status
            macd = stock.get('MACD', 0)
            macd_signal = stock.get('MACD_Signal', 0)
            macd_status = "Bullish" if macd > macd_signal else "Bearish"
            
            # RSI
            rsi = stock.get('RSI', 0)
            
            # Stop loss
            chandelier = stock.get('Chandelier_Exit', 0)
            
            # Judgment criteria (key indicators)
            rvol = stock.get('RVOL', 0)
            adx = stock.get('ADX', 0)
            high50 = stock.get('High50', 0)
            current_price = stock.get('Price', 0)
            
            # Calculate distance from 50-day high (for Breakout signals)
            high50_pct = ((current_price / high50 - 1) * 100) if high50 > 0 else 0
            
            # Format in watchlist style
            emoji = "🟢" if signal_type != 'Sell' else "🔴"
            section_lines.append(f"{emoji} ${ticker} | {reason_jp}")
            section_lines.append(f"  現在値: {price_display}")
            
            # Indicator line (RSI | MACD)
            indicators = []
            if rsi > 0:
                indicators.append(f"RSI: {rsi:.0f}")
            indicators.append(f"MACD: {macd_status}")
            section_lines.append(f"  {' | '.join(indicators)}")
            
            # Add judgment criteria based on signal type
            criteria_parts = []
            if rvol > 0:
                criteria_parts.append(f"📊 出来高: {rvol:.1f}倍")
            
            if signal_type == 'Breakout' and high50_pct != 0:
                criteria_parts.append(f"📈 高値接近: {high50_pct:+.1f}%")
            elif signal_type == 'Reentry' and adx > 0:
                # ADX > 25 is strong trend, otherwise medium (as >15 is required for signal)
                strength = "強" if adx >= 25 else "中"
                criteria_parts.append(f"🔥 上昇トレンド: {strength}")
            elif signal_type == 'Sell' and chandelier > 0:
                # For Sell signals, show distance to stop loss if available
                dist_pct = ((current_price / chandelier - 1) * 100)
                criteria_parts.append(f"🛑 損切ライン: ${chandelier:.2f} ({dist_pct:+.1f}%)")
            
            if criteria_parts:
                # Use a different separator for Japanese text to improve readability
                section_lines.append(f"  {'  '.join(criteria_parts)}")
                
            # For Buy signals, show stop loss below if available
            if signal_type != 'Sell' and chandelier > 0:
                section_lines.append(f"  損切: ${chandelier:.2f}\n")
            elif signal_type != 'Sell':
                 section_lines.append("") # Spacing for buy signals without chandelier
            else:
                 section_lines.append("") # Spacing for sell signals
        
        if has_more:
            section_lines.append(f"...他 {len(signal_list) - max_items}銘柄\n")
        
        return section_lines
    
    # Format each signal type (top 5 each for Discord limit)
    lines.extend(format_signal_section(buy_breakout, "Breakout", max_per_type))
    lines.extend(format_signal_section(buy_reversal, "Reversal", max_per_type))
    lines.extend(format_signal_section(buy_reentry, "Reentry", max_per_type))
    lines.extend(format_signal_section(sells, "Sell", max_per_type))
    
    lines.append("🚀Momentum Master")
    lines.append("https://momentummaster.streamlit.app/")
    
    return "\n".join(lines)






def main():

    """Main function to generate tweet"""
    print("Loading cache data...")
    df = load_cache()
    
    print(f"Loaded {len(df)} tickers")
    
    # Get major indices
    print("\n📈 主要指数を取得中...")
    indices = get_major_indices()
    for name, ret in indices:
        print(f"  {name}: {ret:+.1f}%")
    
    # Get top movers
    gainers, losers = get_top_movers(df)
    print(f"\nTop Gainers: {gainers}")
    print(f"Top Losers: {losers}")
    
    # Get sector performance
    sector_perf = get_sector_performance(df)
    print(f"\nSector Performance (top 5):\n{sector_perf.head(5)}")
    print(f"\nSector Performance (bottom 5):\n{sector_perf.tail(5)}")
    
    # Generate market summary tweet
    tweet = format_tweet(gainers, losers, sector_perf, indices)
    
    print("\n" + "="*50)
    print("GENERATED TWEET:")
    print("="*50)
    print(tweet)
    print("="*50)
    print(f"Character count: {len(tweet)}")
    
    # Post market summary to Discord
    print("\n📤 Discordに市場サマリーを投稿中...")
    post_to_discord(tweet)
    
    # --- Watchlist Analysis ---
    print("\n📋 ウォッチリスト分析を実行中...")
    watchlist = load_watchlist()
    
    if watchlist:
        print(f"Watchlist: {watchlist}")
        watchlist_analyses = []
        
        for ticker in watchlist:
            print(f"  分析中: {ticker}")
            analysis = get_stock_analysis(ticker)
            if analysis:
                watchlist_analyses.append(analysis)
        
        if watchlist_analyses:
            # Format and post watchlist tweet
            watchlist_tweet = format_watchlist_tweet(watchlist_analyses)
            if watchlist_tweet:
                print("\n" + "="*50)
                print("WATCHLIST TWEET:")
                print("="*50)
                print(watchlist_tweet)
                print("="*50)
                print(f"Character count: {len(watchlist_tweet)}")
                
                print("\n📤 Discordにウォッチリスト分析を投稿中...")
                post_to_discord(watchlist_tweet)
            
            # Check if today is Saturday (5 = Saturday in Python's weekday())
            today_weekday = datetime.now().weekday()
            if today_weekday == 5:  # Saturday
                print("\n📊 土曜日なので週間サマリーを生成中...")
                weekly_tweet = format_weekly_summary(watchlist_analyses)
                if weekly_tweet:
                    print("\n" + "="*50)
                    print("WEEKLY SUMMARY TWEET:")
                    print("="*50)
                    print(weekly_tweet)
                    print("="*50)
                    print(f"Character count: {len(weekly_tweet)}")
                    
                    print("\n📤 Discordに週間サマリーを投稿中...")
                    post_to_discord(weekly_tweet)
    else:
        print("ウォッチリストが空です。スキップします。")
    
    # --- Signal Alert ---
    print("\n📊 シグナル発動銘柄を抽出中...")
    daily_signals = get_signal_stocks_from_history()
    
    if daily_signals and any(len(v) > 0 for v in daily_signals.values()):
        signal_message = format_signal_alert_message(daily_signals)
        if signal_message:
            print("\n" + "="*50)
            print("SIGNAL ALERT:")
            print("="*50)
            print(signal_message)
            print("="*50)
            print(f"Character count: {len(signal_message)}")
            
            print("\n📤 Discordにシグナルアラートを投稿中...")
            post_to_discord(signal_message)
    else:
        print("シグナル発動銘柄がありません。")
    
    
    return tweet



if __name__ == "__main__":
    main()
