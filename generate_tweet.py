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

# Import sector definitions from market_logic
from market_logic import SECTOR_DEFINITIONS, TICKER_TO_SECTOR

# Discord Webhook URL (環境変数から取得)
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '')

# Japanese sector name mapping (same as app)
SECTOR_JP_MAP = {
    "🧠 Semi: AI Compute & Logic": "半導体: AIコンピュート & ロジック",
    "🏗️ Semi: Equipment & Foundry": "半導体: 製造装置 & ファウンドリ",
    "🖥️ AI Infra: Server & Memory": "AIインフラ: サーバー & メモリ",
    "🔌 Semi: Analog & Power": "半導体: アナログ & パワー",
    "🧠 AI: Big Tech": "AI: ビッグテック",
    "🛡️ AI: Cybersecurity": "AI: サイバーセキュリティ",
    "☁️ AI: SaaS & Data Apps": "AI: SaaS & データアプリ",
    "🤖 Robotics & Automation": "ロボティクス & 自動化",
    "🪙 Crypto: Miners & Assets": "クリプト: マイナー & 資産",
    "💳 FinTech & Payments": "フィンテック: 決済",
    "🛡️ Defense: Major Contractors": "防衛: 大手請負",
    "🚀 Space & Future Mobility": "宇宙 & 次世代モビリティ",
    "🚁 Defense: Drones & Tech": "防衛: ドローン & テック",
    "☢️ Energy: Nuclear": "エネルギー: 原子力",
    "💡 Utilities: Regulated": "公益: 規制電力",
    "☀️ Energy: Solar & Clean Tech": "エネルギー: 太陽光 & クリーンテック",
    "🛢️ Energy: Integrated Majors": "エネルギー: 統合石油メジャー",
    "🏗️ Energy: E&P (Upstream)": "エネルギー: E&P (上流)",
    "🔧 Energy: Services & Equipment": "エネルギー: サービス & 設備",
    "🛤️ Energy: Midstream": "エネルギー: ミッドストリーム",
    "💊 BioPharma: Big Pharma & Obesity": "製薬: 大手製薬 & 肥満薬",
    "🧬 Biotech: Commercial Leaders": "バイオ: 商用リーダー",
    "🧪 Biotech: Gene & Cell Therapy": "バイオ: 遺伝子 & 細胞治療",
    "🔬 Biotech: Clinical & Growth": "バイオ: 臨床 & グロース",
    "🦾 MedTech & Devices": "医療機器 & デバイス",
    "🏥 Health Services & Insurers": "ヘルスケアサービス & 保険",
    "📱 MedTech: Digital Health & Services": "デジタルヘルス & サービス",
    "🍔 Consumer: Restaurants": "消費財: レストラン",
    "🥤 Consumer: Food & Bev Staples": "消費財: 食品 & 飲料",
    "🛒 Consumer: Retail & E-Com": "消費財: 小売 & Eコマース",
    "✈️ Consumer: Travel & Leisure": "消費財: 旅行 & レジャー",
    "👗 Consumer: Apparel & Luxury": "消費財: アパレル & ラグジュアリー",
    "🚗 Auto & EV": "自動車 & EV",
    "📡 Real Estate: Digital Infra": "不動産: デジタルインフラ",
    "🏘️ Real Estate: Traditional": "不動産: 伝統的REIT",
    "🏠 Homebuilders & Residential": "住宅建設 & 不動産",
    "🏛️ Finance: Mega Banks": "金融: メガバンク",
    "🏦 Finance: Regional Banks": "金融: 地方銀行",
    "📈 Finance: Capital Markets & PE": "金融: 資本市場 & PE",
    "💳 Finance: Credit Cards & Consumer": "金融: クレジットカード",
    "☂️ Finance: Insurance": "金融: 保険",
    "🏭 Industrials: Machinery": "資本財: 機械 & 製造",
    "✈️ Transport & Logistics": "輸送: 物流 & 輸送",
    "🏗️ Engineering & Construction": "建設: エンジニアリング",
    "🥇 Resources: Gold & Silver": "資源: 金 & 銀",
    "🏗️ Resources: Base Metals (Cu, Fe, Al)": "資源: ベースメタル (銅鉄アルミ)",
    "🔋 Resources: Battery & EV Materials": "資源: 電池材料 & EV素材",
    "🧲 Resources: Rare Earths & Specialty": "資源: レアアース & 特殊金属",
    "⚗️ Resources: Chemicals & Materials": "資源: 化学 & 素材",
    "💍 Resources: PGM & Royalty": "資源: 白金族 & ロイヤルティ",
    "⚛️ Tech: Quantum Computing": "テック: 量子コンピュータ"
}

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


def generate_comment(gainers, losers, sector_perf):
    """Generate a casual comment based on market conditions"""
    top_sector = sector_perf.index[0] if len(sector_perf) > 0 else None
    top_gainer = gainers[0][0] if gainers else None
    top_gain = gainers[0][1] if gainers else 0
    
    # Extract sector category from emoji+name format
    def get_sector_keyword(sector_name):
        if not sector_name:
            return None
        # Clean up sector name
        keywords = {
            'Semi': '半導体',
            'AI': 'AI・テック',
            'Crypto': 'マイニング',
            'Nuclear': '原子力',
            'Gold': '金鉱',
            'Silver': '銀鉱',
            'Resources': '資源',
            'Defense': '防衛',
            'Space': '宇宙',
            'Energy': 'エネルギー',
            'Tech': 'テック',
        }
        for key, val in keywords.items():
            if key in sector_name:
                return val
        return sector_name
    
    sector_keyword = get_sector_keyword(top_sector)
    
    # Comment patterns
    patterns = {
        'strong_sector': [
            f"{sector_keyword}が鬼つよでしたね〜(´∀｀∩)↑age↑",
            f"今日は{sector_keyword}の日！(*^^*)",
            f"{sector_keyword}強い日だった👍",
        ],
        'big_gainer': [
            f"${top_gainer}が爆上げ(´∀｀∩)↑age↑",
            f"${top_gainer}きましたねー！🔥",
            f"${top_gainer}つええー(*^^*)",
        ],
        'overall_strong': [
            "今日は全体的に調子よき👍",
            "全体的に強い日でしたね〜(*^^*)",
        ],
        'overall_weak': [
            "今日は厳しい1日でしたな…(´・ω・`)",
            "なかなか厳しい相場でしたね…",
        ],
    }
    
    # Decide which pattern to use
    avg_gain = sum([g[1] for g in gainers]) / len(gainers) if gainers else 0
    avg_loss = sum([l[1] for l in losers]) / len(losers) if losers else 0
    
    if top_gain > 10:
        comment = random.choice(patterns['big_gainer'])
    elif sector_keyword and sector_perf.iloc[0] > 2:
        comment = random.choice(patterns['strong_sector'])
    elif avg_gain > 2:
        comment = random.choice(patterns['overall_strong'])
    elif avg_loss < -3:
        comment = random.choice(patterns['overall_weak'])
    else:
        comment = random.choice(patterns['strong_sector'] if sector_keyword else patterns['overall_strong'])
    
    return comment

def format_tweet(gainers, losers, sector_perf):
    """Format the final tweet"""
    today = datetime.now().strftime("%m/%d")
    
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
        # Clean sector name (remove emoji prefix for brevity)
        clean_name = sector.split(':')[-1].strip() if ':' in sector else sector.lstrip('🧠🏗️🖥️🔌☁️🪙💳🛡️🚀🚁☢️💡☀️🛢️🔧🛤️💊🧬🔬🦾🏥📱🍔🥤🛒✈️👗🚗📡🏘️🏛️🏦📈💳☂️🏭⛽🥇💍⚗️🏠⚛️🤖 ')
        top_sector_lines.append(f"{clean_name} {ret:+.1f}%")
    
    # Bottom 5 sectors
    bottom_sectors = sector_perf.tail(5)[::-1]
    bottom_sector_lines = []
    for sector, ret in bottom_sectors.items():
        clean_name = sector.split(':')[-1].strip() if ':' in sector else sector.lstrip('🧠🏗️🖥️🔌☁️🪙💳🛡️🚀🚁☢️💡☀️🛢️🔧🛤️💊🧬🔬🦾🏥📱🍔🥤🛒✈️👗🚗📡🏘️🏛️🏦📈💳☂️🏭⛽🥇💍⚗️🏠⚛️🤖 ')
        bottom_sector_lines.append(f"{clean_name} {ret:+.1f}%")
    
    # Generate comment
    comment = generate_comment(gainers, losers, sector_perf)
    
    # Build tweet
    tweet = f"""📊 {today} 米国株マーケット速報

🔥 爆上げTOP5
{' | '.join(gainer_lines[:3])}
{' | '.join(gainer_lines[3:5])}

💀 下落TOP5
{' | '.join(loser_lines[:3])}
{' | '.join(loser_lines[3:5])}

📦 セクター上位5
🚀 {top_sector_lines[0]}
{' | '.join(top_sector_lines[1:3])}
{' | '.join(top_sector_lines[3:5])}

📦 セクター下位5
💨 {bottom_sector_lines[0]}
{' | '.join(bottom_sector_lines[1:3])}
{' | '.join(bottom_sector_lines[3:5])}

{comment}

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


def main():
    """Main function to generate tweet"""
    print("Loading cache data...")
    df = load_cache()
    
    print(f"Loaded {len(df)} tickers")
    
    # Get top movers
    gainers, losers = get_top_movers(df)
    print(f"\nTop Gainers: {gainers}")
    print(f"Top Losers: {losers}")
    
    # Get sector performance
    sector_perf = get_sector_performance(df)
    print(f"\nSector Performance (top 5):\n{sector_perf.head(5)}")
    print(f"\nSector Performance (bottom 5):\n{sector_perf.tail(5)}")
    
    # Generate tweet
    tweet = format_tweet(gainers, losers, sector_perf)
    
    print("\n" + "="*50)
    print("GENERATED TWEET:")
    print("="*50)
    print(tweet)
    print("="*50)
    print(f"Character count: {len(tweet)}")
    
    # Post to Discord
    print("\n📤 Discordに投稿中...")
    post_to_discord(tweet)
    
    return tweet


if __name__ == "__main__":
    main()
