import yfinance as yf
import pandas as pd
import numpy as np
import requests
from io import StringIO
import time
import random
import concurrent.futures
import re
from datetime import datetime
import json
import os
from deep_translator import GoogleTranslator

# --- Constants ---

# "Momentum Universe" - High Beta, Liquid, & Thematic Leaders

STATIC_MENU_ITEMS = [
    "--- 🌏 指数・為替・債券 (Indices/Forex/Bonds) ---",
    'USDJPY=X', '^TNX', 'BTC-USD', 'GLD',
    "--- 💻 米国株：AI・ハイテク (US Tech/AI) ---",
    'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'AAPL', 'META', 'AMD', 'PLTR', 'AVGO',
    "--- 📊 米国ETF：セクター (US Sector ETFs) ---",
    'QQQ', 'SPY', 'SMH', 'VGT', 'XLV', 'XLP', 'XLE', 'XLF',
    "--- 🚀 テーマ別ETF (Thematic ETFs) ---",
    'URA', 'COPX', 'QTUM', 'ARKX', 'NLR'
]

# Japanese sector name mapping
SECTOR_JP_MAP = {
    # --- 1. Semi & AI Compute ---
    "🧠 Semi: AI Compute & Logic": "🧠 半導体: AIコンピュート [Semi: Compute]",
    "🏗️ Semi: Front-End & Foundry Back-End, Test & Materials": "🏗️ 半導体: 製造/検査/素材 [Semi: Front/Back]",
    "🔌 Semi: Analog & Power (Ind)": "🔌 半導体: アナログ/パワー (産業) [Semi: Analog Ind]",
    "⚡ Semi: Auto & RF Power": "⚡ 半導体: 車載/RFパワ [Semi: Auto/RF]",
    
    # --- 2. AI Infrastructure ---
    "🖥️ AI Infra: Server & Compute": "🖥️ AIインフラ: サーバー [AI Infra: Server]",
    "💾 AI Infra: Storage & Memory": "💾 AIインフラ: ストレージ/メモリ [AI Infra: Storage]",
    "🌐 AI Infra: Networking & Optical": "🌐 AIインフラ: ネットワーク/光 [AI Infra: Network]",
    "❄️ AI Infra: Power & Cooling": "❄️ AIインフラ: 電力/冷却 [AI Infra: Power]",
    
    # --- 3. Software, SaaS & Cyber ---
    "👑 FANG+": "👑 FANG+ (米・大型テック10社) [FANG+]",
    "🏰 SaaS: Enterprise Giants": "🏰 SaaS: 巨大エンタープライズ [SaaS: Giants]",
    "⚙️ SaaS: Data & Dev": "⚙️ SaaS: データ基盤/Dev [SaaS: Data]",
    "📝 SaaS: Productivity": "📝 SaaS: 生産性/業務効率 [SaaS: Prod]",
    "🚀 SaaS: AI & Niche Apps": "🚀 SaaS: AI/ニッチアプリ [SaaS: AI/Niche]",
    "🛡️ Cyber: Leaders": "🛡️ サイバー: リーダー [Cyber: Leaders]",
    "🕵️ Cyber: Challengers": "🕵️ サイバー: チャレンジャー [Cyber: Challengers]",
    
    # --- 4. Crypto & Digital Assets ---
    "🪙 Crypto: Exchange & Staking": "🪙 クリプト: 取引所/ステーキング [Crypto: Exch]",
    "💻 Crypto: AI & HPC Pivot": "💻 クリプト: AI/HPC転換 [Crypto: AI/HPC]",
    "⛏️ Crypto: Pure Miners": "⛏️ クリプト: 純粋マイナー [Crypto: Miners]",
    
    # --- 5. FinTech ---
    "📱 FinTech: Consumer": "📱 フィンテック: 消費者向け [FinTech: Consumer]",
    "💳 FinTech: Infra & B2B": "💳 フィンテック: インフラ/B2B [FinTech: Infra]",
    "💸 FinTech: Lending": "💸 フィンテック: レンディング [FinTech: Lend]",
    
    # --- 6. Aerospace & Defense ---
    "🛡️ Defense: Primes": "🛡️ 防衛: プライム(完成品) [Defense: Primes]",
    "✈️ Aerospace: Suppliers": "✈️ 防衛: サプライヤー/部品 [Aero: Suppliers]",
    "💻 Defense: Gov Services": "💻 防衛: 政府ITサービス [Defense: Gov]",
    "🚀 Space: Leaders": "🚀 宇宙: リーダー [Space: Leaders]",
    "☄️ Space: Speculative": "☄️ 宇宙: 小型/投機的 [Space: Spec]",
    "🚁 Defense: Drones": "🚁 防衛: ドローン [Defense: Drones]",
    
    # --- 7. Energy: Nuclear & Utilities ---
    "☢️ Nuclear: Utilities": "☢️ 原子力: 電力会社 [Nuclear: Util]",
    "⚛️ Nuclear: Fuel & Tech": "⚛️ 原子力: 燃料/技術 [Nuclear: Tech]",
    "💡 Utilities: Growth": "💡 公益: グロース/DC電力 [Util: Growth]",
    "🏠 Utilities: Defensive": "🏠 公益: ディフェンシブ [Util: Defense]",
    "☀️ Energy: Solar": "☀️ エネルギー: 太陽光 [Energy: Solar]",
    "🔋 Energy: H2 & Battery": "🔋 エネルギー: 水素/電池 [Energy: H2/Batt]",
    
    # --- 8. Energy: Oil & Gas ---
    "🛢️ Energy: Majors": "🛢️ エネルギー: 石油メジャー [Energy: Majors]",
    "🏗️ Energy: E&P": "🏗️ エネルギー: E&P(採掘) [Energy: E&P]",
    "🔧 Energy: Services": "🔧 エネルギー: サービス [Energy: Svcs]",
    "🛤️ Energy: Midstream": "🛤️ エネルギー: ミッドストリーム [Energy: Mid]",
    
    # --- 9. Resources & Materials ---
    "🥇 Resources: Gold Majors": "🥇 資源: 金(メジャー) [Res: Gold Maj]",
    "🥈 Resources: Silver & Mid": "🥈 資源: 銀/中堅 [Res: Silver]",
    "🧨 Resources: Junior Miners": "🧨 資源: ジュニアマイナー [Res: Junior]",
    "👑 Resources: Royalty": "👑 資源: ロイヤルティ [Res: Royalty]",
    "🥉 Resources: Copper": "🥉 資源: 銅 [Res: Copper]",
    "🏗️ Resources: Steel & Aluminum": "🏗️ 資源: 鉄鋼/アルミ [Res: Steel/Al]",
    "🔋 Resources: Battery Chain": "🔋 資源: 電池サプライチェーン [Res: Batt Chain]",
    "🧲 Resources: Strategic": "🧲 資源: 戦略物資 [Res: Strategic]",
    "⚗️ Resources: Chem & Ag": "⚗️ 資源: 化学/農業 [Res: Chem/Ag]",
    "📦 Resources: Packaging": "📦 資源: 梱包/パッケージ [Res: Pkg]",
    
    # --- 10. Healthcare ---
    "💊 Pharma: Majors": "💊 製薬: メジャー [Pharma: Majors]",
    "🌍 Pharma: Global": "🌍 製薬: グローバル [Pharma: Global]",
    "🧬 Biotech: Leaders": "🧬 バイオ: リーダー [Bio: Leaders]",
    "🧪 Biotech: Clinical": "🧪 バイオ: 臨床/ゲノム [Bio: Clinical]",
    "🦾 MedTech: Devices": "🦾 医療機器: デバイス [MedTech: Dev]",
    "🔬 MedTech: Services": "🔬 医療機器: サービス/診断 [MedTech: Svcs]",
    "📱 MedTech: Digital": "📱 医療機器: デジタルヘルス [MedTech: Digital]",
    
    # --- 11. Consumer Staples ---
    "🥤 Consumer: Beverages": "🥤 消費財: 飲料 [Cons: Bev]",
    "🥪 Consumer: Food": "🥪 消費財: 食品 [Cons: Food]",
    "🚬 Consumer: Tobacco": "🚬 消費財: タバコ [Cons: Tob]",
    
    # --- 12. Retail & E-Commerce ---
    "🛒 Retail: Major": "🛒 小売: メジャー [Retail: Major]",
    "🛍️ Retail: Specialty": "🛍️ 小売: 専門店 [Retail: Spec]",
    "📦 E-Commerce: US": "📦 EC: 米国 [EC: US]",
    "🌏 E-Commerce: Global": "🌏 EC: グローバル [EC: Global]",
    "🐉 Asian Tech": "🐉 アジア: テック [Asia: Tech]",
    "🚗 Services: Gig Economy": "🚗 サービス: ギグエコノミー [Svcs: Gig]",
    "🍔 Restaurants: All": "🍔 レストラン: 外食 [Rest: All]",
    
    # --- 13. Travel & Goods ---
    "✈️ Travel: Platforms": "✈️ 旅行: プラットフォーム [Travel: Plat]",
    "🎰 Travel: Leisure": "🎰 旅行: レジャー/カジノ [Travel: Leis]",
    "🎮 Consumer: Media": "🎮 消費財: メディア/ゲーム [Cons: Media]",
    "👟 Consumer: Sportswear": "👟 消費財: スポーツウェア [Cons: Sport]",
    "💎 Consumer: Luxury": "💎 消費財: ラグジュアリー [Cons: Lux]",
    
    # --- 14. Auto & Mobility ---
    "⚡ Auto: EV Pure": "⚡ 自動車: EV専業 [Auto: EV]",
    "🚗 Auto: Legacy": "🚗 自動車: レガシー [Auto: Legacy]",
    "🤖 Auto: Tech": "🤖 自動車: 自動運転/テック [Auto: Tech]",
    "⚙️ Auto: Parts": "⚙️ 自動車: 部品 [Auto: Parts]",
    "🏪 Auto: Dealers": "🏪 自動車: ディーラー [Auto: Deal]",
    
    # --- 15. Housing & Infra ---
    "🏠 Housing: Builders": "🏠 住宅: ビルダー [House: Bldr]",
    "🔨 Housing: Products": "🔨 住宅: 建材 [House: Prod]",
    "📱 Housing: Tech": "📱 住宅: 不動産テック [House: Tech]",
    "⚡ Infra: Specialty": "⚡ インフラ: 専門工事 [Infra: Spec]",
    "🏗️ Infra: Civil": "🏗️ インフラ: 土木 [Infra: Civil]",
    
    # --- 16. Industrials & Transport ---
    "🚜 Industrials: Heavy": "🚜 資本財: 重機 [Ind: Heavy]",
    "🏢 Industrials: HVAC": "🏢 資本財: 空調 [Ind: HVAC]",
    "🏭 Industrials: Major": "🏭 資本財: 複合企業 [Ind: Major]",
    "🚂 Transport: Rail": "🚂 輸送: 鉄道 [Trans: Rail]",
    "🚚 Transport: Logistics": "🚚 輸送: 物流 [Trans: Log]",
    "✈️ Transport: Airlines": "✈️ 輸送: 航空 [Trans: Air]",
    "🚢 Transport: Shipping": "🚢 輸送: 海運 [Trans: Ship]",
    
    # --- 17. Future Tech ---
    "⚛️ Tech: Quantum": "⚛️ テック: 量子コンピュータ [Tech: Quantum]",
    "🤖 Robotics: Industrial": "🤖 ロボティクス: 産業用 [Robot: Ind]",
    "🦾 Robotics: Service": "🦾 ロボティクス: サービス [Robot: Svc]",
    "👓 Tech: ARVR": "👓 テック: AR/VR ウェアラブル端末 [Tech: ARVR]"    
}

# Signal Reason Japanese Mapping
REASON_JP_MAP = {
    "BB Break": "BB Break (ブレイク)",
    "50日高値更新": "New High (高値更新)",
    "Big Bounce": "Big Bounce (反発)",
    "MACD GC": "MACD GC (ゴールデンクロス)",
    "Early Turn (Hist↑)": "Early Turn (ヒストグラム改善)",
    "Dip Buy (押し目)": "Dip Buy (押し目)",  # Already has JP but mapping just in case
    "Profit Take (MACD DC)": "Profit Take (利確)",
    "Stop Loss (Chandelier)": "Stop Loss (損切り)"
}

# --- Major Indices Configuration ---
MAJOR_INDICES = {
    "^DJI": ("ダウ30", "📊"),
    "^GSPC": ("S&P500", "📈"), 
    "^NDX": ("ナス100", "💻"),
    "^RUT": ("ラッセル2000", "🏭"),
    "BTC-USD": ("ビットコイン", "🪙"),
    "GC=F": ("金", "🥇")
}

SECTOR_DEFINITIONS = {

    # =========================================================
    # 1. Semiconductor (Chips)
    # =========================================================

    # 1-A. AI Compute & Logic (Designers)
    # AIの頭脳。
    "🧠 Semi: AI Compute & Logic": [
        "AMD", "QCOM", "ARM", "INTC", "MRVL", "ALAB", "CRDO","CDNS", "SNPS"
    ],

    # 1-B. Semi Equipment: Front-End
    # 前工程製造装置・ファウンドリ。後工程・検査・素材
    "🏗️ Semi: Front-End & Foundry Back-End, Test & Materials": [
        "TSM", "ASML", "AMAT", "LRCX", "KLAC", "GFS", "UMC","ENTG", "AMKR", "ONTO", "CAMT"
    ],

    # 1-D. Analog & Power Semi (Industrial)
    # 産業機器向け。
    "🔌 Semi: Analog & Power (Ind)": [
        "TXN", "ADI", "STM", "MCHP", "SWKS"
    ],

    # 1-E. Analog & Power Semi (Auto/RF)
    # 車載・通信向け。
    "⚡ Semi: Auto & RF Power": [
        "ON", "NXPI", "QRVO", "SLAB", "WOLF"
    ],

    # =========================================================
    # 2. AI Infrastructure
    # =========================================================

    # 2-A. AI Server & Compute
    # サーバー筐体。
    "🖥️ AI Infra: Server & Compute": [
        "SMCI", "DELL", "HPE", "ORCL", "IBM", "CLS","CRWV","NBIS"
    ],

    # 2-B. AI Storage & Memory
    # ストレージ・メモリ。
    "💾 AI Infra: Storage & Memory": [
        "MU", "WDC", "STX", "PSTG", "NTAP", "SNDK"
    ],

    # 2-C. AI Networking
    # スイッチ・光通信。
    "🌐 AI Infra: Networking & Optical": [
        "ANET", "COHR", "LITE", "POET", "CIEN", "LUMN", "GLW","APH"
    ],

    # 2-D. AI Power & Thermal
    # 熱対策・電力管理。
    "❄️ AI Infra: Power & Cooling": [
        "VRT", "MOD", "NVT", "PH", "ETN"
    ],

    # =========================================================
    # 3. Software, SaaS & Cyber
    # =========================================================

    # 3-A. FANG+ (The Magnificent 10)
    # ビッグテック。
    "👑 FANG+": [
        "MSFT", "GOOGL", "META", "AMZN", "AAPL", "NFLX", 
        "NVDA", "AVGO", "PLTR", "CRWD"
    ],

    # 3-B. SaaS: Enterprise Giants
    # 巨大ソフトウェア。
    "🏰 SaaS: Enterprise Giants": [
        "ADBE", "CRM", "SAP", "NOW", "WDAY", "TEAM", "TTD", "APP"
    ],

    # 3-C. SaaS: Data Infrastructure
    # データ基盤。
    "⚙️ SaaS: Data & Dev": [
        "SNOW", "DDOG", "MDB", "ESTC", "CFLT", "GTLB", "IOT"
    ],

    # 3-D. SaaS: Productivity & Apps
    # 業務効率化。
    "📝 SaaS: Productivity": [
        "DOCU", "ZM", "BOX", "DBX", "ASAN", "FRSH", "HUBS"
    ],

    # 3-E. SaaS: AI & Niche
    # AI専業・ニッチ。
    "🚀 SaaS: AI & Niche Apps": [
        "AI", "SOUN", "BBAI", "KVYO", "UPWK", "DOCN", "RDDT", "DUOL"
    ],

    # 3-G. Cyber: Leaders
    # セキュリティ大手。
    "🛡️ Cyber: Leaders": [
        "PANW", "FTNT", "ZS", "OKTA", "NET", "CYBR"
    ],

    # 3-H. Cyber: Challengers
    # セキュリティ中堅。
    "🕵️ Cyber: Challengers": [
        "SENT", "GEN", "VRNS", "TENB", "QLYS"
    ],

    # =========================================================
    # 4. Crypto & Digital Assets
    # =========================================================
    # ここは銘柄数が多いので3つに分割して全銘柄格納

    # 4-A. Crypto: Exchange & Staking
    # 取引所・ステーキング・資産保有。
    "🪙 Crypto: Exchange & Staking": [
        "COIN", "MSTR", "GLXY", "BKKT", "BTBT", "CRCL", "BMNR", "FIGR", "BTCS", "CAN"
    ],

    # 4-B. Crypto: AI & HPC Pivot
    # 元マイナー→AIデータセンター転換組。
    "💻 Crypto: AI & HPC Pivot": [
        "CORZ", "HIVE", "IREN", "WULF", "APLD", "HUT", "BTDR", "CIFR", "BITF"
    ],

    # 4-C. Crypto: Pure Miners
    # 純粋なマイニング専業（ASIC）。
    "⛏️ Crypto: Pure Miners": [
        "MARA", "RIOT", "CLSK"
    ],

    # =========================================================
    # 5. FinTech
    # =========================================================

    # 5-A. FinTech: Consumer & Neobanks
    # 個人向け・ネオバンク。
    "📱 FinTech: Consumer": [
        "PYPL", "XYZ", "SOFI", "HOOD", "NU", "STNE", "XP", "DLO"
    ],

    # 5-B. FinTech: Infra & B2B
    # インフラ・法人。
    "💳 FinTech: Infra & B2B": [
        "FIS", "FISV", "GPN", "INTU", "BILL", "TOST", "FOUR", "PAGS"
    ],

    # 5-C. FinTech: Lending
    # 融資・国際送金。
    "💸 FinTech: Lending": [
        "AFRM", "UPST", "LC", "RELY", "INTR", "WU"
    ],

    # =========================================================
    # 6. Aerospace & Defense
    # =========================================================

    # 6-A. Defense Primes
    # 完成品メーカー。
    "🛡️ Defense: Primes": [
        "RTX", "LMT", "GD", "NOC", "LHX", "HII", "BA"
    ],

    # 6-B. Aerospace Suppliers
    # 部品・エンジン。
    "✈️ Aerospace: Suppliers": [
        "GE", "HWM", "TDG", "HEI", "TXT"
    ],

    # 6-C. Gov Services
    # 政府IT・コンサル。
    "💻 Defense: Gov Services": [
        "LDOS", "BAH", "CACI", "SAIC", "PSN"
    ],

    # 6-D. Space Leaders
    # 宇宙（主力）。
    "🚀 Space: Leaders": [
        "RKLB", "ASTS", "PL", "IRDM", "SATS", "LUNR"
    ],

    # 6-E. Space Speculative
    # 宇宙（小型）。
    "☄️ Space: Speculative": [
        "RDW", "SPIR", "MNTS", "SIDU", "VOYG", "GSAT", "VSAT"
    ],

    # 6-F. Drones
    # ドローン。
    "🚁 Defense: Drones": [
        "AVAV", "KTOS", "AXON", "RCAT", "PDYN", "POWW", "UMAC", "ONDS","ACHR","JOBY"
    ],

    # =========================================================
    # 7. Energy: Nuclear & Utilities
    # =========================================================

    # 7-A. Nuclear: Utilities
    # 原発稼働電力。
    "☢️ Nuclear: Utilities": [
        "VST", "CEG", "TLN", "PEG", "ETR"
    ],

    # 7-B. Nuclear: Fuel & Tech
    # ウラン・SMR。
    "⚛️ Nuclear: Fuel & Tech": [
        "CCJ", "UEC", "NXE", "LEU", "GEV", "BWXT", "OKLO", "SMR", "NNE", "UUUU"
    ],

    # 7-C. Utilities: Growth
    # データセンター公益。
    "💡 Utilities: Growth": [
        "NEE", "SO", "AEP", "D", "AES", "SRE"
    ],

    # 7-D. Utilities: Defensive
    # 安定公益。
    "🏠 Utilities: Defensive": [
        "ED", "XEL", "WEC", "ES", "EIX", "FE", "PPL", "CMS", "CNP"
    ],

    # 7-E. Clean Tech: Solar
    # 太陽光。
    "☀️ Energy: Solar": [
        "FSLR", "ENPH", "NXT", "ARRY", "SEDG", "RUN", "SHLS"
    ],

    # 7-F. Clean Tech: H2 & Battery
    # 水素・電池。
    "🔋 Energy: H2 & Battery": [
        "FLNC", "BE", "PLUG", "LIN", "STEM", "EOSE", "FCEL", "BLDP", "ENVX", "QS"
    ],

    # =========================================================
    # 8. Energy: Oil & Gas
    # =========================================================

    # 8-A. Oil Majors
    # メジャー。
    "🛢️ Energy: Majors": [
        "XOM", "CVX", "SHEL", "TTE", "BP", "EQNR", "PBR", "EC", "ENB"
    ],

    # 8-B. E&P
    # 掘削。
    "🏗️ Energy: E&P": [
        "EOG", "COP", "OXY", "DVN", "FANG", "CTRA", "APA", "AR", "EQT", "RRC"
    ],

    # 8-C. Services
    # サービス。
    "🔧 Energy: Services": [
        "SLB", "HAL", "BKR", "FTI", "NOV", "WHD", "LBRT", "RIG", "VAL"
    ],

    # 8-D. Midstream
    # パイプライン。
    "🛤️ Energy: Midstream": [
        "ET", "EPD", "KMI", "WMB", "TRP", "OKE", "PAA", "MPLX"
    ],

    # =========================================================
    # 9. Resources & Materials
    # =========================================================

    # 9-A. Gold Majors
    # 金（大手）。
    "🥇 Resources: Gold Majors": [
        "NEM", "GOLD", "AEM", "KGC", "AU", "GFI", "PHYS"
    ],

    # 9-B. Silver & Mid-Tier
    # 銀・中堅。
    "🥈 Resources: Silver & Mid": [
        "PAAS", "HL", "AG", "CDE", "EQX", "IAG", "PSLV"
    ],

    # 9-C. Junior Miners
    # 小型・探鉱。
    "🧨 Resources: Junior Miners": [
        "HYMC", "IDR", "NGD", "USAS", "EXK", "FSM", "VZLA", "SVM"
    ],

    # 9-D. Royalty
    # ロイヤルティ。
    "👑 Resources: Royalty": [
        "WPM", "FNV", "RGLD", "TFPM", "MTA", "SBSW", "PLG"
    ],

    # 9-E. Copper
    # 銅。
    "🥉 Resources: Copper": [
        "FCX", "SCCO", "TECK", "HBM", "ERO", "IE", "TGB"
    ],

    # 9-F. Steel & Aluminum
    # 鉄・アルミ。
    "🏗️ Resources: Steel & Aluminum": [
        "NUE", "CLF", "STLD", "AA", "CENX", "BHP", "RIO", "VALE"
    ],

    # 9-G. Battery Chain
    # 電池素材。
    "🔋 Resources: Battery Chain": [
        "ALB", "SQM", "LAC", "SGML", "TMC", "WWR", "NMG", "CC","TROX"
    ],

    # 9-H. Strategic Metals
    # 戦略物資。
    "🧲 Resources: Strategic": [
        "MP", "LYSDY", "UAMY", "PPTA", "IPX", "TMQ", "CRML", "EU", "USAR"
    ],

    # 9-I. Chemicals & Ag
    # 化学・農業。
    "⚗️ Resources: Chem & Ag": [
        "APD", "SHW", "CTVA", "NTR", "MOS", "CF", "DOW", "DD"
    ],

    # 9-J. Packaging
    # 包装。
    "📦 Resources: Packaging": [
        "SW", "IP", "PKG", "BALL", "AVY", "GPK", "AMCR"
    ],

    # =========================================================
    # 10. Healthcare
    # =========================================================

    # 10-A. Pharma Majors
    # 大手製薬。
    "💊 Pharma: Majors": [
        "LLY", "NVO", "JNJ", "MRK", "ABBV", "PFE", "AMGN", "BMY"
    ],

    # 10-B. Global Pharma
    # 海外製薬。
    "🌍 Pharma: Global": [
        "GILD", "AZN", "SNY", "TEVA", "NVS"
    ],

    # 10-C. Biotech Leaders
    # 黒字バイオ。
    "🧬 Biotech: Leaders": [
        "VRTX", "REGN", "BIIB", "INCY", "UTHR", "BMRN", "ALNY","DVA"
    ],

    # 10-D. Biotech Clinical
    # 臨床・ゲノム。
    "🧪 Biotech: Clinical": [
        "CRSP", "NTLA", "BEAM", "EDIT", "SRPT", "AXSM", "KOD", "VKTX", "MDGL","PLSE"
    ],

    # 10-E. MedTech Devices
    # 機器。
    "🦾 MedTech: Devices": [
        "ABT", "SYK", "MDT", "BSX", "EW", "DXCM", "GEHC", "RMD", "PODD"
    ],

    # 10-F. MedTech Services
    # サービス。
    "🔬 MedTech: Services": [
        "ZTS", "ILMN", "TMO", "DHR", "A", "BRKR", "UNH", "CVS", "HCA"
    ],

    # 10-G. Digital Health
    # AI医療。
    "📱 MedTech: Digital": [
        "HIMS", "TDOC", "DOCS", "OSCR", "ALHC", "SDGR", "RXRX", "TXG", "TEM"
    ],

    # =========================================================
    # 11. Consumer Staples
    # =========================================================

    # 11-A. Beverages
    # 飲料。
    "🥤 Consumer: Beverages": [
        "KO", "PEP", "MNST", "CELH", "KDP", "STZ", "TAP"
    ],

    # 11-B. Food
    # 食品。
    "🥪 Consumer: Food": [
        "MDLZ", "HSY", "GIS", "MKC", "KHC", "TSN", "CAG"
    ],

    # 11-C. Tobacco
    # タバコ。
    "🚬 Consumer: Tobacco": [
        "PM", "MO", "BTI", "UVV"
    ],

    # =========================================================
    # 12. Retail & E-Commerce
    # =========================================================

    # 12-A. Major Retail
    # 小売大手。
    "🛒 Retail: Major": [
        "WMT", "COST", "TGT", "HD", "LOW", "KR", "DG", "DLTR", "BJ"
    ],

    # 12-B. Specialty Retail
    # 専門店。
    "🛍️ Retail: Specialty": [
        "TJX", "ROST", "ULTA", "BBY", "TSCO", "ANF", "AEO", "KSS", "M"
    ],

    # 12-C. US E-Commerce
    # 米国EC。
    "📦 E-Commerce: US": [
        "AMZN", "SHOP", "EBAY", "CHWY", "ETSY", "W", "CART"
    ],

    # 12-D. Global E-Commerce
    # 海外EC。
    "🌏 E-Commerce: Global": [
        "BABA", "JD", "PDD", "MELI", "SE", "CPNG"
    ],

    # 12-E. Asian Tech
    # アジアテック。
    "🐉 Asian Tech": [
        "BILI", "TME", "VIPS", "VNET", "YMM", "GRAB", "WB"
    ],

    # 12-F. Gig Economy
    # シェアリング。
    "🚗 Services: Gig Economy": [
        "UBER", "DASH", "LYFT", "GRND", "MTCH", "ABNB"
    ],

    # 12-G. Restaurants
    # 外食。
    "🍔 Restaurants: All": [
        "MCD", "SBUX", "CMG", "CAVA", "YUM", "DRI", "DPZ", "WING", "SHAK"
    ],

    # =========================================================
    # 13. Travel & Goods
    # =========================================================

    # 13-A. Travel Platforms
    # 旅行。
    "✈️ Travel: Platforms": [
        "BKNG", "MAR", "HLT", "EXPE", "H", "WH", "TRIP"
    ],

    # 13-B. Leisure & Casino
    # レジャー。
    "🎰 Travel: Leisure": [
        "RCL", "CCL", "LVS", "MGM", "WYNN", "DKNG", "LYV", "NCLH", "CZR"
    ],

    # 13-C. Media & Gaming
    # メディア。
    "🎮 Consumer: Media": [
        "DIS", "SPOT", "TKO", "EA", "TTWO", "RBLX", "SONY", "WMG", "NTDOY","U"
    ],

    # 13-D. Sportswear
    # スポーツ。
    "👟 Consumer: Sportswear": [
        "NKE", "ONON", "DECK", "LULU", "CROX", "BIRK"
    ],

    # 13-E. Luxury
    # 高級品。
    "💎 Consumer: Luxury": [
        "LVMUY", "RACE", "HESAY", "TPR", "RL", "PVH", "VFC", "LEVI", "CPRI"
    ],

    # =========================================================
    # 14. Auto & Mobility
    # =========================================================

    # 14-A. EV Pure Plays
    # EV。
    "⚡ Auto: EV Pure": [
        "TSLA", "BYDDY", "RIVN", "LCID", "LI", "XPEV", "NIO", "PSNY"
    ],

    # 14-B. Legacy OEMs
    # 既存メーカー。
    "🚗 Auto: Legacy": [
        "TM", "F", "GM", "STLA", "HMC", "HOG"
    ],

    # 14-C. Auto Tech
    # 自動運転。
    "🤖 Auto: Tech": [
        "QS", "AUR", "HSAI", "OUST", "LAZR"
    ],

    # 14-D. Parts
    # 部品。
    "⚙️ Auto: Parts": [
        "APTV", "BWA", "GNTX", "ALV", "LEA", "GT", "GTX", "LKQ"
    ],

    # 14-E. Dealers
    # ディーラー。
    "🏪 Auto: Dealers": [
        "CVNA", "KMX", "AN", "LAD", "PAG"
    ],

    # =========================================================
    # 15. Housing & Infra
    # =========================================================

    # 15-A. Homebuilders
    # ビルダー。
    "🏠 Housing: Builders": [
        "DHI", "LEN", "PHM", "TOL", "NVR", "KBH", "TMHC", "MTH"
    ],

    # 15-B. Building Products
    # 建材。
    "🔨 Housing: Products": [
        "BLDR", "OC", "MAS", "TREX", "POOL", "EXP", "AOS"
    ],

    # 15-C. Real Estate Tech
    # 不動産テック。
    "📱 Housing: Tech": [
        "Z", "CSGP", "OPEN", "EXPI", "COMP", "FNF", "ARLO"
    ],

    # 15-D. Specialty Contractors
    # 専門工事。
    "⚡ Infra: Specialty": [
        "PWR", "EME", "FIX", "MTZ", "STRL", "IESC", "MYRG", "AGX", "PRIM"
    ],

    # 15-F. Heavy Civil
    # 土木。
    "🏗️ Infra: Civil": [
        "FLR", "GVA", "VMC", "MLM", "CRH"
    ],

    # =========================================================
    # 16. Industrials & Transport
    # =========================================================

    # 16-A. Heavy Machinery
    # 重機。
    "🚜 Industrials: Heavy": [
        "CAT", "DE", "URI", "CMI", "PCAR", "CNH", "OSK", "AGCO"
    ],

    # 16-B. Building Tech
    # 空調。
    "🏢 Industrials: HVAC": [
        "CARR", "TT", "OTIS", "JCI", "FIX", "LII", "XYL"
    ],

    # 16-C. Conglomerates
    # 複合。
    "🏭 Industrials: Major": [
        "HON", "EMR", "ITW", "MMM", "DOV", "AME", "ROK"
    ],

    # 16-D. Railroads
    # 鉄道。
    "🚂 Transport: Rail": [
        "UNP", "CSX", "NSC", "CNI", "CP"
    ],

    # 16-E. Logistics
    # 物流。
    "🚚 Transport: Logistics": [
        "UPS", "FDX", "ODFL", "XPO", "JBHT", "KNX", "SAIA", "CHRW"
    ],

    # 16-F. Airlines
    # 航空。
    "✈️ Transport: Airlines": [
        "DAL", "UAL", "LUV", "AAL", "ALK"
    ],

    # 16-G. Shipping
    # 海運。
    "🚢 Transport: Shipping": [
        "ZIM", "FRO", "STNG", "SBLK", "TRMD"
    ],

    # =========================================================
    # 17. Future Tech
    # =========================================================

    # 17-A. Quantum Computing
    # 量子。
    "⚛️ Tech: Quantum": [
        "IONQ", "QBTS", "RGTI", "QMCO", "ARQQ", "LAES", "QUBT"
    ],

    # 17-B. Industrial Robots
    # 産業用ロボット。
    "🤖 Robotics: Industrial": [
        "TER", "ROK", "DE", "CGNX", "ZBRA", "CLPT", "FANUY"
    ],

    # 17-C. Service & Software Robots
    # サービス・ソフトロボット。
    "🦾 Robotics: Service": [
        "PATH", "SYM", "SERV", "ISRG", "PRCT", "MBLY"
    ],

    # 18. AR/VR & Wearables
    # AR/VR ウェアラブル端末。
    " 👓 Future Tech: AR/VR & Wearables": [
        "KOPN", "VUZI", "MVIS", "HIMX", "SNAP", "UEIC","VRAR"
    ]
}


# Create Mapping
TICKER_TO_SECTOR = {}
for sector, tickers in SECTOR_DEFINITIONS.items():
    for t in tickers:
        TICKER_TO_SECTOR[t.upper()] = sector

STATIC_MOMENTUM_WATCHLIST = list(TICKER_TO_SECTOR.keys())

# --- Thematic ETF List (Metrics Benchmark) ---
THEMATIC_ETFS = {
    # --- 🤖 Future Tech (High Growth) ---
    "Cloud Computing": "CLOU",
    "Cybersecurity": "CIBR",
    "Robotics & AI": "BOTZ",
    "Semiconductors": "SMH",
    "Genomics": "GNOM",
    "Healthcare Providers": "IHF",
    "Medical Devices": "IHI",

    # --- 🛒 消費・トレンド (Consumer) ---
    "E-commerce": "IBUY",
    "Fintech": "FINX",
    "Millennials": "MILN",
    "Homebuilders": "XHB",
    
    # --- 🛡️ ディフェンシブ・マクロ (Defensive/Macro) ---
    "Healthcare": "XLV",
    "Consumer Staples": "XLP",
    "Utilities": "XLU",
    "High Dividend": "VYM",
    "Treasury 20Y+": "TLT",
    "VIX Short-Term": "VIXY", 

    # --- ⛏️ コモディティ・暗号資産 (Hard Assets) ---
    "Gold": "GLD",
    "Silver": "SLV",
    "Oil & Gas": "XOP",
    "Copper Miners": "COPX",
    "Bitcoin Strategy": "BITO"
}

# Extend Static List with ETFs & Register to Sector Map
for name, ticker in THEMATIC_ETFS.items():
    if ticker not in STATIC_MOMENTUM_WATCHLIST:
        STATIC_MOMENTUM_WATCHLIST.append(ticker)
    TICKER_TO_SECTOR[ticker] = name

# --- Functions ---

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def get_momentum_candidates(mode="hybrid"):
    """
    Builds a 'Momentum Universe' candidates list.
    Performance Optimization: Parallelized scraping.
    Returns: List of unique ticker strings.
    """
    
    # 1. Dynamic Sources (Yahoo Finance)
    sources = [
        "https://finance.yahoo.com/markets/stocks/gainers/",
        "https://finance.yahoo.com/markets/stocks/most-active/",
        "https://finance.yahoo.com/markets/stocks/52-week-gainers/",
        "https://finance.yahoo.com/markets/stocks/52-week-losers/" # Added for Reversal Hunting
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    all_candidates = set()
    
    # Add Static List first
    for t in STATIC_MOMENTUM_WATCHLIST:
        all_candidates.add(t)
    
    # Add ETFs
    for t in THEMATIC_ETFS.values():
        all_candidates.add(t)

    # Scrape Dynamic Movers (Parallel)
    # print("Scraping Dynamic Sources...")
    
    def fetch_source(url):
        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            dfs = pd.read_html(StringIO(response.text))
            if dfs:
                return dfs[0]
        except Exception:
            # print(f"Source fetch failed {url}: {e}")
            return None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor: # Increased workers
        futures = {executor.submit(fetch_source, url): url for url in sources}
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            if df is not None:
                 # Yahoo usually has 'Symbol' and 'Name'
                if 'Symbol' in df.columns:
                    # Take top 50 (was 15) to catch early/smaller moves
                    top_df = df.head(50)
                    for _, row in top_df.iterrows():
                        sym = str(row['Symbol']).split()[0]
                        all_candidates.add(sym)

    return list(all_candidates)

def calculate_momentum_metrics(tickers):
    """
    Calculates detailed metrics for the given tickers.
    Optimized for batch processing (offline).
    """
    if not tickers:
        return None, None

    # Download 1y data to calculate long-term MA and 1y return
    # Download 1y data to calculate long-term MA and 1y return
    # Optimized Chunking to avoid Rate Limits
    chunk_size = 10 # Extremely conservative batch size
    dfs = []
    
    print(f"Fetching data for {len(tickers)} tickers in chunks of {chunk_size}...")
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # Variable sleep to mimic human behavior slightly and respect limits
            time.sleep(5.0) 
            
            # Re-enabling threads for speed within small batches, but carefully
            # Disable threads to avoid 429 errors
            batch_data = yf.download(chunk, period="1y", group_by='ticker', auto_adjust=True, progress=False, threads=False)
            
            if not batch_data.empty:
                dfs.append(batch_data)
                
        except Exception as e:
            print(f"Batch fetch failed: {e}")
            continue

    if not dfs:
        print("No data fetched.")
        return None, None

    # Combine all batches
    try:
        if len(dfs) > 1:
            # Determine if we need to adjust columns for concatenation
            # (Usually yf.download(group_by='ticker') returns consistent MultiIndex)
            df = pd.concat(dfs, axis=1)
        else:
            df = dfs[0]
            
    except Exception as e:
        print(f"Data merge error: {e}")
        return None, None

    stats_list = []
    history_dict = {}

    for t in tickers:
        try:
            # Handle df structure
            t_data = pd.DataFrame()
            if isinstance(df.columns, pd.MultiIndex):
                if t in df.columns.levels[0]:
                    t_data = df[t]
                elif t in df.columns:
                     t_data = df[t]
            else:
                 t_data = df

            if t_data.empty: continue
            if 'Close' not in t_data.columns: continue

            t_data = t_data.dropna()
            if t_data.empty: continue
            
            # --- 1. Validation Filter (Integrated) ---
            # Check most recent data point
            current_price = t_data['Close'].iloc[-1]
            current_vol = t_data['Volume'].iloc[-1]
            
            # For Static List: We skipping filter (Assume they are valid)
            # Normalize to avoid key errors
            t_clean = t.upper()
            if t_clean not in STATIC_MOMENTUM_WATCHLIST:
                # For Dynamic: Strict Penny Filter
                if current_price < 2.0 or current_vol < 200000:
                    continue

            # --- 2. Calculations ---
            metrics = {}
            metrics['Ticker'] = t
            metrics['Price'] = current_price
            
            # Returns
            def get_ret(days):
                if len(t_data) < days: return 0.0
                return (current_price - t_data['Close'].iloc[-days]) / t_data['Close'].iloc[-days] * 100

            metrics['1d'] = get_ret(2)
            metrics['5d'] = get_ret(5)
            metrics['1mo'] = get_ret(21)
            metrics['3mo'] = get_ret(63)
            metrics['6mo'] = get_ret(126)
            
            # YTD
            current_date = t_data.index[-1]
            current_year = current_date.year
            
            # Find last trading day of previous year
            # Condition: Year < current_year
            prev_year_data = t_data[t_data.index.year < current_year]
            
            if not prev_year_data.empty:
                # Benchmark is Close of Last Year
                base_price = prev_year_data['Close'].iloc[-1]
                metrics['YTD'] = (current_price - base_price) / base_price * 100
            else:
                # Fallback for new listings in current year: Use First Day Open
                ytd_data = t_data[t_data.index.year == current_year]
                if not ytd_data.empty:
                    base_price = ytd_data['Open'].iloc[0] # Use Open instead of Close for first day
                    metrics['YTD'] = (current_price - base_price) / base_price * 100
                else:
                    metrics['YTD'] = 0.0

            if len(t_data) >= 252:
                metrics['1y'] = get_ret(252)
            else:
                # For < 1y data, return from start
                metrics['1y'] = (current_price - t_data['Close'].iloc[0]) / t_data['Close'].iloc[0] * 100
            
            # RVOL
            if len(t_data) > 21:
                avg_vol_20 = t_data['Volume'].iloc[-21:-1].mean()
                rvol = (current_vol / avg_vol_20) if (not pd.isna(avg_vol_20) and avg_vol_20 != 0) else 0
            else:
                rvol = 0
            metrics['RVOL'] = rvol
            
            # --- Advanced Technicals ---
            
            # 1. Moving Averages & Crosses
            sma50 = t_data['Close'].rolling(window=50).mean()
            sma200 = t_data['Close'].rolling(window=200).mean()
            
            metrics['SMA50'] = sma50.iloc[-1] if len(t_data) >= 50 else 0
            metrics['SMA200'] = sma200.iloc[-1] if len(t_data) >= 200 else 0
            metrics['Above_SMA50'] = metrics['Price'] > metrics['SMA50']
            
            # Golden Cross / Death Cross Checks (Last 5 days)
            metrics['GC_Just_Now'] = False
            metrics['DC_Just_Now'] = False
            
            if len(t_data) >= 200:
                cross_window = 5
                # Check for 50MA crossed 200MA in the last few days
                # Iterate last few days (index -5 to -1)
                for i in range(2, cross_window + 2):
                    if (len(sma50) > i) and (len(sma200) > i):
                        was_above = sma50.iloc[-i] > sma200.iloc[-i]
                        is_above = sma50.iloc[-i+1] > sma200.iloc[-i+1]
                        
                        if not was_above and is_above:
                            metrics['GC_Just_Now'] = True
                        if was_above and not is_above:
                            metrics['DC_Just_Now'] = True

            # 2. Bollinger Bands (20, 2)
            if len(t_data) >= 20:
                sma20 = t_data['Close'].rolling(window=20).mean()
                std20 = t_data['Close'].rolling(window=20).std()
                bb_upper = sma20 + 2 * std20
                bb_lower = sma20 - 2 * std20
                bb_current_upper = bb_upper.iloc[-1]
                bb_current_lower = bb_lower.iloc[-1]
                
                # Check for div by zero
                if pd.isna(sma20.iloc[-1]) or sma20.iloc[-1] == 0:
                     bb_width = 1.0
                else:
                     bb_width = (bb_current_upper - bb_current_lower) / sma20.iloc[-1]
                
                metrics['BB_Upper'] = bb_current_upper
                metrics['BB_Lower'] = bb_current_lower
                metrics['BB_Width'] = bb_width
                
                # Squeeze: Current width < 0.8 * Average Width(20)
                bb_width_series = (bb_upper - bb_lower) / sma20
                avg_width_20 = bb_width_series.rolling(window=20).mean().iloc[-1]
                
                # Squeeze condition threshold
                squeeze_threshold = avg_width_20 * 0.8 if not pd.isna(avg_width_20) else 0.0
                metrics['Is_Squeeze'] = metrics['BB_Width'] < squeeze_threshold
                
                # Calculate Squeeze Duration (How many consecutive days)
                squeeze_days = 0
                if metrics['Is_Squeeze']:
                    squeeze_days = 1
                    # Look back up to 20 days
                    recent_widths = bb_width_series.iloc[-20:-1][::-1] # Reverse to check backwards from yesterday
                    # Check against dynamic threshold (using historical rolling mean would be more accurate but expensive)
                    # Approximation: Use current threshold
                    for w in recent_widths:
                        if w < squeeze_threshold:
                            squeeze_days += 1
                        else:
                            break
                metrics['Squeeze_Days'] = squeeze_days

            else:
                metrics['BB_Upper'] = 999999
                metrics['BB_Lower'] = 0
                metrics['BB_Width'] = 1.0
                metrics['Is_Squeeze'] = False
                metrics['Squeeze_Days'] = 0

            # 3. 52-Week High/Low
            metrics['High52'] = t_data['Close'].max()
            metrics['Low52'] = t_data['Close'].min()
            
            # Max Drawdown (1y)
            # Calculate running max
            running_max = t_data['Close'].cummax()
            drawdown = (t_data['Close'] - running_max) / running_max
            # Max drawdown is the minimum value (e.g., -0.25 for -25%)
            # We store it as positive percentage (25.0)
            metrics['MaxDD'] = abs(drawdown.min()) * 100 if not drawdown.empty else 0.0

            # RSI
            rsi_series = calculate_rsi(t_data['Close'], 14)
            metrics['RSI'] = rsi_series.iloc[-1] if not rsi_series.empty else 50
            
            # Signals
            signals = []
            if metrics['RVOL'] > 2.0: signals.append('⚡')
            if metrics['Above_SMA50'] and metrics['3mo'] > 0: signals.append('🐂')
            
            # 🛒 Dip Buy: Uptrend (Above SMA50) but Short-term cool (RSI < 45)
            if metrics['Above_SMA50'] and metrics['RSI'] < 45: signals.append('🛒')

            # 🐻 Bear Trend: Downtrend (Below SMA50) & Negative Mom
            if not metrics['Above_SMA50'] and metrics['3mo'] < 0: signals.append('🐻')

            if metrics['RSI'] > 70: signals.append('🔥')
            if metrics['RSI'] < 30: signals.append('🧊')
            
            # New Signals
            if metrics['GC_Just_Now']: signals.append('✨')
            if metrics['DC_Just_Now']: signals.append('💀')
            if metrics['Is_Squeeze']: signals.append('🤐')
            # Check price vs High52 for New Highs (approaching high)
            if metrics['Price'] >= metrics['High52'] * 0.98: signals.append('🚀')

            metrics['Signal'] = "".join(signals)
            
            stats_list.append(metrics)
            
            # Save full OHLCV history for signal detection (not just normalized close)
            history_dict[t] = t_data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()

        except Exception as e:
            # print(f"Error calc {t}: {e}")
            continue

    # --- Fetch Fundamentals (ShortRatio + Crash Risk Indicators) for valid tickers ---
    if stats_list:
        valid_tickers = [m['Ticker'] for m in stats_list]
        
        def get_fund(tick):
            """Fetch fundamental data including crash risk indicators."""
            try:
                inf = yf.Ticker(tick).info
                return (
                    tick, 
                    inf.get('shortRatio', 0),
                    inf.get('heldPercentInstitutions', 0),  # Institutional ownership %
                    inf.get('heldPercentInsiders', 0),       # Insider ownership %
                    inf.get('floatShares', 0),               # Float shares
                    inf.get('beta', 1.0),                    # Beta (volatility)
                    inf.get('forwardPE', 0),                 # Forward P/E ratio
                    inf.get('marketCap', 0),                 # Market cap for context
                )
            except:
                return (tick, 0, 0, 0, 0, 1.0, 0, 0)
        
        # Limit max workers to avoid overload
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
             results = list(executor.map(get_fund, valid_tickers))
        
        # Build fund_map with all indicators
        fund_map = {}
        for r in results:
            fund_map[r[0]] = {
                'ShortRatio': r[1] if r[1] else 0,
                'InstOwnership': r[2] if r[2] else 0,
                'InsiderOwnership': r[3] if r[3] else 0,
                'Float': r[4] if r[4] else 0,
                'Beta': r[5] if r[5] else 1.0,
                'ForwardPE': r[6] if r[6] else 0,
                'MarketCap': r[7] if r[7] else 0,
            }
        
        for m in stats_list:
            t = m['Ticker']
            fund_data = fund_map.get(t, {})
            m['ShortRatio'] = fund_data.get('ShortRatio', 0)
            m['InstOwnership'] = fund_data.get('InstOwnership', 0)
            m['InsiderOwnership'] = fund_data.get('InsiderOwnership', 0)
            m['Float'] = fund_data.get('Float', 0)
            m['Beta'] = fund_data.get('Beta', 1.0)
            m['ForwardPE'] = fund_data.get('ForwardPE', 0)
            m['MarketCap'] = fund_data.get('MarketCap', 0)
            
            # Calculate SMA50 deviation (for crash risk)
            price = m.get('Price', 0)
            sma50 = m.get('SMA50', 0)
            if sma50 > 0:
                m['SMA50_Deviation'] = ((price - sma50) / sma50) * 100
            else:
                m['SMA50_Deviation'] = 0
            
    if not stats_list:
        return None, None
        
    df_metrics = pd.DataFrame(stats_list)
    
    # --- RS Rating Calculation (Percentile of 1y Return) ---
    # Calculates Relative Strength Rating against the universe (0-99)
    if '1y' in df_metrics.columns:
        df_metrics['RS_Rating'] = df_metrics['1y'].rank(pct=True) * 99
    else:
        df_metrics['RS_Rating'] = 50 # Default if no 1y data
    # Ensure all new columns are present
    cols = [
        'Ticker', 'Signal', 'Price', '1d', '5d', '1mo', '3mo', '6mo', 'YTD', '1y', 
        'RVOL', 'RSI', 'ShortRatio', 
        'High52', 'Low52', 'SMA50', 'SMA200', 'BB_Upper', 'BB_Lower', 'Is_Squeeze', 'BB_Width',
        'GC_Just_Now', 'DC_Just_Now', 'Above_SMA50',
        # New crash risk indicators
        'InstOwnership', 'InsiderOwnership', 'Float', 'Beta', 'ForwardPE', 'MarketCap', 'SMA50_Deviation'
    ]
    
    # Filter to only existing columns (safeguard) but we just added them to dict so they should exist
    # If some failed to calc (e.g. key error), we rely on pandas to fill NaN
    
    # We want these specific columns order if possible, but keep others if we missed any
    existing_cols = df_metrics.columns.tolist()
    final_cols = []
    for c in cols:
        if c in existing_cols:
            final_cols.append(c)
        else:
             # If missing in DF (shouldn't happen if we added to metrics), ignore or add NaN?
             # Better to let simple assignment work
             pass
    
    # Add any other columns we might have missed in `cols` list but are in `df_metrics` (like history or debugs)?
    # Nah, we want a clean DF.
    
    df_metrics = df_metrics[final_cols]
    
    return df_metrics, history_dict

def check_opportunity_alerts(df, period='3mo', top_n=10):
    """
    Checks for 'Opportunity Alert':
    1. Ticker is in Top N for Today, Yesterday, and 2 Days Ago (Persistence).
    2. Volume Spike: Current Volume > 2.0 * 20-day Average (RVOL > 2.0).
    """
    try:
        # T0 (Today)
        metrics_t0, _ = calculate_momentum_metrics(df)
        if metrics_t0.empty: return []
        top_t0 = metrics_t0.sort_values(period, ascending=False).head(top_n)['Ticker'].tolist()
        
        # T-1 (Yesterday)
        # Check sufficient length
        if len(df) <= 1: return [] # Cannot slice
        
        # Slice MultiIndex df: we need to slice on the index (Date) level
        # df.iloc[:-1] works for MultiIndex columns if the index is Date
        df_t1 = df.iloc[:-1]
        metrics_t1, _ = calculate_momentum_metrics(df_t1)
        if metrics_t1.empty: return []
        top_t1 = metrics_t1.sort_values(period, ascending=False).head(top_n)['Ticker'].tolist()
        
        # T-2 (2 Days Ago)
        if len(df) <= 2: return []
        df_t2 = df.iloc[:-2]
        metrics_t2, _ = calculate_momentum_metrics(df_t2)
        if metrics_t2.empty: return []
        top_t2 = metrics_t2.sort_values(period, ascending=False).head(top_n)['Ticker'].tolist()
        
        # Intersection
        persistent_tickers = set(top_t0) & set(top_t1) & set(top_t2)
        
        alerts = []
        for t in persistent_tickers:
            row = metrics_t0[metrics_t0['Ticker'] == t].iloc[0]
            rvol = row.get('RVOL', 0)
            
            # If RVOL wasn't calculated for some reason, try manual check or skip
            if rvol >= 2.0:
                alerts.append({
                    'Ticker': t,
                    'Gain': row.get(period, 0),
                    'RVOL': rvol
                })
                
        return alerts

    except Exception as e:
        # print(f"Alert Check Failed: {e}")
        return []

# --- AI Stock Recommendation Scoring ---

# Sector to ETF Mapping (for sector momentum scoring)
SECTOR_TO_ETF = {
    "🖥️ AI: Hardware & Cloud Infra": "SMH",
    "🧠 AI: Software & SaaS": "CLOU",
    "💸 Crypto & FinTech": "FINX",
    "🌌 Space & Defense": "ITA",  # iShares Aerospace & Defense
    "☢️ Energy: Nuclear": "XLU",
    "⚡ Energy: Power & Renewables": "XLU",
    "🛢️ Energy: Oil & Gas": "XOP",
    "💊 BioPharma: Big Pharma & Obesity": "XLV",
    "🧬 BioPharma: Biotech & Gene": "GNOM",
    "🏥 MedTech & Health": "IHF",
    "🍔 Consumer: Food & Bev": "XLP",
    "🛒 Consumer: Retail & E-Com": "IBUY",
    "👗 Consumer: Apparel & Leisure": "MILN",
    "🚗 Auto & EV": "MILN",
    "🏘️ Real Estate & REITs": "VNQ",
    "🏦 Finance: Banks & Capital": "XLF",
    "🏗️ Industrials & Transport": "XLI",
    "⛏️ Resources & Materials": "XLB",
    "🏠 Homebuilders & Residential": "XHB",
    "⚛️ Tech: Quantum Computing": "SMH",
}

def _normalize_score(value, min_val, max_val):
    """Normalize a value to 0-100 scale."""
    if max_val == min_val:
        return 50
    return max(0, min(100, (value - min_val) / (max_val - min_val) * 100))

def _rsi_score(rsi, min_ideal, max_ideal):
    """Score RSI based on ideal range. Returns 0-100."""
    if min_ideal <= rsi <= max_ideal:
        return 100  # Perfect range
    elif rsi < min_ideal:
        return max(0, 100 - (min_ideal - rsi) * 3)  # Penalize oversold
    else:
        return max(0, 100 - (rsi - max_ideal) * 3)  # Penalize overbought

def calculate_crash_risk_score(row):
    """
    Calculate crash risk score (0-100, higher = more risky).
    Optimized for momentum investing - focus on REAL risks, not growth stock characteristics.
    
    REAL Risk factors:
    - RSI > 85: Extreme overbought (only very extreme)
    - SMA50 deviation > 40%: Severely overextended
    - Volume-less new high: Price up without volume (fake move)
    - Sector weakness: Individual stock strong but sector ETF weak
    
    REMOVED (not real risks for momentum):
    - Forward P/E (growth stocks always expensive)
    - Institution ownership high (not necessarily bad)
    - ShortRatio high (squeeze fuel, not risk)
    """
    risk = 0
    
    # 1. RSI extreme overheating (only 85+)
    rsi = row.get('RSI', 50)
    if rsi > 90:
        risk += 25  # Very extreme
    elif rsi > 85:
        risk += 10  # Extended
    # RSI 70-85 is NORMAL for momentum stocks (bandwalk)
    
    # 2. SMA50 deviation (severely overextended only)
    sma50_dev = row.get('SMA50_Deviation', 0)
    if sma50_dev > 50:
        risk += 25  # Way too extended
    elif sma50_dev > 40:
        risk += 15
    elif sma50_dev > 30:
        risk += 5   # Just starting to extend
    
    # 3. Volume-less move (fake breakout risk)
    rvol = row.get('RVOL', 1.0)
    ret_5d = row.get('5d', 0)
    if ret_5d > 10 and rvol < 0.8:
        # Big move without volume = potential fake out
        risk += 20
    elif ret_5d > 5 and rvol < 0.7:
        risk += 10
    
    # 4. Extremely high beta (above 4 = casino stock)
    beta = row.get('Beta', 1.0)
    if beta > 4:
        risk += 15
    elif beta > 3.5:
        risk += 5
    
    # 5. Dead cat bounce pattern: Big drop followed by weak bounce
    ret_1mo = row.get('1mo', 0)
    ret_3mo = row.get('3mo', 0)
    if ret_3mo < -20 and ret_1mo > 0 and ret_1mo < 10:
        # Down big in 3mo, small bounce in 1mo = potential dead cat
        risk += 15
    
    return min(100, risk)

def calculate_short_term_score(row, df_all, etf_perf, regime='neutral'):
    """
    Calculate short-term (swing) score for a stock.
    Optimized for momentum investing.
    Weights: RVOL=30%, High52=20%, 5d=20%, RSI=10%, SMA50=5%, News=10%, SectorETF=5%
    """
    score = 0
    details = []
    
    # RVOL (30%) - MOST IMPORTANT. Volume confirms the move.
    rvol = min(row.get('RVOL', 0), 5.0)  # Cap at 5x
    pts = 0.30 * _normalize_score(rvol, 0.5, 5.0)
    score += pts
    details.append(f"RVOL({rvol:.1f}x): +{pts:.1f}")
    
    # High52 proximity (20%) - New highs = strongest signal
    price = row.get('Price', 0)
    high52 = row.get('High52', price)
    if high52 > 0:
        proximity = (price / high52) * 100
        # Bonus for breaking 52w high
        if proximity >= 100:
            pts = 20
            score += pts  # Max score for new highs
            details.append("新高値更新: +20")
        else:
            pts = 0.20 * _normalize_score(proximity, 80, 100)
            score += pts
            details.append(f"高値接近({proximity:.1f}%): +{pts:.1f}")
    
    # 5d Return (20%) - Recent momentum (Granular Bell Curve)
    ret_5d = row.get('5d', 0)
    
    if ret_5d > 60:
         # Too extended
         score -= 10
         details.append(f"⚠️短期過熱({ret_5d:.1f}%): -10")
    elif ret_5d > 40:
         # Very hot, high risk
         score += 0
         details.append(f"短期高値圏({ret_5d:.1f}%): 0")
    elif ret_5d > 20:
         # Strong, getting hot
         score += 15
         details.append(f"5日急伸({ret_5d:.1f}%): +15")
    elif ret_5d > 10:
         # Sweet Spot (The meat of the move)
         score += 20
         details.append(f"5日最適({ret_5d:.1f}%): +20")
    elif ret_5d > 5:
         # Just starting
         score += 10
         details.append(f"5日初動({ret_5d:.1f}%): +10")
    elif ret_5d > 0:
         # Slow drift
         score += 5
         details.append(f"5日微増({ret_5d:.1f}%): +5")
    
    # RSI 50-80 range (10%) - Allow bandwalk (strong trends stay overbought)
    rsi = row.get('RSI', 50)
    if 50 <= rsi <= 75:
        score += 10
        details.append(f"RSI適正({rsi:.0f}): +10")
    elif 75 < rsi <= 90: # Relaxed threshold from 85 to 90
        score += 5   # Give small bonus even for high RSI in strong momentum
        details.append(f"RSI強気圏({rsi:.0f}): +5")
    elif rsi > 90:
        score -= 10
        details.append(f"⚠️RSI過熱({rsi:.0f}): -10")
    elif 40 <= rsi < 50:
        score += 5
        details.append(f"RSI中立({rsi:.0f}): +5")
    else:
        score += 2
        details.append(f"RSI弱({rsi:.0f}): +2")
    
    # Above SMA50 (5%) - Trend filter
    above_sma50 = row.get('Above_SMA50', False)
    if above_sma50:
        score += 5
        details.append("SMA50上: +5")
    
    # News presence (10%) - Catalyst
    has_news = row.get('HasNews', False)
    if has_news:
        score += 10
        details.append("News: +10")
    
    # Sector ETF 5d (5%) - Sector tailwind & Relative Strength
    sector = TICKER_TO_SECTOR.get(row.get('Ticker', '').upper(), '')
    etf = SECTOR_TO_ETF.get(sector, None)
    if etf and etf in etf_perf:
        etf_5d = etf_perf[etf].get('5d', 0)
        # 1. Sector Tailwind check
        pts = 0.05 * _normalize_score(etf_5d, -5, 10)
        score += pts
        details.append(f"セクター({etf_5d:.1f}%): +{pts:.1f}")
        
        # 2. Relative Strength (Alpha) vs Sector (Optional Bonus)
        # If stock is outperforming its sector significantly
        alpha = ret_5d - etf_5d
        if alpha > 5.0:
            score += 5
            details.append(f"対セクター強(+{alpha:.1f}%): +5")
        elif alpha < -5.0:
            score -= 5
            details.append(f"対セクター弱({alpha:.1f}%): -5")
    
    # --- Distribution / Churn Check ---
    # Stricter Effort vs Result Logic
    rvol = row.get('RVOL', 1.0)
    if rvol > 3.0 and ret_5d < 2.0:
        score -= 15
        details.append("⚠️空回り(Vol過大/株価不振): -15")
    elif rvol > 1.5 and ret_5d < 2.0:
        score -= 12
        details.append("⚠️Vol増/不振: -12")
    elif rvol > 1.2 and ret_5d < 0:
        # Modest volume increase + negative return = selling pressure
        score -= 8
        details.append("⚠️売り圧力: -8")
    
    # --- Crash Risk Penalty (Relaxed for momentum) ---
    crash_risk = calculate_crash_risk_score(row)
    if crash_risk > 70:
        pts = 0.10 * (crash_risk / 100) * 100
        score -= pts
        details.append(f"暴落リスク({crash_risk:.0f}): -{pts:.1f}")
    
    # --- Regime Adjustments (5-Level) ---
    if 'greed' in regime: # extreme_greed or greed
        # Bull Mode: Boost RVOL
        # FIX: Only boost if price is actually moving UP (Avoid boosting distribution/churn)
        if rvol > 3.0 and ret_5d > 2.0: 
            score += 15 # Huge bonus for explosive volume WITH price action
            details.append("🐂Greed Vol Bonus High: +15")
        elif rvol > 2.0 and ret_5d > 0:
            score += 5
            details.append("🐂Greed Vol Bonus: +5")
            
        # Reduce crash risk penalty
        score = max(0, score + 5)
        details.append("🐂Greed Bonus: +5")
        
        # Symmetrical Penalty: High Vol but Price Down = Churning/Distribution
        if rvol > 1.5 and ret_5d < 0:
            score -= 15 # Trap! Everyone buying but price falling
            details.append("🐂Greed Trap Penalty: -15")
        
    elif 'fear' in regime: # extreme_fear or fear
        # Bear Mode: Penalty for volatility
        crash_risk = calculate_crash_risk_score(row)
        if crash_risk > 50:
            score -= 20 # Extra penalty
            details.append("😨Fear Vol Penalty: -20")
        
        # Extreme Fear Special
        if regime == 'extreme_fear':
             if crash_risk > 30: 
                 score -= 30 # Nuclear winter mode
                 details.append("😱ExFear Safety: -30")
    
    return max(0, score), details

def calculate_mid_term_score(row, df_all, etf_perf, regime='neutral'):
    """
    Calculate mid-term (1-3mo) score for a stock.
    Optimized for momentum investing.
    Weights: 1mo=25%, 3mo=15%, GC/SMA50=15%, BB_Squeeze=15%, RSI=10%, SectorETF=10%, RVOL_trend=10%
    """
    score = 0
    details = []
    
    # 1mo Return (25%) - Mid-term momentum Sweet Spot
    ret_1mo = row.get('1mo', 0)
    
    if ret_1mo > 100:
        score -= 10
        details.append(f"⚠️中期過熱({ret_1mo:.1f}%): -10")
    elif ret_1mo > 70:
        score += 5
        details.append(f"中期急騰({ret_1mo:.1f}%): +5")
    elif ret_1mo > 40:
        score += 15
        details.append(f"中期強力({ret_1mo:.1f}%): +15")
    elif ret_1mo > 20:
        score += 25
        details.append(f"中期最適({ret_1mo:.1f}%): +25")
    elif ret_1mo > 5:
        score += 10
        details.append(f"中期堅調({ret_1mo:.1f}%): +10")
    else:
        # Negative or flat
        pass
    
    # 3mo Return (15%)
    ret_3mo = row.get('3mo', 0)
    all_3mo = df_all['3mo'].dropna()
    if len(all_3mo) > 0:
        pts = 0.15 * _normalize_score(ret_3mo, all_3mo.min(), all_3mo.max())
        score += pts
        details.append(f"3ヶ月騰落({ret_3mo:.1f}%): +{pts:.1f}")
    
    # GC or Above SMA50 (15%) - reduced, lagging indicator
    gc = row.get('GC_Just_Now', False)
    above_sma50 = row.get('Above_SMA50', False)
    if gc:
        score += 15
        details.append("GC発生: +15")
    elif above_sma50:
        score += 10.5
        details.append("SMA50上: +10.5")
    
    # BB Squeeze (15%) - Energy charging state
    is_squeeze = row.get('Is_Squeeze', False)
    bb_width = row.get('BB_Width', 0.1)
    squeeze_days = row.get('Squeeze_Days', 0)
    
    if is_squeeze:
        pts = 15
        # Bonus for long squeeze (energy accumulation)
        if squeeze_days >= 3:
            pts += 5
            details.append(f"BBスクイーズ({squeeze_days}日): +{pts}")
        else:
            details.append("BBスクイーズ: +15")
        score += pts
    elif bb_width < 0.1:
        score += 12
        details.append("BB幅極狭: +12")
    elif bb_width < 0.2:
        score += 7.5
        details.append("BB幅狭: +7.5")
    else:
        score += 4.5
        details.append("BB幅広: +4.5")
    
    # RSI 50-75 range (10%) - Mid-term prefers stability over extreme heat
    rsi = row.get('RSI', 50)
    if 50 <= rsi <= 75:
        score += 10
        details.append(f"RSI適正({rsi:.0f}): +10")
    elif 75 < rsi <= 85:
        # User defined penalty: -5 pts
        score -= 5
        details.append(f"RSI加熱気味({rsi:.0f}): -5")
    elif rsi > 85:
        # User defined penalty: -15 pts
        score -= 15
        details.append(f"⚠️RSI過熱({rsi:.0f}): -15")
    else:
        score += 4
        details.append(f"RSI弱({rsi:.0f}): +4")
    
    # Sector ETF 1mo (10%) & Relative Strength
    sector = TICKER_TO_SECTOR.get(row.get('Ticker', '').upper(), '')
    etf = SECTOR_TO_ETF.get(sector, None)
    if etf and etf in etf_perf:
        etf_1mo = etf_perf[etf].get('1mo', 0)
        # 1. Sector Tailwind
        pts = 0.10 * _normalize_score(etf_1mo, -10, 20)
        score += pts
        details.append(f"セクター({etf_1mo:.1f}%): +{pts:.1f}")

        # 2. Relative Strength vs Sector
        alpha = ret_1mo - etf_1mo
        if alpha > 10.0:
            score += 5
            details.append(f"対セクター強(+{alpha:.1f}%): +5")
        elif alpha < -5.0:
            score -= 5
            details.append(f"対セクター弱({alpha:.1f}%): -5")
    
    # RVOL trend (10%) - Volume confirmation
    rvol = row.get('RVOL', 1.0)
    if rvol > 2.0:
        score += 10
        details.append(f"RVOL({rvol:.1f}x): +10")
    elif rvol > 1.5:
        score += 7
        details.append(f"RVOL({rvol:.1f}x): +7")
    elif rvol > 1.0:
        score += 5
        details.append(f"RVOL({rvol:.1f}x): +5")
    else:
        score += 2
        details.append(f"RVOL({rvol:.1f}x): +2")
    
    # --- Penalty for "short-term spike disguised as mid-term" ---
    ret_5d = row.get('5d', 0)
    if ret_1mo > 0 and ret_5d > 0:
        spike_ratio = ret_5d / ret_1mo if ret_1mo != 0 else 0
        if spike_ratio > 0.8:  # 5d is >80% of 1mo
            score -= 15
            details.append("⚠️短期急騰(騙し): -15")
        elif spike_ratio > 0.6:  # 5d is >60% of 1mo
            score -= 8
            details.append("⚠️短期集中: -8")
    
    # --- Distribution Detection (High volume + weak returns = selling) ---
    if rvol > 1.5 and ret_1mo < 5:
        score -= 10
        details.append("⚠️Vol増/株価弱: -10")
    elif rvol > 1.3 and ret_1mo < 0:
        score -= 8
        details.append("⚠️戻り売り: -8")
    
    # NEW: "Effort vs Result" (Churning) - Stricter check
    if rvol > 3.0 and ret_1mo < 3.0:
        score -= 15
        details.append("⚠️空回り(Vol過大): -15")

    # --- Crash Risk Penalty (Relaxed) ---
    crash_risk = calculate_crash_risk_score(row)
    if crash_risk > 70:
        pts = 0.08 * (crash_risk / 100) * 100
        score -= pts
        details.append(f"暴落リスク({crash_risk:.0f}): -{pts:.1f}")
    
    # --- Regime Adjustments (5-Level) ---
    if 'greed' in regime:
        # FIX: Ensure 1mo return is positive before boosting for volume
        if row.get('RVOL', 0) > 2.0 and row.get('1mo', 0) > 0: 
            score += 10
            details.append("🐂Greed Vol Bonus: +10")
            
        # Symmetrical Penalty: High Vol but Price Down
        if row.get('RVOL', 0) > 1.5 and row.get('1mo', 0) < 0:
            score -= 15
            details.append("🐂Greed Trap Penalty: -15")
        
    elif 'fear' in regime:
         if row.get('RSI', 50) > 70: 
             score -= 10
             details.append("😨Fear RSI Overbought: -10")
             
         if regime == 'extreme_fear':
             details.append("😱Extreme Fear Mode")
             pass

    return max(0, score), details

def calculate_long_term_score(row, df_all, etf_perf, regime='neutral'):
    """
    Calculate long-term (6mo+) score for a stock.
    Optimized for momentum investing (beat the market).
    Weights: Stability=30%, 1y=20%, YTD=15%, SMA200=10%, Beta(適正)=10%, ShortRatio=5%, SectorETF=5%, RVOL=5%
    """
    score = 0
    details = []
    
    # --- FILTER: Extreme movers & Pump-and-Dump patterns ---
    ret_1y = row.get('1y', 0)
    ret_6mo = row.get('6mo', 0)
    ret_3mo = row.get('3mo', 0)
    ret_1mo = row.get('1mo', 0)
    price = row.get('Price', 0)
    high52 = row.get('High52', price)
    
    # Calculate distance from 52-week high
    if high52 > 0 and price > 0:
        pct_from_high = ((high52 - price) / high52) * 100  # How far below 52w high
    else:
        pct_from_high = 0
    
    # Pattern 1: Super Stocks vs Pump & Dump (Refined)
    max_dd = row.get('MaxDD', 100)
    
    if ret_1y > 300:
        # Check for Super Stock characteristics
        if max_dd < 30:
            score += 10 # Bonus for stable super-growth (e.g. NVDA)
            details.append("💎SuperStockボーナス: +10")
        elif max_dd > 60:
            score -= 25 # Penalty for extreme volatility (likely P&D)
            details.append("⚠️Pump&Dump懸念: -25")
            
    elif ret_1y > 200:
        if max_dd > 50:
            score -= 15
            details.append("⚠️高ボラティリティ: -15")
    
    # Pattern 2: Spiked and crashed (52w high is way above current price)
    # If stock is >50% below its high AND has positive 1y return, it likely pumped and dumped
    if pct_from_high > 60 and ret_1y > 50:
        return 0, ["🚫崩壊チャート(高値から-60%): 0点"]
    elif pct_from_high > 50 and ret_1y > 30:
        score -= 25  # Heavy penalty for crash pattern
        details.append("⚠️崩壊チャート(高値から-50%): -25")
    elif pct_from_high > 40 and ret_1y > 20:
        score -= 15  # Moderate penalty
        details.append("⚠️大幅調整中: -15")
    
    # Stability: Minerrvini trend template (30%) - MOST IMPORTANT
    stability_score = 0
    if ret_1y > ret_6mo > ret_3mo > 0:
        stability_score = 100  # Perfect Minervini template
    elif ret_1y > 0 and ret_6mo > 0 and ret_3mo > 0:
        stability_score = 80
    elif ret_6mo > 0 and ret_3mo > 0:
        stability_score = 60
    elif ret_3mo > 0:
        stability_score = 40
    
    pts = 0.30 * stability_score
    score += pts
    details.append(f"トレンド安定度: +{pts:.1f}")
    
    # 1y Return (20%) - Long-term Alpha
    # Normalized against the market, but we want absolute winners
    if ret_1y > 300:
        # Already handled by Super Stock check, but base score is neutral to prevent double counting or penalize volatility
        score += 10
        details.append(f"年間超騰({ret_1y:.0f}%): +10")
    elif ret_1y > 150:
        # Very strong
        score += 15
        details.append(f"年間急騰({ret_1y:.0f}%): +15")
    elif ret_1y > 50:
        # Ideal Multi-bagger zone
        score += 20
        details.append(f"年間最適({ret_1y:.0f}%): +20")
    elif ret_1y > 20:
        # Solid
        score += 10
        details.append(f"年間堅調({ret_1y:.0f}%): +10")
    elif ret_1y > 0:
        score += 5
        details.append(f"年間プラス({ret_1y:.0f}%): +5")
    
    # YTD Return (15%)
    ret_ytd = row.get('YTD', 0)
    all_ytd = df_all['YTD'].dropna()
    if len(all_ytd) > 0:
        pts = 0.15 * _normalize_score(ret_ytd, all_ytd.min(), all_ytd.max())
        score += pts
        details.append(f"年初来({ret_ytd:.0f}%): +{pts:.1f}")
    
    # Above SMA200 (10%) - Must be above for long-term trend
    price = row.get('Price', 0)
    sma200 = row.get('SMA200', 0)
    above_sma200 = price > sma200 if sma200 > 0 else False
    if above_sma200:
        score += 10
        details.append("SMA200上: +10")
    
    # Beta (10%) - 1.0-2.5 is ideal for momentum (not too defensive, not too crazy)
    beta = row.get('Beta', 1.0)
    if 1.0 <= beta <= 2.5:
        score += 10
        details.append(f"適正ベータ({beta:.2f}): +10")
    elif 0.8 <= beta < 1.0:
        score += 5
        details.append(f"低ベータ({beta:.2f}): +5")
    elif beta < 0.8:
        score += 2
        details.append(f"超低ベータ({beta:.2f}): +2")
    elif 2.5 < beta <= 3.5:
        score += 6
        details.append(f"高ベータ({beta:.2f}): +6")
    else:
        score += 3
        details.append(f"超高ベータ({beta:.2f}): +3")
    
    # Short Ratio (5%) - Neutral/slight positive (fuel for squeeze)
    short_ratio = row.get('ShortRatio', 2)
    if 2 <= short_ratio <= 5:
        score += 4
        details.append(f"空売り比率({short_ratio:.1f}): +4")
    elif short_ratio < 2:
        score += 3
        details.append("低空売り: +3")
    else:
        score += 2
        details.append("高空売り: +2")
    
    # Sector ETF YTD (5%)
    sector = TICKER_TO_SECTOR.get(row.get('Ticker', '').upper(), '')
    etf = SECTOR_TO_ETF.get(sector, None)
    if etf and etf in etf_perf:
        etf_ytd = etf_perf[etf].get('YTD', 0)
        pts = 0.05 * _normalize_score(etf_ytd, -20, 50)
        score += pts
        details.append(f"セクター({etf_ytd:.1f}%): +{pts:.1f}")
    
    # RVOL (5%) - Volume trend
    rvol = row.get('RVOL', 1.0)
    if rvol > 1.5:
        score += 5
        details.append(f"RVOL({rvol:.1f}x): +5")
    elif rvol > 1.0:
        score += 3
        details.append(f"RVOL({rvol:.1f}x): +3")
    else:
        score += 1.5
        details.append(f"RVOL({rvol:.1f}x): +1.5")
    
    # --- Distribution Detection (High volume + weak YTD = selling) ---
    if rvol > 1.5 and ret_ytd < 10:
        score -= 8
        details.append("⚠️Distribution(Vol増/YTD弱): -8")
    elif rvol > 1.3 and ret_ytd < 0:
        score -= 6
        details.append("⚠️Distribution(Vol増/YTD負): -6")
    
    # --- Crash Risk: Penalty for high, BONUS for low (long-term only) ---
    crash_risk = calculate_crash_risk_score(row)
    if crash_risk > 80:
        pts = 0.05 * (crash_risk / 100) * 100
        score -= pts
        details.append(f"暴落リスク高({crash_risk:.0f}): -{pts:.1f}")
    elif crash_risk < 15:
        score += 5  # Low risk bonus for long-term stability
        details.append("低リスクボーナス: +5")
    
    # NEW: Institutional Ownership (Smart Money Support)
    inst_own = row.get('InstOwnership', 0) * 100 # Convert to %
    if inst_own > 40:
        score += 5 # Strong institutional backing
        details.append(f"機関保有({inst_own:.0f}%): +5")
    elif inst_own > 70:
        score += 2 # Very high (crowded but strong)
        details.append(f"機関保有超高({inst_own:.0f}%): +2")
    elif inst_own < 10:
        score -= 2 # Retail driven, potentially volatile
        details.append("機関保有過少: -2")
    
    # NEW: Market Regime & RS Rating Logic
    rs_rating = row.get('RS_Rating', 50)
    
    # RS Rating Bonus (True Leaders)
    if rs_rating > 90:
        score += 10 # Top 10% of universe -> Huge bonus
        details.append(f"👑RS値({rs_rating:.0f}): +10")
    elif rs_rating > 80:
        score += 5
        details.append(f"RS値({rs_rating:.0f}): +5")
    
    
    # Regime Adjustments (5-Level)
    # 1. Extreme Greed (Aggressive)
    if regime == 'extreme_greed':
         # Huge bonus for leaders with minor volatility
         if row.get('MaxDD', 0) > 40:
             score += 10 # Forgive volatility, focus on upside
             details.append("🤑ExGreed Volatility Pardon: +10")
             
    # 2. Greed (Bull)
    elif regime == 'greed':
         if row.get('MaxDD', 0) > 40:
             score += 5 
             details.append("🐂Greed Volatility Pardon: +5")

    # 3. Neutral
    elif regime == 'neutral':
        pass # Standard scoring

    # 4. Fear (Bear)
    elif regime == 'fear':
        max_dd = row.get('MaxDD', 100)
        if max_dd > 40:
            score -= 20 # Strict penalty
            details.append("😨Fear Volatility Penalty: -20")
        if inst_own < 20:
            score -= 5 # Require institutional support
            details.append("😨Fear Low Inst Penalty: -5")

    # 5. Extreme Fear (Crash Protection)
    elif regime == 'extreme_fear':
        max_dd = row.get('MaxDD', 100)
        if max_dd > 30:
            score -= 35 # MASSIVE PENALTY for any volatility
            details.append("😱ExFear Volatility Excl: -35")
        if inst_own < 40:
            score -= 10 # Must be high conviction
            details.append("😱ExFear Low Inst Excl: -10")
        if row.get('Beta', 1.0) > 1.2:
            score -= 10 # Penalty for high beta
            details.append("😱ExFear High Beta Penalty: -10")
            
    return max(0, score), details

def calculate_market_regime(df_metrics):
    """
    Determines Market Regime based on VIX and SPY Trend.
    Returns: (regime_key, display_label, color_code)
    """
    try:
        # 1. Fetch VIX
        vix_ticker = yf.Ticker("^VIX")
        # Get latest price efficiently
        vix_hist = vix_ticker.history(period="5d")
        if not vix_hist.empty:
            vix = vix_hist['Close'].iloc[-1]
        else:
            vix = 20.0 # Default if fetch fails
            
        # 2. Extract SPY Data from df_metrics
        spy_row = df_metrics[df_metrics['Ticker'] == 'SPY']
        if not spy_row.empty:
            spy_price = spy_row.iloc[0]['Price']
            spy_sma50 = spy_row.iloc[0]['SMA50']
            spy_sma200 = spy_row.iloc[0]['SMA200']
        else:
            # Fallback if SPY not in metrics
            s = yf.Ticker("SPY").history(period="1y")
            spy_price = s['Close'].iloc[-1]
            spy_sma50 = s['Close'].rolling(50).mean().iloc[-1]
            spy_sma200 = s['Close'].rolling(200).mean().iloc[-1]

        # 3. Decision Logic (5-Levels)
        
        # Level 1: Extreme Greed (Super Bull)
        if vix < 15 and spy_price > spy_sma50:
            return 'extreme_greed', f"🤑 Extreme Greed (VIX={vix:.1f}, SPY>SMA50)", "#00FF00"
            
        # Level 2: Greed (Bull)
        elif vix < 20 and spy_price > spy_sma50:
             return 'greed', f"🐂 Greed (VIX={vix:.1f})", "#90EE90"
        
        # Level 5: Extreme Fear (Crash)
        elif vix > 30 or spy_price < spy_sma200:
             return 'extreme_fear', f"😱 Extreme Fear (VIX={vix:.1f} / SPY<SMA200)", "#FF0000"
             
        # Level 4: Fear (Correction)
        elif vix > 25 or spy_price < spy_sma50:
             return 'fear', f"😨 Fear (VIX={vix:.1f} / SPY<SMA50)", "#FF7F7F"
             
        # Level 3: Neutral
        else:
             return 'neutral', f"⚖️ Neutral (VIX={vix:.1f})", "#FFFF00"
             
    except Exception as e:
        print(f"Regime Check Failed: {e}")
        return 'neutral', "⚖️ Neutral (Error)", "#FFFF00"

def generate_recommendation_reason(row, timeframe, etf_perf):
    """Generate a human-readable reason for the recommendation."""
    ticker = row.get('Ticker', '???')
    sector = TICKER_TO_SECTOR.get(ticker.upper(), 'その他')
    
    reasons = []
    
    if timeframe == 'short':
        # Short-term reasons
        ret_5d = row.get('5d', 0)
        rvol = row.get('RVOL', 0)
        rsi = row.get('RSI', 50)
        
        # RS Rating callout
        rs_rating = row.get('RS_Rating', 0)
        if rs_rating > 90:
            reasons.append(f"👑RS値{rs_rating:.0f}")
        
        if ret_5d > 5:
            reasons.append(f"5日+{ret_5d:.1f}%の強い勢い")
        elif ret_5d > 0:
            reasons.append(f"5日+{ret_5d:.1f}%")
        
        if rvol > 2:
            reasons.append(f"出来高{rvol:.1f}倍急増")
        elif rvol > 1.5:
            reasons.append(f"出来高増加中")
        
        if row.get('HasNews', False):
            reasons.append("📰ニュース")
        
        price = row.get('Price', 0)
        high52 = row.get('High52', 0)
        if high52 > 0 and price >= high52 * 0.98:
            reasons.append("🚀新高値")
            
    elif timeframe == 'mid':
        # Mid-term reasons
        ret_1mo = row.get('1mo', 0)
        
        if ret_1mo > 10:
            reasons.append(f"1ヶ月+{ret_1mo:.1f}%")
        
        # Add details
        is_squeeze = row.get('Is_Squeeze', False)
        if is_squeeze:
            reasons.append("⚡BBスクイーズ(爆発前夜)")
        
        if row.get('GC_Just_Now', False):
            reasons.append("✨GC発生")
        elif row.get('Above_SMA50', False):
            reasons.append("📈SMA50上")
            
        rvol = row.get('RVOL', 0)
        if rvol > 1.5:
            reasons.append(f"出来高増")

    else:  # long
        # Long-term reasons
        ret_1y = row.get('1y', 0)
        
        # Return summary
        if ret_1y > 50:
            reasons.append(f"年間+{ret_1y:.0f}%🔥")
        
        # Stability / Quality
        max_dd = row.get('MaxDD', 100)
        if max_dd < 30:
            reasons.append(f"安定成長(MaxDD-{max_dd:.0f}%)")
            
        inst_own = row.get('InstOwnership', 0) * 100
        if inst_own > 40:
            reasons.append(f"機関保有{inst_own:.0f}%")
            
        # Minervini check visualization
        ret_6mo = row.get('6mo', 0)
        ret_3mo = row.get('3mo', 0)
        if ret_1y > ret_6mo > ret_3mo > 0:
            reasons.append("✨トレンド")
        
        short_ratio = row.get('ShortRatio', 5)
        if 2 <= short_ratio <= 5:
            reasons.append(f"空売{short_ratio}倍(踏上期待)")
    
    # Add sector ETF info
    etf = SECTOR_TO_ETF.get(sector, None)
    if etf and etf in etf_perf:
        etf_data = etf_perf[etf]
        if timeframe == 'short' and etf_data.get('5d', 0) > 2:
            reasons.append(f"{sector[:10]}セクター好調")
        elif timeframe == 'mid' and etf_data.get('1mo', 0) > 5:
            reasons.append(f"セクター資金流入中")
        elif timeframe == 'long' and etf_data.get('YTD', 0) > 10:
            reasons.append(f"セクター年初来好調")
    
    return " / ".join(reasons[:4]) if reasons else "総合スコア上位"

def get_ai_stock_picks(df_metrics, etf_metrics=None, news_checker=None, top_n=3, regime='neutral'):
    """
    Main function: Get AI stock picks for short/mid/long term.
    
    Args:
        df_metrics: DataFrame with stock metrics (from calculate_momentum_metrics)
        etf_metrics: DataFrame with ETF metrics (optional, for sector scoring)
        news_checker: Function to check if ticker has recent news (optional)
        top_n: Number of picks per timeframe
        
    Returns:
        dict: {'short': [...], 'mid': [...], 'long': [...]}
              Each list contains dicts with 'ticker', 'score', 'reason', 'metrics'
    """
    if df_metrics is None or df_metrics.empty:
        return {'short': [], 'mid': [], 'long': []}
    
    # Build ETF performance dict
    etf_perf = {}
    if etf_metrics is not None and not etf_metrics.empty:
        for _, row in etf_metrics.iterrows():
            ticker = row.get('Ticker', '')
            etf_perf[ticker] = {
                '5d': row.get('5d', 0),
                '1mo': row.get('1mo', 0),
                'YTD': row.get('YTD', 0),
            }
    
    # Filter out ETFs from stock picks (we only want individual stocks)
    etf_tickers = set(THEMATIC_ETFS.values())
    df_stocks = df_metrics[~df_metrics['Ticker'].isin(etf_tickers)].copy()
    
    if df_stocks.empty:
        return {'short': [], 'mid': [], 'long': []}
    
    # Add news info if checker provided
    if news_checker:
        df_stocks['HasNews'] = df_stocks['Ticker'].apply(
            lambda t: len(news_checker(t)) > 0
        )
    else:
        df_stocks['HasNews'] = False
    
    # Calculate scores for each timeframe (unpacking tuple returns)
    df_stocks[['ShortScore', 'ShortDetails']] = df_stocks.apply(
        lambda row: pd.Series(calculate_short_term_score(row, df_stocks, etf_perf, regime)), axis=1
    )
    df_stocks[['MidScore', 'MidDetails']] = df_stocks.apply(
        lambda row: pd.Series(calculate_mid_term_score(row, df_stocks, etf_perf, regime)), axis=1
    )
    df_stocks[['LongScore', 'LongDetails']] = df_stocks.apply(
        lambda row: pd.Series(calculate_long_term_score(row, df_stocks, etf_perf, regime)), axis=1
    )
    
    results = {'short': [], 'mid': [], 'long': []}
    
    # Get top picks for each timeframe
    for timeframe, score_col in [('short', 'ShortScore'), ('mid', 'MidScore'), ('long', 'LongScore')]:
        top_df = df_stocks.nlargest(top_n, score_col)
        
        for _, row in top_df.iterrows():
            # Calculate crash risk for this stock
            crash_risk = calculate_crash_risk_score(row.to_dict())
            
            # Prepare metrics dictionary
            metrics_dict = {
                'price': row.get('Price', 0),
                '5d': row.get('5d', 0),
                '1mo': row.get('1mo', 0),
                '3mo': row.get('3mo', 0),
                'YTD': row.get('YTD', 0),
                '1y': row.get('1y', 0),
                'RSI': row.get('RSI', 50),
                'RVOL': row.get('RVOL', 1),
                'Beta': row.get('Beta', 1.0),
                'InstOwnership': row.get('InstOwnership', 0),
                'SMA50_Deviation': row.get('SMA50_Deviation', 0),
                'sector': TICKER_TO_SECTOR.get(row['Ticker'].upper(), 'その他'),
            }

            # Append to results
            results[timeframe].append({
                'ticker': row['Ticker'],
                'score': row[score_col],
                'reason': generate_recommendation_reason(row.to_dict(), timeframe, etf_perf),
                'details': row.get(f'{timeframe.capitalize()}Details', []),
                'crash_risk': crash_risk,
                'metrics': metrics_dict
            })
    
    return results

def get_todays_signals(history_dict):
    """
    Scan all cached history to find signals on the LATEST day only.
    OPTIMIZED: No full history loop - only check latest day conditions.
    
    Performance: ~10x faster than full history scan.
    """
    signals = {
        'Buy_Breakout': [],
        'Buy_Reversal': [],
        'Buy_Reentry': [],
        'Sell': []
    }
    
    if not history_dict:
        return signals
        
    for ticker, df_raw in history_dict.items():
        if df_raw is None or df_raw.empty or len(df_raw) < 55:
            continue
            
        try:
            df = df_raw.copy()
            
            # === Fast Indicator Calculation (Vectorized) ===
            df['SMA20'] = df['Close'].rolling(20).mean()
            df['SMA50'] = df['Close'].rolling(50).mean()
            
            std20 = df['Close'].rolling(20).std()
            df['BB_Upper'] = df['SMA20'] + (std20 * 2)
            
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            df['AvgVol20'] = df['Volume'].rolling(20).mean()
            df['RVOL'] = df['Volume'] / df['AvgVol20']
            df['High50'] = df['High'].rolling(50).max()
            
            ema12 = df['Close'].ewm(span=12, adjust=False).mean()
            ema26 = df['Close'].ewm(span=26, adjust=False).mean()
            df['MACD'] = ema12 - ema26
            df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
            
            high_low = df['High'] - df['Low']
            high_close = (df['High'] - df['Close'].shift(1)).abs()
            low_close = (df['Low'] - df['Close'].shift(1)).abs()
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df['ATR'] = tr.rolling(14).mean()
            df['Chandelier_Exit'] = df['High'].rolling(22).max() - (df['ATR'] * 5.0)
            
            up_move = df['High'] - df['High'].shift(1)
            down_move = df['Low'].shift(1) - df['Low']
            plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
            minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
            tr_smooth = pd.Series(tr, index=df.index).ewm(alpha=1/14, adjust=False).mean()
            plus_di = 100 * (pd.Series(plus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / tr_smooth)
            minus_di = 100 * (pd.Series(minus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / tr_smooth)
            dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
            df['ADX'] = dx.ewm(alpha=1/14, adjust=False).mean()
            
            # === LATEST DAY ONLY - Fast Check ===
            row = df.iloc[-1]
            prev = df.iloc[-2]
            prev2 = df.iloc[-3] if len(df) > 2 else prev
            
            # --- Detect signal type on latest day ---
            signal_type = None
            reason = ''
            
            # BUY BREAKOUT
            cond_trend = (row['Close'] > row['SMA50']) or (row['Close'] > row['SMA20'])
            cond_bb_break = (row['Close'] > row['BB_Upper'])
            cond_near_high = (row['Close'] >= row['High50'] * 0.98)
            cond_breakout = cond_bb_break or cond_near_high
            cond_vol = (row['RVOL'] > 1.1)
            cond_macd = (row['MACD'] > row['MACD_Signal']) or (row['MACD'] > 0)
            cond_safe_rsi = (row['RSI'] < 80)
            
            if cond_trend and cond_breakout and cond_vol and cond_macd and cond_safe_rsi:
                signal_type = 'Breakout'
                reason = 'BB Break' if cond_bb_break else '50日高値圏'
            
            # BUY REVERSAL (Only for downtrend)
            if signal_type is None:
                cond_downtrend = (row['Close'] < row['SMA50'])
                cond_rsi_low = (row['RSI'] < 55)
                cross_today = (row['MACD'] > row['MACD_Signal']) and (prev['MACD'] <= prev['MACD_Signal'])
                cross_yest = (prev['MACD'] > prev['MACD_Signal']) and (prev2['MACD'] <= prev2['MACD_Signal'])
                cond_macd_cross = cross_today or cross_yest
                cond_hist_up = (row['MACD_Hist'] > prev['MACD_Hist']) and (prev['MACD_Hist'] > prev2['MACD_Hist'])
                cond_early = cond_rsi_low and cond_hist_up and (row['MACD_Hist'] < 0)
                cond_big = (row['Close'] > row['Open'] * 1.03) and (row['RVOL'] > 1.2)
                
                if cond_downtrend and cond_rsi_low and cond_macd_cross:
                    signal_type = 'Reversal'
                    reason = 'MACD GC'
                elif cond_downtrend and cond_early and (row['RVOL'] > 1.0):
                    signal_type = 'Reversal'
                    reason = 'Early Turn (Hist↑)'
                elif cond_downtrend and cond_big:
                    signal_type = 'Reversal'
                    reason = 'Big Bounce'
            
            # BUY REENTRY
            if signal_type is None:
                cond_trend_up = (row['ADX'] > 15) and (row['Close'] > row['SMA50'])
                cond_pullback = (40 < row['RSI'] < 60)
                cross_today = (row['MACD'] > row['MACD_Signal']) and (prev['MACD'] <= prev['MACD_Signal'])
                cond_hist_up = (row['MACD_Hist'] > prev['MACD_Hist']) and (prev['MACD_Hist'] > prev2['MACD_Hist'])
                
                if cond_trend_up and cond_pullback and (cross_today or cond_hist_up):
                    signal_type = 'Reentry'
                    reason = 'Dip Buy (押し目)'
            
            # SELL
            if signal_type is None:
                chandelier_break = (row['Close'] < row['Chandelier_Exit']) and (prev['Close'] >= prev['Chandelier_Exit'])
                rsi_climax = (row['RSI'] > 90) and (prev['RSI'] <= 90)
                rsi_was_high = df['RSI'].iloc[-10:].max() > 70 if len(df) >= 10 else False
                macd_dead = (row['MACD'] < row['MACD_Signal']) and (prev['MACD'] >= prev['MACD_Signal'])
                profit_take = macd_dead and (row['RSI'] < 60) and rsi_was_high
                
                if chandelier_break:
                    signal_type = 'Sell_Stop'
                    reason = 'Stop Loss (Chandelier)'
                elif rsi_climax:
                    signal_type = 'Sell_Profit'
                    reason = f"RSI Climax ({row['RSI']:.0f})"
                elif profit_take:
                    signal_type = 'Sell_Profit'
                    reason = 'Profit Take (MACD DC)'
            
            # === Skip if no signal on latest day ===
            if signal_type is None:
                continue
            
            # === Quick Cooldown Check (last 5 days only) ===
            # Check if same signal type was triggered recently
            recent = df.iloc[-6:-1]  # Last 5 days (excluding today)
            
            if signal_type in ['Breakout', 'Reversal', 'Reentry']:
                # Check if any BUY signal was already triggered in last 5 days
                has_recent_buy = False
                for i in range(-5, -1):
                    if len(df) + i < 0:
                        continue
                    r = df.iloc[i]
                    p = df.iloc[i-1] if len(df) + i - 1 >= 0 else r
                    
                    # Quick buy check
                    trend_ok = (r['Close'] > r['SMA50']) or (r['Close'] > r['SMA20'])
                    bb_ok = (r['Close'] > r['BB_Upper']) or (r['Close'] >= r['High50'] * 0.98)
                    vol_ok = (r['RVOL'] > 1.1)
                    macd_ok = (r['MACD'] > r['MACD_Signal']) or (r['MACD'] > 0)
                    rsi_ok = (r['RSI'] < 80)
                    if trend_ok and bb_ok and vol_ok and macd_ok and rsi_ok:
                        has_recent_buy = True
                        break
                    
                    # Reversal check
                    down_ok = (r['Close'] < r['SMA50'])
                    cross_ok = (r['MACD'] > r['MACD_Signal']) and (p['MACD'] <= p['MACD_Signal'])
                    if down_ok and (r['RSI'] < 55) and cross_ok:
                        has_recent_buy = True
                        break
                
                if has_recent_buy:
                    continue  # Skip - cooldown active
            
            # === Calculate Bull Trend Probability Score (for Reversals) ===
            daily_change = (row['Close'] - prev['Close']) / prev['Close'] * 100 if prev['Close'] > 0 else 0
            sma50_distance = (row['SMA50'] - row['Close']) / row['SMA50'] * 100 if row['SMA50'] > 0 else 0  # % below SMA50
            hist_improvement = row['MACD_Hist'] - prev['MACD_Hist'] if not pd.isna(prev['MACD_Hist']) else 0
            
            # RSI score: 30-50 is ideal (oversold but recovering)
            rsi_score = 0
            if 30 <= row['RSI'] <= 50:
                rsi_score = 1.0  # Perfect zone
            elif 20 <= row['RSI'] < 30:
                rsi_score = 0.7  # Very oversold
            elif 50 < row['RSI'] <= 55:
                rsi_score = 0.5  # Slightly high but ok
            else:
                rsi_score = 0.3
            
            # Composite Score for Bull Trend Probability
            # Higher = more likely to transition to bull trend
            bull_score = (
                daily_change * 3.0 +          # 30% weight - momentum
                min(row['RVOL'], 5) * 5.0 +   # 25% weight - volume confirmation (cap at 5x)
                max(0, 15 - sma50_distance) * 1.67 +  # 25% weight - closer to SMA50 = better
                rsi_score * 10 +              # 10% weight - RSI position
                hist_improvement * 100        # 10% weight - MACD hist improvement
            )
            
            # === Add to signals ===
            entry = {
                'Ticker': ticker,
                'Price': row['Close'],
                'RVOL': row['RVOL'],
                'RSI': row['RSI'],
                'Reason': reason,
                'DailyPct': daily_change,
                'BullScore': bull_score,
                'SMA50Dist': sma50_distance,
                'ADX': row['ADX'],
                'High50': row['High50'],
                'MACD': row['MACD'],
                'MACD_Signal': row['MACD_Signal'],
                'Chandelier_Exit': row['Chandelier_Exit']
            }
            
            if signal_type == 'Breakout':
                signals['Buy_Breakout'].append(entry)
            elif signal_type == 'Reversal':
                signals['Buy_Reversal'].append(entry)
            elif signal_type == 'Reentry':
                signals['Buy_Reentry'].append(entry)
            elif signal_type.startswith('Sell'):
                signals['Sell'].append(entry)

        except Exception as e:
            continue
    
    # === Sort by BullScore (descending) ===
    for key in ['Buy_Breakout', 'Buy_Reversal', 'Buy_Reentry']:
        signals[key] = sorted(signals[key], key=lambda x: x.get('BullScore', 0), reverse=True)
            
    return signals

# === Momentum Analyzer Logic (New) ===

def analyze_stock_history(ticker, period="1y"):
    """
    Fetch history and calculate detailed signals for a specific ticker.
    Used for on-demand "Deep Dive" analysis.
    
    Args:
        ticker (str): Ticker symbol.
        period (str): Data period to fetch (default "1y").
        
    Returns:
        tuple: (DataFrame with signals, dict summary_status)
    """
    try:
        # Fetch history with retry logic
        max_retries = 3
        df = pd.DataFrame()
        
        for attempt in range(max_retries):
            try:
                # Method 1: Ticker.history (Standard)
                ticker_obj = yf.Ticker(ticker)
                df = ticker_obj.history(period=period)
                
                if df.empty:
                    # Method 2: yf.download (Fallback)
                    # sometimes download works when history doesn't
                    time.sleep(1)
                    df = yf.download(ticker, period=period, progress=False)
                
                if not df.empty:
                    break
                else:
                    time.sleep(2 * (attempt + 1)) # Exponential backoff
            except Exception as e:
                # print(f"Retry {attempt} failed: {e}") # Debug
                time.sleep(2 * (attempt + 1))
        
        if df.empty:
             # Fallback: Try 'max' if '1y' failed? Or maybe ticker is wrong.
             # Return error with detail.
            return None, {"error": f"No data found for {ticker} (Network/API Error). Try again later."}
            
        # Ensure single level columns (Fix for MultiIndex error)
        if isinstance(df.columns, pd.MultiIndex):
            # If standard yf structure (Price, Ticker), drop ticker level
            try:
                df.columns = df.columns.droplevel(1)
            except:
                pass
        
        # Double check: if 'Close' is still a DataFrame (duplicate columns?), resolve it.
        if 'Close' in df.columns and isinstance(df['Close'], pd.DataFrame):
             # This happens if droplevel failed or we have duplicate columns
             df = df.T.groupby(level=0).first().T

        
        # Calculate Indicators
        # SMA
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA50'] = df['Close'].rolling(window=50).mean()
        df['SMA150'] = df['Close'].rolling(window=150).mean()
        df['SMA200'] = df['Close'].rolling(window=200).mean()
        
        # Bollinger Bands (20, 2)
        sma20 = df['SMA20']
        std20 = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = sma20 + (std20 * 2)
        df['BB_Lower'] = sma20 - (std20 * 2)
        # Handle division by zero or NaN
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / sma20
        
        # RSI (14)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # RVOL (20-day average volume)
        df['AvgVol20'] = df['Volume'].rolling(window=20).mean()
        df['RVOL'] = df['Volume'] / df['AvgVol20']
        
        # 50-day High/Low
        df['High50'] = df['High'].rolling(window=50).max()
        df['Low50'] = df['Low'].rolling(window=50).min()

        # === Advanced Indicators (v2) ===
        # 1. MACD (12, 26, 9)
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
        
        # 2. ATR (14) - For Volatility & Stop Loss
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift(1)).abs()
        low_close = (df['Low'] - df['Close'].shift(1)).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['ATR'] = tr.rolling(window=14).mean()
        
        # 3. Chandelier Exit (Long) - Stop Loss Line (Widened for Momentum Swing)
        # Standard is 3.0, but user reported "Stop Loss Poor" in chop.
        # Widen to 5.0 to allow for volatility in momentum stocks.
        rolling_high_22 = df['High'].rolling(window=22).max()
        df['Chandelier_Exit'] = rolling_high_22 - (df['ATR'] * 5.0)
        
        # 4. MFI (14) - Money Flow Index
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        money_flow = typical_price * df['Volume']
        
        # Positive/Negative Flow using comparison with previous typical price, not diff of itself for vectorized
        up_flow = money_flow.where(typical_price > typical_price.shift(1), 0)
        down_flow = money_flow.where(typical_price < typical_price.shift(1), 0)
        
        mfi_period = 14
        up_sum = up_flow.rolling(window=mfi_period).sum()
        down_sum = down_flow.rolling(window=mfi_period).sum()
        
        mfi_ratio = up_sum / down_sum
        df['MFI'] = 100 - (100 / (1 + mfi_ratio))
        
        # 5. BB Expansion (Squeeze Release)
        df['BB_Expanding'] = df['BB_Width'] > df['BB_Width'].shift(1)

        # 6. ADX (Average Directional Index) - Trend Strength Filter
        # Measures if trend is strong (ADX > 25) or ranging (ADX < 20).
        # Calculation:
        # +DM = High - PrevHigh (if > 0 and > |Low - PrevLow|)
        # -DM = PrevLow - Low (if > 0 and > |High - PrevHigh|)
        # TR is already calc'd.
        
        up_move = df['High'] - df['High'].shift(1)
        down_move = df['Low'].shift(1) - df['Low']
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        
        # Smooth TR, +DM, -DM (EMA-like smoothing usually, or Rolling sum for simplified ADX)
        # Standard ADX uses Wilder's Smoothing. Let's use EWM with span=14 (approx).
        # Or simple rolling for robustness in pandas (Wilder's is slightly different).
        # Let's use EWM com=13 (span=27 roughly matches Wilder's 1/14).
        
        tr_smooth = pd.Series(tr).ewm(alpha=1/14, adjust=False).mean()
        plus_di = 100 * (pd.Series(plus_dm).ewm(alpha=1/14, adjust=False).mean() / tr_smooth)
        minus_di = 100 * (pd.Series(minus_dm).ewm(alpha=1/14, adjust=False).mean() / tr_smooth)
        
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
        df['ADX'] = dx.ewm(alpha=1/14, adjust=False).mean()
        
        # === Signal Detection (v3: Ultra-Relaxed for Candidate Discovery) ===
        df['Signal'] = None 
        df['Reason'] = ''
        
        # Helper: Event happened in last N days
        def happened_recently(series_bool, window=3):
            # Check if any True in rolling window (but we just have the column, need to roll it)
            # Efficient way for single row logic: Just check current.
            # But for scanners, we might want "Signal triggered yesterday".
            # For now, let's stick to "Current Condition" to avoid "Old News", but relax the precision.
            return series_bool
            
        # --- BUY Condition (Breakout) ---
        # 1. Trend: Above SMA50 OR Above SMA20 (Short term momentum)
        cond_trend = (df['Close'] > df['SMA50']) | (df['Close'] > df['SMA20'])
        
        # 2. Breakout Trigger
        # Close > Upper Band OR Close near 50d High (>98%)
        cond_bb_break = (df['Close'] > df['BB_Upper'])
        cond_near_high = (df['Close'] >= df['High50'] * 0.98) 
        cond_breakout = cond_bb_break | cond_near_high
        
        # 3. Volume (Relaxed)
        # 1.1x volume is enough if price action is strong
        cond_vol = (df['RVOL'] > 1.1)
        
        # 4. Momentum (MACD Positive or Rising)
        cond_macd = (df['MACD'] > df['MACD_Signal']) | (df['MACD'] > 0)
        
        # 5. RSI Safety (Not > 80)
        cond_safe_rsi = (df['RSI'] < 80)
        
        buy_mask = cond_trend & cond_breakout & cond_vol & cond_macd & cond_safe_rsi
        
        # --- REOVERSAL BUY Condition (Bottom Fish) ---
        # Strategy: Catch the Turn.
        
        # 1. Oversold Context (RSI < 55)
        cond_rsi_low = (df['RSI'] < 55)
        
        # 2. Golden Cross (MACD Cross UP) - Check Last 2 Days
        # Today Cross OR Yesterday Cross (so we don't miss it by a few hours)
        cross_today = (df['MACD'] > df['MACD_Signal']) & (df['MACD'].shift(1) <= df['MACD_Signal'].shift(1))
        cross_yesterday = (df['MACD'].shift(1) > df['MACD_Signal'].shift(1)) & (df['MACD'].shift(2) <= df['MACD_Signal'].shift(2))
        cond_macd_cross = cross_today | cross_yesterday
        
        # 3. Early Turn (Histogram Improving while Negative) - "Approaching Cross"
        # Captures the V-bounce before the actual cross
        cond_hist_improving = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['MACD_Hist'].shift(1) > df['MACD_Hist'].shift(2))
        cond_early_turn = cond_rsi_low & cond_hist_improving & (df['MACD_Hist'] < 0)
        
        # 4. Big Candle (Panic Reversal)
        cond_big_candle = (df['Close'] > df['Open'] * 1.03)
        
        # Logic A: Classic Golden Cross (Recent)
        cond_reversal_classic = cond_rsi_low & cond_macd_cross
        
        # Logic B: Early Turn (Aggressive)
        cond_reversal_early = cond_early_turn & (df['RVOL'] > 1.0) # Require slight vol for early turn
        
        # Logic C: Big Bounce
        cond_reversal_bounce = cond_big_candle & (df['RVOL'] > 1.2)
        
        reversal_mask = cond_reversal_classic | cond_reversal_early | cond_reversal_bounce
        
        # --- RE-ENTRY BUY Condition (Dip Buy) ---
        # Trend Up + Pullback + Turn Up
        cond_trend_up = (df['ADX'] > 15) & (df['Close'] > df['SMA50'])
        cond_pullback = (df['RSI'] < 60) & (df['RSI'] > 40) # Healthy pullback zone
        cond_turn_up = cond_hist_improving | cond_macd_cross
        
        reentry_mask = cond_trend_up & cond_pullback & cond_turn_up
        
        # --- SELL Condition ---
        # TRIGGER:
        # 1. RSI Extreme: Crosses ABOVE 90 (Climax)
        # 2. MACD Dead Cross: ONLY if we were recently overheated.
        #    If we just wobbled around RSS 50, a MACD cross is noise. 
        #    Rule: Max RSI of last 10 days must be > 70 for a MACD Profit Take to be valid.
        
        rsi_was_high = (df['RSI'].rolling(10).max() > 70)
        
        # RSI Climax (> 90)
        rsi_climax_90 = (df['RSI'] > 90) & (df['RSI'].shift(1) <= 90)
        
        # MACD Dead Cross (Strict: Only if RSI < 60 AND we were recently high)
        # BUG FIX: If MACD crosses while RSI > 60, we ignore it (good).
        # But if price continues to drop, RSI falls below 60, and MACD is STILL dead... we missed the exit!
        # ADDITION: Trigger if "MACD is Dead" AND "RSI crosses below 60".
        
        macd_is_dead = (df['MACD'] < df['MACD_Signal'])
        rsi_cross_down_60 = (df['RSI'] < 60) & (df['RSI'].shift(1) >= 60)
        
        # 2a. Standard DC with RSI < 60 (Define it here as we deleted the previous definition)
        cond_dc_immediate = (df['MACD'] < df['MACD_Signal']) & (df['MACD'].shift(1) >= df['MACD_Signal'].shift(1)) & (df['RSI'] < 60) & rsi_was_high
        
        # 2b. Delayed DC (RSI confirmation)
        cond_dc_delayed = macd_is_dead & rsi_cross_down_60 & rsi_was_high
        
        sell_profit_mask = rsi_climax_90 | cond_dc_immediate | cond_dc_delayed
        
        # --- SELL Condition (Stop Loss / Trend End) ---
        # TRIGGER:
        # 1. Close Crosses Below Chandelier Exit (Robust)
        # 2. REMOVED: SMA50 Break. (Caused too many whipsaws in flat markets. Chandelier @ ATRx5 is safer.)
        
        chandelier_break = (df['Close'] < df['Chandelier_Exit']) & (df['Close'].shift(1) >= df['Chandelier_Exit'].shift(1))
        # sma50_break = (df['Close'] < df['SMA50']) & (df['Close'].shift(1) >= df['SMA50'].shift(1))
        
        sell_stop_mask = chandelier_break 
        
        # Combine all Buy triggers
        # Priority logic inside iter loop or pre-merge?
        # Let's merge basic buys.
        
        # We need to iterate to apply "Cool Down" (No multiple buys in 15 days)
        # This prevents "Cluster Buying".
        
        # 1. Initialize Signal Series
        df['Signal'] = None
        df['Reason'] = ''
        
        # Convert masks to boolean series for easier access
        s_buy = buy_mask
        s_reversal = reversal_mask
        s_reentry = reentry_mask
        
        s_sell_profit = sell_profit_mask
        s_sell_stop = sell_stop_mask
        
        cooldown_days = 0 
        cooldown_sell_days = 0 # Added Sell Cooldown
        trend_buy_count = 0 # Count number of buys in current trend (Max 3)
        
        # 2. Iterative Application
        # We start from index 0. 
        # Note: Iterating pandas is slow? For 1 ticker (max 3000 rows) it is sub-second. safe.
        
        for i in range(len(df)):
            # Decrease cooldowns
            if cooldown_days > 0:
                cooldown_days -= 1
            if cooldown_sell_days > 0:
                cooldown_sell_days -= 1
                
            # Check Trend Status for Reset (If Trend Logic Breaks, Reset Count)
            # REVERT: Don't silent reset on SMA50 break. Rely on Chandelier Stop.
            # If we dip below SMA50 but hold Chandelier, we are still IN.
            # current_close = df['Close'].iloc[i]
            # current_sma50 = df['SMA50'].iloc[i]
            # if current_close < current_sma50:
            #      trend_buy_count = 0
            
            # Check Sell First (Priority: Stop > Profit)
            # Sell always overrides Buy? Yes.
            
            # STOP LOSS (Must trigger regardless of cooldown usually, but if we just sold, maybe ignore?
            # Actually Stop Loss is distinct. If we hit stop, we hit stop.
            # But let's apply cooldown to avoid noise if it flickers.
            is_sell = False
            reason_sell = ''
            
            if s_sell_stop.iloc[i] and cooldown_sell_days == 0:
                is_sell = True
                reason_sell = '損切り/撤退 (サポートライン割れ)'
                
            elif s_sell_profit.iloc[i] and cooldown_sell_days == 0:
                is_sell = True
                reason_sell = '利確推奨 (RSI90超/トレンド転換)'
                
            if is_sell:
                # STRICT RULE: Only Sell if we actually "Bought" (Count > 0)
                # This prevents "Phantom Sells" when we are already flat.
                if trend_buy_count > 0:
                    df.at[df.index[i], 'Signal'] = 'Sell'
                    df.at[df.index[i], 'Reason'] = reason_sell
                    cooldown_days = 0 
                    cooldown_sell_days = 5 # Prevent repetitive sells
                    trend_buy_count = 0 # Reset buy count on Sell (Full Exit)
                
                # If trend_buy_count == 0, we ignore the Sell (we are flat).
                continue
                
            # BUY LOGIC (Only if cooldown is 0 AND Buy Count < 3)
            if cooldown_days == 0 and trend_buy_count < 3:
                is_buy = False
                reason_buy = ''
                
                # Check Triggers (Priority: Buy > Reversal > Reentry)
                if s_buy.iloc[i]:
                    is_buy = True
                    reason_buy = 'モメンタム始動 (ブレイク+出来高+MACD)'
                elif s_reversal.iloc[i]:
                    is_buy = True
                    reason_buy = 'トレンド転換 (大陽線/MACD好転)'
                elif s_reentry.iloc[i]:
                    is_buy = True
                    reason_buy = '再エントリー (押し目完了/MACD好転)'
                    
                if is_buy:
                    # Also check if we recently sold? (wash sale / noise)
                    # If is_buy is True, and we passed checks.
                    
                    df.at[df.index[i], 'Signal'] = 'Buy'
                    df.at[df.index[i], 'Reason'] = reason_buy
                    cooldown_days = 5 # Wait 5 days (1 week) to prevent immediate cluster buys
                    trend_buy_count += 1 # Increment buy count
                    
        # Done.
        # Note: This replaces the vectorized assignments above.


        # === Current Status Determination ===
        latest = df.iloc[-1]
        summary = {
            'price': latest['Close'],
            'rsi': latest['RSI'],
            'rvol': latest['RVOL'],
            'bb_width': latest['BB_Width'],
            'sma50': latest['SMA50'],
            'macd': latest['MACD'],
            'chandelier': latest['Chandelier_Exit'],
            'mfi': latest['MFI'],
            'status': 'WAIT', 
            'action': 'シグナルなし',
            'last_signal_date': None
        }
        
        # Check last signal logic... same as before roughly
        last_signals = df.iloc[-20:]
        last_buy = last_signals[last_signals['Signal'] == 'Buy'].last_valid_index()
        last_sell = last_signals[last_signals['Signal'] == 'Sell'].last_valid_index()
        
        if latest['Signal'] == 'Buy':
            summary['status'] = 'BUY'
            summary['action'] = '★エントリー推奨: 強いエネルギー放出を確認。'
        elif latest['Signal'] == 'Sell':
            summary['status'] = 'SELL'
            if '利確' in latest['Reason']:
                summary['action'] = '★利益確定推奨: クライマックス(RSI>90)、またはMACD反転。'
            else:
                summary['action'] = '★撤退推奨: 重要なサポートラインを割り込みました。'
        elif latest['Close'] > latest['Chandelier_Exit'] and latest['Close'] > latest['SMA50']:
             summary['status'] = 'HOLD'
             
             # Check Trend Strength
             is_macd_bull = latest['MACD'] > latest['MACD_Signal']
             is_rsi_hot = latest['RSI'] > 80
             
             if is_rsi_hot:
                 summary['action'] = f"【最強モメンタム】RSI{latest['RSI']:.0f}。90を超えるクライマックスまで利益を伸ばしましょう。"
             elif is_macd_bull:
                 summary['action'] = f"上昇トレンド順調。逆指値: ${latest['Chandelier_Exit']:.2f} (Chandelier) にセットして静観推奨。"
             else:
                 summary['action'] = f"トレンド継続中ですが調整の兆し。逆指値: ${latest['Chandelier_Exit']:.2f} を厳守。"
                 
        else:
             summary['status'] = 'WAIT'
             summary['action'] = "トレンド不明確、または調整中。次のチャンスを待ちます。"

        # Calculate Simple Momentum Score for Input Ticker (approximate)
        # Using Short Term Score Logic Proxy: RVOL, RSI, Price vs SMA
        input_score = 0
        if latest['RVOL'] > 2.0: input_score += 30
        elif latest['RVOL'] > 1.5: input_score += 20
        elif latest['RVOL'] > 1.0: input_score += 10

        
        if latest['Close'] > latest['SMA50']: input_score += 20
        if latest['Close'] > latest['SMA150']: input_score += 10
        
        # RSI Sweet spot 50-70
        if 50 <= latest['RSI'] <= 70: input_score += 20
        elif 70 < latest['RSI'] <= 85: input_score += 10
        
        # Squeeze bonus
        if latest['BB_Width'] < 0.15: input_score += 10
        
        summary['score'] = input_score
                
        return df, summary
        
    except Exception as e:
        return None, {"error": str(e)}

def find_better_alternatives(current_ticker, df_metrics, top_n=3):
    """
    Find better momentum stocks in the same sector.
    Prioritizes stocks with valid BUY signals or strong uptrends.
    """
    if df_metrics is None or df_metrics.empty:
        return []
        
    # Get sector of current ticker
    current_sector = TICKER_TO_SECTOR.get(current_ticker.upper())
    
    if not current_sector:
        return []

    candidates = []
    for _, row in df_metrics.iterrows():
        t = row['Ticker']
        if t == current_ticker: continue
        
        s = TICKER_TO_SECTOR.get(t, 'Unknown')
        if s == current_sector:
            # === Filter for "Buy-worthy" conditions ===
            # We want alternatives that are actually GOOD to buy now.
            
            # 1. Must be in uptrend
            price = row.get('Price', 0)
            if price <= 0: continue
            
            # Simple Uptrend Check (if we don't have SMA in metrics, use score)
            # Assuming 'ShortScore' or 'MidScore' is high enough
            
            # Using ShortScore/MidScore from get_ai_stock_picks logic if available?
            # df_metrics passed here is usually the raw metrics + scores.
            
            # Let's calculate a fresh score based on "Buy Signal" proximity
            buy_power = 0
            
            # Trend
            # Use 1mo and 3mo return as trend proxy
            ret_1mo = row.get('1mo', 0)
            ret_3mo = row.get('3mo', 0)
            if ret_1mo > 0 and ret_3mo > 0: buy_power += 20
            
            # Volume
            rvol = row.get('RVOL', 0)
            if rvol > 1.5: buy_power += 30 # High demand
            
            # RSI (Not too hot)
            rsi = row.get('RSI', 50)
            if 50 <= rsi <= 75: buy_power += 30 # Ideal entry zone
            elif rsi > 85: buy_power -= 20 # Too hot
            
            # Total Score from cache
            # We rely on the pre-calculated score if available
            # row index is ticker in some cases? No, iterrows on DataFrame
            # Check if columns exist
            cached_score = 0
            if 'MidScore' in row: cached_score = row['MidScore']
            
            final_score = cached_score + buy_power
            
            candidates.append({
                'Ticker': t,
                'Score': final_score, # Use this combined score for ranking
                'RawScore': cached_score,
                '1mo': ret_1mo,
                'RVOL': rvol
            })
    
    # Sort by Score
    candidates.sort(key=lambda x: x['Score'], reverse=True)
    return candidates[:top_n]

# === Metadata & Localization Helpers ===

def get_ticker_metadata_jp(tickers):
    """
    Fetch or load Japanese metadata (Name, Sector) for given tickers.
    If not in cache, fetches from Yahoo Finance and translates using GoogleTranslator.
    Updates cache automatically.
    
    Returns:
        dict: {ticker: {'name': '銘柄和名', 'sector': 'セクター和名'}}
    """
    metadata_path = "data/metadata_cache.json"
    cache = {}
    
    # Load cache
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except Exception as e:
            print(f"Error loading metadata cache: {e}")
            cache = {}
            
    # Identify missing or non-Japanese entries
    missing_tickers = []
    
    def is_japanese(text):
        for char in text:
            if '\u3040' <= char <= '\u309F' or '\u30A0' <= char <= '\u30FF' or '\u4E00' <= char <= '\u9FFF':
                return True
        return False

    for t in tickers:
        if t not in cache:
            missing_tickers.append(t)
        else:
            # Check if name is Japanese -> (SKIP) Because we disabled translation, name is English.
            # So is_japanese() will fail, causing re-fetch every time. This is BAD.
            # Instead, check if industry is missing (If it's empty, try re-fetching)
            if not cache[t].get('industry'):
                 missing_tickers.append(t)
                
    # If all found, return formatted dict
    if not missing_tickers:
        res = {}
        for t in tickers:
            entry = cache[t]
            sec_key = TICKER_TO_SECTOR.get(t, '')
            
            # Revert to English
            if sec_key:
                sec_disp = sec_key
            else:
                sec_disp = entry.get('industry', '')
                if not sec_disp:
                    sec_disp = '-'
            
            res[t] = {
                'name': entry.get('name', t),
                'sector': sec_disp
            }
        return res
        
    # Fetch missing
    print(f"Fetching/Translating metadata for {len(missing_tickers)} tickers...")
    translator = GoogleTranslator(source='auto', target='ja')
    
    for t in missing_tickers:
        try:
            ticker_obj = yf.Ticker(t)
            info = ticker_obj.info
            
            # Get English name
            name_en = info.get('shortName', info.get('longName', t))
            sector_en = info.get('industry', info.get('sector', ''))
            
            # Translate Name
            # SKIP TRANSLATION FOR PERFORMANCE (User Request)
            name_jp = name_en
            # try:
            #     name_jp = translator.translate(name_en)
            # except:
            #     name_jp = name_en
                
            # Update Cache
            if t not in cache:
                cache[t] = {}
            
            cache[t]['name'] = name_jp
            cache[t]['industry'] = sector_en # Keep EN industry in cache for reference
            cache[t]['summary'] = info.get('longBusinessSummary', '') # Might be English if not translated here

            # Sleep to avoid rate limits
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching metadata for {t}: {e}")
            # Fallback
            if t not in cache:
                cache[t] = {'name': t, 'industry': ''}

    # Save cache
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving metadata cache: {e}")
        
    # Return final dict
    result = {}
    for t in tickers:
        entry = cache.get(t, {'name': t})
        # Use our Sector Map for Sector, fall back to what we have
        my_sector_key = TICKER_TO_SECTOR.get(t, '')
        
        # Revert to English (User Request: "English is fine if it shows up properly")
        if my_sector_key:
            sector_disp = my_sector_key  # Use defined key (e.g. "🧠 Semi: ...")
        else:
            sector_disp = entry.get('industry', '') # Use raw YF industry (English)
            if not sector_disp:
                sector_disp = "-"
        
        # sector_disp = SECTOR_JP_MAP.get(my_sector_key, entry.get('industry', ''))
        
        result[t] = {
            'name': entry.get('name', t),
            'sector': sector_disp
        }
        
    return result
