import yfinance as yf
import pandas as pd
import requests
from io import StringIO
import time
import random
import concurrent.futures
import re
from datetime import datetime

# --- Constants ---

# "Momentum Universe" - High Beta, Liquid, & Thematic Leaders
SECTOR_DEFINITIONS = {
    # ---------------------------------------------------------
    # 1. AI & Semiconductor (Hardware / Cloud)
    # ---------------------------------------------------------
    "🖥️ AI: Hardware & Cloud Infra": [
        "CRWV", "NVDA", "AMD", "SMCI", "VRT", "ANET", "PSTG", "DELL", "HPE", 
        "TSM", "AVGO", "ARM", "MU", "QCOM", "AMAT", "LRCX", "GFS", "STM", 
        "UMC", "ASX", "WDC", "ENTG", "AMKR", "ALAB", "NVTS", "SWKS", "KLAR", 
        "MCHP", "TXN", "ADI", "ON", "Q", "APLD", "FYBR", "LUMN", "VIAV", "CIEN"
    ],

    # ---------------------------------------------------------
    # 2. AI Software & Services
    # ---------------------------------------------------------
    "🧠 AI: Software & SaaS": [
        "FIG", "PLTR", "MSFT", "GOOGL", "GOOG", "META", "NOW", "DDOG", "SNOW", 
        "MDB", "PATH", "AI", "BBAI", "SOUN", "ESTC", "CDNS", "SNPS", "ZETA", 
        "SYM", "DOCN", "APP", "TTD", "TEAM", "HUBS", "GTLB", "CFLT", "NET", 
        "OKTA", "CRWD", "PANW", "FTNT", "ZS", "ORCL", "SAP", "IBM", "INTU", 
        "ADBE", "CRM", "WDAY", "DOCU", "ZM", "GEN", "BOX", "DBX", "ASAN", 
        "VRNS", "CCC", "FRSH", "KVYO", "UPWK", "MGNI", "OPCH", "PRO", "PAYC", 
        "TYL", "RDDT", "DJT"
    ],

    # ---------------------------------------------------------
    # 3. Crypto & FinTech
    # ---------------------------------------------------------
    "💸 Crypto & FinTech": [
        "CRCL", "XYZ", "MSTR", "COIN", "MARA", "RIOT", "HOOD", "PYPL", "XYZ", 
        "SOFI", "AFRM", "UPST", "BILL", "TOST", "FOUR", "PAYX", "ADP", "FIS", 
        "FISV", "GPN", "FLUT", "DKNG", "RELY", "INTR", "PAGS", "WU", "STNE", 
        "XP", "NU", "LC", "DLO", "BLSH", "GLXY","CORZ", "IREN", "WULF", 
        "CIFR", "CLSK", "BTDR",  "HIVE", "BITF", "HUT"
    ],

    # ---------------------------------------------------------
    # 4. Space & Defense
    # ---------------------------------------------------------
    "🌌 Space & Defense": [
        "RKLB", "ASTS", "LUNR", "JOBY", "ACHR", "BA", "PL", "SPIR", "SPCE", 
        "IRDM", "SATS", "ONDS", "RTX", "KTOS", "HWM", "LMT", "GD", "NOC", 
        "LHX", "AMTM", "AVAV", "AXON", "BWXT"
    ],

    # ---------------------------------------------------------
    # 5. Energy: Nuclear (AI Power Theme)
    # ---------------------------------------------------------
    "☢️ Energy: Nuclear": [
        "OKLO", "SMR", "UEC", "UUUU", "CCJ", "NXE", "LEU", "DNN", "NNE", "GEV"
    ],

    # ---------------------------------------------------------
    # 6. Energy: Power & Renewables
    # ---------------------------------------------------------
    "⚡ Energy: Power & Renewables": [
        "VST", "CEG", "NRG", "NEE", "DUK", "SO", "AEP", "EXC", "PEG", "PPL", 
        "SRE", "CNP", "ED", "EIX", "ETR", "LNT", "NI", "WEC", "WTRG", "CMS", 
        "ES", "XEL", "PCG", "AES", "FLNC", "BE", "ENPH", "SEDG", "RUN", "NXT", 
        "EOSE", "STEM"
    ],

    # ---------------------------------------------------------
    # 7. Energy: Oil & Gas
    # ---------------------------------------------------------
    "🛢️ Energy: Oil & Gas": [
        "PR", "XOM", "CVX", "OXY", "EOG", "SLB", "HAL", "BKR", "COP", "DVN", 
        "VLO", "MPC", "PSX", "PBR", "PBR-A", "BP", "SU", "EC", "EQNR", "YPF", 
        "TRP", "KMI", "WMB", "ET", "EPD", "CTRA", "AR", "EQT", "SM", "OKE", 
        "FTI", "DINO", "PBF", "MUR", "AM", "LBRT", "CNQ", "APA", "SHEL", "VZLA", 
        "MTDR", "CHYM"
    ],

    # ---------------------------------------------------------
    # 8. BioPharma (Major & Obesity)
    # ---------------------------------------------------------
    "💊 BioPharma: Big Pharma & Obesity": [
        "LLY", "NVO", "VKTX", "PFE", "MRK", "AMGN", "BMY", "ABBV", "JNJ", 
        "GILD", "AZN", "SNY", "TEVA"
    ],

    # ---------------------------------------------------------
    # 9. BioPharma (Biotech & Gene)
    # ---------------------------------------------------------
    "🧬 BioPharma: Biotech & Gene": [
        "CRSP", "BEAM", "ARWR", "SRPT", "VRTX", "ALKS", "INCY", "EXEL", "LEGN", 
        "RPRX", "HALO", "ADMA", "BBIO", "SMMT", "FOLD", "TVTX", "ROIV", "NTLA", 
         "APQT", "LQDA", "NUVB", "ERAS", "SNDK", "TAK", "INSM", "BMRN", 
        "BMNR", "AXSM", "VVV", "INDV", "OCUL", "RNA", "ADPT", "KOD", "ARQT", 
        "CPRX", "VIR", "BNTX"
    ],

    # ---------------------------------------------------------
    # 10. MedTech & Health Services
    # ---------------------------------------------------------
    "🏥 MedTech & Health": [
        "UNH", "CVS", "ABT", "DHR", "TMO", "SYK", "BSX", "EW", "MDT", "DXCM", 
        "ZTS", "GEHC", "CNC", "DOCS", "ALHC", "NVST", "BRKR", "OGN", "BAX", 
        "XRAY", "CAH", "BHC", "SHC", "COO", "HIMS", "WRBY", "NEOG", "OSCR", 
        "ALGN", "RMD", "HCA", "ELV", "CI", "HUM", "MCK", "COR"
    ],

    # ---------------------------------------------------------
    # 11. Consumer: Food & Beverage
    # ---------------------------------------------------------
    "🍔 Consumer: Food & Bev": [
        "MICC", "KO", "PEP", "MNST", "CELH", "MCD", "SBUX", "CMG", "CAVA", 
        "HRL", "KHC", "MDLZ", "CPB", "CAG", "GIS", "TAP", "BUD", "STZ", "MO", 
        "PM", "BTI"
    ],

    # ---------------------------------------------------------
    # 12. Consumer: Retail & E-Commerce
    # ---------------------------------------------------------
    "🛒 Consumer: Retail & E-Com": [
        "AMZN", "WMT", "COST", "TGT", "LOW", "TJX", "ROST", "ETSY", "EBAY", 
        "CHWY", "CART", "DASH", "UBER", "LYFT", "GRND", "MTCH", "W", "BBY", 
        "ANF", "AEO", "KSS", "M", "VSCO", "BROS", "YMM", "PDD", "BABA", "JD", 
        "VIPS", "CPNG", "VNET", "BILI", "TME"
    ],

    # ---------------------------------------------------------
    # 13. Consumer: Apparel & Leisure
    # ---------------------------------------------------------
    "👗 Consumer: Apparel & Leisure": [
        "NKE", "LULU", "DECK", "ONON", "BIRK", "VFC", "LEVI", "CPRI", "UA", 
        "UAA", "RCL", "CCL", "NCLH", "VIK", "LVS", "MGM", "CZR", "DIS", "NFLX", 
        "SPOT", "PINS", "SNAP", "TTWO", "EA", "ROKU", "LYV", "IHRT", "CNK", 
        "GENI", "SBET", "STUB", "VISN", "RUM"
    ],

    # ---------------------------------------------------------
    # 14. Auto & EV
    # ---------------------------------------------------------
    "🚗 Auto & EV": [
        "TSLA", "RIVN", "LCID", "LI", "XPEV", "NIO", "ZETA", "PSNY", "F", 
        "GM", "STLA", "TM", "HMC", "CNH", "GNTX", "APTV", "GT", "LKQ", "CVNA", 
        "KMX", "ALV", "BWA", "QS", "GTX", "HOG", "MBLY", "HSAI"
    ],

    # ---------------------------------------------------------
    # 15. Real Estate & REITs
    # ---------------------------------------------------------
    "🏘️ Real Estate & REITs": [
        "MRP", "PLD", "AMT", "CCI", "O", "VICI", "GLPI", "WELL", "VTR", "ARE", 
        "CUBE", "REXR", "INVH", "AMH", "EQR", "UDR", "IRM", "WY", "Z", "OPEN", 
        "CSGP", "BEKE", "HR", "APLE", "STWD", "AGNC", "NLY", "RITM", "MPW", 
        "DBRG", "IRT", "DOC", "COLD", "SBRA", "BRX", "PDI", "COMP", "HST"
    ],

    # ---------------------------------------------------------
    # 16. Finance: Banks & Capital Markets
    # ---------------------------------------------------------
    "🏦 Finance: Banks & Capital": [
        "JPM", "BAC", "WFC", "C", "MS", "GS", "SCHW", "BLK", "AXP", "V", "MA", 
        "BRK-B", "AIG", "MET", "KKR", "BX", "APO", "ARES", "STT", "BK", "USB", 
        "PNC", "TFC", "COF", "FITB", "RF", "KEY", "CFG", "HBAN", "CADE", "FNB", 
        "IBKR", "DB", "UBS", "BCS", "LYG", "ITUB", "MFC", "TD", "AFL", "WRB", 
        "PGR", "EQH", "GNW", "SEI", "GOF", "ARCC", "OBDC", "BGC", "BANC", "EBC", 
        "MFG", "SMFG", "MUFG", "JEF", "PRMB", "BPRE", "COLB"
    ],

    # ---------------------------------------------------------
    # 17. Industrials & Transport
    # ---------------------------------------------------------
    "🏗️ Industrials & Transport": [
        "TIC", "CAT", "ETN", "EMR", "PWR", "URI", "JCI", "IR", "OTIS", "CARR", 
        "FIX", "GVA", "MLM", "VMC", "MMM", "GE", "HON", "ITW", "PH", "FAST", 
        "MAS", "BLDR", "CRH", "ESI", "AL", "CPRT", "RHI", "FTV", "FLR", "NVT", 
        "GTES", "APG", "TTEK", "FLEX", "CLS", "AXTA", "PCAR", "DAL", "UAL", 
        "AAL", "LUV", "ALK", "CSX", "UNP", "CP", "FDX", "UPS", "ODFL", "KNX", 
        "RXO", "ZIM", "FRO", "FLY", "QXO"
    ],

    # ---------------------------------------------------------
    # 18. Resources & Materials
    # ---------------------------------------------------------
    "⛏️ Resources & Materials": [
        "FCX", "VALE", "RIO", "BHP", "CLF", "NEM", "GOLD", "AEM", "ALB", "AA", 
        "SCCO", "MP", "CENX", "CDE", "HL", "AG", "EXK", "TGB", "FSM", "SSRM", 
        "IAG", "SVM", "PAAS", "TECK", "HBM", "GFI", "AU", "NG", "AGI", "ORLA", 
        "CC", "OLN", "IFF", "BALL", "IP", "GPK", "SUZ", "CE", "EMN", "HUN", 
        "MOS", "NTR", "HYMC", "VZLA", "AMCR", "DOW", "LIN", "DD", "LYB", "PHYS", 
        "PSLV", "IE", "NGD"
    ],

    # ---------------------------------------------------------
    # 20. Housing & Construction
    # ---------------------------------------------------------
    "🏠 Homebuilders & Residential": [
        "DHI", "LEN", "PHM", "TOL", "NVR", "KBH", "TMHC", "MTH", "ARLO", 
        "BLDR", "MAS", "MHK", "ABNB", "Z", "OPEN", "EXP", "HD", "SHW"
    ],
    
    # ---------------------------------------------------------
    # 21. Tech: Quantum Computing
    # ---------------------------------------------------------
    "⚛️ Tech: Quantum Computing": [
        "IONQ", "QBTS", "RGTI", "QMCO","ARQQ","LAES","QUBT"
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
    "Cloud Computing (クラウド)": "CLOU",
    "Cybersecurity (サイバー)": "CIBR",
    "Robotics & AI (ロボット)": "BOTZ",
    "Semiconductors (半導体)": "SMH",
    "Genomics (ゲノム)": "GNOM",
    "Healthcare Providers (医療)": "IHF",
    "Medical Devices (医療機器)": "IHI",

    # --- 🛒 消費・トレンド (Consumer) ---
    "E-commerce (EC)": "IBUY",
    "Fintech (フィンテック)": "FINX",
    "Millennials (若者消費)": "MILN",
    "Homebuilders (住宅)": "XHB",
    
    # --- 🛡️ ディフェンシブ・マクロ (Defensive/Macro) ---
    "Healthcare (ヘルスケア全体)": "XLV",
    "Consumer Staples (必需品)": "XLP",
    "Utilities (公益)": "XLU",
    "High Dividend (高配当)": "VYM",
    "Treasury 20Y+ (米国債)": "TLT",
    "VIX Short-Term (恐怖指数)": "VIXY", 

    # --- ⛏️ コモディティ・暗号資産 (Hard Assets) ---
    "Gold (金)": "GLD",
    "Silver (銀)": "SLV",
    "Oil & Gas (石油)": "XOP",
    "Copper Miners (銅)": "COPX",
    "Bitcoin Strategy (ビットコイン)": "BITO"
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
        "https://finance.yahoo.com/markets/stocks/52-week-gainers/"
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
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_source, url): url for url in sources}
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            if df is not None:
                 # Yahoo usually has 'Symbol' and 'Name'
                if 'Symbol' in df.columns:
                    # Take top 15
                    top_df = df.head(15)
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
    chunk_size = 30 # Conservative batch size
    dfs = []
    
    print(f"Fetching data for {len(tickers)} tickers in chunks of {chunk_size}...")
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # Variable sleep to mimic human behavior slightly and respect limits
            time.sleep(1.5) 
            
            # Re-enabling threads for speed within small batches, but carefully
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
                # Check if 50MA crossed 200MA in the last few days
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
                metrics['Is_Squeeze'] = metrics['BB_Width'] < (avg_width_20 * 0.8) if not pd.isna(avg_width_20) else False
            else:
                metrics['BB_Upper'] = 999999
                metrics['BB_Lower'] = 0
                metrics['BB_Width'] = 1.0
                metrics['Is_Squeeze'] = False

            # 3. 52-Week High/Low
            metrics['High52'] = t_data['Close'].max()
            metrics['Low52'] = t_data['Close'].min()

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
            
            # Save history
            norm_hist = (t_data['Close'] / t_data['Close'].iloc[0]) * 100
            history_dict[t] = norm_hist

        except Exception as e:
            # print(f"Error calc {t}: {e}")
            continue

    # --- Fetch Fundamentals (ShortRatio) for valid tickers ---
    if stats_list:
        valid_tickers = [m['Ticker'] for m in stats_list]
        
        def get_fund(tick):
            try:
                inf = yf.Ticker(tick).info
                return (tick, inf.get('shortRatio', 0))
            except:
                return (tick, 0)
        
        # Limit max workers to avoid overload
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
             results = executor.map(get_fund, valid_tickers)
        
        fund_map = {r[0]: r[1] for r in results}
        
        for m in stats_list:
            m['ShortRatio'] = fund_map.get(m['Ticker'], 0)
            
    if not stats_list:
        return None, None
        
    df_metrics = pd.DataFrame(stats_list)
    # Ensure all new columns are present
    cols = [
        'Ticker', 'Signal', 'Price', '1d', '5d', '1mo', '3mo', '6mo', 'YTD', '1y', 
        'RVOL', 'RSI', 'ShortRatio', 
        'High52', 'Low52', 'SMA50', 'SMA200', 'BB_Upper', 'BB_Lower', 'Is_Squeeze', 'BB_Width',
        'GC_Just_Now', 'DC_Just_Now', 'Above_SMA50'
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
