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
    ],
    
    # ---------------------------------------------------------
    # 22. Engineering & Construction
    # ---------------------------------------------------------
    "🏗️ Engineering & Construction": [
        "AGX"
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
            batch_data = yf.download(chunk, period="1y", group_by='ticker', auto_adjust=True, progress=False, threads=True)
            
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
            
            # Save history
            norm_hist = (t_data['Close'] / t_data['Close'].iloc[0]) * 100
            history_dict[t] = norm_hist

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
