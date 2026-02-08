import pandas as pd
import numpy as np
import pickle
import os
import json
from datetime import datetime
import yfinance as yf
import market_logic # Custom Logic Module

def fetch_metadata_batch(tickers):
    """
    複数ティッカーのメタデータを一括取得（並列処理）
    Returns: dict {ticker: {'name': str, 'industry': str, 'summary': str}}
    """
    import concurrent.futures
    import threading
    import time
    
    metadata = {}
    total = len(tickers)
    
    # Existing cache handling restricted to NOT loading old file if we want to purge Japanese?
    # Actually, let's load it ONLY if we want to preserve correct English entries? 
    # But to be safe, let's start fresh or just rely on API. 
    # If we want to be 100% sure, we should probably ignore the old file if it has Japanese.
    # For now, let's just rely on the API and if it fails, we get empty (better than Japanese).
    
    metadata_path = "data/metadata_cache.json"
    existing_metadata = {}
    # Skip loading existing to force fresh English
    # if os.path.exists(metadata_path):
    #     try:
    #         with open(metadata_path, 'r', encoding='utf-8') as f:
    #             existing_metadata = json.load(f)
    #     except:
    #         pass

    # 全て処理対象にする
    targets = tickers 
    
    print(f"  Fetching metadata for {len(targets)} tickers in parallel (English, Slow Mode)...")

    # スレッドセーフなカウンタ
    lock = threading.Lock()
    completed_count = 0

    def fetch_single(tkr):
        nonlocal completed_count
        res = {'name': tkr, 'industry': '', 'summary': ''}
        
        try:
            # Rate limit protection
            time.sleep(0.2) 
            
            t = yf.Ticker(tkr)
            info = t.info
            
            # 1. Name & Industry (English)
            name = info.get('shortName', info.get('longName', tkr))
            industry = info.get('industry', info.get('sector', ''))

            # 2. Summary (English Only)
            summary_en = info.get('longBusinessSummary', '')
            if summary_en:
                summary_en = summary_en[:300] + "..." if len(summary_en) > 300 else summary_en
            
            res = {
                'name': name,
                'industry': industry,
                'summary': summary_en 
            }
        except Exception:
            # Fallback to empty (English preference)
            res = {'name': tkr, 'industry': '', 'summary': ''}

        with lock:
            completed_count += 1
            if completed_count % 10 == 0 or completed_count == len(targets):
                print(f"    Processing... {completed_count}/{len(targets)}", end='\r')
        
        return tkr, res

    # 並列実行 (レート制限回避のためスレッド数削減)
    # 429防止のためシングルスレッドに近い動きにする (max_workers=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future_to_ticker = {executor.submit(fetch_single, t): t for t in targets}
        for future in concurrent.futures.as_completed(future_to_ticker):
            tkr, data = future.result()
            metadata[tkr] = data
            
    print("") # 改行
    return metadata

def main():
    print(f"Starting Data Update: {datetime.now()}")
    
    # 1. 候補取得
    print("Fetching Candidates...")
    candidates = market_logic.get_momentum_candidates()
    print(f"Candidates Count: {len(candidates)}")
    
    # 2. データ計算
    print("Calculating Metrics...")
    df_metrics, history_dict = market_logic.calculate_momentum_metrics(candidates)
    
    if df_metrics is not None and not df_metrics.empty:
        # 3. 保存 (dataフォルダを作成して保存)
        os.makedirs("data", exist_ok=True)
        
        # ランキングデータ
        csv_path = "data/momentum_cache.csv"
        df_metrics.to_csv(csv_path, index=False)
        print(f"Saved {csv_path}")
        
        # チャート用履歴データ (Pickle形式が軽くて速い)
        pkl_path = "data/history_cache.pkl"
        with open(pkl_path, "wb") as f:
            pickle.dump(history_dict, f)
        print(f"Saved {pkl_path}")

        # 🚨 Pre-calculate Daily Signals (Speed up App Startup)
        print("Calculating Daily Signals...")
        daily_signals = market_logic.get_todays_signals(history_dict)
        
        # Save as JSON
        sig_path = "data/daily_signals_cache.json"
        
        # Helper to convert numpy/pandas types to native python for JSON
        def convert_types(obj):
            if isinstance(obj, dict):
                return {k: convert_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types(i) for i in obj]
            elif isinstance(obj, (np.int64, np.int32, np.int16)):
                return int(obj)
            elif isinstance(obj, (np.float64, np.float32, float)):
                return float(obj)
            return obj
            
        daily_signals_clean = convert_types(daily_signals)
        
        with open(sig_path, "w", encoding='utf-8') as f:
            json.dump(daily_signals_clean, f, ensure_ascii=False, indent=2)
        print(f"Saved {sig_path}")
        
        # 4. メタデータ取得・保存（新規追加）
        print("Fetching Metadata for All Candidates...")
        # metadata = fetch_metadata_batch(candidates) # Skip for speed during this fix
        # Define metadata_path for safety in case we uncomment
        metadata_path = "data/metadata_cache.json"
        
        # Check if we should run metadata fetch (Time consuming)
        # For now, let's skip it if we just want to fix the indices/earnings
        # But if the file is missing, we might need it. 
        # Actually, let's run it because we need the file. 
        # To make it faster for this run, maybe I can just load existing if available?
        # The previous run CRASHED before writing. So we lost the data in memory.
        # We have to fetch again.
        
        metadata = fetch_metadata_batch(candidates)
        with open(metadata_path, "w", encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"Saved {metadata_path} ({len(metadata)} tickers)")

        # 5. 主要指数データ取得・保存
        print("Fetching Major Indices Data...")
        indices_data = {}
        # Fetch 5 days history for calculation
        period_days = {'1d': 5, '5d': 10, '1mo': 35, '3mo': 100, '6mo': 200, 'YTD': 400, '1y': 400}
        
        from market_logic import MAJOR_INDICES
        
        for ticker, (jp_name, emoji) in MAJOR_INDICES.items():
            try:
                # Fetch max needed history (approx 400 days for YTD/1y)
                t_obj = yf.Ticker(ticker)
                hist = t_obj.history(period="2y") # Fetch enough
                
                if len(hist) < 2:
                    indices_data[ticker] = {"name": jp_name, "emoji": emoji, "error": True}
                    continue

                period_returns = {}
                last_close = hist['Close'].iloc[-1]
                
                for pid, days_back_approx in period_days.items():
                    try:
                        base_close = None
                        if pid == '1d':
                            base_close = hist['Close'].iloc[-2]
                        elif pid == 'YTD':
                            this_year = datetime.now().year
                            ytd_data = hist[hist.index.year == this_year]
                            if not ytd_data.empty:
                                base_close = ytd_data['Close'].iloc[0]
                            else:
                                base_close = hist['Close'].iloc[0]
                        else:
                            # Use business days approximation
                            target_days = {'5d': 5, '1mo': 21, '3mo': 63, '6mo': 126, '1y': 252}
                            db = target_days.get(pid, 5)
                            if len(hist) > db:
                                base_close = hist['Close'].iloc[-db - 1] 
                            else:
                                base_close = hist['Close'].iloc[0]
                        
                        if base_close:
                            pct = ((last_close - base_close) / base_close) * 100
                            period_returns[pid] = pct
                        else:
                            period_returns[pid] = 0.0
                    except:
                        period_returns[pid] = 0.0
                
                indices_data[ticker] = {
                    "name": jp_name, 
                    "emoji": emoji, 
                    "returns": period_returns,
                    "price": last_close,
                    "error": False
                }
            except Exception as e:
                print(f"  Error fetching {ticker}: {e}")
                indices_data[ticker] = {"name": jp_name, "emoji": emoji, "error": True}
        
        indices_path = "data/indices_cache.json"
        with open(indices_path, "w", encoding='utf-8') as f:
            json.dump(indices_data, f, ensure_ascii=False, indent=2)
        print(f"Saved {indices_path}")

        # 6. 決算日データ取得・保存
        print("Fetching Earnings Dates...")
        earnings_data = {}
        
        import concurrent.futures
        def fetch_earnings(tkr):
            # Skip ETFs (they don't have standard earnings dates and cause 404s)
            # We need to access etf_tickers here. It's not defined in this scope yet.
            # Moving the import and set creation up.
            pass 

        # Import explicitly here for clarity or use the one from main scope if passed
        from market_logic import THEMATIC_ETFS
        etf_tickers = set(THEMATIC_ETFS.values())
        
        # Suppress yfinance error output context manager
        import contextlib
        import io

        def fetch_earnings_safe(tkr):
            if tkr in etf_tickers:
                return tkr, "-"
            
            # Additional known problematic tickers
            if tkr in ["SENT", "CDNS", "SNPS"]: # Add others if found
                 return tkr, "-"
                
            try:
                t = yf.Ticker(tkr)
                
                # Suppress stderr to hide "HTTP Error 404" from yfinance
                f = io.StringIO()
                with contextlib.redirect_stderr(f):
                    cal = t.calendar
                    
                next_date_str = "-"
                
                if isinstance(cal, dict):
                    dates = cal.get('Earnings Date', [])
                    if dates:
                        d = dates[0]
                        if isinstance(d, (datetime, pd.Timestamp)):
                             next_date_str = d.strftime("%Y-%m-%d")
                        else:
                             next_date_str = str(d)
                elif isinstance(cal, pd.DataFrame):
                     # Handle DataFrame return (sometimes yfinance returns DF)
                     if not cal.empty:
                         # Trying to find earnings date column
                         if 'Earnings Date' in cal.columns:
                             d = cal['Earnings Date'].iloc[0]
                             if isinstance(d, (datetime, pd.Timestamp)):
                                 next_date_str = d.strftime("%Y-%m-%d")
                             else:
                                 next_date_str = str(d)
                         elif 'Earnings High' in cal.index: # Transposed?
                             pass
                         elif not cal.empty:
                             pass
                return tkr, next_date_str
            except:
                return tkr, "-"

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future_to_earnings = {executor.submit(fetch_earnings_safe, t): t for t in candidates}
            count = 0
            for future in concurrent.futures.as_completed(future_to_earnings):
                t, d = future.result()
                earnings_data[t] = d
                count += 1
                if count % 20 == 0: print(f"    Earnings: {count}/{len(candidates)}", end='\r')
        
        print("")
        earnings_path = "data/earnings_cache.json"
        with open(earnings_path, "w", encoding='utf-8') as f:
            json.dump(earnings_data, f, ensure_ascii=False, indent=2) 
        print(f"Saved {earnings_path}")

        # 7. Trending Tickers (New)
        print("Fetching Trending Tickers...")
        try:
            import requests # Lazy import
            from io import StringIO
            from market_logic import STATIC_MENU_ITEMS, THEMATIC_ETFS
            
            # Create a set of ETFs to skip earnings fetch
            etf_tickers = set(THEMATIC_ETFS.values())

            url = "https://finance.yahoo.com/most-active"
            fallback_tickers = ['RKLB', 'MU', 'OKLO', 'LLY', 'SOFI']
            exclusion_set = {t for t in STATIC_MENU_ITEMS if not t.startswith('---')}
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                dfs = pd.read_html(StringIO(response.text))
                candidates_tr = []
                if dfs:
                    df_scrape = dfs[0]
                    if 'Symbol' in df_scrape.columns:
                        candidates_raw = df_scrape['Symbol'].head(30).dropna().astype(str).tolist()
                        candidates_tr = [t.split()[0] for t in candidates_raw if t]

                        # Filter
                        filtered = [t for t in candidates_tr if t not in exclusion_set]
                        final_list = filtered[:5]
                        if not final_list: final_list = fallback_tickers
                    else:
                        final_list = fallback_tickers
                else:
                    final_list = fallback_tickers
            else:
                final_list = fallback_tickers
                
            data_tr = {"tickers": final_list}
            with open("data/trending_cache.json", "w", encoding='utf-8') as f:
                json.dump(data_tr, f)
            print(f"Saved data/trending_cache.json: {final_list}")
            
        except Exception as e:
            print(f"Trending Fetch Failed: {e}")

        # 更新時刻を記録
        txt_path = "data/last_updated.txt"
        # Use JST (UTC+9) for Japan time
        from datetime import timezone, timedelta
        JST = timezone(timedelta(hours=9))
        with open(txt_path, "w") as f:
            f.write(datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"))
        print(f"Saved {txt_path}")
            
    else:
        print("Data update failed (Empty DataFrame)")
        exit(1) # エラー終了

if __name__ == "__main__":
    main()
