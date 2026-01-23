import pandas as pd
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
    from deep_translator import GoogleTranslator
    import concurrent.futures
    import threading
    
    metadata = {}
    total = len(tickers)
    
    # 既存のキャッシュをロード（増分更新のため）
    metadata_path = "data/metadata_cache.json"
    existing_metadata = {}
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                existing_metadata = json.load(f)
            print(f"  ℹ️ Loaded existing metadata for {len(existing_metadata)} tickers.")
        except:
            pass

    # 処理対象のリストを作成（スキップ判定）
    targets = []
    skipped_count = 0
    
    # 日本語判定用関数
    def contains_japanese(text):
        for char in text:
            if '\u3040' <= char <= '\u309F' or '\u30A0' <= char <= '\u30FF' or '\u4E00' <= char <= '\u9FFF':
                return True
        return False

    for i, ticker in enumerate(tickers, 1):
        # キャッシュヒット確認（スキップ条件: キャッシュにあり、かつsummaryが日本語を含む）
        if ticker in existing_metadata:
            cached_data = existing_metadata[ticker]
            summary = cached_data.get('summary', '')
            name = cached_data.get('name', '')
            
            # 日本語が含まれている場合のみスキップ
            if summary and name and contains_japanese(summary):
                metadata[ticker] = cached_data
                skipped_count += 1
                continue
        targets.append(ticker)
    
    if skipped_count > 0:
        print(f"  Skipped {skipped_count}/{total} tickers (Cached).")
    
    if not targets:
        return metadata

    print(f"  Fetching metadata for {len(targets)} tickers in parallel...")

    # スレッドセーフなカウンタ
    lock = threading.Lock()
    completed_count = 0

    def fetch_single(tkr):
        nonlocal completed_count
        res = {'name': tkr, 'industry': '', 'summary': ''}
        
        # エラー時のフォールバック用にキャッシュ値を確認
        if tkr in existing_metadata:
             default_val = existing_metadata[tkr]
        else:
             default_val = {'name': tkr, 'industry': '', 'summary': ''}

        try:
            t = yf.Ticker(tkr)
            info = t.info
            
            summary_en = info.get('longBusinessSummary', '')[:160]
            summary_ja = ""
            if summary_en:
                try:
                    translator = GoogleTranslator(source='auto', target='ja')
                    summary_ja = translator.translate(summary_en)
                except:
                    summary_ja = summary_en
            
            res = {
                'name': info.get('shortName', info.get('longName', tkr)),
                'industry': info.get('industry', info.get('sector', '')),
                'summary': summary_ja
            }
        except Exception:
            res = default_val

        with lock:
            completed_count += 1
            if completed_count % 10 == 0 or completed_count == len(targets):
                print(f"    Processing... {completed_count}/{len(targets)}", end='\r')
        
        return tkr, res

    # 並列実行 (max_workers=2程度が安全)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
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
        
        # 4. メタデータ取得・保存（新規追加）
        print("Fetching Metadata for All Candidates...")
        metadata = fetch_metadata_batch(candidates)
        
        metadata_path = "data/metadata_cache.json"
        with open(metadata_path, "w", encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"Saved {metadata_path} ({len(metadata)} tickers)")
        
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
