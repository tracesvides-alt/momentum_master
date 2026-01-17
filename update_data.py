import pandas as pd
import pickle
import os
from datetime import datetime
import market_logic # Custom Logic Module

def main():
    print(f"🚀 Starting Data Update: {datetime.now()}")
    
    # 1. 候補取得
    print("📋 Fetching Candidates...")
    candidates = market_logic.get_momentum_candidates()
    print(f"📋 Candidates Count: {len(candidates)}")
    
    # 2. データ計算
    print("📊 Calculating Metrics...")
    df_metrics, history_dict = market_logic.calculate_momentum_metrics(candidates)
    
    if df_metrics is not None and not df_metrics.empty:
        # 3. 保存 (dataフォルダを作成して保存)
        os.makedirs("data", exist_ok=True)
        
        # ランキングデータ
        csv_path = "data/momentum_cache.csv"
        df_metrics.to_csv(csv_path, index=False)
        print(f"✅ Saved {csv_path}")
        
        # チャート用履歴データ (Pickle形式が軽くて速い)
        pkl_path = "data/history_cache.pkl"
        with open(pkl_path, "wb") as f:
            pickle.dump(history_dict, f)
        print(f"✅ Saved {pkl_path}")
        
        # 更新時刻を記録
        txt_path = "data/last_updated.txt"
        # Use JST (UTC+9) for Japan time
        from datetime import timezone, timedelta
        JST = timezone(timedelta(hours=9))
        with open(txt_path, "w") as f:
            f.write(datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"))
        print(f"✅ Saved {txt_path}")
            
    else:
        print("❌ Data update failed (Empty DataFrame)")
        exit(1) # エラー終了

if __name__ == "__main__":
    main()
