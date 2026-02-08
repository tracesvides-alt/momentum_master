# 🚀 Momentum Master (モメンタム・マスター)

**Momentum Master** は、米国株市場における「真の強者」を見つけ出すための Streamlit アプリケーションです。
単なる騰落率ランキングではなく、**ミネルヴィニ流の成長株投資基準（Trend Templates）** や **レラティブ・ストレングス（RS）** を考慮し、持続的な上昇トレンドにある銘柄を厳選します。

さらに、AIを活用した**ニュース分析・要約機能**や**セクターヒートマップ**、**Discord連携**など、トレーダーに必要な機能をオールインワンで提供します。

---

## ✨ 主な機能

### 1. 🏆 モメンタム・ランキング (Top 10 / Worst 10)

数千の米国株の中から、以下の基準でスクリーニングを行います。

- **RS Rating (レラティブ・ストレングス)**: 市場の90%以上の銘柄より強いか？
- **Trend Templates**:
  - 株価 > SMA50 > SMA150 > SMA200
  - 52週高値から25%以内にあるか
  - 出来高の急増（Volume Spike）
- **除外基準**: ペニーストック、超低流動性銘柄、買収予定銘柄などを自動除外

### 2. 🤖 AI Analyst & Deep News

Gemini AIを活用し、市場の動きを深く分析します。

- **AI Analyst**: 今日の市場全体の動き、セクターの強弱、注目の銘柄をAIが要約・解説。
- **Deep News Translation**: 英語のニュース記事を DeepL/Google 翻訳で高精度に日本語化。「なぜその銘柄が上がっているのか？」を即座に把握できます。
- **Earnings Focus**: 決算発表や重要カタリストに焦点を当ててニュースを抽出。

### 3. 🌡️ セクターヒートマップ & テーマ分析

- **ヒートマップ**: Tech, Semi, Software, Energy, Bio などの主要セクターの強弱を色分けで可視化。
- **テーマETF**: AI, Cyber, Crypto, Nuclear, Space など、特定のテーマ株のモメンタムも追跡。
- **Crypto & Altcoins**: ビットコインや主要アルトコインの動きもセクターマップ内で確認可能。

### 4. 🚨 Daily Signals & Alerts

起動時に以下の「買い合図」を自動検知して表示します。

- **Persistence (持続性)**: 3日連続でランキング入りしている銘柄。
- **Volume Spike**: 出来高が急増している銘柄。
- **Reversal**: 逆張りシグナルや押し目買いのチャンスを示唆。

### 5. 💬 Discord 連携

- `generate_tweet.py` や `discord_utils.py` を介して、その日のランキングやAI分析結果を Discord サーバーに自動投稿可能。

### 6. ⚡ 高速化アーキテクチャ (Caching)

- `update_data.py` によるプレ・コンピューテーション（事前計算）システムを採用。
- 重いデータ取得や計算をバックグラウンドで処理し、結果を `data/` フォルダにキャッシュすることで、アプリの起動と操作を爆速化。

---

## 🛠️ インストール & 実行方法

### 必要要件

- Python 3.10+
- [Git](https://git-scm.com/)

### セットアップ手順

1. **リポジトリのクローン**

   ```bash
   git clone https://github.com/YourName/momentum_master.git
   cd momentum_master
   ```

2. **依存ライブラリのインストール**

   ```bash
   pip install -r requirements.txt
   ```

3. **データの更新 (初回および毎日)**
   アプリを起動する前に、最新の株価データを取得・計算します。

   ```bash
   python update_data.py
   ```

   ※ これにより `data/` フォルダ内にキャッシュファイルが生成されます。

4. **アプリの起動**

   ```bash
   streamlit run momentum_master_app.py
   ```

   ブラウザが開き、アプリが表示されます。

---

## 📂 ファイル構成

- `momentum_master_app.py`: メインのアプリケーションファイル (UI, ニュースロジック)
- `market_logic.py`: 株価計算、スクリーニング、セクター定義、テクニカル分析のコアロジック
- `update_data.py`: データ取得・計算・キャッシュ生成用スクリプト (毎日実行推奨)
- `generate_tweet.py`: 市場分析結果のテキスト生成・Discord投稿用スクリプト
- `discord_utils.py`: Discord Webhook連携用ユーティリティ
- `data/`: 生成されたキャッシュデータ (Git管理外)
- `requirements.txt`: 必要なPythonライブラリ一覧

## 🛡️ ライセンス & 注意事項

- 本ツールは投資判断の参考情報を提供するものであり、利益を保証するものではありません。投資は自己責任で行ってください。
- Yahoo Finance API (yfinance) を使用しています。商用利用の際はデータプロバイダーの規約をご確認ください。
