import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import requests
from io import StringIO
import random
from datetime import datetime, date
import plotly.express as px
import re
import pickle
import os
from deep_translator import GoogleTranslator
from newspaper import Article, Config
import nltk

# Ensure NLTK data is available
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)

@st.cache_data(show_spinner=False, ttl=86400)
def get_article_summary(url):
    """
    Downloads article content using newspaper3k, extracts summary via NLP,
    and translates it to Japanese.
    """
    try:
        if not url or url == "#": return None
        
        # Optimization
        config = Config()
        config.fetch_images = False
        config.request_timeout = 10
        
        article = Article(url, config=config)
        article.download()
        article.parse()
        article.nlp()
        
        original_summary = article.summary
        if not original_summary:
            return "No summary could be extracted from this article."
            
        # --- Cleaning Promotional/Clickbait Text ---
        # Yahoo Finance/Motley Fool often append these.
        import re
        
        # Phrases to identify likely promotional sentences
        bad_patterns = [
            r"See also", r"Read also", r"Read next", 
            r"free report", r"Click here", r"Motley Fool",
            r"Zacks Rank", r"Insider Monkey", r"investing.com",
            r"stocks to buy", r"Top 10 stocks", r"Should you invest",
            r"Story continues", r"Advertisement",
            r"higher return potential", r"limited downside risk", 
            r"acknowledge the potential .* but", r"better buy",
            r"conviction buy", r"Top Stock to Buy", r"Five Stocks"
        ]
        
        # Split into sentences (simple split by newline or period space)
        # newspaper3k summary is usually paragraph text.
        # We'll split by newlines first, then maybe sentence boundary? 
        # Simpler approaches first: Remove lines containing bad patterns.
        
        cleaned_lines = []
        for line in original_summary.split('\n'):
            # Check sentence level cleaning if needed, but often these are separate paragraphs/lines in summary
            if any(re.search(pat, line, re.IGNORECASE) for pat in bad_patterns):
                continue
            cleaned_lines.append(line)
            
        original_summary = "\n".join(cleaned_lines)
            
        # Translate
        # Truncate if extremely long to avoid timeout/limits
        if len(original_summary) > 4000:
            original_summary = original_summary[:4000]
            
        translated = GoogleTranslator(source='auto', target='ja').translate(original_summary)
        return translated
        
    except Exception as e:
        return f"Summary failed: {str(e)}"

import market_logic
import importlib
importlib.reload(market_logic)
from market_logic import SECTOR_DEFINITIONS, TICKER_TO_SECTOR, STATIC_MOMENTUM_WATCHLIST, THEMATIC_ETFS

# --- Risk Management Helpers ---
def get_ticker_news(ticker, company_name=None):
    """
    Fetches top 3 news.
    Filters:
    1. Valid Title (Not empty)
    2. Recency (< 3 days)
    3. Relevance (Title must contain Ticker or Company Name)
    """
    try:
        news = yf.Ticker(ticker).news
        if not news: return []
        
        results = []
        now = datetime.now()
        
        # Prepare Regex for Ticker (Case-insensitive word boundary? No, Ticker usually CAPS, but let's be flexible)
        # Actually for Ticker, Case Sensitive is safer for short ones like 'BE' vs 'be'.
        # But some titles might lower case? "Bloom Energy (be) ..." Unlikely.
        # Let's simple check: 
        # 1. Ticker (Case Sensitive) in Title (Word Bound)
        # 2. Company Name (First Word) in Title (Case Insensitive)
        
        patterns = [r'\b{}\b'.format(re.escape(ticker))] # Exact Ticker Match
        
        if company_name:
            # Clean name: "Bloom Energy Corporation" -> "Bloom"
            # "NVIDIA Corp" -> "NVIDIA"
            # "Advanced Micro Devices" -> "Advanced" (Risk? "Advanced" is common word)
            # Maybe use full string up to common suffixes?
            
            # Simple heuristic: Split by space
            parts = company_name.split()
            if parts:
                main_name = parts[0]
                # If short basic word, maybe skip? But let's trust it for now.
                # Avoid very short words if they are not the ticker
                if len(main_name) > 2:
                    patterns.append(r'\b{}\b'.format(re.escape(main_name)))
                
                # Also try full name string (e.g. "Bloom Energy")
                if len(parts) > 1:
                     patterns.append(re.escape(company_name))

        for n in news:
            # 1. Normalize Logic
            content = n.get('content', n)
            title = content.get('title', '')
            
            if not title or title == "No Title":
                continue

            # --- SUBJECT FILTER ---
            # Check if any pattern matches title
            is_relevant = False
            for pat in patterns:
                if re.search(pat, title, re.IGNORECASE):
                    is_relevant = True
                    break
            
            if not is_relevant:
                # Debug print? No.
                continue
            # ----------------------

            # 2. Time Extraction
            pub_time = None
            if 'pubDate' in content:
                try:
                    ts_str = content['pubDate'].replace('Z', '')
                    pub_time = datetime.fromisoformat(ts_str)
                except: pass
            
            if not pub_time and 'providerPublishTime' in n:
                try:
                    pub_time = datetime.fromtimestamp(n['providerPublishTime'])
                except: pass
                    
            if not pub_time:
                pub_time = now # Skip if unknown? Or assume recent?
                # Let's skip to be strict
                continue

            # 3. Filter: Within 3 days
            if (now - pub_time).days > 3:
                continue

            dt = pub_time.strftime('%Y-%m-%d %H:%M')
            
            # 4. Link Extraction
            link = content.get('clickThroughUrl')
            if not link:
                link = content.get('link')
                if isinstance(link, dict): link = link.get('url')
                
            if not link: link = "#"

            results.append({
                'title': title,
                'publisher': content.get('publisher', 'Unknown'),
                'link': link,
                'time': dt
            })
            
            if len(results) >= 3:
                break
                
        return results
    except Exception as e:
        return []
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

# ... (rest of constants stays same until end of lists) ...

# --- Constants (Imported from market_logic) ---
# SECTOR_DEFINITIONS, TICKER_TO_SECTOR, STATIC_MOMENTUM_WATCHLIST are imported.

# --- Thematic ETF List (Metrics Benchmark) ---

# --- Thematic ETFs (Imported from market_logic) ---
# THEMATIC_ETFS is imported.

# --- Risk Management Helpers ---
def get_earnings_next(ticker):
    """
    Fetches the next earnings date.
    Returns: formatted string (e.g., '⚠️ In 3 days' or '2025-10-30') or '-'
    """
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        
        # Handle dictionary return (newer yfinance)
        if isinstance(cal, dict):
            # Key varies: 'Earnings Date' or 'Earnings High' etc.
            # Usually 'Earnings Date' is a list of dates
            dates = cal.get('Earnings Date', [])
            if not dates:
                return "-"
            next_date = dates[0] # Take the first one
        
        # Handle DataFrame return (older yfinance)
        elif isinstance(cal, pd.DataFrame):
            if cal.empty: return "-"
            # Often index is 0, 1... and columns are dates
            # Or formatted differently. Let's try to grab the first date available.
            # This part is tricky without exact dataframe structure from recent fix, 
            # but usually it finds 'Earnings Date' in dict form now.
            return "-" 
        else:
            return "-"

        # Calculate days until
        if isinstance(next_date, (datetime, date)):
            d = next_date.date() if isinstance(next_date, datetime) else next_date
            today = date.today()
            delta = (d - today).days
            
            if 0 <= delta <= 7:
                return f"⚠️ In {delta} days"
            elif delta < 0:
                # Past earnings (sometimes API returns previous)
                return "-"
            else:
                return d.strftime("%Y-%m-%d")
        return "-"
    except:
        return "-"

def get_ticker_news(ticker, company_name=None):
    """
    Fetches top 3 news.
    Filters:
    1. Valid Title (Not empty)
    2. Recency (< 3 days)
    3. Relevance (Title must contain Ticker or Company Name)
    """
    try:
        news = yf.Ticker(ticker).news
        if not news: return []
        
        results = []
        now = datetime.now()
        
        # Prepare Regex for Ticker (Case-insensitive word boundary? No, Ticker usually CAPS, but let's be flexible)
        # Actually for Ticker, Case Sensitive is safer for short ones like 'BE' vs 'be'.
        # But some titles might lower case? "Bloom Energy (be) ..." Unlikely.
        # Let's simple check: 
        # 1. Ticker (Case Sensitive) in Title (Word Bound)
        # 2. Company Name (First Word) in Title (Case Insensitive)
        
        patterns = [r'\b{}\b'.format(re.escape(ticker))] # Exact Ticker Match
        
        if company_name:
            # Clean name: "Bloom Energy Corporation" -> "Bloom"
            # "NVIDIA Corp" -> "NVIDIA"
            # "Advanced Micro Devices" -> "Advanced" (Risk? "Advanced" is common word)
            # Maybe use full string up to common suffixes?
            
            # Simple heuristic: Split by space
            parts = company_name.split()
            if parts:
                main_name = parts[0]
                # If short basic word, maybe skip? But let's trust it for now.
                # Avoid very short words if they are not the ticker
                if len(main_name) > 2:
                    patterns.append(r'\b{}\b'.format(re.escape(main_name)))
                
                # Also try full name string (e.g. "Bloom Energy")
                if len(parts) > 1:
                     patterns.append(re.escape(company_name))

        # --- FILTER & SORT CONFIG ---
        CATALYST_KEYWORDS = [
            r"Earnings", r"Revenue", r"EPS", r"Guidance", r"Results", r"Report",
            r"Acquisition", r"Merger", r"Deal", r"Partnership", r"Contract", r"Agreement",
            r"FDA", r"Approval", r"Trial", r"Launch", r"Announce", r"Unveil",
            r"CEO", r"CFO", r"Appoint", r"Resign", r"Management",
            r"Lawsuit", r"Settlement", r"Investigation",
            r"Upgrade", r"Downgrade"
        ]
        
        NOISE_KEYWORDS = [
            r"Implied Volatility", r"Options", r"Relative Strength", r"Technical Analysis",
            r"Zacks Rank", r"Motley Fool Stock Pick", r"Short Interest",
            r"Why .* is Moving", r"Why .* is Up", r"Why .* is Down",
            r"Stock Alert", r"Prediction", r"Forecast",
            r"ETF", r"Mutual Fund", r"Insiders are Selling", r"Insiders are Buying",
            r"Stock Market Today", r"Here is what happened"
        ]

        scored_candidates = []

        for n in news:
            # 1. Normalize Logic (Handle New vs Old API)
            content = n.get('content', n) # Fallback to n if content missing
            
            title = content.get('title', '')
            if not title or title == "No Title":
                continue

            # --- SUBJECT FILTER (RELEVANCE) ---
            is_relevant = False
            for pat in patterns:
                if re.search(pat, title, re.IGNORECASE):
                    is_relevant = True
                    break
            
            if not is_relevant:
                continue

            # 2. Time Extraction
            pub_time = None
            if 'pubDate' in content:
                try:
                    ts_str = content['pubDate'].replace('Z', '')
                    pub_time = datetime.fromisoformat(ts_str)
                except: pass
            
            if not pub_time and 'providerPublishTime' in n:
                try:
                    pub_time = datetime.fromtimestamp(n['providerPublishTime'])
                except: pass
                    
            if not pub_time:
                continue

            # 3. Filter: Within 3 days
            days_diff = (now - pub_time).days
            if days_diff > 3:
                continue
            
            dt_str = pub_time.strftime('%Y-%m-%d %H:%M')

            # --- SCORING LOGIC ---
            score = 0
            
            # Catalyst Check (+5)
            for pat in CATALYST_KEYWORDS:
                if re.search(pat, title, re.IGNORECASE):
                    score += 5
                    break
                
            # Noise Check (-10)
            for pat in NOISE_KEYWORDS:
                if re.search(pat, title, re.IGNORECASE):
                    score -= 10
                    break
            
            # Provider Check
            provider = content.get('publisher', 'Unknown')
            # Penalize known noise providers slightly if not already caught
            if 'Zacks' in provider or 'Fool' in provider:
                score -= 2

            # 4. Link Extraction
            link = content.get('clickThroughUrl')
            if not link: link = content.get('link') 
            if isinstance(link, dict): link = link.get('url')
            if not link: link = "#"
            
            scored_candidates.append({
                'title': title, # English Title for now
                'publisher': provider, 
                'link': link,
                'time': dt_str,
                'raw_time': pub_time,
                'score': score,
                'summary': content.get('summary', '') or content.get('description', '')
            })

        # --- SORTING & SELECTION ---
        # Sort by: Score (Desc) -> Time (Desc)
        scored_candidates.sort(key=lambda x: (x['score'], x['raw_time']), reverse=True)
        
        # Take Top 3
        top_results = scored_candidates[:3]
        
        results = []
        for res in top_results:
            title = res['title']
            # 5. Translation (EN -> JA)
            try:
                # Simple check: if title contains mostly ascii, assume English
                if len(title) > 0 and ord(title[0]) < 128:
                    translated_title = GoogleTranslator(source='auto', target='ja').translate(title)
                    display_title = translated_title
                    
                    # Also translate summary if exists
                    raw_summary = res['summary']
                    if raw_summary and len(raw_summary) > 20: 
                        trunc_summary = raw_summary[:300]
                        translated_summary = GoogleTranslator(source='auto', target='ja').translate(trunc_summary)
                        translated_summary += "..." if len(raw_summary) > 300 else ""
                    else:
                        translated_summary = ""
                else:
                    display_title = title
                    translated_summary = res['summary']
            except Exception as e:
                # print(f"Translation failed: {e}")
                display_title = title 
                translated_summary = ""

            results.append({
                'title': display_title,
                'original_title': title,
                'publisher': res['publisher'], 
                'link': res['link'],
                'time': res['time'],
                'summary': translated_summary
            })

        return results
    except Exception as e:
        # print(f"News Error: {e}") 
        return []

# --- Logic Functions: Shared / Correlation (Existing) ---
def get_data(tickers, period):
    # Parse tickers
    if isinstance(tickers, list):
        ticker_list = [t.strip() for t in tickers if t.strip()]
    else:
        # Fallback for string input
        ticker_list = [t.strip() for t in tickers.split(',') if t.strip()]
        
    if not ticker_list:
        return None
    
    try:
        data_frames = []
        for t in ticker_list:
            if t.startswith('---'): continue # Skip separators just in case
            try:
                # Fetch one by one to avoid bulk download header/cache issues
                df = yf.download(t, period=period, auto_adjust=True, progress=False)
                
                # Check if data is empty
                if df is None or df.empty:
                    continue
                    
                # Standardize column to Ticker name
                if isinstance(df, pd.DataFrame):
                    # Should have 'Close'
                    if 'Close' in df.columns:
                        df = df[['Close']]
                    
                    # Force rename columns to simple string ticker
                    df.columns = [t]
                
                data_frames.append(df)
            except Exception as e:
                st.warning(f"Failed to fetch {t}: {e}")
                continue

        if not data_frames:
            return None

        # Concatenate all
        data = pd.concat(data_frames, axis=1)
        
        # Align data: Forward fill to handle mismatching trading days
        data = data.ffill()
        
        # Drop only if data is still missing (e.g. leading NaNs)
        aligned_data = data.dropna()
        
        return aligned_data
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return None

def calculate_stats(df_prices):
    """
    Calculates daily returns, correlation matrix, and cumulative returns.
    """
    if df_prices is None or df_prices.empty:
        return None, None, None
        
    # 1. Daily Returns (for Correlation)
    returns = df_prices.pct_change().dropna()
    
    # 2. Correlation Matrix
    corr_matrix = returns.corr()
    
    # 3. Cumulative Returns (for Performance Chart)
    # Rebase to 0%
    cumulative_returns = (df_prices / df_prices.iloc[0]) - 1
    
    return returns, corr_matrix, cumulative_returns

@st.cache_data(ttl=3600)
def get_dynamic_trending_tickers():
    """
    Fetches 'Most Active' tickers from Yahoo Finance.
    Existing logic for Correlation Radar default items.
    """
    fallback_tickers = ['RKLB', 'MU', 'OKLO', 'LLY', 'SOFI']
    url = "https://finance.yahoo.com/most-active"
    
    # Create exclusion set from static menu
    exclusion_set = {t for t in STATIC_MENU_ITEMS if not t.startswith('---')}
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        dfs = pd.read_html(StringIO(response.text))
        
        if dfs:
            df_scrape = dfs[0]
            if 'Symbol' in df_scrape.columns:
                candidates_raw = df_scrape['Symbol'].head(30).dropna().astype(str).tolist()
                candidates = [t.split()[0] for t in candidates_raw if t]

                # Quick filtering logic (simplified from original for brevity)
                filtered = [t for t in candidates if t not in exclusion_set]
                return filtered[:5]

        return fallback_tickers
        
    except Exception as e:
        print(f"Failed to fetch trending tickers: {e}")
        return fallback_tickers

def generate_insights(corr_matrix):
    insights = []
    
    # Define Asset Classes for Fake Hedge Detection
    defensive_assets = {'GLD', 'IAU', 'TLT', 'IEF', 'AGG', 'BND', 'XLP', 'XLV', 'XLU', 'LQD', 'USDJPY=X'}
    risky_assets = {'QQQ', 'TQQQ', 'NVDA', 'SOXL', 'SMH', 'BTC-USD', 'ETH-USD', 'MSTR', 'COIN', 'PLTR', 'TSLA', 'ARKK', 'SPY'}

    # 1. Pairwise checks
    processed_pairs = set()
    columns = corr_matrix.columns
    
    for i in range(len(columns)):
        for j in range(i+1, len(columns)):
            ticker_a = columns[i]
            ticker_b = columns[j]
            val = corr_matrix.iloc[i, j]
            
            pair_key = tuple(sorted((ticker_a, ticker_b)))
            if pair_key in processed_pairs:
                continue
            processed_pairs.add(pair_key)
            
            # Condition: Fake Hedge Detection (Priority)
            is_def_a = ticker_a in defensive_assets
            is_risk_a = ticker_a in risky_assets
            is_def_b = ticker_b in defensive_assets
            is_risk_b = ticker_b in risky_assets
            
            if (is_def_a and is_risk_b) or (is_risk_a and is_def_b):
                if val >= 0.5:
                    def_name = ticker_a if is_def_a else ticker_b
                    risk_name = ticker_b if is_def_a else ticker_a
                    
                    insights.append({
                        "type": "fake_hedge",
                        "display": f"🚨 **ヘッジ機能不全**: {def_name} と {risk_name} (相関: {val:.2f})",
                        "message": f"安全資産とされる {def_name} が、リスク資産 {risk_name} と強く連動しています。暴落時にクッションの役割を果たさない可能性があります。",
                        "score": abs(val) + 0.5
                    })

            # Condition A: High Correlation
            if val > 0.7:
                insights.append({
                    "type": "risk",
                    "display": f"⚠️ **集中リスク警告**: {ticker_a} と {ticker_b} (相関: {val:.2f})",
                    "message": "この2つは非常に似た動きをしています。分散効果が低いため、ポジション調整を検討してください。",
                    "score": abs(val)
                })
            
            # Condition B: Inverse Correlation
            elif val < -0.3:
                insights.append({
                    "type": "hedge",
                    "display": f"🛡️ **ヘッジ機能**: {ticker_a} と {ticker_b} (相関: {val:.2f})",
                    "message": "逆の動きをする傾向があります。ポートフォリオのリスク低減に役立っています。",
                    "score": abs(val)
                })

    # 2. Individual Asset check (Independence)
    for ticker in columns:
        encounters = corr_matrix[ticker].drop(ticker)
        max_corr = encounters.abs().max()
        if max_corr < 0.25:
             insights.append({
                "type": "independent",
                "display": f"🧘 **独立独歩**: {ticker}",
                "message": f"他の資産との連動性が低く（最大相関 {max_corr:.2f}）、独自の要因で動いています。分散投資の観点で優秀です。",
                "score": (1 - max_corr)
            })



    # --- Filtering Logic: Max 2 per Type ---
    # Sort by score descending to keep the "most important" ones
    insights.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    final_insights = []
    type_counts = {}
    
    for item in insights:
        t = item['type']
        count = type_counts.get(t, 0)
        
        if count < 2:
            final_insights.append(item)
            type_counts[t] = count + 1
            
    return final_insights

# --- Portfolio Logic (New) ---
def generate_ai_portfolios(df_sorted, corr_matrix, exclude_tickers=None):
    """
    Generates 3 Portfolio Models based on momentum & logic.
    Returns dict: {'Hunter': [...], 'Fortress': [...], 'Bento': [...]}
    Each item is dict: {Ticker, Price, Weight, LatestReturn}
    """
    portfolios = {}
    
    # Pre-filter exclusions (Short-term losers)
    if exclude_tickers:
        # Filter out excluded tickers from potential candidates
        pool = df_sorted[~df_sorted['Ticker'].isin(exclude_tickers)].copy()
    else:
        pool = df_sorted.copy()
    
    # --- Model A: 🐯 The Hunter (Aggressive) ---
    # Top 5 by 1mo, RVOL > 1.2
    hunter_pool = pool[pool['RVOL'] > 1.2].copy()
    hunter_pool = hunter_pool.sort_values(by='1mo', ascending=False).head(5)
    
    if len(hunter_pool) < 5:
        # Fallback: Just top 1mo if not enough RVOL
        fallback = pool.sort_values(by='1mo', ascending=False).head(5)
        hunter_pool = fallback 
        
    portfolios['Hunter'] = hunter_pool
    
    # --- Model B: 🏰 The Fortress (Consistent) ---
    # 3mo, 6mo, YTD all > 0. Sort by 3mo. Top 8.
    fortress_pool = pool[
        (pool['3mo'] > 0) & 
        (pool['6mo'] > 0) & 
        (pool['YTD'] > 0)
    ].copy()
    fortress_pool = fortress_pool.sort_values(by='3mo', ascending=False).head(8)
    
    if len(fortress_pool) < 5:
         # Fallback: Just top 3mo positive
         fortress_pool = pool[pool['3mo'] > 0].sort_values(by='3mo', ascending=False).head(8)
         
    portfolios['Fortress'] = fortress_pool
    
    
    # --- Model C: 🥗 The Bento Box (Diversified) ---
    # Pick Top 1 (by 1mo) from each Core Sector
    # Core Sectors defined in SECTOR_DEFINITIONS keys or simplified logic
    # Keys mappings based on SECTOR_DEFINITIONS
    
    # ... (Bento logic handled in next block, just inserting Sniper before or after? 
    # Let's insert Sniper BEFORE Bento to keep alphabetical or logic flow)
    # Actually user asked for "4th portfolio". I'll put it after Bento or before. 
    # Let's put it as Model D.
    
    # --- Model D: 🦅 The Sniper (Precision) ---
    # Like Hunter, but strictly NO Overheating (RSI < 70).
    # Ideal entry point: High Momentum + Volume + But not yet Overbought.
    # Base criteria: RSI < 70 AND 1mo > 0 (Must be rising)
    
    # 1. Strict: RVOL > 1.2
    sniper_pool = pool[
        (pool['RVOL'] > 1.2) & 
        (pool['RSI'] < 70) &
        (pool['1mo'] > 0)
    ].copy()
    
    # 2. Fallback if empty: Relax RVOL
    if len(sniper_pool) < 3:
        fallback_pool = pool[
            (pool['RSI'] < 70) &
            (pool['1mo'] > 0)
        ].copy()
        # Sort by 1mo to get "Strongest among non-overheated"
        sniper_pool = fallback_pool
    
    sniper_pool = sniper_pool.sort_values(by='1mo', ascending=False).head(5)
    
    portfolios['Sniper'] = sniper_pool

    # --- Model C: 🥗 The Bento Box (Diversified) ---
    
    # 1. Map Tickers to Broad Category
    # We already have TICKER_TO_SECTOR
    # Broad Categories:
    # 1. AI/Semi ("🧠 AI & Semi")
    # 2. Energy ("⚛️ Energy & Resources")
    # 3. FinTech/Crypto ("🏦 FinTech & Real Estate")
    # 4. Space/Defense ("🌌 Space & Defense")
    # 5. Consumer/Bio ("💊 Consumer & Health", "🚗 Auto & EV")
    
    bento_picks = []
    
    # Define Target Groups (Regex friendly or exact match)
    target_groups = [
        ["AI", "Semi"], 
        ["Energy", "Resources", "Infra"],
        ["FinTech", "Crypto"],
        ["Space", "Defense"],
        ["Consumer", "Health", "Auto"]
    ]
    
    used_tickers = set()
    
    for keywords in target_groups:
        # Filter df for tickers in this sector
        candidates = []
        for t in pool['Ticker']:
            sec = TICKER_TO_SECTOR.get(t, "")
            if any(k in sec for k in keywords):
                candidates.append(t)
                
        # Get subset
        subset = pool[pool['Ticker'].isin(candidates)].sort_values(by='1mo', ascending=False)
        
        # Pick best not already satisfying correlation check?
        # Simplified: Just pick Top 1 for now, correlation check is bonus
        if not subset.empty:
            pick = subset.iloc[0]
            bento_picks.append(pick)
            used_tickers.add(pick['Ticker'])
    
    # Check if we have 5?
    if len(bento_picks) < 5:
        # Fill with "Independent" stocks if missing sectors
        # Find low correlation stocks
        pass # Keep what we have
        
    portfolios['Bento'] = pd.DataFrame(bento_picks)
    
    return portfolios

def calculate_simulated_return(portfolio_df, weight_pct=1.0):
    # Virtual Return: Sum of (1mo return * weight)
    # Simple equal weight
    if portfolio_df.empty: return 0.0
    avg_ret = portfolio_df['1mo'].mean()
    return avg_ret # This is portfolio return over last month

# --- Logic Functions: Momentum Master (New) ---

# --- Logic Functions: Momentum Master (Offline Logic Integration) ---
# Constants are imported from market_logic.


# --- Metadata Helpers ---
@st.cache_data(ttl=86400) # Cache metadata for a day
def get_ticker_metadata(ticker):
    """
    Fetches basic info (Short Name, Sector/Industry) for a single ticker.
    Used only for Top 5-10 to save API calls.
    Returns: (name, category_label)
    """
    # 1. Check Scraped Cache (Fastest)
    if 'dynamic_names' in st.session_state:
        if ticker in st.session_state['dynamic_names']:
             return st.session_state['dynamic_names'][ticker], '🌊 Market Mover'

    # 2. Fallback to API (Slow)
    try:
        t = yf.Ticker(ticker)
        info = t.info
        name = info.get('shortName', info.get('longName', ticker))
        
        # Priority: Industry > Sector > 'Unknown'
        # e.g. "Aerospace & Defense" is better than "Industrials"
        industry = info.get('industry')
        sector = info.get('sector')
        
        category = industry if industry else (sector if sector else '🌊 Market Mover')
        
        return name, category
    except:
        return ticker, '🌊 Market Mover'

@st.cache_data(ttl=None) # TTLなし。引数のmtimeが変わるまでキャッシュ維持
def load_cached_data(mtime_param):
    """
    保存されたCSVとPickleを読み込む。
    mtime_param: キャッシュの無効化（更新検知）に使われる擬似パラメータ
    """
    if os.path.exists("data/momentum_cache.csv") and os.path.exists("data/history_cache.pkl"):
        try:
            # キャッシュからロード
            df = pd.read_csv("data/momentum_cache.csv")
            with open("data/history_cache.pkl", "rb") as f:
                history = pickle.load(f)
            
            # 更新時刻の確認
            last_update = "Unknown"
            if os.path.exists("data/last_updated.txt"):
                with open("data/last_updated.txt", "r") as f:
                    last_update = f.read().strip()
                    
            return df, history, last_update
        except Exception as e:
            st.warning(f"Cache load failed: {e}. Falling back to live fetch.")
    
    # 初回起動時などファイルがない場合は、market_logicを使って直接取得
    candidates = market_logic.get_momentum_candidates()
    df, hist = market_logic.calculate_momentum_metrics(candidates)
    if df is not None:
        return df, hist, "Live Fetch (No Cache Found)"
    return None, None, "Failed"

# ... (Previous code) ...
# Note: I am not including the entire file content here, just the function replacement. 
# But wait, I need to replace the call site too which is far away.
# I will do this in two steps to be safe.
# First Tool Call: Update load_cached_data definition (Lines 695-723)
# Second Tool Call: Update call site (Lines 1101-1107)

# THIS IS THE FIRST TOOL CALL CONTENT FOR load_cached_data


# Page Config (Must be first Streamlit command)
st.set_page_config(
    page_title="Momentum Master",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "# Momentum Master\nPowered by AI Analyst"
    }
)

# --- Main App ---
def main():
    # st.set_page_config is now called globally at line 15 (actually 20 in original, but we replaced it)
    
# --- Hide Streamlit Style (Force Z-Index Method) ---
    hide_st_style = """
        <style>
        /* 1. ヘッダーの背景は透明にする */
        header[data-testid="stHeader"] {
            background: transparent !important;
            border-bottom: none !important;
            pointer-events: none !important; /* ヘッダー自体のクリック判定を消す */
        }

        /* 2. 右上の不要な要素を消す */
        [data-testid="stHeaderActionElements"] { display: none !important; }
        [data-testid="stToolbar"] { display: none !important; }
        [data-testid="stDecoration"] { display: none !important; }
        [data-testid="stStatusWidget"] { display: none !important; }

        /* 3. フッター完全消去 */
        footer { visibility: hidden !important; height: 0px !important; }
        [data-testid="stFooter"] { display: none !important; }
        div[class^='viewerBadge'] { display: none !important; }

        /* 4. レイアウト調整 */
        .block-container {
            padding-top: 3rem !important;
        }
        </style>
    """
    st.markdown(hide_st_style, unsafe_allow_html=True)

    # --- Sidebar: Global Navigation REMOVED ---
    
    # Run Momentum Master
    render_momentum_master()



# --- View: Correlation Radar ---
def render_correlation_radar():
    st.title("📊 Market Correlation Radar")
    st.markdown("""
    **目的**: 為替、株式、債券、暗号資産など、異なるアセット間の「現在の連動性」を可視化します。
    単なる価格比較ではなく、**日次リターン（変化率）** に基づく純粋な相関を表示します。
    """)
    
    # Load Settings (Only once per session)
    if 'tickers' not in st.session_state:
        st.session_state['tickers'] = ["USDJPY=X", "^TNX", "GLD", "QQQ", "SMH", "BTC-USD", "XLP", "XLV"]
            
    if 'period' not in st.session_state:
        st.session_state['period'] = "1y"

    # --- Configuration ---
    with st.sidebar:
        st.header("⚙️ Radar Settings")
        
        # 1. Fetch Trending for Radar
        trending_tickers = get_dynamic_trending_tickers()
        popular_tickers = []
        if trending_tickers:
            popular_tickers.extend(["--- 🔥 Trending (Yahoo Finance) ---"] + trending_tickers)
        popular_tickers.extend(STATIC_MENU_ITEMS)
        
        # Merge saved tickers
        current_selection = st.session_state.get('tickers', [])
        options = list(popular_tickers)
        for t in current_selection:
            if t not in options:
                options.append(t)

        tickers_input = st.multiselect(
            "対象銘柄 (Tickers)",
            options=options,
            key="tickers",
            default=st.session_state['tickers'],
            max_selections=10
        )
        
        st.caption("※「---」ヘッダーは無視されます。")
        
        # Custom input
        def add_custom_ticker():
            new_ticker = st.session_state.new_ticker_input.strip().upper()
            if new_ticker:
                current = list(st.session_state['tickers'])
                if new_ticker not in current:
                    if len(current) < 10:
                        current.append(new_ticker)
                        st.session_state['tickers'] = current
        
        st.text_input(
            "➕ Add Ticker",
            key="new_ticker_input",
            on_change=add_custom_ticker
        )
        
        period_options = {
            '1y': '1 Year (長期)', '3mo': '3 Months', '1mo': '1 Month', '5d': '5 Days'
        }
        st.selectbox(
            "Analysis Period", 
            list(period_options.keys()), 
            key="period", 
            format_func=lambda x: period_options.get(x, x)
        )

    # --- Main Content ---
    if tickers_input:
        with st.spinner('Fetching Radar data...'):
            df_prices = get_data(tickers_input, st.session_state['period'])

        if df_prices is not None and not df_prices.empty:
            if len(df_prices) < 2:
                st.warning("データ不足。期間を延ばしてください。")
            else:
                returns, corr_matrix, cumulative_returns = calculate_stats(df_prices)
                
                # 1. Heatmap
                st.subheader("Correlation Matrix")
                if corr_matrix is not None:
                    fig_corr, ax_corr = plt.subplots(figsize=(10, 8))
                    sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap='coolwarm', vmin=-1, vmax=1, center=0, ax=ax_corr, square=True)
                    st.pyplot(fig_corr, use_container_width=False)
                
                st.markdown("---")
                
                # 2. Chart
                st.subheader("Relative Performance")
                if cumulative_returns is not None:
                    fig_perf, ax_perf = plt.subplots(figsize=(10, 5))
                    for column in cumulative_returns.columns:
                        ax_perf.plot(cumulative_returns.index, cumulative_returns[column] * 100, label=column)
                    ax_perf.set_ylabel("Return (%)")
                    ax_perf.grid(True, linestyle='--', alpha=0.6)
                    ax_perf.legend(loc='upper left', bbox_to_anchor=(1, 1))
                    plt.tight_layout()
                    st.pyplot(fig_perf, use_container_width=False)
                
                # 3. AI Insights
                st.markdown("---")
                st.subheader("📊 AI Analyst Insights")
                insights = generate_insights(corr_matrix)
                if insights:
                    for item in insights:
                        t = item['type']
                        msg = f"**{item['display']}**\n\n{item['message']}"
                        
                        if t == 'fake_hedge' or t == 'risk':
                            st.warning(msg, icon="⚠️")
                        elif t == 'hedge':
                            st.success(msg, icon="🛡️")
                        else:
                            st.info(msg, icon="ℹ️")
                else:
                    st.info("特筆すべき相関パターンはありません。")
        else:
            st.error("No data found.")


import random # Add at top if not exists (handling in instruction context)

# --- AI Comment Logic ---
def generate_dynamic_comment(ticker, row):
    """
    複数のシグナルを考慮したスマートなコメント生成関数
    """
    # --- データ準備 ---
    current_price = row.get('Price', row.get('Close', 0))
    rvol = row.get('RVOL', 0)
    rsi = row.get('RSI', 50)
    
    # Fundamentals
    short_ratio = row.get('ShortRatio', 0)

    # トレンド判定
    sma50 = row.get('SMA50', 0)
    sma200 = row.get('SMA200', 0)
    
    # 判定フラグ
    try:
        is_bull_trend = sma50 > sma200       # 50日 > 200日 (上昇トレンド)
        is_bear_trend = sma50 < sma200       # 50日 < 200日 (下降トレンド)
    except:
        is_bull_trend = False
        is_bear_trend = False
    
    # 特殊判定: 価格が長期線をブレイクしている場合 (デッドクロス中だが価格は上)
    is_price_above_long_term = False
    if sma200 > 0:
        is_price_above_long_term = current_price > sma200

    is_high_vol = rvol > 2.0             # 出来高急増
    is_super_vol = rvol > 5.0            # 出来高爆増
    is_overbought = rsi > 70             # 買われすぎ
    is_oversold = rsi < 30               # 売られすぎ
    
    # Daily Return Check
    ret_1d = row.get('1d', 0)
    is_crash = ret_1d < -5.0             # 5%以上の急落
    is_rocket = ret_1d > 5.0             # 5%以上の急騰
    
    # --- 優先度SS: 矛盾・特異点（AI Analysis） ---

    # 0. 【緊急】足元の急落 (トレンド関係なしに最優先で警告)
    if is_crash:
        templates = [
            f"😱 {ticker}が急落中({ret_1d:.1f}%)。今はトレンドよりもこの落下速度に注意。",
            f"📉 {ticker}に売り殺到。落ちるナイフは掴むな、底打ちを確認せよ。",
            f"🛑 {ticker}、危険水域。上昇トレンドだろうが何だろうが、今の下げは無視できない。"
        ]
        return random.choice(templates)

    # 1. 【反転兆候】長期トレンドは下向きだが、価格は長期線をブレイクしている (Recovery)
    # これを「デッドクロス中」と呼ぶとユーザーの感覚とズレるため「トレンド転換」とする
    if is_bear_trend and is_price_above_long_term and is_high_vol:
        templates = [
            f"🚀 {ticker}が長期線(SMA200)をブレイク！下落トレンドからの強力な反転シグナル。",
            f"🔥 長期の重しを跳ねのけた。{ticker}はデッドクロス状態を解消し、新たな上昇トレンドへ向かうか。",
            f"👀 {ticker}にトレンド転換の兆し。SMA200超えは本物の強さの証。"
        ]
        return random.choice(templates)

    # 2. 【反転兆候】デッドクロス中で、まだ価格も下だが、モメンタムが強すぎる
    if is_bear_trend and (not is_price_above_long_term) and is_high_vol and is_overbought:
        templates = [
            f"⚡ {ticker}に異変。長期トレンドは下向きだが、このRSIと出来高は強すぎる。「初動」の可能性も。",
            f"🔥 売り方は逃げろ！{ticker}は下落トレンドを力技でねじ伏せようとしている。",
            f"🤔 {ticker}、ただのリバウンドにしては強すぎる。ショートカバー（踏み上げ）発生中か？"
        ]
        return random.choice(templates)

    # 3. 上昇トレンド中の急落（押し目か崩壊か）
    if is_bull_trend and is_high_vol and is_oversold:
        templates = [
            f"🔪 {ticker}が上昇トレンド中に急落。押し目買いチャンスか、それともナイフか？",
            f"📉 パニック売り発生中。{ticker}のトレンドが本物なら、ここが絶好の拾い場だが...",
            f"🚑 {ticker}、救急車通過。過熱感は冷めたが、冷めすぎかもしれない。"
        ]
        return random.choice(templates)

    # 4. 閑散としたゴールデンクロス（騙し警戒）
    if is_bull_trend and rvol < 0.8: # 出来高が普段より少ない
        templates = [
            f"⚠️ {ticker}がGCしたが、出来高がスカスカだ。誰も気づいていないか、騙しか。",
            f"🍃 風が吹けば飛びそうな上昇トレンド。{ticker}にはパワー（出来高）が必要だ。",
        ]
        return random.choice(templates)
        
    # 5. Short Squeeze Potential (High Short Ratio + Price Up + Vol Up)
    ret_1d = row.get('1d', 0)
    if ret_1d > 3.0 and is_high_vol and short_ratio > 5:
        templates = [
            f"🔥 踏み上げ（ショートスクイズ）警報！{ticker}の売り豚が焼かれている。",
            f"🥓 空売りの買い戻しが燃料だ。{ticker}の急騰は止まらないかも。",
            f"🎢 {ticker}でマネーゲーム発生中。ボラティリティに注意せよ。"
        ]
        return random.choice(templates)

    # --- 優先度S: 強烈な単一イベント ---

    # 出来高爆増（トレンド関係なしに何か起きてる）
    if is_super_vol:
        return f"📢 {ticker}の出来高がバグっている(RVOL {rvol:.1f})。材料が出たか？イナゴタワー建設開始。"
        
    # Blue Sky
    high_52 = row.get('High52', 999999)
    if current_price >= high_52 * 0.98:
         return f"🚀 {ticker}は青天井モード突入！上には宇宙しかない。"

    # --- 優先度A: 通常のテクニカル判定 ---
    
    # Squeeze
    # Squeeze
    if row.get('Is_Squeeze', False):
         if is_high_vol:
             return f"💥 {ticker}がスクイズから放たれた！エネルギー充填完了、ビッグバンの始まりか。"
         else:
             return f"🤐 {ticker}は嵐の前の静けさ(Squeeze)。次のビッグムーブに備えよ。"

    # 直近でクロスしたか？
    if row.get('DC_Just_Now', False):
         return f"💀 {ticker}がデッドクロス...長期的な冬の時代到来か。"
         
    if is_bear_trend and not is_high_vol and not is_price_above_long_term:
        return f"💀 {ticker}は長期下落トレンド継続中。トレンドに逆らわず、冬の時代を耐え忍ぶ時。"
    
    # 普通のゴールデンクロス（順当な上げ）
    if row.get('GC_Just_Now', False):
         return f"🌟 {ticker}がゴールデンクロス達成！長期トレンド転換のファンファーレ。"

    if is_bull_trend and rsi > 50:
        return f"🐂 {ticker}は順調な上昇トレンド。素直に乗るのが吉。"

    # 単なる買われすぎ
    if is_overbought:
        return f"🔥 {ticker}はアチアチ(RSI {rsi:.0f})。火傷する前に利確も検討を。"

    # 単なる売られすぎ
    if is_oversold:
        return f"🧊 {ticker}は売られすぎ(RSI {rsi:.0f})。自律反発狙いのスケベ買いチャンス？"

    # --- その他 ---
    templates = [
        f"👀 {ticker}は様子見。次のアクションを待て。",
        f"😴 出来高が足りない。{ticker}は寝かせておこう。",
        f"🤔 {ticker}の方向性が定まらない。"
    ]
    return random.choice(templates)

# --- View: Momentum Master ---
def render_momentum_master():
    # Check File Modification Time (Trigger Cache Invalidation)
    cache_path = "data/momentum_cache.csv"
    mtime = os.path.getmtime(cache_path) if os.path.exists(cache_path) else 0

    # Load Data First
    with st.spinner('Loading data...'):
        df_metrics, history_dict, last_updated = load_cached_data(mtime)

    # Display Title & Update Time
    col_title, col_time = st.columns([0.7, 0.3])
    with col_title:
        # Styled Title: Gradient, Single Line, No Japanese
        st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@800&display=swap');
        .main-title {
            font-family: 'Inter', sans-serif;
            font-size: 2.2rem;
            font-weight: 800;
            margin: 0;
            padding: 0;
            background: -webkit-linear-gradient(45deg, #FF4B2B, #FF416C);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            line-height: 1.2;
        }
        /* Mobile Adjustment: Pixel 8a width approx 412px. Column is 70% (~290px). 
           Text needs to scale down to fit one line. */
        @media (max-width: 640px) {
            .main-title {
                font-size: 1.5rem !important;
            }
        }
        </style>
        <div style="display: flex; align-items: center; gap: 8px;">
            <span style="font-size: 2.0rem;">🚀</span>
            <span class="main-title">Momentum Master</span>
        </div>
        """, unsafe_allow_html=True)
    with col_time:
        st.caption(f"📅 Last Update")
        st.success(f"**{last_updated}**", icon="⏱️")

    st.markdown("""
    **目的**: 指定した期間において、最もパフォーマンスが良い「最強の10銘柄」を瞬時に特定します。
    """)

    if df_metrics is None or df_metrics.empty:
        st.error("Data cache is empty and live fetch failed.")
        return

    # --- UI: Control Panel ---
    st.markdown("### 🎯 Focus Period Selector")
    
    period_map = {
        '1d': '1 Day (本日)',
        '5d': '5 Days (週間)',
        '1mo': '1 Month (月間)',
        '3mo': '3 Months (四半期)',
        '6mo': '6 Months (半年)',
        'YTD': 'YTD (年初来)',
        '1y': '1 Year (年間)'
    }
    
    # Default to 1d
    selected_period = st.selectbox(
        "どの期間のモメンタムを見ますか？",
        options=list(period_map.keys()),
        index=0, 
        format_func=lambda x: period_map[x]
    )

    if df_metrics is None or df_metrics.empty:
        st.error("Data cache is empty and live fetch failed.")
        return
            
    # --- UI: Top 5 Filter ---
    
    # Ensure column exists
    if selected_period not in df_metrics.columns:
        st.error(f"Data for {selected_period} is missing.")
        return

    # Sort Descending
    df_sorted = df_metrics.sort_values(selected_period, ascending=False)

    # Filter: Market Movers (Dynamic) only for 1d?
    # NO: User requested to allow Market Movers for all periods, BUT with a "Consistency Filter".
    # Logic: If selected_period is long (e.g. 1mo), exclude stocks where shorter period return > long period return.
    # This filters out "Pump & Dump" or recent spikes that aren't consistent with the long term trend.
    
    # Hierarchy definition
    period_hierarchy = ['1d', '5d', '1mo', '3mo', '6mo', '1y']
    
    # Dynamic Filter
    if selected_period != '1d' and selected_period in period_hierarchy:
        # Get index
        target_idx = period_hierarchy.index(selected_period)
        
        # Check all shorter periods
        shorter_periods = period_hierarchy[:target_idx]
        
        # Filter Condition: Keep only if Return(Shorter) <= Return(Target)
        # We need to apply this to df_sorted.
        # Note: df_metrics contains all period columns.
        
        valid_indices = []
        for idx, row in df_sorted.iterrows():
            is_consistent = True
            target_ret = row[selected_period]
            
            # Skip if target is NaN
            if pd.isna(target_ret):
                continue
                
            for sp in shorter_periods:
                if sp in row and not pd.isna(row[sp]):
                    short_ret = row[sp]
                    # STRICT FILTER: If Short Return > Target Return, it implies momentum is fading or it was a spike.
                    # User: "1d(100%) > 1m(30%) -> OUT"
                    # User: "1d(10%) < 1m(30%) -> IN"
                     
                    # Tolerance? Let's use strict for now as requested.
                    if short_ret > target_ret:
                        is_consistent = False
                        break
            
            if is_consistent:
                valid_indices.append(idx)
                
        df_sorted = df_sorted.loc[valid_indices]
    
    # Also, we do NOT filter by STATIC_MOMENTUM_WATCHLIST anymore if it's consistent.
    # Unless... wait, if it's NOT in static list, it MUST be a market mover.
    # So we are now allowing Market Movers into the main ranking provided they are consistent.
    
    # Take Top 10
    top_10 = df_sorted.head(10).copy() # Copy to avoid SettingWithCopyWarning
    
    # Enrich with Name, Sector, AI Strategy, AND Earnings
    names = []
    sectors = []
    strategies = []
    earnings_dates = []
    
    for _, row in top_10.iterrows():
        t = row['Ticker']
        
        # 1. Metadata Fetch
        static_sec = TICKER_TO_SECTOR.get(t)
        d_name, d_cat = get_ticker_metadata(t)
        
        names.append(d_name)
        
        if static_sec:
            sectors.append(static_sec)
        elif "🌊" in d_cat:
             # Logic fix: d_cat already has emoji if it comes from get_ticker_metadata default
            sectors.append(d_cat)
        else:
            sectors.append(f"🌊 {d_cat}")
            
        # 2. AI Strategy
        strategies.append(generate_dynamic_comment(t, row))
        
        # 3. Earnings Date (Lazy fetch for Top 10 only)
        earnings_dates.append(get_earnings_next(t))
        
    top_10['Name'] = names
    top_10['Sector'] = sectors
    top_10['AI Strategy'] = strategies
    top_10['Earnings'] = earnings_dates
    
    # --- Mobile View Toggle ---
    use_mobile_view = st.toggle("📱 Card View Mode", value=True)
    
    # Define Column Config (Reusable)
    column_config = {
        "Ticker": st.column_config.TextColumn("Ticker", width="small", pinned=True),
        "Name": st.column_config.TextColumn("Company", width="medium"),
        "Sector": st.column_config.TextColumn("Sector", width="medium"),
        "Price": st.column_config.NumberColumn("Price", format="$%.2f"),
        "Signal": st.column_config.TextColumn(
            "Signal", 
            width="medium",
            help="🚀:青天井 | ✨:GC | 💀:DC | 🤐:Squeeze | ⚡:出来高 | 🐂:上昇 | 🛒:押し目 | 🔥:加熱 | 🐻:下降 | 🧊:底値"
        ),
        "AI Strategy": st.column_config.TextColumn("🤖 AI Analysis", width="large"),
        "Earnings": st.column_config.TextColumn("Earnings (Next)", width="medium"),
        selected_period: st.column_config.NumberColumn(
            f"{selected_period.upper()} Return", 
            format="%.2f%%",
        )
    }
    
    context_cols = ['Ticker', 'Name', 'Sector', selected_period, 'Price', 'Signal', 'AI Strategy', 'Earnings']

    # Signal Legend
    with st.expander("ℹ️ Signal Legend (シグナルの意味)", expanded=False):
        st.markdown("""
        - 🚀 **青天井 (Blue Sky)**: 現在価格が52週高値付近 (High52 * 0.98以上)。新高値ブレイクの可能性。
        - ✨ **ゴールデンクロス (GC)**: 過去数日以内にSMA50がSMA200を上抜け。長期上昇トレンドの示唆。
        - 💀 **デッドクロス (DC)**: 過去数日以内にSMA50がSMA200を下抜け。長期下降トレンドの示唆。
        - 🤐 **スクイーズ (Squeeze)**: ボリンジャーバンドが収縮中。大きな価格変動の前触れ。
        - ⚡ **高出来高 (High Vol)**: 相対出来高(RVOL)が2.0倍以上。市場の注目度が高い。
        - 🐂 **上昇トレンド (Bull)**: 価格がSMA50より上 & 3ヶ月リターンがプラス。
        - 🐻 **下降トレンド (Bear)**: 価格がSMA50より下 & 3ヶ月リターンがマイナス。
        - 🔥 **加熱 (Overbought)**: RSIが70以上。買われすぎ警告。
        - 🧊 **底値 (Oversold)**: RSIが30以下。売られすぎ（反発の可能性）。
        - 🛒 **押し目 (Dip Buy)**: 上昇トレンド中だが、短期的にはRSI < 45で調整中。押し目買いの好機か。
        """)

    # Style Helpers (Global in this function)
    def highlight_focus(val):
        return 'background-color: #ffeb3b; color: black; font-weight: bold;' 
    
    # --- Mobile Card Helper ---
    def render_mobile_card_view(df, period, title_col='Name', subtitle_col='Sector', limit=5):
        # st.caption("💡 Card View") 

        # Split Data
        visible_df = df.head(limit)
        hidden_df = df.iloc[limit:]
        
        def render_rows(target_df):
            for idx, row in target_df.iterrows():
                ticker = row['Ticker']
                ret_val = row.get(period, 0)
                price = row.get('Price', 0)
                signal = row.get('Signal', '')
                comment = row.get('AI Strategy')
                if not comment:
                    comment = generate_dynamic_comment(ticker, row)
                    
                name = row.get(title_col, '')
                sub = row.get(subtitle_col, '')
                
                color = "#00FF00" if ret_val > 0 else "#FF4444"
                bg_color = "rgba(0, 255, 0, 0.1)" if ret_val > 0 else "rgba(255, 0, 0, 0.1)"
                
                # Compact CSS (Ultra Density for Small Screens)
                card_html = f"""
                <div style="
                    border: 1px solid #444; 
                    border-radius: 8px; 
                    padding: 8px 10px; 
                    margin-bottom: 4px; 
                    background-color: #0e1117; 
                    box-shadow: 0 1px 2px rgba(0,0,0,0.3);
                ">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                        <div>
                            <div style="display: flex; align-items: baseline; gap: 6px;">
                                <span style="font-size: 1.3em; font-weight: 900; color: #ffffff; letter-spacing: 0.5px;">{ticker}</span>
                                <span style="
                                    font-size: 1.0em; 
                                    font-weight: bold; 
                                    color: {color}; 
                                    background-color: {bg_color}; 
                                    padding: 0px 4px; 
                                    border-radius: 4px;
                                ">
                                    {ret_val:+.2f}%
                                </span>
                            </div>
                            <div style="font-size: 0.75em; color: #aaaaaa; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 200px;">{name}</div>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-size: 0.9em; color: #eeeeee; font-weight: 600;">${price:.2f}</div>
                            <div style="font-size: 1.0em; margin-top: 0px;">{signal}</div>
                        </div>
                    </div>
                    <div style="
                        font-size: 0.75em; 
                        color: #cccccc; 
                        border-top: 1px solid #333; 
                        padding-top: 4px; 
                        margin-top: 4px; 
                        line-height: 1.25;
                    ">
                        🤖 {comment}
                    </div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)

        # Render Top N
        render_rows(visible_df)
        
        # Render Remaining in Expander
        if not hidden_df.empty:
            remaining_count = len(hidden_df)
            with st.expander(f"👇 View Remaining {remaining_count} (6-{len(df)})", expanded=False):
                render_rows(hidden_df)

    
    # --- Part 1.5: Worst 10 Stocks Calculation ---
    # Take Bottom 10 (Worst Performers) from the ORIGINAL df_metrics (unfiltered)
    # We do NOT apply the "Consistency Filter" to losers, as we want to see the absolute worst drops.
    bottom_10 = df_metrics.sort_values(selected_period, ascending=True).head(10).copy()
    
    # Enrichment for Bottom 10
    b_names = []
    b_sectors = []
    b_strategies = []
    b_earnings = []
    
    for _, row in bottom_10.iterrows():
        t = row['Ticker']
        static_sec = TICKER_TO_SECTOR.get(t)
        d_name, d_cat = get_ticker_metadata(t)
        
        b_names.append(d_name)
        if static_sec:
            b_sectors.append(static_sec)
        elif "🌊" in d_cat:
            b_sectors.append(d_cat)
        else:
            b_sectors.append(f"🌊 {d_cat}")
        
        b_strategies.append(generate_dynamic_comment(t, row))
        b_earnings.append(get_earnings_next(t))
        
    bottom_10['Name'] = b_names
    bottom_10['Sector'] = b_sectors
    bottom_10['AI Strategy'] = b_strategies
    bottom_10['Earnings'] = b_earnings

    bottom_10['Earnings'] = b_earnings

    # --- 🚨 Opportunity Alert (Short-Term Focus) ---
    # Reconstruct raw DF from history_dict for retroactive calculation
    # Only run if we have history
    if history_dict:
        try:
             # Reconstruct MultiIndex DF: columns=(Ticker, Attributes)
             # This allows check_opportunity_alerts to slice by date easily.
             raw_history_df = pd.concat(history_dict.values(), axis=1, keys=history_dict.keys())
             
             # Calculate Alerts (Using selected period for ranking)
             # Note: This might take a second, so ideally we catch it.
             alerts = market_logic.check_opportunity_alerts(raw_history_df, period=selected_period)
             
             if alerts:
                 for a in alerts:
                     t = a['Ticker']
                     # Create a flashy alert box
                     st.success(
                         f"🚨 **Opportunity Alert: {t}**\n\n"
                         f"✅ **3 Days Persistence**: {t} has been in the Top 10 ({selected_period}) for 3 consecutive days!\n"
                         f"✅ **Volume Spike**: RVOL {a['RVOL']:.1f}x (Avg Vol exceeded by {(a['RVOL']-1)*100:.0f}%)"
                     )
        except Exception as e:
            # st.warning(f"Alert check skipped: {e}")
            pass

    # --- TABS Layout for Clean Screenshots ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏆 Top 10 Stocks", "📉 Worst 10 Stocks", "🔥 Hottest Themes", "🥶 Coldest Themes", "🌡️ Sector Heatmap"])
    
    # 1. Top 10
    with tab1:
        st.markdown(f"### 🏆 Top 10 Strongest Stocks<br><span style='font-size: 0.8em; color: gray;'>{period_map[selected_period]}</span>", unsafe_allow_html=True)
        if use_mobile_view:
            render_mobile_card_view(top_10, selected_period)
        else:
             st.dataframe(
                top_10[context_cols].style.applymap(
                    highlight_focus, subset=[selected_period]
                ).format({
                    selected_period: "{:+.2f}%",
                    'Price': "${:.2f}"
                }),
                column_config=column_config,
                use_container_width=True,
                hide_index=True
            )
            
    # 2. Worst 10
    with tab2:
        st.markdown(f"### 📉 Worst 10 Performers<br><span style='font-size: 0.8em; color: gray;'>{period_map[selected_period]}</span>", unsafe_allow_html=True)
        if use_mobile_view:
             render_mobile_card_view(bottom_10, selected_period)
        else:
            st.dataframe(
                bottom_10[context_cols].style.applymap(
                    lambda x: 'background-color: #ffebee; color: black;', subset=[selected_period]
                ).format({
                    selected_period: "{:+.2f}%",
                    'Price': "${:.2f}"
                }),
                column_config=column_config,
                use_container_width=True,
                hide_index=True
            )

    # --- ETF Preparation ---
    # st.header("🌍 Global Theme & Sector Analysis") # In Tabs now
    
    etf_ready = False
    top_etf = pd.DataFrame()
    bottom_etf = pd.DataFrame()
    
    # 1. Prepare ETF list
    etf_tickers = list(THEMATIC_ETFS.values())
    if df_metrics is not None and not df_metrics.empty:
        df_etf = df_metrics[df_metrics['Ticker'].isin(etf_tickers)].copy()
        if not df_etf.empty and selected_period in df_etf.columns:
            ticker_to_theme = {v: k for k, v in THEMATIC_ETFS.items()}
            df_etf['Theme'] = df_etf['Ticker'].map(ticker_to_theme)
            df_etf_sorted = df_etf.sort_values(selected_period, ascending=False)
            top_etf = df_etf_sorted.head(10).copy()
            bottom_etf = df_etf_sorted.tail(10).sort_values(selected_period, ascending=True).copy()
            etf_ready = True

    # 3. Hottest Themes
    with tab3:
        st.subheader(f"🔥 Hottest Themes ({period_map[selected_period]})")
        if etf_ready:
            if use_mobile_view:
                render_mobile_card_view(top_etf, selected_period, title_col='Theme', subtitle_col='Ticker')
            else:
                 etf_cols = {
                    "Theme": st.column_config.TextColumn("Theme (Sector)", width="medium"),
                    "Ticker": st.column_config.TextColumn("ETF", width="small"),
                    "Price": st.column_config.NumberColumn("Price", format="$%.2f"),
                    "Signal": st.column_config.TextColumn("Signal", width="small"),
                    selected_period: st.column_config.NumberColumn(f"{selected_period.upper()} Return", format="%.2f%%")
                }
                 etf_display_cols = ['Theme', 'Ticker', 'Price', 'Signal', selected_period]
                 st.dataframe(
                    top_etf[etf_display_cols].style.applymap(
                        highlight_focus, subset=[selected_period]
                    ).format({selected_period: "{:+.2f}%", 'Price': "${:.2f}"}),
                    column_config=etf_cols, use_container_width=True, hide_index=True
                )
        else:
            st.info("No ETF Data")

    # 4. Coldest Themes
    with tab4:
        st.subheader(f"🥶 Coldest Themes ({period_map[selected_period]})")
        if etf_ready:
            if use_mobile_view:
                render_mobile_card_view(bottom_etf, selected_period, title_col='Theme', subtitle_col='Ticker')
            else:
                 # Reuse etf_cols
                 etf_display_cols = ['Theme', 'Ticker', 'Price', 'Signal', selected_period]
                 st.dataframe(
                    bottom_etf[etf_display_cols].style.applymap(
                         lambda x: 'background-color: #ffebee; color: black;', subset=[selected_period]
                    ).format({selected_period: "{:+.2f}%", 'Price': "${:.2f}"}),
                    # Re-define config here or assume avail
                    column_config={
                        "Theme": st.column_config.TextColumn("Theme (Sector)", width="medium"),
                        "Ticker": st.column_config.TextColumn("ETF", width="small"),
                        selected_period: st.column_config.NumberColumn(format="%.2f%%")
                    }, 
                    use_container_width=True, hide_index=True
                )
        else:
            st.info("No ETF Data")
    
    st.markdown("---")
    
    # --- UI: Chart ---
    # --- UI: Chart ---
    # Collapsible Chart
    with st.expander(f"📈 Performance Comparison (Top 10: {selected_period})", expanded=False):
        top_tickers = top_10['Ticker'].tolist()
        
        if top_tickers:
            fig, ax = plt.subplots(figsize=(10, 5))
            
            # Decide chart window based on period (approx trading days)
            window_map = {
                '1d': 2, '5d': 5, '1mo': 22, '3mo': 65, '6mo': 130, 'YTD': 252, '1y': 252
            }
            days = window_map.get(selected_period, 65)
            
            for t in top_tickers:
                if t in history_dict:
                    s = history_dict[t]
                    
                    # Slice data to relevant period + padding
                    # If dataframe is shorter than days, take all
                    slice_data = s.tail(days)
                    if slice_data.empty: continue
                    
                    # Rebase to 0% at start of chart
                    rebased = (slice_data / slice_data.iloc[0] - 1) * 100
                    ax.plot(rebased.index, rebased, label=t)
            
            ax.set_ylabel("Return (%)")
            ax.set_title(f"Relative Performance (Last ~{days} Trading Days)")
            ax.grid(True, linestyle='--', alpha=0.5)
            ax.legend()
            st.pyplot(fig, use_container_width=True)

    # --- UI: News Section for Top Stocks ---
    # (News Section Removed for Compactness / or moved down? User didn't ask to remove, but previous context had it. Keeping it is fine.)
    # Actually, let's keep the user flow: Lists -> Chart -> News -> Heatmap -> Portfolio.
    
    st.markdown("---")
    st.subheader("📰 Latest News & Analysis")
    # ... (Keeping existing news logic, assuming it's short enough or collapsible)
    
    # Select box default to top 1
    default_ix = 0 if len(top_tickers) > 0 else None
    
    if top_tickers:
        news_ticker = st.selectbox("Select Ticker to View News:", top_tickers, index=default_ix)
        
        # Clear stale session_state keys when ticker changes
        if 'last_news_ticker' not in st.session_state:
            st.session_state['last_news_ticker'] = None
        
        if news_ticker != st.session_state['last_news_ticker']:
            # Cleanup old summary keys
            keys_to_remove = [k for k in st.session_state.keys() if k.startswith('sum_') or k.startswith('btn_')]
            for k in keys_to_remove:
                del st.session_state[k]
            st.session_state['last_news_ticker'] = news_ticker
        
        if news_ticker:
            selected_row = top_10[top_10['Ticker'] == news_ticker]
            if not selected_row.empty:
                c_name = selected_row.iloc[0]['Name']
            else:
                c_name, _ = get_ticker_metadata(news_ticker)

            with st.spinner(f"Fetching news for {news_ticker} ({c_name})..."):
                news_items = get_ticker_news(news_ticker, company_name=c_name)
                if news_items:
                    for item in news_items:
                        pub_str = f" ({item['publisher']})" if item['publisher'] != 'Unknown' else ""
                        with st.expander(f"📰 {item['title']}{pub_str}", expanded=True):
                            st.write(f"**Published**: {item['time']}")
                            st.write(f"[Read Article]({item['link']})")

                            # Unique key for button. Use link hash to ensure uniqueness even across tickers/reruns.
                            import hashlib
                            link_hash = hashlib.md5(item['link'].encode()).hexdigest()[:8]
                            btn_key = f"sum_{news_ticker}_{link_hash}" 
                            
                            # Session State Check
                            if btn_key not in st.session_state:
                                st.session_state[btn_key] = None
                            
                            if st.session_state[btn_key]:
                                st.success("✅ Deep Summary Generated")
                                st.info(st.session_state[btn_key])
                            else:
                                if st.button("✨ AI詳細要約 (Read Article)", key=f"btn_{btn_key}"):
                                    with st.spinner("記事を解析中... (これには数秒かかります)"):
                                        deep_val = get_article_summary(item['link'])
                                        st.session_state[btn_key] = deep_val
                                        st.rerun()
                else:
                    st.info(f"No specific news found for {news_ticker} in the last 3 days.")
    
    # --- Part 3: Sector Heatmap (New) ---
    # --- Part 3: Sector Heatmap (New) ---
    with tab5:
        st.markdown(f"<h2>🌡️ Sector Heatmap<br><span style='font-size: 0.6em; color: gray;'>{period_map[selected_period]}</span></h2>", unsafe_allow_html=True)
        st.caption("各セクターの「勝ち組 Top 3」と「負け組 Bottom 3」をヒートマップ表示")

    SECTOR_JP_MAP = {
        "🖥️ AI: Hardware & Cloud Infra": "🖥️ AI: ハードウェア & クラウド",
        "🧠 AI: Software & SaaS": "🧠 AI: ソフトウェア & SaaS",
        "💸 Crypto & FinTech": "💸 クリプト & フィンテック",
        "🌌 Space & Defense": "🌌 宇宙 & 防衛",
        "☢️ Energy: Nuclear": "☢️ エネルギー: 原子力",
        "⚡ Energy: Power & Renewables": "⚡ エネルギー: 電力 & 再エネ",
        "🛢️ Energy: Oil & Gas": "🛢️ エネルギー: 石油 & ガス",
        "💊 BioPharma: Big Pharma & Obesity": "💊 製薬: 大手 & 肥満薬",
        "🧬 BioPharma: Biotech & Gene": "🧬 製薬: バイオテク & 遺伝子",
        "🏥 MedTech & Health": "🏥 メドテック & ヘルスケア",
        "🍔 Consumer: Food & Bev": "🍔 消費財: 食品 & 飲料",
        "🛒 Consumer: Retail & E-Com": "🛒 消費財: 小売 & Eコマース",
        "👗 Consumer: Apparel & Leisure": "👗 消費財: アパレル & レジャー",
        "🚗 Auto & EV": "🚗 自動車 & EV",
        "🏘️ Real Estate & REITs": "🏘️ 不動産 & REITs",
        "🏦 Finance: Banks & Capital": "🏦 金融: 銀行 & 資本市場",
        "🏗️ Industrials & Transport": "🏗️ 資本財 & 輸送",
        "⛏️ Resources & Materials": "⛏️ 資源 & 素材",
        "📱 Tech: Communication": "📱 テック: 通信",
        "🏠 Homebuilders & Residential": "🏠 住宅 & 建設",
        "⚛️ Tech: Quantum Computing": "⚛️ テック: 量子コンピュータ"
    }
    
    def render_sector_heatmap(df, period):
        # 1. Calculate Sector Performance
        sector_stats = []
        for sector_name, tickers in SECTOR_DEFINITIONS.items():
            df_sec = df[df['Ticker'].isin(tickers)]
            if df_sec.empty: continue
            
            # Map to JP Name (Fallback to English if missing)
            jp_name = SECTOR_JP_MAP.get(sector_name, sector_name)
            
            avg_ret = df_sec[period].mean()
            sector_stats.append({
                'name': jp_name,
                'avg': avg_ret,
                'tickers': tickers,
                'df': df_sec
            })
            
        # Sort by Avg Return
        sector_stats.sort(key=lambda x: x['avg'], reverse=True)
        
        if not sector_stats: return

        # Identify Top 3 and Bottom 3
        top_3 = []
        bottom_3 = []
        others = []
        
        if len(sector_stats) >= 6:
            top_3 = sector_stats[:3]
            bottom_3 = sector_stats[-3:] 
            
            featured = [x['name'] for x in top_3] + [x['name'] for x in bottom_3]
            others = [x for x in sector_stats if x['name'] not in featured]
        else:
            others = sector_stats

        # Construct Display Order
        for x in top_3: x['type'] = 'TOP'
        for x in bottom_3: x['type'] = 'BOTTOM'
        for x in others: x['type'] = 'NORMAL'
        
        display_order = top_3 + bottom_3 + others
        
        # Container Style
        html_content = '<div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center;">'
        
        for sec in display_order:
            sector_name = sec['name']
            # Remove emoji for clean text if needed, strictly speaking user asked for JP name.
            # Keeping Emoji is good. 
            # The previous code cleaned naming by splitting ':'. We should probably just use the mapped JP name as is?
            # Or split it if it has emoji prefix?
            # Let's clean it similarly: Split by ':' if present, but for JP map I included emojis.
            # Actually, "AI: ハードウェア..." -> "ハードウェア..." might be cleaner?
            # User request: "ヒートマップのセクター名は日本語も欲しいな" -> "I want Japanese sector names too"
            # It implies full Japanese translation.
            
            cleaned_name = sector_name # Use full name for now or split?
            # The previous logic was: display_name = sector_name.split(":")[-1].strip()
            # If I mapped keys to "Emoji Name: JP Name", then splitting by ":" works well.
            # My map keys are full keys (e.g. "🖥️ AI: Hardware & Cloud Infra")
            # My map values are like "🖥️ AI: ハードウェア & クラウド"
            # So splitting by ":" gives " ハードウェア & クラウド" -> "ハードウェア & クラウド". Perfect.
            
            # Special handling if no ':'
            if ":" in sector_name:
                cleaned_name = sector_name.split(":")[-1].strip()
            else:
                # Remove leading emoji for cleaner look or keep it?
                # Previous code kept emoji in the `sector_name` loop variable but cleaned it for `display_name`.
                # Actually, wait. Previous code: display_name = sector_name.split(":")[-1].strip() if ":" in sector_name else sector_name
                # I'll stick to that logic to keep it compact.
                cleaned_name = sector_name
                
            avg = sec['avg']
            df_sec = sec['df']
            stype = sec['type']
            
            # Header Styling based on Type
            if stype == 'TOP':
                header_bg = "linear-gradient(90deg, #b8860b, #daa520)" # Golden
                header_text = f"🏆 {cleaned_name} (Avg {avg:+.1f}%)"
                border_color = "#daa520"
                container_shadow = "0 0 10px rgba(218, 165, 32, 0.3)"
            elif stype == 'BOTTOM':
                header_bg = "linear-gradient(90deg, #8b0000, #400000)" # Dark Red
                header_text = f"📉 {cleaned_name} (Avg {avg:+.1f}%)"
                border_color = "#8b0000"
                container_shadow = "0 0 10px rgba(139, 0, 0, 0.3)"
            else:
                header_bg = "#262730"
                header_text = f"{cleaned_name} (Avg {avg:+.1f}%)"
                border_color = "#333"
                container_shadow = "none"

            # Prepare Cell Data
            df_sec_sorted = df_sec.sort_values(period, ascending=False)
            
            if len(df_sec_sorted) <= 6:
                display_tickers = df_sec_sorted
            else:
                s_top3 = df_sec_sorted.head(3)
                s_bottom3 = df_sec_sorted.tail(3).sort_values(period, ascending=False)
                display_tickers = pd.concat([s_top3, s_bottom3])
            
            # Build HTML
            sector_html = f"""
            <div style="flex: 1 1 300px; max-width: 400px; background-color: #1a1a1a; border: 2px solid {border_color}; border-radius: 8px; overflow: hidden; box-shadow: {container_shadow};">
                <div style="background: {header_bg}; padding: 5px 10px; font-size: 0.9em; font-weight: bold; border-bottom: 1px solid #333; text-align: center; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{header_text}</div>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1px; background-color: #333;">
            """
            
            for _, row in display_tickers.iterrows():
                t = row['Ticker']
                ret = row.get(period, 0)
                
                if ret > 3.0: bg = "#006400"
                elif ret > 0.0: bg = "#2E8B57"
                elif ret > -3.0: bg = "#CD5C5C"
                else: bg = "#8B0000"
                
                cell_html = f"""<div style="background-color: {bg}; padding: 10px 4px; text-align: center; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 60px;"><div style="font-weight: 900; font-size: 1.0em; color: white; letter-spacing: 0.5px;">{t}</div><div style="font-size: 0.8em; color: rgba(255,255,255,0.9);">{ret:+.1f}%</div></div>"""
                sector_html += cell_html
                
            sector_html += "</div></div>"
            html_content += sector_html
            
        html_content += "</div>"
        st.markdown(html_content, unsafe_allow_html=True)
        
    if df_metrics is not None:
        with tab5:
            render_sector_heatmap(df_metrics, selected_period)


    
    # --- Part 4: 🤖 AI Portfolio Builder ---
    st.markdown("---")
    st.subheader("🤖 AI Portfolio Builder (Alpha)")
    st.caption("現在の市場環境（Momentum/Trend/Correlation）に基づき、AIが推奨する3つのポートフォリオ案です。")
    
    
    # Generate Portfolios
    # Need correlation matrix for Bento Box
    # Construct from history_dict
    with st.spinner("Calculating portfolio correlations..."):
        # Align histories
        try:
             # Create DataFrame from dict (keys=tickers, values=Series)
             # Values are normalized history. 
             # We need to make sure they share index or align. 
             # history_dict contains normalized series with DateTime index.
             
             # Filter only relevant candidates to speed up? 
             # Or just use all candidates history.
             
             price_history_df = pd.DataFrame(history_dict)
             
             # Some series might be shorter, align on recent date?
             # corr() handles NaNs by ignoring pairs.
             corr_matrix = price_history_df.corr()
             
             # If empty (unexpected)
             if corr_matrix.empty:
                 corr_matrix = pd.DataFrame()
        except Exception as e:
            # st.error(f"Correlation calc failed: {e}")
            corr_matrix = pd.DataFrame()

    # Identify Short-term Losers (to exclude from AI Portfolios)
    # Worst 10 for 1d and 5d
    # Note: df_metrics contains '1d' and '5d' for ALL candidates
    exclude_list = set()
    try:
        if '1d' in df_metrics.columns:
            worst_1d = df_metrics.sort_values('1d', ascending=True).head(10)['Ticker'].tolist()
            exclude_list.update(worst_1d)
        if '5d' in df_metrics.columns:
            worst_5d = df_metrics.sort_values('5d', ascending=True).head(10)['Ticker'].tolist()
            exclude_list.update(worst_5d)
    except:
        pass # Safety

    ai_portfolios = generate_ai_portfolios(df_sorted, corr_matrix, exclude_tickers=exclude_list)
    
    tab1, tab2, tab3, tab4 = st.tabs(["🐯 The Hunter", "🦅 The Sniper", "🏰 The Fortress", "🥗 The Bento Box"])
    
    def render_portfolio_tab(name, df, emoji, desc):
        if df.empty:
            st.warning("条件に合致する銘柄が見つかりませんでした。")
            return
            
        col1, col2 = st.columns([1.5, 1])
        
        with col1:
            st.markdown(f"### {emoji} {name}")
            st.caption(desc)
            
            # Display Table
            display_cols = ['Ticker', 'Price', '1mo', '3mo', 'RVOL', 'RSI', 'Signal']
            # Ensure cols exist
            valid_cols = [c for c in display_cols if c in df.columns]
            st.dataframe(df[valid_cols].style.format({
                'Price': "{:.2f}",
                '1mo': "{:+.2f}%",
                '3mo': "{:+.2f}%",
                'RVOL': "{:.2f}",
                'RSI': "{:.1f}"
            }), hide_index=True)
            
            # Virtual Performance
            sim_return = calculate_simulated_return(df)
            st.metric("📊 過去1ヶ月の仮想リターン (直近実績)", f"{sim_return:+.2f}%")
            
        with col2:
            # Pie Chart
            # Equal weight for now
            df['Weight'] = 100 / len(df)
            fig = px.pie(df, values='Weight', names='Ticker', title=f"{name} Allocation", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)

    with tab1:
        render_portfolio_tab("The Hunter (短期集中)", ai_portfolios['Hunter'], "🐯", 
                             "**攻撃型:** リターン・出来高重視。加熱感（RSI高）を問わず、とにかく「今強い」銘柄に乗る戦略。※高値掴み注意")
        
    with tab2:
        render_portfolio_tab("The Sniper (精密射撃)", ai_portfolios['Sniper'], "🦅", 
                             "**厳選型:** Hunterと同様に強いモメンタムを持ちつつ、RSI < 70 の「まだ加熱していない」銘柄に絞った戦略。安全マージン重視。")
                             
    with tab3:
        render_portfolio_tab("The Fortress (堅実トレンド)", ai_portfolios['Fortress'], "🏰",
                             "**順張り型:** 3ヶ月、6ヶ月、年初来がすべてプラスの「負けない」トレンド銘柄。安定した上昇気流に乗るための構成。")
        
    with tab4:
        render_portfolio_tab("The Bento Box (セクター分散)", ai_portfolios['Bento'], "🥗",
                             "**バランス型:** 主要テーマ（AI・エネ・金融・宇宙・消費）からそれぞれ最強の1銘柄をピックアップ。相関係数を抑えつつリターンを狙う幕の内弁当。")
                             
    # --- Footer: Disclaimer ---
    st.markdown("---")
    st.caption("⚠️ **免責事項**: 本アプリケーションは情報提供のみを目的としており、投資勧誘や助言を意図するものではありません。表示されるデータやAIによる分析結果は過去の実績に基づいており、将来の運用成果を保証するものではありません。投資判断はご自身の責任において行ってください。")

if __name__ == "__main__":
    main()
