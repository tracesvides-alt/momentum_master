import streamlit as st
import yfinance as yf
from deep_translator import GoogleTranslator
import time
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import requests
from io import StringIO
import random
from datetime import datetime, date
import plotly.express as px
import plotly.graph_objects as go
import re
import pickle
import os
import json
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
from market_logic import SECTOR_DEFINITIONS, TICKER_TO_SECTOR, STATIC_MOMENTUM_WATCHLIST, THEMATIC_ETFS, get_ai_stock_picks, SECTOR_TO_ETF

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
        # Sort by: Score (Desc) -> Time (Desc, newest first)
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

            # Skip if title is empty after translation
            if not display_title or display_title.strip() == '':
                continue

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


@st.cache_data(ttl=86400)
def translate_to_japanese(text):
    """
    Translates text to Japanese using deep_translator.
    """
    if not text:
        return ""
    try:
        # Simple truncation to avoid limits (Google Translate max 5000 chars, usually fine)
        translator = GoogleTranslator(source='auto', target='ja')
        return translator.translate(text)
    except Exception as e:
        return text

@st.cache_data(ttl=None)
def load_metadata_cache(mtime):
    """
    メタデータキャッシュを読み込み
    mtime: キャッシュ無効化のための更新時刻パラメータ
    Returns: dict {ticker: {'name': str, 'industry': str, 'summary': str}}
    """
    cache_path = "data/metadata_cache.json"
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

@st.cache_data(ttl=3600)  # フォールバック用キャッシュ（1時間）
def get_ticker_metadata(ticker):
    """
    Fetches info (Short Name, Sector/Industry, Summary) for a single ticker.
    Returns: (name, category_label, summary_text)
    キャッシュ優先、なければAPI呼び出し
    """
    # 1. Try Cache First (Fast Path)
    # ファイルの更新時刻を取得してキャッシュキーにする
    cache_path = "data/metadata_cache.json"
    mtime = 0
    if os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
    
    metadata_cache = load_metadata_cache(mtime)
    
    if ticker in metadata_cache:
        data = metadata_cache[ticker]
        name = data.get('name', ticker)
        industry = data.get('industry', '')
        summary = data.get('summary', '')
        
        # category優先順位: industry > 'Unknown'
        category = industry if industry else '🌊 Market Mover'
        
        return name, category, summary
    
    # 2. Fallback to API (Slow Path - for new tickers not in cache)
    try:
        t = yf.Ticker(ticker)
        info = t.info
        name = info.get('shortName', info.get('longName', ticker))
        
        # Priority: Industry > Sector > 'Unknown'
        industry = info.get('industry')
        sector = info.get('sector')
        category = industry if industry else (sector if sector else '🌊 Market Mover')
        
        # Summary (First 160 chars, no translation for performance)
        summary_en = info.get('longBusinessSummary', '')
        summary = ""
        if summary_en:
            summary_en = summary_en.replace('\n', ' ').strip()
            if len(summary_en) > 160:
                summary_en = summary_en[:160]
            # 翻訳（フォールバック用：通常はキャッシュから日本語が読み込まれる）
            try:
                summary = translate_to_japanese(summary_en)
            except:
                summary = summary_en  # 翻訳失敗時は英語のまま
        
        return name, category, summary
    except:
        # 3. Last Resort Fallback
        if 'dynamic_names' in st.session_state:
            if ticker in st.session_state['dynamic_names']:
                return st.session_state['dynamic_names'][ticker], '🌊 Market Mover', ''
        
        return ticker, '🌊 Market Mover', ''

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
    **目的**: 米国株のモメンタム（勢い）を可視化し、今注目すべき銘柄を素早く発見。
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
    
    # --- Market Regime Auto-Detection (AI Attitude) ---
    # Calc Regime
    regime_key, regime_label, regime_color = market_logic.calculate_market_regime(df_metrics)
    selected_regime = regime_key
    
    # Hide the big banner (User wants it next to title)
    # st.markdown(f"""...""") 

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
        d_name, d_cat, _ = get_ticker_metadata(t)
        
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

    # --- 🚨 Pre-Calculate Daily Signals (Inserted for Tabs Access) ---
    buy_breakout = []
    buy_reversal = []
    sells = []
    
    if history_dict:
        try:
             daily_signals = market_logic.get_todays_signals(history_dict)
             
             # Extract Lists
             buy_breakout = daily_signals.get('Buy_Breakout', [])
             buy_reversal = daily_signals.get('Buy_Reversal', [])
             sells = daily_signals.get('Sell', [])
             
             total_scanned = len(history_dict)
             
             # ALWAYS show section (User can verify scanning works)
             st.markdown(f"### 🔔 本日の売買シグナル速報 <span style='font-size:0.6em; color:gray;'>(Scanned {total_scanned} stocks)</span>", unsafe_allow_html=True)
             
             cols_sig = st.columns(3)
             
             # 1. Breakout
             with cols_sig[0]:
                 if buy_breakout:
                     st.success(f"**🚀 Breakout ({len(buy_breakout)})**")
                     st.caption("高値更新 & トレンド継続")
                     df_b = pd.DataFrame(buy_breakout)
                     st.dataframe(df_b[['Ticker', 'Price', 'Reason']].style.format({'Price': '{:.2f}'}), hide_index=True)
                 else:
                     st.info("🚀 Breakout: None")

             # 2. Reversal
             with cols_sig[1]:
                 if buy_reversal:
                     st.success(f"**🎣 Reversal ({len(buy_reversal)})**")
                     st.caption("MACD水面下からの反転")
                     df_r = pd.DataFrame(buy_reversal)
                     st.dataframe(df_r[['Ticker', 'Reason', 'Price']].style.format({'Price': '{:.2f}'}), hide_index=True)
                 else:
                     st.info("🎣 Reversal: None")

             # 3. Sells
             with cols_sig[2]:
                 if sells:
                     st.error(f"**👋 Sell Signals ({len(sells)})**")
                     st.caption("Stop Loss Triggered")
                     df_s = pd.DataFrame(sells)
                     st.dataframe(df_s[['Ticker', 'Price', 'Reason']].style.format({'Price': '{:.2f}'}), hide_index=True)
                 else:
                     st.markdown("👋 Sell: None")
             
             st.markdown("---")
             
        except Exception as e:
            st.error(f"Signal scan error: {e}")


    
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
        d_name, d_cat, _ = get_ticker_metadata(t)
        
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
    tab1, tab_rev, tab2, tab3, tab4, tab5 = st.tabs(["🏆 Top 10 Stocks", "🎣 Reversal Hunters", "📉 Worst 10 Stocks", "🔥 Hottest Themes", "🥶 Coldest Themes", "🌡️ Sector Heatmap"])
    
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
            
    # 2. Reversal Hunters (New)
    with tab_rev:
        st.markdown(f"### 🎣 Reversal Candidates (MACD Golden Cross from Lows)")
        if buy_reversal:
            # Convert to DF for display
            # We need to enrich it slightly to match the card view structure if possible, 
            # or just use a simple DF for now.
            # Let's try to reuse render_mobile_card_view by creating a mock DF with necessary columns.
            rev_tickers = [item['Ticker'] for item in buy_reversal]
            
            # Filter original df_metrics to get full data for these tickers
            if df_metrics is not None:
                df_rev_full = df_metrics[df_metrics['Ticker'].isin(rev_tickers)].copy()
                
                # Add "Reason" from buy_reversal to the DF
                ticker_to_reason = {item['Ticker']: item['Reason'] for item in buy_reversal}
                df_rev_full['Signal_Reason'] = df_rev_full['Ticker'].map(ticker_to_reason)
                
                # Override AI Strategy with Reason for clarity
                df_rev_full['AI Strategy'] = df_rev_full['Signal_Reason']
                
                if use_mobile_view:
                     render_mobile_card_view(df_rev_full, selected_period)
                else:
                    # Desktop
                    st.dataframe(
                        df_rev_full[['Ticker', 'Name', 'Sector', 'Price', 'Signal_Reason', selected_period]].style.format({
                            'Price': '{:.2f}', 
                            selected_period: '{:+.2f}%'
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.warning("Metrics data missing.")
        else:
            st.info("No Reversal Candidates found currently.")

    # 3. Worst 10
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
    st.caption("プルダウンから 上昇率Top 10 銘柄を選んで、最新ニュースをチェック（AIによるタイトル翻訳・要約機能付き）")
    
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
                c_name, _, _ = get_ticker_metadata(news_ticker)

            with st.spinner(f"Fetching news for {news_ticker} ({c_name})..."):
                news_items = get_ticker_news(news_ticker, company_name=c_name)
                if news_items:
                    # Initialize summary storage for this ticker if not exists
                    if 'news_summaries' not in st.session_state:
                        st.session_state['news_summaries'] = {}
                    if news_ticker not in st.session_state['news_summaries']:
                        st.session_state['news_summaries'][news_ticker] = {}
                    
                    # WORKAROUND: Dummy element to absorb Streamlit Cloud orphan widget bug
                    st.markdown("<div style='display:none;'></div>", unsafe_allow_html=True)
                    
                    for idx, item in enumerate(news_items):
                        pub_str = f" ({item['publisher']})" if item['publisher'] != 'Unknown' else ""
                        with st.expander(f"📰 {item['title']}{pub_str}", expanded=True):
                            st.write(f"**Published**: {item['time']}")
                            st.write(f"[Read Article]({item['link']})")

                            # Check if summary already exists for this article
                            summary_key = str(idx)
                            stored_summary = st.session_state['news_summaries'][news_ticker].get(summary_key)
                            
                            if stored_summary:
                                st.success("✅ Deep Summary Generated")
                                st.info(stored_summary)
                            else:
                                btn_key = f"btn_{news_ticker}_{idx}"
                                if st.button("✨ AI詳細要約 (Read Article)", key=btn_key):
                                    with st.spinner("記事を解析中... (これには数秒かかります)"):
                                        deep_val = get_article_summary(item['link'])
                                        st.session_state['news_summaries'][news_ticker][summary_key] = deep_val
                                        st.rerun()
                else:
                    st.info(f"No specific news found for {news_ticker} in the last 3 days.")
    
    # --- Part 3: Sector Heatmap (New) ---
    # --- Part 3: Sector Heatmap (New) ---
    with tab5:
        st.markdown(f"<h2>🌡️ Sector Heatmap<br><span style='font-size: 0.6em; color: gray;'>{period_map[selected_period]}</span></h2>", unsafe_allow_html=True)
        st.caption("各セクターの「勝ち組 Top 3」と「負け組 Bottom 3」をヒートマップ表示")

    SECTOR_JP_MAP = {
        # --- 1. Semi & AI Compute ---
        "🧠 Semi: AI Compute & Logic": "🧠 半導体: AIコンピュート & ロジック",
        "🏗️ Semi: Equipment & Foundry": "🏗️ 半導体: 製造装置 & ファウンドリ",
        "🖥️ AI Infra: Server & Memory": "🖥️ AIインフラ: サーバー & メモリ",
        "🔌 Semi: Analog & Power": "🔌 半導体: アナログ & パワー",
        
        # --- 2. AI Software & Security ---
        "🧠 AI: Big Tech": "🧠 AI: ビッグテック",
        "🛡️ AI: Cybersecurity": "🛡️ AI: サイバーセキュリティ",
        "☁️ AI: SaaS & Data Apps": "☁️ AI: SaaS & データアプリ",
        "🤖 Robotics & Automation": "🤖 ロボティクス & 自動化",
        
        # --- 3. Crypto & FinTech ---
        "🪙 Crypto: Miners & Assets": "🪙 クリプト: マイナー & 資産",
        "💳 FinTech & Payments": "💳 フィンテック: 決済",
        
        # --- 4. Defense & Space ---
        "🛡️ Defense: Major Contractors": "🛡️ 防衛: 大手請負",
        "🚀 Space & Future Mobility": "🚀 宇宙: 宇宙 & 次世代モビリティ",
        "🚁 Defense: Drones & Tech": "🚁 防衛: ドローン & テック",
        
        # --- 5 & 6. Energy & Utilities ---
        "☢️ Energy: Nuclear": "☢️ エネルギー: 原子力",
        "💡 Utilities: Regulated": "💡 公益: 規制電力",
        "☀️ Energy: Solar & Clean Tech": "☀️ エネルギー: 太陽光 & クリーンテック",
        
        # --- 7. Oil & Gas ---
        "🛢️ Energy: Integrated Majors": "🛢️ エネルギー: 統合石油メジャー",
        "🏗️ Energy: E&P (Upstream)": "🏗️ エネルギー: E&P (上流)",
        "🔧 Energy: Services & Equipment": "🔧 エネルギー: サービス & 設備",
        "🛤️ Energy: Midstream": "🛤️ エネルギー: ミッドストリーム",
        
        # --- 8-10. Bio & Health ---
        "💊 BioPharma: Big Pharma & Obesity": "💊 製薬: 大手製薬 & 肥満薬",
        "🧬 Biotech: Commercial Leaders": "🧬 バイオ: 商用リーダー",
        "🧪 Biotech: Gene & Cell Therapy": "🧪 バイオ: 遺伝子 & 細胞治療",
        "🔬 Biotech: Clinical & Growth": "🔬 バイオ: 臨床 & グロース",
        "🦾 MedTech & Devices": "🦾 医療: 医療機器 & デバイス",
        "🏥 Health Services & Insurers": "🏥 医療: ヘルスケアサービス & 保険",
        "📱 MedTech: Digital Health & Services": "📱 医療: デジタルヘルス & サービス",
        
        # --- 11-13. Consumer ---
        "🍔 Consumer: Restaurants": "🍔 消費財: レストラン",
        "🥤 Consumer: Food & Bev Staples": "🥤 消費財: 食品 & 飲料",
        "🛒 Consumer: Retail & E-Com": "🛒 消費財: 小売 & Eコマース",
        "✈️ Consumer: Travel & Leisure": "✈️ 消費財: 旅行 & レジャー",
        "👗 Consumer: Apparel & Luxury": "👗 消費財: アパレル & ラグジュアリー",
        
        # --- 14. Auto ---
        "🚗 Auto & EV": "🚗 自動車: 自動車 & EV",
        
        # --- 15. Real Estate ---
        "📡 Real Estate: Digital Infra": "📡 不動産: デジタルインフラ",
        "🏘️ Real Estate: Traditional": "🏘️ 不動産: 伝統的REIT",
        "🏠 Homebuilders & Residential": "🏠 住宅: 住宅建設 & 不動産",
        
        # --- 16. Finance ---
        "🏛️ Finance: Mega Banks": "🏛️ 金融: メガバンク",
        "🏦 Finance: Regional Banks": "🏦 金融: 地方銀行",
        "📈 Finance: Capital Markets & PE": "📈 金融: 資本市場 & PE",
        "💳 Finance: Credit Cards": "💳 金融: クレジットカード", # Old Key Cleanup might be needed if logic changed in market_logic, but I checked key is 'Credit Cards & Consumer'
        "💳 Finance: Credit Cards & Consumer": "💳 金融: クレジットカード & 消費者金融",
        "☂️ Finance: Insurance": "☂️ 金融: 保険",
        
        # --- 17. Industrials ---
        "🏭 Industrials: Machinery": "🏭 資本財: 機械 & 製造",
        "✈️ Transport & Logistics": "✈️ 輸送: 物流 & 輸送",
        "🏗️ Engineering & Construction": "🏗️ 建設: エンジニアリング & 建設",
        
        # --- 18. Resources ---
        "🥇 Resources: Gold & Silver": "🥇 資源: 金 & 銀",
        "🏗️ Resources: Base Metals (Cu, Fe, Al)": "🏗️ 資源: ベースメタル (銅鉄アルミ)",
        "🔋 Resources: Battery & EV Materials": "🔋 資源: 電池材料 & EV素材",
        "🧲 Resources: Rare Earths & Specialty": "🧲 資源: レアアース & 特殊金属",
        "⚗️ Resources: Chemicals & Materials": "⚗️ 資源: 化学 & 素材",
        "💍 Resources: PGM & Royalty": "💍 資源: 白金族 & ロイヤルティ",
        
        # --- Tech ---
        "⚛️ Tech: Quantum Computing": "⚛️ テック: 量子コンピュータ"
    }
    
    def render_sector_heatmap(df, period):
        # 1. Calculate Sector Performance
        sector_stats = []
        for sector_name, tickers in SECTOR_DEFINITIONS.items():
            df_sec = df[df['Ticker'].isin(tickers)]
            if df_sec.empty: continue
            
            # Use Japanese Name for EVERYTHING now
            jp_name = SECTOR_JP_MAP.get(sector_name, sector_name)
            
            avg_ret = df_sec[period].mean()
            sector_stats.append({
                'name': jp_name, # Storing JP Name
                'avg': avg_ret,
                'tickers': tickers,
                'df': df_sec
            })
            
        # Sort by Avg Return
        sector_stats.sort(key=lambda x: x['avg'], reverse=True)
        
        if not sector_stats: return

        st.markdown(f"### 📊 Sector Momentum Ranking <span style='font-size:0.8em; color:gray;'>(Click 'Details' to expand)</span>", unsafe_allow_html=True)
        
        # Styles (flush left HTML) - Restored Style A
        styles = """
<style>
.rank-card {
    background-color: #151515;
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 2px; /* Close gap to expander */
    display: grid;
    grid-template-columns: 40px 1fr 100px 120px;
    align-items: center;
    gap: 10px;
    border: 1px solid #333;
    transition: transform 0.2s;
}
.rank-card:hover { border-color: #555; }

.rank-num { font-size: 1.4rem; font-weight: 900; color: #555; text-align: center; }
.rank-1 { color: #FFD700; text-shadow: 0 0 10px rgba(255, 215, 0, 0.5); }
.rank-2 { color: #C0C0C0; text-shadow: 0 0 10px rgba(192, 192, 192, 0.3); }
.rank-3 { color: #CD7F32; text-shadow: 0 0 10px rgba(205, 127, 50, 0.3); }

.sec-name { font-weight: 700; font-size: 1.0rem; color: #eee; }
.sec-meta { font-size: 0.75rem; color: #aaa; display: flex; gap: 10px; margin-top: 4px; }
.win-rate { background: #333; padding: 2px 6px; border-radius: 4px; }

.top-gainer { font-size: 0.8rem; color: #888; text-align: right; }
.gainer-tick { color: #bbb; font-weight: 600; }

.ret-box { text-align: right; }
.ret-val { font-size: 1.2rem; font-weight: 800; }
.ret-bar-bg { width: 100%; height: 4px; background: #333; border-radius: 2px; margin-top: 4px; overflow: hidden; }
.ret-bar-fill { height: 100%; border-radius: 2px; }

/* Mobile adjustment */
@media (max-width: 600px) {
    .rank-card { grid-template-columns: 30px 1fr 80px; }
    .top-gainer { display: none; }
}
</style>
"""
        st.markdown(styles, unsafe_allow_html=True)

        # Helper to render a single sector block (HYBRID: Card + Expander)
        def render_sector_block(stat, i, max_abs_ret):
            rank = i + 1
            avg = stat['avg']
            df_s = stat['df']
            
            # Rank Style
            rank_class = "rank-num"
            icon = f"{rank}"
            if rank == 1: 
                rank_class += " rank-1"
                icon = "🥇"
            elif rank == 2: 
                rank_class += " rank-2"
                icon = "🥈"
            elif rank == 3: 
                rank_class += " rank-3"
                icon = "🥉"
            
            # Color Logic
            if avg >= 0:
                val_color = "#00FF00"
                bar_color = "linear-gradient(90deg, #004400, #00FF00)"
            else:
                val_color = "#FF4444"
                bar_color = "linear-gradient(90deg, #440000, #FF4444)"
                
            # Bar Width
            bar_width = min(100, (abs(avg) / max_abs_ret) * 100)
            
            # Meta Stats
            win_count = len(df_s[df_s[period] > 0])
            total_count = len(df_s)
            win_rate_str = f"Win {win_count}/{total_count}"
            
            top_gainer = df_s.sort_values(period, ascending=False).iloc[0] if not df_s.empty else None
            gainer_html = ""
            if top_gainer is not None:
                gainer_html = f"🚀 {top_gainer['Ticker']} <span class='gainer-tick'>{top_gainer[period]:+.1f}%</span>"

            # 1. Render Visual Card
            card_html = f"""
<div class="rank-card">
    <div class="{rank_class}">{icon}</div>
    <div>
        <div class="sec-name">{stat['name']}</div>
        <div class="sec-meta">
            <span class="win-rate">{win_rate_str}</span>
        </div>
    </div>
    <div class="top-gainer">{gainer_html}</div>
    <div class="ret-box">
        <div class="ret-val" style="color: {val_color};">{avg:+.2f}%</div>
        <div class="ret-bar-bg">
            <div class="ret-bar-fill" style="width: {bar_width}%; background: {bar_color};"></div>
        </div>
    </div>
</div>
"""
            st.markdown(card_html, unsafe_allow_html=True)
            
            # 2. Render Detail Expander BELOW the card
            with st.expander("🔽 全銘柄を表示 / Show Details", expanded=False):
                # Inside: Render ALL tickers in this sector
                df_sorted = df_s.sort_values(period, ascending=False)
                cols_grid = st.columns(3) if not use_mobile_view else st.columns(1)
                for idx, row in df_sorted.iterrows():
                    ticker = row['Ticker']
                    ret_val = row.get(period, 0)
                    price = row.get('Price', 0)
                    color = "#00FF00" if ret_val > 0 else "#FF4444"
                    bg_color = "rgba(0, 255, 0, 0.1)" if ret_val > 0 else "rgba(255, 0, 0, 0.1)"
                    
                    mini_card = f"""
                    <div style="border: 1px solid #444; border-radius: 6px; padding: 8px; margin-bottom: 6px; background-color: #0e1117;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <span style="font-weight: bold; font-size: 1.1em; color: #eee;">{ticker}</span>
                                <span style="font-size: 0.8em; color: #aaa;">${price:.2f}</span>
                            </div>
                            <span style="font-weight: bold; color: {color}; background-color: {bg_color}; padding: 2px 6px; border-radius: 4px; font-size: 0.9em;">
                                {ret_val:+.2f}%
                            </span>
                        </div>
                    </div>
                    """
                    if use_mobile_view:
                        st.markdown(mini_card, unsafe_allow_html=True)
                    else:
                        with cols_grid[idx % 3]:
                            st.markdown(mini_card, unsafe_allow_html=True)

        # Determine Max Return for Bar scaling
        max_abs_ret = 0.1
        if sector_stats:
            max_val = max([abs(x['avg']) for x in sector_stats])
            if max_val > 0: max_abs_ret = max_val

        # Logic to Split Top/Middle/Bottom
        if len(sector_stats) > 10:
             # Top 5
             for i in range(5):
                 render_sector_block(sector_stats[i], i, max_abs_ret)
             
             # Middle
             middle_count = len(sector_stats) - 10
             mid_start = 5
             mid_end = len(sector_stats) - 5
             
             st.markdown(f"<div style='margin: 10px 0;'>", unsafe_allow_html=True)
             if st.checkbox(f"🔽 6位 〜 {mid_end}位 を表示 ({middle_count}セクター)", value=False):
                 for i in range(mid_start, mid_end):
                     render_sector_block(sector_stats[i], i, max_abs_ret)
             st.markdown("</div>", unsafe_allow_html=True)
             
             # Bottom 5
             for i in range(mid_end, len(sector_stats)):
                 render_sector_block(sector_stats[i], i, max_abs_ret)
                 
        else:
             # Regular Full List
             for i, stat in enumerate(sector_stats):
                 render_sector_block(stat, i, max_abs_ret)

        st.markdown("---")


            

        
    if df_metrics is not None:
        with tab5:
            render_sector_heatmap(df_metrics, selected_period)

    
    # --- Part 4: 🤖 AI Portfolio Builder ---
    
    # --- UI: 🎯 AI Stock Picks (Before AI Portfolio Builder) ---
    st.markdown("---")
    
    # Custom Header with Regime Label
    st.markdown(f"""
    <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 10px;">
        <h3 style="margin: 0;">🎯 AI銘柄ピック</h3>
        <div style="
            background-color: #1E1E1E; 
            border: 1px solid {regime_color}; 
            color: {regime_color}; 
            padding: 2px 10px; 
            border-radius: 12px; 
            font-size: 0.9rem; 
            font-weight: bold;
            display: flex; align-items: center; gap: 5px;
        ">
            <span>🧠 {regime_label}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("投資期間別オススメ銘柄 (詳細)", expanded=True):
        st.caption("短期・中期・長期の各観点からスコアリングし、トップ3銘柄を自動選出します。")
        
        # Get ETF metrics from the same cache (they are in df_metrics)
        etf_tickers = list(THEMATIC_ETFS.values())
        etf_df = df_metrics[df_metrics['Ticker'].isin(etf_tickers)]
        
        # Get AI picks (news checker is optional, skip for performance)
        # Pass the selected regime
        ai_picks = get_ai_stock_picks(df_metrics, etf_metrics=etf_df, news_checker=None, top_n=3, regime=selected_regime)
        
        # Display in 3 columns
        col_short, col_mid, col_long = st.columns(3)
        
        timeframe_config = [
            (col_short, 'short', '⚡ 短期 (1-2週間)', '#FF6B6B'),
            (col_mid, 'mid', '📈 中期 (1-3ヶ月)', '#4ECDC4'),
            (col_long, 'long', '🏆 長期 (6ヶ月+)', '#F4A460'),
        ]
        
        for col, tf_key, tf_label, color in timeframe_config:
            with col:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, {color}22, {color}11); 
                            border-left: 4px solid {color}; 
                            padding: 8px 12px; 
                            border-radius: 8px; 
                            margin-bottom: 10px;">
                    <span style="font-weight: 700; font-size: 0.95rem;">{tf_label}</span>
                </div>
                """, unsafe_allow_html=True)
                
                picks = ai_picks.get(tf_key, [])
                
                if not picks:
                    st.info("データ不足")
                    continue
                    
                for i, pick in enumerate(picks):
                    ticker = pick['ticker']
                    score = pick['score']
                    reason = pick['reason']
                    metrics = pick['metrics']
                    sector = metrics.get('sector', '')[:15]
                    crash_risk = pick.get('crash_risk', 0)
                    
                    # Key metric based on timeframe
                    if tf_key == 'short':
                        key_metric = f"5d: {metrics['5d']:+.1f}%"
                    elif tf_key == 'mid':
                        key_metric = f"1mo: {metrics['1mo']:+.1f}%"
                    else:
                        key_metric = f"1y: {metrics['1y']:+.1f}%"
                    
                    # Build risk factor breakdown for tooltip
                    risk_factors = []
                    rsi = metrics.get('RSI', 50)
                    if rsi > 75:
                        risk_factors.append(f"RSI過熱({rsi:.0f})")
                    beta = metrics.get('Beta', 1.0)
                    if beta > 2:
                        risk_factors.append(f"高Beta({beta:.1f})")
                    sma_dev = metrics.get('SMA50_Deviation', 0)
                    if sma_dev > 20:
                        risk_factors.append(f"SMA乖離+{sma_dev:.0f}%")
                    inst_own = metrics.get('InstOwnership', 0)
                    if inst_own > 0.8:
                        risk_factors.append(f"機関{inst_own*100:.0f}%")
                    
                    risk_detail = " / ".join(risk_factors) if risk_factors else ""
                    
                    # Crash risk badge with numerical display
                    if crash_risk > 50:
                        risk_badge = f'<span style="background: #FF4444; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; margin-left: 6px;" title="{risk_detail}">🔴高リスク</span>'
                        risk_bar_color = "#FF4444"
                    elif crash_risk > 30:
                        risk_badge = f'<span style="background: #FFA500; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; margin-left: 6px;" title="{risk_detail}">🟠中リスク</span>'
                        risk_bar_color = "#FFA500"
                    elif crash_risk < 15:
                        risk_badge = f'<span style="background: #28A745; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; margin-left: 6px;">🟢低リスク</span>'
                        risk_bar_color = "#28A745"
                    else:
                        risk_badge = ''
                        risk_bar_color = "#888"
                    
                    # Risk factor display (show below card if factors exist)
                    risk_factors_html = ""
                    if risk_factors and crash_risk > 30:
                        risk_factors_html = f'<div style="font-size: 0.65rem; color: #FF6B6B; margin-top: 4px; opacity: 0.9;">⚡ {risk_detail}</div>'
                    
                    # Get company name and industry
                    company_name, industry, summary = get_ticker_metadata(ticker)
                    
                    # Prepare short summary
                    short_summary = summary[:80] + "..." if len(summary) > 80 else summary
                    if not short_summary:
                         short_summary = f"{company_name}は{industry}セクターの主要企業です。"
                    

                    st.markdown(f"""
                    <div style="background: rgba(255,255,255,0.05); 
                                border-radius: 8px; 
                                padding: 10px 12px; 
                                margin-bottom: 8px;
                                border: 1px solid rgba(255,255,255,0.1);">
                        <div style="display: flex; justify-content: space-between; align-items: start;">
                            <div>
                                <span style="font-weight: 700; font-size: 1.1rem; color: {color};">
                                    #{i+1} {ticker} <span style="font-size: 0.8rem; color: #aaa;">(Score: {score:.1f})</span>{risk_badge}
                                </span>
                                <div style="font-size: 0.8rem; color: #ccc; margin-top: 2px;">
                                    {company_name} | <span style="color: #4ECDC4;">{industry}</span>
                                </div>
                            </div>
                            <span style="font-size: 0.85rem; opacity: 0.8; font-weight: bold;">
                                ${metrics['price']:.2f}
                            </span>
                        </div>
                        <div style="font-size: 0.75rem; opacity: 0.7; margin: 6px 0 8px 0; font-style: italic; color: #eee; border-left: 2px solid #555; padding-left: 8px;">
                            {short_summary}
                        </div>
                        <div style="font-size: 0.9rem; font-weight: 600; color: #4CAF50; margin-bottom: 4px;">
                            {key_metric}
                        </div>
                        <div style="font-size: 0.72rem; opacity: 0.6;">
                            💡 {reason}
                        </div>
                        {risk_factors_html}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Score Breakdown Expander
                    with st.expander("📊 スコア詳細を見る", expanded=False):
                        details = pick.get('details', [])
                        if details:
                            # Color code details
                            for d in details:
                                if ": +" in d:
                                    color = "#4CAF50" # Green for bonus
                                    d_fmt = d.replace(":", f': <span style="color:{color}; font-weight:bold;">') + '</span>'
                                elif ": -" in d:
                                    color = "#FF4444" # Red for penalty
                                    d_fmt = d.replace(":", f': <span style="color:{color}; font-weight:bold;">') + '</span>'
                                else:
                                    d_fmt = d
                                st.markdown(f"- {d_fmt}", unsafe_allow_html=True)
                        else:
                            st.write("詳細データなし")
        
        # Risk badge legend
        st.markdown("""
        <div style="margin-top: 12px; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 6px; font-size: 0.75rem;">
            <span style="font-weight: 600; opacity: 0.9;">📊 リスクバッジ凡例:</span>
            <span style="background: #FF4444; color: white; padding: 1px 5px; border-radius: 3px; margin-left: 8px;">🔴高リスク</span> <span style="opacity: 0.7;">(50+)</span>
            <span style="background: #FFA500; color: white; padding: 1px 5px; border-radius: 3px; margin-left: 8px;">🟠中リスク</span> <span style="opacity: 0.7;">(30-50)</span>
            <span style="background: #28A745; color: white; padding: 1px 5px; border-radius: 3px; margin-left: 8px;">🟢低リスク</span> <span style="opacity: 0.7;">(0-15)</span>
            <br><span style="opacity: 0.6; margin-top: 4px; display: inline-block;">※リスクスコアはRSI過熱・高Beta・SMA乖離・機関保有率・ShortRatio等から算出。高リスク銘柄はスコアリングでペナルティが適用されます。</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Scoring Explanation Expander
        with st.expander("📊 スコアリング指標の詳細説明", expanded=False):
            st.markdown("""
            ### スコアリングロジック（投資期間別）
            
            | 指標 | 短期 (1-2週間) | 中期 (1-3ヶ月) | 長期 (6ヶ月+) |
            | :--- | :--- | :--- | :--- |
            | **重視点** | **爆発力・初動** | **トレンド安定性** | **業績・ファンダメンタルズ** |
            | **RVOL (出来高)** | **最重要 (30%)**<br>急増で高評価 | 重要 (10%)<br>トレンド継続確認 | 参考 (5%) |
            | **リターン** | **5日騰落 (20%)**<br>初動〜急伸を狙う | **1ヶ月騰落 (25%)**<br>安定上昇を評価 | 1年/YTD (30%)<br>長期上昇トレンド |
            | **RSI** | **90まで許容 (10%)**<br>強気相場の過熱を許容 | **過熱警戒 (10%)**<br>75-85で減点、安定重視 | 中立 (5%)<br>極端な過熱・売られすぎ回避 |
            | **テクニカル** | **高値更新 (20%)**<br>52週高値ブレイク重視 | **BBスクイーズ (15%)**<br>エネルギー蓄積(期間ボーナス有) | トレンド (20%)<br>SMA200/50乖離、押し目 |
            | **その他** | **相対強度 (RS)**<br>対セクターでアウトパフォーム加点 | **相対強度 (RS)**<br>対セクターでアウトパフォーム加点 | **リスク管理 (30%)**<br>低ベータ、機関保有率、業績 |

            ---
            #### 用語解説
            - **RVOL (Relative Volume)**: 過去平均に対する当日の出来高倍率。2倍以上は強い資金流入を示唆。
            - **BBスクイーズ**: ボリンジャーバンドの幅が収縮している状態。エネルギーが蓄積され、次の大きな動きの前兆とされる。**3日以上継続でボーナス加点。**
            - **相対強度 (RS)**: セクターETFや市場全体と比較した強さ。セクター平均を上回る（アウトパフォーム）銘柄を高く評価。
            
            """)
        



    
    # ==========================================
    # 🔍 Momentum Analyzer (Deep Dive)
    # ==========================================
    # --- Momentum Analyzer Tab ---
    # With tab removed, this is now a top-level section
    
    # st.markdown("---") # Already present in previous context potentially, but let's ensure structure
    st.subheader("🔍 個別銘柄詳細分析 & 売買シグナル")
    st.caption("個別銘柄のモメンタム状態を詳細分析し、過去のチャートからAIが売買判断とアクションプランを提示します。")
    
    col_input, col_btn = st.columns([3, 1])
    with col_input:
        analyzer_ticker = st.text_input("ティッカーシンボルを入力 (例: NVDA, 7203.T)", value="NVDA").upper()
    with col_btn:
        st.write("") # Spacer
        run_analysis = st.button("詳細分析を実行", type="primary")
        
    if run_analysis and analyzer_ticker:
        with st.spinner(f"{analyzer_ticker} のデータを取得・分析中..."):
            # Call Logic
            df_hist, summary = market_logic.analyze_stock_history(analyzer_ticker)
            
            if df_hist is None:
                st.error(f"エラー: {summary.get('error', '不明なエラーが発生しました')}")
            else:
                # --- 1. Status Badge & Action ---
                status = summary['status']
                action = summary['action']
                score = summary.get('score', 0)
                
                # Color mapping
                status_color = "#888"
                if status == "BUY": status_color = "#00CC00" # Green
                elif status == "HOLD": status_color = "#00AAFF" # BIue
                elif status == "SELL": status_color = "#FF4444" # Red
                elif status == "WAIT": status_color = "#FFA500" # Orange
                
                # Status Details with New Indicators
                st.markdown(f"""
                <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px; border-left: 5px solid {status_color}; margin-bottom: 20px;">
                    <div style="display:flex; align-items:center; gap: 15px;">
                        <span style="font-size: 2rem; font-weight: 900; color: {status_color};">{status}</span>
                        <div style="flex-grow: 1;">
                            <div style="font-size: 1.2rem; font-weight: bold;">{action}</div>
                            <div style="font-size: 0.8rem; color: #ccc;">
                                現在値: ${summary['price']:.2f} | RSI: {summary['rsi']:.1f} | MFI: {summary['mfi']:.1f}<br>
                                RVOL: {summary['rvol']:.2f}倍 | MACDシグナル: {"Bullish (上昇)" if summary['macd'] > 0 else "Bearish (下落)"}<br>
                                <b>損切ライン(Chandelier): ${summary['chandelier']:.2f}</b>
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # --- Signal Explanation (Updated) ---
                with st.expander("ℹ️ 新・シグナル判定基準と指標解説"):
                    st.markdown("""
                    **🎯 エントリー (Buy)**
                    - **条件**: [バンド拡大/高値更新] + [RVOL>1.5] + [MACD好転] + **[ADX>25]**
                    - **意味**: 「勢い(Momentum)」と「エネルギー(Expansion)」に加え、**「明確なトレンド発生(ADX)」** を確認します。
                    - **制限**: 買い増し過熱防止のため、**1トレンドにつき最大3回まで** とし、かつ **5日間の間隔** を空けます。
                    - **例外**: 大陽線によるトレンド転換(Reversal)や、明確な押し目(Re-entry)は別途判定します。
                    
                    **👋 エグジット (Sell/Profit Take/Stop)**
                    - **条件**: ポジション保有中（Buy発生後）のみシグナルを監視します。ノーポジ迷子（Phantom Sell）は防ぎます。
                    - **利確 (Profit)**: **RSI > 90** または MACDデッドクロス(※過熱後のみ)。逃げ遅れ防止(RSI<60割れ)も完備。
                    - **損切 (Stop)**: **Chandelier Exit** を下回ったら即撤退。明確なトレンド終了サインです。
                    
                    **📉 Chandelier Exit (シャンデリア・イグジット)**
                    - チャート上の **紫色の点線** です。振るい落としを防ぐため、**ATR×5.0** の広めに設定しています。
                    - 株価がこれを割り込んだら、トレンド完全終了とみなして撤退してください。
                    """)
                
                # --- 2. Interactive Chart (Plotly) ---
                # Title outside chart to prevent overlap
                st.markdown(f"##### 📈 {analyzer_ticker} 強化版モメンタムチャート")
                
                # Candlestick
                fig = go.Figure(data=[go.Candlestick(x=df_hist.index,
                                open=df_hist['Open'],
                                high=df_hist['High'],
                                low=df_hist['Low'],
                                close=df_hist['Close'],
                                name='株価')])
                
                # SMAs
                fig.add_trace(go.Scatter(x=df_hist.index, y=df_hist['SMA50'], line=dict(color='#FFA500', width=1), name='SMA50 (中期)'))
                fig.add_trace(go.Scatter(x=df_hist.index, y=df_hist['SMA150'], line=dict(color='green', width=1), name='SMA150'))
                
                # Chandelier Exit (Trailing Stop)
                fig.add_trace(go.Scatter(x=df_hist.index, y=df_hist['Chandelier_Exit'], 
                                         line=dict(color='violet', width=1.5, dash='dot'), 
                                         name='Chandelier Exit (損切ライン)'))
                
                # BB
                fig.add_trace(go.Scatter(x=df_hist.index, y=df_hist['BB_Upper'], line=dict(color='gray', width=0), showlegend=False))
                fig.add_trace(go.Scatter(x=df_hist.index, y=df_hist['BB_Lower'], line=dict(color='gray', width=0), fill='tonexty', fillcolor='rgba(128,128,128,0.1)', showlegend=False, name='BB'))
                
                # Buy/Sell Markers
                signals = df_hist[df_hist['Signal'].notna()]
                
                for date, row in signals.iterrows():
                    sig = row['Signal']
                    reason = row['Reason']
                    
                    marker_color = "#00CC00" 
                    # Use B/S text instead of simple arrow-only or long text
                    symbol_text = "B" 
                    bgcolor = "#00CC00"
                    offset = 20
                    y_anchor = row['Low']
                    ay = 25 # Arrow length
                    
                    if sig == "Sell":
                        marker_color = "#FF4444"
                        symbol_text = "S"
                        bgcolor = "#FF4444"
                        offset = -20
                        y_anchor = row['High']
                        ay = -25
                        
                    # Detailed Hover Text
                    hover_text = f"<b>{sig} Signal</b><br>{reason}<br>Price: ${row['Close']:.2f}"
                    
                    fig.add_annotation(
                        x=date, y=y_anchor,
                        text=symbol_text,
                        showarrow=True,
                        arrowhead=1, # Simpler arrowhead
                        arrowsize=0.8, # Smaller head
                        arrowwidth=1.0, # Thinner line
                        arrowcolor=marker_color,
                        ax=0,
                        ay=ay * 0.6, # Shorter arrow (was 25, now ~15)
                        bgcolor=bgcolor,
                        bordercolor="#ffffff",
                        borderwidth=1,
                        borderpad=1, # Tighter box
                        opacity=0.8,
                        font=dict(color="white", size=8, weight="bold"), # Smaller font
                        hovertext=hover_text
                    )

                fig.update_layout(
                    # Title Removed from here to avoid overlap
                    yaxis_title="", 
                    yaxis_side="right", # Move price axis to right (TradingView style)
                    xaxis_title="",
                    xaxis_rangeslider_visible=False,
                    height=450, # Slightly Taller
                    margin=dict(l=0, r=10, t=10, b=40), # Left margin 0, Small right margin for axis
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="top", y=-0.1, # Position below chart
                        xanchor="center", x=0.5, # Center horizontally
                        font=dict(size=10),
                        bgcolor="rgba(0,0,0,0)"
                    ),
                    dragmode='pan'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # --- 3. Alternatives ---
                st.markdown("#### 💡 同セクターの有望銘柄（Buy候補）")
                st.caption(f"入力された銘柄 ({analyzer_ticker}) と同じセクターで、**現在Buyシグナル点灯中または強い上昇トレンド**にある高スコア銘柄を提案します。")
                
                alternatives = market_logic.find_better_alternatives(analyzer_ticker, df_metrics)
                
                if alternatives:
                    cols = st.columns(3)
                    for i, alt in enumerate(alternatives):
                        with cols[i % 3]:
                            st.markdown(f"""
                            <div style="border:1px solid #444; padding:10px; border-radius:5px; text-align:center;">
                                <div style="color:#4ECDC4; font-weight:bold;">{alt['Ticker']}</div>
                                <div style="font-size:0.8rem;">総合スコア: {alt['Score']:.1f}</div>
                                <div style="font-size:0.7rem; color:#aaa;">RVOL: {alt['RVOL']:.1f}倍 | 1ヶ月: {alt['1mo']:+.1f}%</div>
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.info("キャッシュ内に、より有望な（Buy条件を満たす）同セクター銘柄は見つかりませんでした。")

    # ==========================================
    # 🤖 AI Portfolio Builder (Alpha) - Collapsible
    # ==========================================
    st.markdown("---")
    
    # Generate Portfolios Logic (Moved from Top)
    # Need correlation matrix for Bento Box
    with st.spinner("Calculating portfolio correlations..."):
        try:
             price_history_df = pd.DataFrame(history_dict)
             corr_matrix = price_history_df.corr()
             if corr_matrix.empty:
                 corr_matrix = pd.DataFrame()
        except Exception as e:
            corr_matrix = pd.DataFrame()

    # Identify Short-term Losers
    exclude_list = set()
    try:
        if '1d' in df_metrics.columns:
            worst_1d = df_metrics.sort_values('1d', ascending=True).head(10)['Ticker'].tolist()
            exclude_list.update(worst_1d)
        if '5d' in df_metrics.columns:
            worst_5d = df_metrics.sort_values('5d', ascending=True).head(10)['Ticker'].tolist()
            exclude_list.update(worst_5d)
    except:
        pass 

    ai_portfolios = generate_ai_portfolios(df_sorted, corr_matrix, exclude_tickers=exclude_list)
    
    with st.expander("🤖 AI Portfolio Builder (Alpha) - クリックして展開", expanded=False):
        st.caption("現在の市場環境（Momentum/Trend/Correlation）に基づき、AIが推奨する3つのポートフォリオ案です。")
        
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

        tab1, tab2, tab3, tab4 = st.tabs(["🐯 The Hunter", "🦅 The Sniper", "🏰 The Fortress", "🥗 The Bento Box"])
        
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
