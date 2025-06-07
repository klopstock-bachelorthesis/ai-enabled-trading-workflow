# data_fetchers/news_scraper.py
import requests
import json
import os
from datetime import datetime, timezone, timedelta
import logging # Import logging
import pandas as pd # Import pandas for CSV handling
import time # For potential rate limiting

def fetch_alpha_vantage_news(ticker_symbol, api_key, time_from_dt=None, time_to_dt=None, sort='LATEST', limit=10):
    """
    Fetches news articles for a given ticker using the Alpha Vantage NEWS_SENTIMENT API.

    Args:
        ticker_symbol (str): Stock ticker symbol.
        api_key (str): Alpha Vantage API key.
        limit (int): The number of news articles to return (default 10, max 1000 for premium plans).
        time_from_dt (datetime, optional): The start datetime for news articles.
        time_to_dt (datetime, optional): The end datetime for news articles.
        sort (str, optional): Sort order ('LATEST', 'EARLIEST', 'RELEVANCE'). Defaults to 'LATEST'.

    Returns:
        list: A list of news articles (dictionaries) from the API response, or None if an error occurs.
    """
    logging.debug(f"    fetch_alpha_vantage_news called with limit: {limit}") # Changed to debug logging
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker_symbol,
        "sort": sort,
        "limit": limit,
        "apikey": api_key
    }

    if time_from_dt:
        params["time_from"] = time_from_dt.strftime("%Y%m%dT%H%M") # Format: YYYYMMDDTHHMM
    if time_to_dt:
        params["time_to"] = time_to_dt.strftime("%Y%m%dT%H%M")     # Format: YYYYMMDDTHHMM

    url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
    logging.debug(f"    Requesting URL: {url}") # Changed to debug logging
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Information" in data or "Note" in data: # Rate limit or other API messages
            logging.warning(f"    Alpha Vantage API message for news for {ticker_symbol}: {data.get('Information') or data.get('Note')}") # Changed to warning
            # Consider a retry or backoff strategy here if rate limits are frequent
            return None
        if "Error Message" in data:
            logging.error(f"    Alpha Vantage API Error for news for {ticker_symbol}: {data['Error Message']}") # Changed to error
            return None
        if "feed" not in data or not isinstance(data["feed"], list):
            logging.warning(f"    No 'feed' data or unexpected format for {ticker_symbol}. API response: {data}") # Changed to warning
            return [] # Return empty list if feed is missing or not a list, keep this return

        return data["feed"]
    except requests.exceptions.RequestException as e:
        print(f"    [!] Request failed for Alpha Vantage news for {ticker_symbol}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        print(f"    [!] Failed to decode JSON for Alpha Vantage news for {ticker_symbol}: {e}")
        return None
    except Exception as e:
        print(f"    [!] An unexpected error occurred fetching Alpha Vantage news for {ticker_symbol}: {e}")
        return None

def fetch_and_save_news(ticker_symbol, week_start_date, week_end_date, api_key, output_dir, limit=10):
    """
    Fetches news for a ticker for a specific week (Mon-Sun) using Alpha Vantage
    NEWS_SENTIMENT API and saves it as a JSON file.
    """
    print(f"    Fetching Alpha Vantage news for {ticker_symbol} from {week_start_date.strftime('%Y-%m-%d')} to {week_end_date.strftime('%Y-%m-%d')}...")
    
    # Define the end of the week for the API query (Sunday 23:59)
    # The API's time_to is inclusive if it matches the publication time.
    api_time_to = week_end_date + timedelta(days=1) - timedelta(minutes=1) # Sunday 23:59

    all_news_items = fetch_alpha_vantage_news(
        ticker_symbol, api_key, limit=limit, time_from_dt=week_start_date, time_to_dt=api_time_to, sort='RELEVANCE'
    )

    if all_news_items is None: # Indicates an error during fetch
        logging.error(f"    Failed to fetch news for {ticker_symbol} from Alpha Vantage. Skipping save.") # Changed to error
        return None
    if not all_news_items: # Empty list, no news found by API
        logging.info(f"    No news articles returned by Alpha Vantage for {ticker_symbol}.") # Changed to info
        return None

    try:
        relevant_news = []
        for item in all_news_items:
            time_published_str = item.get("time_published") # Format: "YYYYMMDDTHHMMSS"
            if not time_published_str:
                continue

            try: # Keep try/except for parsing
                # Parse the datetime string and make it UTC aware
                publish_datetime_utc = datetime.strptime(time_published_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"    [!] Could not parse time_published: {time_published_str} for article: {item.get('title')}")
                continue

            # Make week_start_date and week_end_date UTC aware for comparison
            # week_start_date is the Monday 00:00:00
            # week_end_date is the Sunday 00:00:00, so we need to compare up to Sunday 23:59:59
            compare_week_start_utc = week_start_date.replace(tzinfo=timezone.utc)
            compare_week_end_utc = (week_end_date + timedelta(days=1) - timedelta(microseconds=1)).replace(tzinfo=timezone.utc)

            if compare_week_start_utc <= publish_datetime_utc <= compare_week_end_utc:
                article_data = {
                    'title': item.get('title'),
                    'url': item.get('url'),
                    'time_published_str': time_published_str,
                    'time_published_iso': publish_datetime_utc.isoformat(),
                    'authors': item.get('authors', []),
                    'summary': item.get('summary'),
                    'banner_image': item.get('banner_image'),
                    'source': item.get('source'),
                    'source_domain': item.get('source_domain'),
                    'category_within_source': item.get('category_within_source'),
                    'topics': item.get('topics', []),
                    'overall_sentiment_score': item.get('overall_sentiment_score'),
                    'overall_sentiment_label': item.get('overall_sentiment_label'),
                    'ticker_sentiment': [ts for ts in item.get('ticker_sentiment', []) if ts.get('ticker') == ticker_symbol]
                }
                relevant_news.append(article_data)

        if not relevant_news:
            logging.info(f"    No Alpha Vantage news found for {ticker_symbol} within the specified week ({week_start_date.strftime('%Y-%m-%d')} to {week_end_date.strftime('%Y-%m-%d')}).") # Changed to info
            return None

        # Sort news by publish time
        relevant_news.sort(key=lambda x: x['time_published_iso'])

        # File naming
        week_start_str = week_start_date.strftime('%Y-%m-%d')
        week_end_str = week_end_date.strftime('%Y-%m-%d')
        filename = f"news_{ticker_symbol}_{week_start_str}_to_{week_end_str}.csv" # Changed extension to .csv
        filepath = os.path.join(output_dir, filename)

        # Convert list of dictionaries to DataFrame
        news_df = pd.DataFrame(relevant_news)

        if not news_df.empty:
            news_df.to_csv(filepath, index=False, encoding='utf-8') # Keep index=False for CSV
            logging.info(f"    [✓] News saved to {filepath} ({len(relevant_news)} articles)") # Changed to info
            return filepath # Return filepath on successful save
        else:
            # This case should ideally be caught by "if not relevant_news:" earlier,
            # but as a safeguard:
            logging.warning(f"    No relevant news data to save for {ticker_symbol} for the week.") # Changed to warning
            return None

    except Exception as e:
        logging.error(f"    Error processing or saving Alpha Vantage news for {ticker_symbol}: {e}") # Changed to error
        import traceback
        traceback.print_exc()
        return None