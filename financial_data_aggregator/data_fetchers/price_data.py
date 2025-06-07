# data_fetchers/price_data.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time # For rate limiting

def fetch_daily_time_series(symbol, api_key, outputsize='compact'): # compact for last 100, full for full history
    """
    Fetches daily time series data (OHLCV) from Alpha Vantage.
    """
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={api_key}'
    # Using TIME_SERIES_DAILY_ADJUSTED for split/dividend adjusted prices
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'Time Series (Daily)' not in data:
            print(f"    [!] No 'Time Series (Daily)' data found for {symbol}. API response: {data.get('Note') or data.get('Information') or data.get('Error Message')}")
            return None
        
        df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True) # Ensure chronological order
        
        # Rename columns to OHLCV (and adjusted close, volume)
        df.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close', # This is unadjusted close
            '5. volume': 'volume',
        }, inplace=True)
        
        # Select relevant columns (OHLCV) - user requested OHLCV
        # We can decide to use 'close' or 'adj_close'. 'adj_close' is usually preferred.
        ohlcv_df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        
        return ohlcv_df
    except requests.exceptions.RequestException as e:
        print(f"    [!] Request failed for daily time series {symbol}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        print(f"    [!] Failed to decode JSON for daily time series {symbol}: {e}")
        return None
    except Exception as e:
        print(f"    [!] An unexpected error occurred fetching daily time series for {symbol}: {e}")
        return None

def fetch_macd(symbol, api_key, interval='daily', series_type='close', fastperiod=12, slowperiod=26, signalperiod=9):
    """
    Fetches MACD (Moving Average Convergence/Divergence) data from Alpha Vantage.
    """
    url = (f'https://www.alphavantage.co/query?function=MACD&symbol={symbol}&interval={interval}'
           f'&series_type={series_type}&fastperiod={fastperiod}&slowperiod={slowperiod}'
           f'&signalperiod={signalperiod}&apikey={api_key}')
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Technical Analysis: MACD" not in data:
            print(f"    [!] No 'Technical Analysis: MACD' data found for {symbol}. API response: {data.get('Note') or data.get('Information') or data.get('Error Message')}")
            return None

        df = pd.DataFrame.from_dict(data['Technical Analysis: MACD'], orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True) # Ensure chronological order

        # Rename columns for clarity
        df.rename(columns={
            'MACD': 'MACD',
            'MACD_Hist': 'MACD_Hist',
            'MACD_Signal': 'MACD_Signal'
        }, inplace=True)
        
        return df.astype(float)
    except requests.exceptions.RequestException as e:
        print(f"    [!] Request failed for MACD {symbol}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        print(f"    [!] Failed to decode JSON for MACD {symbol}: {e}")
        return None
    except Exception as e:
        print(f"    [!] An unexpected error occurred fetching MACD for {symbol}: {e}")
        return None

def fetch_rsi(symbol, api_key, interval='daily', time_period=14, series_type='close'):
    """
    Fetches RSI (Relative Strength Index) data from Alpha Vantage.
    """
    url = (f'https://www.alphavantage.co/query?function=RSI&symbol={symbol}&interval={interval}'
           f'&time_period={time_period}&series_type={series_type}&apikey={api_key}')
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Technical Analysis: RSI" not in data:
            print(f"    [!] No 'Technical Analysis: RSI' data found for {symbol}. API response: {data.get('Note') or data.get('Information') or data.get('Error Message')}")
            return None

        df = pd.DataFrame.from_dict(data['Technical Analysis: RSI'], orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True) # Ensure chronological order

        # Rename columns for clarity (e.g., RSI_14 if time_period can vary, or just RSI)
        df.rename(columns={'RSI': f'RSI_{time_period}'}, inplace=True)
        
        return df.astype(float)
    except requests.exceptions.RequestException as e:
        print(f"    [!] Request failed for RSI {symbol}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        print(f"    [!] Failed to decode JSON for RSI {symbol}: {e}")
        return None
    except Exception as e:
        print(f"    [!] An unexpected error occurred fetching RSI for {symbol}: {e}")
        return None

def fetch_bbands(symbol, api_key, interval='daily', time_period=14, series_type='close', nbdevup=2, nbdevdn=2, matype=0):
    """
    Fetches Bollinger Bands (BBANDS) data from Alpha Vantage.
    """
    url = (f'https://www.alphavantage.co/query?function=BBANDS&symbol={symbol}&interval={interval}'
           f'&time_period={time_period}&series_type={series_type}&nbdevup={nbdevup}&nbdevdn={nbdevdn}'
           f'&matype={matype}&apikey={api_key}')
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Technical Analysis: BBANDS" not in data:
            print(f"    [!] No 'Technical Analysis: BBANDS' data found for {symbol}. API response: {data.get('Note') or data.get('Information') or data.get('Error Message')}")
            return None

        df = pd.DataFrame.from_dict(data['Technical Analysis: BBANDS'], orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True) # Ensure chronological order

        # Rename columns for clarity
        df.rename(columns={
            'Real Upper Band': f'BB_UPPER_{time_period}',
            'Real Middle Band': f'BB_MIDDLE_{time_period}',
            'Real Lower Band': f'BB_LOWER_{time_period}'
        }, inplace=True)
        
        return df.astype(float)
    except requests.exceptions.RequestException as e:
        print(f"    [!] Request failed for BBANDS {symbol}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        print(f"    [!] Failed to decode JSON for BBANDS {symbol}: {e}")
        return None
    except Exception as e:
        print(f"    [!] An unexpected error occurred fetching BBANDS for {symbol}: {e}")
        return None


def fetch_and_save_ohlcv(ticker, week_start_date, week_end_date, api_key, output_dir):
    """
    Fetches OHLCV, MACD, RSI, and BBANDS data for a ticker for the last 200 days ending by week_end_date and saves it.
    Args:
        ticker (str): Stock ticker symbol.
        week_start_date (datetime): The Monday of the week.
        week_end_date (datetime): The Sunday of the week.
        api_key (str): Alpha Vantage API key.
        output_dir (str): Directory to save the CSV file.
    """
    print(f"    Fetching OHLCV, MACD, RSI & BBANDS for {ticker} ending around {week_end_date.strftime('%Y-%m-%d')}...")
    
    # Determine output size based on how far back week_start_date is.
    # 'compact' is 100 data points, 'full' is 20+ years.
    # If week_start_date is recent, 'compact' is usually enough.
    # For simplicity, let's use 'compact', but if processing very old dates, 'full' might be needed.
    # Alpha Vantage returns data in descending order of date by default.
    outputsize = 'compact'
    # To ensure enough data for 200 days and MACD calculation, 'full' is safer.
    if (datetime.today() - week_start_date).days > 90 or True: # Force full for 200 days + MACD
        outputsize = 'full' # This is a safer bet but uses more data.

    daily_df = fetch_daily_time_series(ticker, api_key, outputsize=outputsize)

    if daily_df is None or daily_df.empty: # Check after OHLCV fetch
        print(f"    [!] No OHLCV data fetched for {ticker}. Skipping CSV save.")
        return None

    # Fetch MACD data
    macd_df = fetch_macd(ticker, api_key) # Using default daily interval and close series_type

    # Merge OHLCV with MACD data
    if macd_df is not None and not macd_df.empty:
        # Use a left merge to keep all OHLCV data and add MACD where available
        merged_df = pd.merge(daily_df, macd_df, left_index=True, right_index=True, how='left')
        print(f"    [✓] MACD data merged for {ticker}.")
    else:
        print(f"    [!] No MACD data fetched for {ticker} or MACD data is empty. Proceeding with OHLCV only.")
        merged_df = daily_df # Proceed with OHLCV if MACD fails

    # Fetch RSI data (daily, 14-period, close price)
    rsi_df = fetch_rsi(ticker, api_key, time_period=20)
    if rsi_df is not None and not rsi_df.empty:
        merged_df = pd.merge(merged_df, rsi_df, left_index=True, right_index=True, how='left')
        print(f"    [✓] RSI data merged for {ticker}.")
    else:
        print(f"    [!] No RSI data fetched for {ticker} or RSI data is empty.")

    # Fetch Bollinger Bands data (daily, 14-period, close price, default deviations and MA type)
    bbands_df = fetch_bbands(ticker, api_key, time_period=20)
    if bbands_df is not None and not bbands_df.empty:
        merged_df = pd.merge(merged_df, bbands_df, left_index=True, right_index=True, how='left')
        print(f"    [✓] BBANDS data merged for {ticker}.")
    else:
        print(f"    [!] No BBANDS data fetched for {ticker} or BBANDS data is empty.")

    # daily_df.index are datetime objects
    # Filter merged_df for the relevant period (up to week_end_date)
    relevant_period_df = merged_df[merged_df.index <= week_end_date]

    if relevant_period_df.empty:
        print(f"    [!] No data found for {ticker} on or before {week_end_date.strftime('%Y-%m-%d')} after merging. Skipping CSV save.")
        return None

    # Get the last 200 trading days from the relevant period
    # The data is already sorted chronologically by fetch_daily_time_series
    last_200_days_df = relevant_period_df.tail(200)

    if last_200_days_df.empty:
        print(f"    [!] Not enough data to extract 200 trading days for {ticker} ending by {week_end_date.strftime('%Y-%m-%d')}. Skipping CSV save.")
        return None

    # Update filename to be more descriptive
    week_end_date_iso_str = week_end_date.strftime('%Y-%m-%d')
    filename = f"technicals_{ticker}_{week_end_date_iso_str}.csv"
    filepath = os.path.join(output_dir, filename)
    
    try:
        last_200_days_df.to_csv(filepath, index=True) # Keep date as index
        print(f"    [✓] Technical data saved to {filepath} ({len(last_200_days_df)} rows)")
        return filepath # Return filepath on successful save
    except Exception as e:
        print(f"    [!] Error saving technical data for {ticker} to {filepath}: {e}")
        return None