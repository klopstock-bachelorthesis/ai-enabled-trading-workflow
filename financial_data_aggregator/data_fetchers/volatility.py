# financial_data_aggregator/data_fetchers/volatility_calculator.py
import pandas as pd
import numpy as np
import os
import logging

def get_recent_daily_closing_prices(ohlcv_filepath, num_days=20):
    """
    Reads an OHLCV CSV file and returns a Series of the last 'num_days' closing prices.
    The CSV file is expected to have a date-like index or a 'date'/'timestamp' column,
    and a 'close' column. Data should be sorted chronologically if not indexed by date.
    """
    if not os.path.exists(ohlcv_filepath):
        logging.warning(f"      OHLCV file not found: {ohlcv_filepath}")
        return None
    try:
        # technical_data.py saves with index=True, so 'date' will be the index.
        df = pd.read_csv(ohlcv_filepath, index_col='date', parse_dates=True)
        if df.empty:
            logging.warning(f"      OHLCV file is empty: {ohlcv_filepath}")
            return None
        
        df = df.sort_index() # Ensure chronological order

        if 'close' not in df.columns:
            logging.warning(f"      'close' column missing in {ohlcv_filepath}")
            return None
        
        # Get the last num_days of closing prices
        recent_closes = df['close'].tail(num_days)
        if len(recent_closes) < 2: # Need at least 2 prices for 1 return
            logging.info(f"      Fewer than 2 daily closing prices ({len(recent_closes)}) available in {ohlcv_filepath} for the last {num_days} days. Volatility will be NaN.")
        return recent_closes
    except Exception as e:
        logging.error(f"      Error reading or processing {ohlcv_filepath}: {e}")
        return None

def calculate_and_save_weekly_volatility(
    tickers_list, 
    base_output_dir, 
    current_week_number, 
    dropbox_uploader,
    dropbox_root_folder,
    dropbox_access_token): # Removed window_size parameter
    """
    Calculates weekly volatility based on the standard deviation of the last 20
    daily returns from the Ticker_TA_Week.csv file for the current_week_number.
    The daily volatility is then annualized to weekly by multiplying by sqrt(5).
    """
    volatility_data = []
    num_daily_prices_for_calc = 20

    for ticker in tickers_list:
        logging.debug(f"      Calculating weekly volatility for {ticker}, week {current_week_number} using last {num_daily_prices_for_calc} daily prices.")
        
        # Use the _TA_ file for the current week
        ohlcv_filename = f"{ticker}_TA_{current_week_number}.csv"
        ohlcv_filepath = os.path.join(base_output_dir, ohlcv_filename)

        daily_closing_prices = get_recent_daily_closing_prices(ohlcv_filepath, num_days=num_daily_prices_for_calc)
        current_ticker_volatility = np.nan # Default to NaN

        if daily_closing_prices is not None and len(daily_closing_prices) >= 2: # Need at least 2 prices for 1 return
            # Calculate daily returns: (P_t - P_{t-1}) / P_{t-1}
            daily_returns = daily_closing_prices.pct_change().dropna() # dropna removes the first NaN from pct_change

            # Need at least 1 return for np.std (though ddof=1 will make it NaN if only 1 return)
            # More robustly, need at least 2 returns for a non-NaN std with ddof=1
            if not daily_returns.empty and len(daily_returns) >= 1: 
                with np.errstate(divide='ignore', invalid='ignore'): # Suppress warnings for std of 0 or 1 return
                    daily_std_dev = np.std(daily_returns, ddof=1) # Sample standard deviation
                
                if not np.isnan(daily_std_dev):
                    weekly_volatility_value = daily_std_dev * np.sqrt(5) # Annualize to weekly
                    current_ticker_volatility = weekly_volatility_value
                    logging.debug(f"      Daily std dev for {ticker} (week {current_week_number}, {len(daily_returns)} returns): {daily_std_dev:.4f}. Weekly Vol: {current_ticker_volatility:.4f}")
                else:
                    logging.info(f"      Daily std dev for {ticker} (week {current_week_number}, {len(daily_returns)} returns) is NaN. Weekly volatility will be NaN.")
            else:
                logging.info(f"      Not enough daily returns ({len(daily_returns)}) from {len(daily_closing_prices)} prices to calculate std dev for {ticker} (week {current_week_number}). Volatility set to NaN.")
        else:
            if daily_closing_prices is None:
                logging.warning(f"      Could not retrieve daily closing prices for {ticker} from {ohlcv_filename}. Volatility set to NaN.")
            else: # Not enough prices (len < 2)
                 logging.info(f"      Not enough daily closing prices ({len(daily_closing_prices)}) from {ohlcv_filename} for {ticker} to calculate returns. Volatility set to NaN.")
            
        volatility_data.append({'Ticker': ticker, 'WeeklyVolatility': current_ticker_volatility})

    if not volatility_data:
        logging.info(f"    No volatility data to save for week {current_week_number}.")
        return None

    volatility_df = pd.DataFrame(volatility_data)
    if not tickers_list: # Should not happen if loop runs, but as a safeguard
        logging.warning(f"    Tickers list is empty for week {current_week_number}. Cannot sort volatility data.")
    else:
        # Ensure the order of tickers in the output CSV is as specified in tickers_list
        volatility_df['Ticker'] = pd.Categorical(volatility_df['Ticker'], categories=tickers_list, ordered=True)
        volatility_df = volatility_df.sort_values('Ticker').reset_index(drop=True)

    filename = f"VOLATILITY_{current_week_number}.csv"
    filepath = os.path.join(base_output_dir, filename)

    try:
        volatility_df.to_csv(filepath, index=False)
        logging.info(f"    [✓] Weekly volatility data saved to {filepath}")
        if dropbox_uploader and dropbox_access_token and dropbox_access_token != 'YOUR_DROPBOX_ACCESS_TOKEN_HERE':
            try:
                dropbox_file_path = f"{dropbox_root_folder}/{filename}"
                dropbox_uploader(filepath, dropbox_file_path, dropbox_access_token)
            except Exception as dbx_e:
                logging.error(f"    [!] Failed to upload {filepath} to Dropbox: {dbx_e}", exc_info=True)
        elif dropbox_uploader and (not dropbox_access_token or dropbox_access_token == 'YOUR_DROPBOX_ACCESS_TOKEN_HERE'):
             logging.warning(f"    [!] Dropbox token not configured or is a placeholder. Skipping upload for {filepath}.")
        return filepath
    except Exception as e:
        logging.error(f"    Error saving weekly volatility data to {filepath}: {e}")
        return None