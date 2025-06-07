# data_fetchers/financial_data.py
import requests
import pandas as pd
import os
import time # For rate limiting
import logging # Import logging

def fetch_alpha_vantage_data(function, symbol, apikey):
    """
    Fetches data from Alpha Vantage API.
    Includes basic retry for rate limiting.
    """
    url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={apikey}'
    try:
        response = requests.get(url, timeout=30) # Added timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX)
        data = response.json()
        if not data:
            logging.warning(f"    No data received for function={function}, symbol={symbol}. Response: {response.text}")
            return None
        if "Note" in data or "Information" in data: # Alpha Vantage rate limit messages
            logging.warning(f"    Rate limit likely hit or API message for function={function}, symbol={symbol}: {data.get('Note') or data.get('Information')}")
            return None 
        if "Error Message" in data:
            logging.error(f"    API Error for function={function}, symbol={symbol}: {data['Error Message']}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"    Request failed for function={function}, symbol={symbol}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        logging.error(f"    Failed to decode JSON for function={function}, symbol={symbol}: {e}")
        return None
    except Exception as e:
        logging.error(f"    An unexpected error occurred in fetch_alpha_vantage_data for {function}, {symbol}: {e}")
        return None

def get_overview_df(data):
    if not data: return pd.DataFrame()
    # Ensure data is a dictionary before trying to make it a list of one dictionary
    if isinstance(data, dict):
        return pd.DataFrame([data])
    return pd.DataFrame()

def get_latest_annual_report_entry(data, key_name='annualReports'):
    """
    Extracts the most recent annual report from API response.
    """
    if not data or key_name not in data or not data[key_name]:
        return pd.DataFrame()
    # Assuming the first entry is the most recent
    return pd.DataFrame([data[key_name][0]])

def get_latest_eps_entry(data, key_name='annualEarnings'):
    """
    Extracts the most recent annual EPS from API response.
    """
    if not data or key_name not in data or not data[key_name]:
        return pd.DataFrame()
    # Assuming the first entry is the most recent
    return pd.DataFrame([data[key_name][0]])

def merge_all_dataframes(dfs_list):
    """
    Merges a list of DataFrames.
    """
    if not dfs_list:
        return pd.DataFrame()
    
    # Filter out empty DataFrames before attempting concat
    valid_dfs = [df for df in dfs_list if not df.empty]
    if not valid_dfs:
        return pd.DataFrame()

    # Reset index for all dataframes to ensure they can be concatenated side-by-side
    # as each DataFrame represents a single entity's snapshot from different API calls.
    processed_dfs = []
    for i, df in enumerate(valid_dfs):
        # Create a unique prefix for columns from this df to avoid collision before merge
        # df_processed = df.add_prefix(f'df{i}_') 
        # Or rely on the duplicate removal later
        processed_dfs.append(df.reset_index(drop=True))

    if not processed_dfs: return pd.DataFrame()

    merged_df = pd.concat(processed_dfs, axis=1)
    
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated(keep='first')]
    return merged_df


def fetch_and_save_financials(ticker, week_start_str, week_end_str, api_key, output_dir):
    """
    Fetches fundamental financial data for a ticker and saves it.
    This function is called weekly, but fetches data that is typically updated quarterly/annually.
    """
    logging.info(f"    Fetching financial fundamentals for {ticker}...")
    overview_data = fetch_alpha_vantage_data('OVERVIEW', ticker, api_key)
    
    income_data = fetch_alpha_vantage_data('INCOME_STATEMENT', ticker, api_key)
    
    balance_sheet_data = fetch_alpha_vantage_data('BALANCE_SHEET', ticker, api_key)
    
    cash_flow_data = fetch_alpha_vantage_data('CASH_FLOW', ticker, api_key)
    
    earnings_data = fetch_alpha_vantage_data('EARNINGS', ticker, api_key)

    data_frames_to_merge = []
    if overview_data: data_frames_to_merge.append(get_overview_df(overview_data))
    if income_data: data_frames_to_merge.append(get_latest_annual_report_entry(income_data, 'annualReports'))
    if balance_sheet_data: data_frames_to_merge.append(get_latest_annual_report_entry(balance_sheet_data, 'annualReports'))
    if cash_flow_data: data_frames_to_merge.append(get_latest_annual_report_entry(cash_flow_data, 'annualReports'))
    if earnings_data: data_frames_to_merge.append(get_latest_eps_entry(earnings_data, 'annualEarnings'))

    if not data_frames_to_merge:
        logging.warning(f"    No fundamental data fetched for {ticker}. Skipping CSV save.")
        return None

    final_df = merge_all_dataframes(data_frames_to_merge)

    if final_df.empty:
        logging.warning(f"    Merged fundamental data is empty for {ticker}. Skipping CSV save.")
        return None

    # File naming convention
    filename = f"financials_{ticker}_{week_start_str}_to_{week_end_str}.csv"
    filepath = os.path.join(output_dir, filename)
    
    try:
        final_df.to_csv(filepath, index=False)
        logging.info(f"    [✓] Financials saved to {filepath}")
        return filepath # Return filepath on successful save
    except Exception as e:
        logging.error(f"    Error saving financials for {ticker} to {filepath}: {e}")
        return None