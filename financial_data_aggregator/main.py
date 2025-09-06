import argparse
import os
import sys
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import dropbox
load_dotenv()

# Configure logging as early as possible
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("data_aggregation.log"), logging.StreamHandler()])

# --- BEGIN SCRIPT EXECUTION CONTEXT SETUP ---
# This block allows the script to be run directly (e.g., python financial_data_aggregator/main.py)
# or as a module (e.g., python -m financial_data_aggregator.main).
if __name__ == "__main__" and __package__ is None:
    # Get the directory of the current script (e.g., .../financial_data_aggregator)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # Get the parent directory (e.g., .../bachelorarbeit) which contains the package
    project_root = os.path.dirname(current_script_dir)
    # Add the project root to sys.path so Python can find 'financial_data_aggregator'
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
# --- END SCRIPT EXECUTION CONTEXT SETUP ---

# Import your data fetching modules
from financial_data_aggregator.data_fetchers import technical_data, fundamental_data, news_scraper, volatility
from financial_data_aggregator import get_weeks
ALPHA_VANTAGE_API_KEY = '3R7AQT3NC3WGCN74'
DROPBOX_ACCESS_TOKEN = 'sl.u.AFzm694D5k22ucCVJhILKS9kELrCL5i4Q5DDIp9qAzOX5jk8aR6EKojv0LC416oBzM2Qu73rlBQ2mtri_L2jAhGkbc26ZPscTB8y3eBSUULy1rYFx_4J02pBk1i4KwpLCx6kDt8MucDivF5LF3a9hSKsYc1AJMJMqsgNEJW4BP5di9KS033iQYGQLmBp8n8tRCY8JCWHSKJmWtO0r16QwKh_ayGTXQYseZAyNGNGt7O2XNBehYEMKmDt8Ys61F7FPni-AHu_VBnJXqcXMn8b3EDQLWcEViiODCjmy3Xgi73zwVq0NKZEg1-FRY1Bj-G3ThJ-aogXbksdDEfSApC2PYM20eVWNNqiC2C6obfW2HNN80eIdpqXr_t6GHbthP0Du-Tb80mYRCEQ1c84YY7I5aGOqlrC8MbhwOc_8nY698cVHCpS9lG_N24WGjic03EOeqcxOjzWB5lFBhWeR00uHnAthVZ2LvPKgKiESopIIbM4wjBTDXUgR_LNyCADhKeV52owCFnCBPWLyfCoMGfL_lslIiOFg-txyQg16PTUGF7cbFrMdhA_snoZexO2t2dgGCe0UPjQtrHlBGUdpKLic-MLZBXoTZ4KW-YF24YaWA86hjUMavXqQW6jtTPesf1JDyI2MS5pOSIU4aQiB-XtiDmlJOYnd1jl2uZ66AD9nkmUS62DO2Mkh1k2f9g508cwu_2fSHdQbFedunwZ8ONPPHzUsGI2vULr1n4Fz24HikXAvx1-AfjnnM2Gx9hvwT1agVupSZuE2mWz7zWyK6Zip1SQGMwxCLSxPvnr9iBCBJHhQ_0MeWhUVD2ldfHxg_Sd_baIO6yOyHiEgHPeeJD0tCCR4Fkrhl6_OTGjy2QOtDx4u2K5CIZWRK6NubrLZXw7teVxQQpXzm1g-bv27613cQk3WD-y8u4VOco9z2PznyTExl_nXIqPqI36Ha9bas4-i0Uleco9K2UTM8Gww9vqoGMnMVu-UsF5wYi3NfLYJgifJrTHUZVd9RVr1uSNfzngllCLWmvVjGL3iPmfNfWXP6yGUZ8gmLQhdpVeQBQVRjA4JUFKqOJwZvPO61ijNY3ZHX00r-6LBreRM5kD_QYLp9SSEpuuoYp3qbrf63HARWxFHdtsvPCcfi0g7ihi_PeiXjn8oxuvCiNsbeMCMpjtoBiYac3-kuWeilMUyAKUtpB7J_sdeVBIT3cILvV4pzrsA3Hb5Y5RtRF6xZuyb3wdGFwU0IW8r4FPhl_Z3y7X0OuAnQx4fbyEndARwo6ut9wfe7DBqQwEW4ELe5nrBDz5F34-'
TICKERS = ['NVDA', 'MSFT', 'AAPL']
def upload_to_dropbox(local_file_path, dropbox_path, access_token):
    """Upload file to Dropbox (existing function)"""
    dbx = dropbox.Dropbox(access_token, timeout=60) # Added timeout for potentially large files
    with open(local_file_path, 'rb') as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
    logging.info(f"  [✓] Uploaded {local_file_path}")

def upload_to_dropbox(local_file_path, dropbox_path, access_token):
    """Upload file to Dropbox (existing function)"""
    dbx = dropbox.Dropbox(access_token, timeout=60) # Added timeout for potentially large files
    with open(local_file_path, 'rb') as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
    logging.info(f"  [✓] Uploaded {local_file_path} to Dropbox at {dropbox_path}")

def main(start_date_str, end_date_str, tickers_list):
    dropbox_root_folder = "/financial_data_aggregator_output" # Root folder in Dropbox

    base_output_dir = "output"
    os.makedirs(base_output_dir, exist_ok=True)
    
    week_counter = 0
    for week_start_dt, week_end_dt in get_weeks(start_date_str, end_date_str):
        week_counter += 1
        week_start_iso = week_start_dt.strftime('%Y-%m-%d')
        week_end_iso = week_end_dt.strftime('%Y-%m-%d')
        
        for ticker in tickers_list:
            logging.info(f"  Processing ticker: {ticker} for week {week_counter} (Original date range: {week_start_iso} to {week_end_iso})")

            # 1. Fetch and save Price Data (OHLCV, Mon-Fri for the week)
            try:
                local_filepath_price = technical_data.fetch_and_save_ohlcv(
                    ticker=ticker,
                    week_start_date=week_start_dt, 
                    week_end_date=week_end_dt, # Used for data filtering
                    api_key = ALPHA_VANTAGE_API_KEY,
                    output_dir=base_output_dir,
                    week_number=week_counter
                )
                if local_filepath_price and DROPBOX_ACCESS_TOKEN and DROPBOX_ACCESS_TOKEN != 'YOUR_DROPBOX_ACCESS_TOKEN_HERE':
                    try:
                        dropbox_file_path_price = f"{dropbox_root_folder}/{os.path.basename(local_filepath_price)}"
                        upload_to_dropbox(local_filepath_price, dropbox_file_path_price, DROPBOX_ACCESS_TOKEN)
                    except Exception as dbx_e:
                        logging.error(f"  [!] Failed to upload {local_filepath_price} to Dropbox: {dbx_e}", exc_info=True)
                elif local_filepath_price and (not DROPBOX_ACCESS_TOKEN or DROPBOX_ACCESS_TOKEN == 'YOUR_DROPBOX_ACCESS_TOKEN_HERE'):
                    logging.warning(f"  [!] Dropbox token not configured or is a placeholder. Skipping upload for {local_filepath_price}.")

            except Exception as e:
                logging.error(f"  [!] Failed to process price data for {ticker}: {e}", exc_info=True)

            # 2. Fetch and save Basic Financial Data (Snapshot for the week)
            try:
                local_filepath_financials = fundamental_data.fetch_and_save_financials(
                    ticker=ticker,
                    api_key=ALPHA_VANTAGE_API_KEY,
                    output_dir=base_output_dir,
                    week_number=week_counter
                )
                if local_filepath_financials and DROPBOX_ACCESS_TOKEN and DROPBOX_ACCESS_TOKEN != 'YOUR_DROPBOX_ACCESS_TOKEN_HERE':
                    try:
                        dropbox_file_path_financials = f"{dropbox_root_folder}/{os.path.basename(local_filepath_financials)}"
                        upload_to_dropbox(local_filepath_financials, dropbox_file_path_financials, DROPBOX_ACCESS_TOKEN)
                    except Exception as dbx_e:
                        logging.error(f"  [!] Failed to upload {local_filepath_financials} to Dropbox: {dbx_e}", exc_info=True)
                elif local_filepath_financials and (not DROPBOX_ACCESS_TOKEN or DROPBOX_ACCESS_TOKEN == 'YOUR_DROPBOX_ACCESS_TOKEN_HERE'):
                    logging.warning(f"  [!] Dropbox token not configured or is a placeholder. Skipping upload for {local_filepath_financials}.")

            except Exception as e:
                logging.error(f"  [!] Failed to process financial fundamentals for {ticker}: {e}", exc_info=True)
            
            # 3. Scrape and save News Articles (Mon-Sun for the week)
            try:
                local_filepath_news = news_scraper.fetch_and_save_news(
                    ticker_symbol=ticker,
                    week_start_date=week_start_dt,
                    week_end_date=week_end_dt,
                    api_key=ALPHA_VANTAGE_API_KEY, # Pass API key for Alpha Vantage news
                    output_dir=base_output_dir,
                    week_number=week_counter
                )
                if local_filepath_news and DROPBOX_ACCESS_TOKEN and DROPBOX_ACCESS_TOKEN != 'YOUR_DROPBOX_ACCESS_TOKEN_HERE':
                    try:
                        # Construct Dropbox path: /dropbox_root_folder/filename.csv
                        dropbox_file_path_news = f"{dropbox_root_folder}/{os.path.basename(local_filepath_news)}" # Keep this comment
                        upload_to_dropbox(local_filepath_news, dropbox_file_path_news, DROPBOX_ACCESS_TOKEN)
                    except Exception as dbx_e:
                        logging.error(f"  [!] Failed to upload {local_filepath_news} to Dropbox: {dbx_e}", exc_info=True)
                elif local_filepath_news and (not DROPBOX_ACCESS_TOKEN or DROPBOX_ACCESS_TOKEN == 'YOUR_DROPBOX_ACCESS_TOKEN_HERE'):
                    logging.warning(f"  [!] Dropbox token not configured or is a placeholder. Skipping upload for {local_filepath_news}.")

            except Exception as e:
                logging.error(f"  [!] Failed to process news for {ticker}: {e}", exc_info=True)
                
        # After processing all tickers for the week, calculate weekly volatility
        # 4. Calculate and save Weekly Volatility
        # This function calculates volatility for all tickers for the current_week_number
        # and handles its own Dropbox upload if configured.
        if tickers_list: # Only calculate if there are tickers
            logging.info(f"  Processing weekly volatility for all tickers for week {week_counter}")
            try:
                local_filepath_volatility = volatility.calculate_and_save_weekly_volatility(
                    tickers_list=tickers_list,
                    base_output_dir=base_output_dir,
                    current_week_number=week_counter,
                    dropbox_uploader=upload_to_dropbox, # Pass the function itself
                    dropbox_root_folder=dropbox_root_folder,
                    dropbox_access_token=DROPBOX_ACCESS_TOKEN
                    # window_size can be left as default (4) or specified here
                )
                if local_filepath_volatility:
                    logging.info(f"  Volatility calculation for week {week_counter} completed. File: {local_filepath_volatility}")
                # else: # The volatility function itself logs success/failure of file creation
                #    logging.warning(f"  Volatility calculation for week {week_counter} did not produce a file or failed to save.")
            except Exception as e:
                logging.error(f"  [!] Failed to process weekly volatility for week {week_counter}: {e}", exc_info=True)

    logging.info("\nData aggregation process complete.")

def prompt_for_date(prompt_message: str, default_date_str: str) -> str:
    """
    Prompts the user for a date and validates its format.
    Uses default_date_str if user input is empty.
    """
    while True:
        user_input = input(f"{prompt_message} (YYYY-MM-DD) [default: {default_date_str}]: ").strip()
        if not user_input:
            return default_date_str
        try:
            datetime.strptime(user_input, '%Y-%m-%d')
            return user_input
        except ValueError:
            logging.error("Invalid date format. Please use YYYY-MM-DD or press Enter for default.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Aggregates financial news, price, and fundamental data for specified stock tickers over a date range.")
    parser.add_argument("--tickers", nargs='+', default=TICKERS, 
                        help=f"A list of stock ticker symbols to process (e.g., AAPL MSFT). Default: {' '.join(TICKERS)}")
    args = parser.parse_args()

    # Always prompt for start and end dates
    default_start_dt = datetime.now() - timedelta(days=30)
    default_start_str = default_start_dt.strftime('%Y-%m-%d')
    start_date_to_use = prompt_for_date("Enter start date", default_start_str)

    default_end_dt = datetime.now()
    default_end_str = default_end_dt.strftime('%Y-%m-%d')
    end_date_to_use = prompt_for_date("Enter end date", default_end_str)

    # Validate final dates
    try:
        start_dt_obj = datetime.strptime(start_date_to_use, '%Y-%m-%d')
        end_dt_obj = datetime.strptime(end_date_to_use, '%Y-%m-%d')
        if start_dt_obj > end_dt_obj:
            logging.error(f"Start date ({start_date_to_use}) cannot be after end date ({end_date_to_use}).")
            exit(1)
    except ValueError:
        logging.error("A date was provided in an invalid format. Please use YYYY-MM-DD.")
        exit(1)

    main(start_date_to_use, end_date_to_use, args.tickers)