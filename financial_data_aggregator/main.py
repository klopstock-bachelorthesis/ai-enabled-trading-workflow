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
from financial_data_aggregator.data_fetchers import technical_data, fundamental_data, news_scraper
from financial_data_aggregator import get_weeks
ALPHA_VANTAGE_API_KEY = '3R7AQT3NC3WGCN74'
DROPBOX_ACCESS_TOKEN = 'sl.u.AFy0JFLWFbexDFDpxOt6SZIMep0FEEWwKOL9lFI17Qd6H-EJLB_ooGLCg7xxzW-ukxfxdH1UXkot_0QxP8hOD7-J-cticU3ySZo0abfQgOWGlZosXTq3L8e9S4qRR4Va-S9goXd8XY5uqgEyKKT7N3w2LVicj-wKQ84Tp8IfqWG-JjQoGEh3P7eNPat2g1fplVY4heyxiYvjs29yTOp1RoffH93Yf4mV2rFznnbezej5JgavjgqQcCj_56W7IVhOZYPizmPYAWnWSLZYpyI4p1AJEVAjrX5ETV8SBF6soM0Ron5khoRCU_6KDNNYmuiBGluykEUy0Yl-pBt1Xj8Q6_kFkvWVUASfw0O3ei_2CtAnHVH-PSRdDQtMYbCIXFFpqONEtg_hejAxCN8baxErz2BXXjgo6hHCKRow1nVXG6UE_fMxAVGEJdTyiaRiF2bMdMhAzMkHz6cEWITY2dfiLlLAg-_nW993Vv8Um3OX2SKGvozVlLQVU_hXaAL-5sSEmpfdTYW7iIDsAx05r24vzKWxZsqDMFRzPo44ZQNIQl8B10Ib30rprftah4hrByl96dEDQU8o03A0r0nNjlQ4mItDkWeQNaZmp7-hlotEYexEHzLsFYZvXEeg7yNkTJ8EszEBwGhJnMokGQLYyriaClAclQpPwIclNHhSaMYqN0PYgjo-L2BxVYWp2uIQzOPXfdUdHuAK4YpJnCHsjVYGMPC9wEz-ybR6SuRqB80T_zo3dNnhEq_frHMca4KSLPG11jgWdPQMNwYRSh8rqr05zwpnlFEqKO-r3p4wMUIhBamSh5sE8zB0x7EdCfkYAIjwTJVKIJi2Nv9qU41kHZGCi2g2qT2cZSJVLs8ObuxC9jxZsPIjS03zErW40Xvsvfszy7DtPrKm5j3hxgjVf9H1DBFjxf9lrX23tqfB6UDWVaJUDy02uScW2mQU6dyILXun4zcKtRzZGf0AGzVovpZo-7cMz3QcM50Z4txGKer4ZC0Caax6ftj68KQvK3mGTPBUX99Hm1s9PcZpM1AQU-4BH349U11yrufpnTh1i6oD_78k-C24EMKF_Ao9v_QdAGeR6Bc1gfYx_THJQ003Bdk5ZKXsNCCq_5xrT-VWIFA3BiHPsByfhF9CR7dGpGBKnuoDoUCqiDu79Xqa_ZXQjLFUfKwm-WAJqiGxJes8XaYKCnwaOX3W4T8DXjGYAnR3x8a0aEtbMN1MBEXBu8317Vw1FcGR-wpwAQu-kA27MG6b1L1n5QDphNSql2ylXuYwbBZ9j0WxvwMhX75oXh0AzuM5xGl2'
TICKERS = ['NVDA', 'MSFT', 'AAPL']
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
    
    logging.info(f"Using Alpha Vantage API Key: {'*' * (len(ALPHA_VANTAGE_API_KEY) - 4) + ALPHA_VANTAGE_API_KEY[-4:] if ALPHA_VANTAGE_API_KEY else 'Not Set'}")
    logging.info(f"Using Dropbox Token: {'*' * (len(DROPBOX_ACCESS_TOKEN) - 4) + DROPBOX_ACCESS_TOKEN[-4:] if DROPBOX_ACCESS_TOKEN and DROPBOX_ACCESS_TOKEN != 'YOUR_DROPBOX_ACCESS_TOKEN_HERE' else 'Not Set or Placeholder'}")
    
    for week_start_dt, week_end_dt in get_weeks(start_date_str, end_date_str):
        week_start_iso = week_start_dt.strftime('%Y-%m-%d')
        week_end_iso = week_end_dt.strftime('%Y-%m-%d')
        
        for ticker in tickers_list:
            logging.info(f"  Processing ticker: {ticker} for week {week_start_iso} to {week_end_iso}")

            # 1. Fetch and save Price Data (OHLCV, Mon-Fri for the week)
            try:
                local_filepath_price = technical_data.fetch_and_save_ohlcv(
                    ticker=ticker,
                    week_start_date=week_start_dt, 
                    week_end_date=week_end_dt, # Used to help name file correctly
                    api_key = ALPHA_VANTAGE_API_KEY,
                    output_dir=base_output_dir
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
                    week_start_str=week_start_iso, # Pass strings for filename consistency
                    week_end_str=week_end_iso,
                    api_key=ALPHA_VANTAGE_API_KEY,
                    output_dir=base_output_dir 
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
                    output_dir=base_output_dir 
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