import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import os

# Global style settings
plt.style.use("default")
plt.rcParams["font.family"] = "Times New Roman"
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["axes.grid"] = True
plt.rcParams["grid.linestyle"] = "--"
plt.rcParams["grid.alpha"] = 0.5
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["axes.labelsize"] = 14
plt.rcParams["legend.fontsize"] = 12
plt.rcParams["xtick.labelsize"] = 10
plt.rcParams["ytick.labelsize"] = 10

TICKERS = ['NVDA', 'MSFT', 'AAPL']

# === 1. CSV-Dateien laden ===
def load_csv(path, **kwargs):
    """Helper function to load CSVs with user path expansion."""
    return pd.read_csv(os.path.expanduser(path), **kwargs)

portfolio = load_csv("~/Downloads/bachelorarbeit/results.csv")
prices = load_csv("~/Downloads/bachelorarbeit/NVDA_MSFT_AAPL_prices.csv", parse_dates=["date"])
benchmark = load_csv("~/Downloads/bachelorarbeit/benchmark_prices_NASDAQ100.csv", parse_dates=["date"])
macd_daily_portfolio = load_csv("~/Downloads/bachelorarbeit/macd_long_short_strategy_weights.csv")


# === 2. Portfolio-Wochen verarbeiten ===
portfolio["Week"] = portfolio["Week"].astype(int)
portfolio["Week"] = pd.to_datetime("2024-01-01") + pd.to_timedelta((portfolio["Week"] - 1) * 7, unit="D")
portfolio["Effective_Week"] = portfolio["Week"] + pd.to_timedelta(7, unit="D")

# === 3. Wöchentliche Assetpreise ===
prices["week"] = prices["date"] - pd.to_timedelta(prices["date"].dt.weekday, unit="D")
weekly_prices = prices.groupby("week").first().reset_index()

# === 4. Portfolio + Preise mergen ===
merged = pd.merge(portfolio, weekly_prices, how="inner", left_on="Effective_Week", right_on="week")

# === 5. Portfolio-Rendite berechnen ===
merged["Portfolio Return"] = (
    merged["NVDA_x"] * merged["NVDA_y"].pct_change()
    + merged["MSFT_x"] * merged["MSFT_y"].pct_change()
    + merged["AAPL_x"] * merged["AAPL_y"].pct_change()
)
merged["Cumulative Return"] = (1 + merged["Portfolio Return"].fillna(0)).cumprod() - 1

# === 6. Benchmark vorbereiten ===
benchmark["week"] = benchmark["date"] - pd.to_timedelta(benchmark["date"].dt.weekday, unit="D")
weekly_benchmark = benchmark.groupby("week").first().reset_index()

# Passe den Spaltennamen ggf. an (z. B. 'Close', 'SP500', 'Benchmark', ...)
benchmark_column = "benchmark_close"
weekly_benchmark["Benchmark Return"] = weekly_benchmark[benchmark_column].pct_change()
weekly_benchmark["Cumulative Benchmark Return"] = (1 + weekly_benchmark["Benchmark Return"].fillna(0)).cumprod() - 1

# === NEW: Benchmark Strategies ===
asset_returns = weekly_prices[TICKERS].pct_change()

# --- 1. Buy & Hold (100% NVDA) ---
weekly_prices['B&H NVDA Return'] = asset_returns['NVDA']
weekly_prices['Cumulative B&H NVDA Return'] = (1 + weekly_prices['B&H NVDA Return'].fillna(0)).cumprod() - 1

# --- 2. Buy & Hold (Equal Weight) ---
weekly_prices['B&H Equal-Weight Return'] = asset_returns[TICKERS].mean(axis=1)
weekly_prices['Cumulative B&H Equal-Weight Return'] = (1 + weekly_prices['B&H Equal-Weight Return'].fillna(0)).cumprod() - 1

# === NEW: MACD Daily Strategy (from CSV) ===
# Convert the integer 'Week' from the CSV into a datetime object to align with price data
macd_daily_portfolio["week"] = pd.to_datetime("2024-01-01") + pd.to_timedelta((macd_daily_portfolio["Week"] - 1) * 7, unit="D")

# Merge the strategy weights with the weekly price data's timeline
temp_df = pd.merge(weekly_prices[['week']], macd_daily_portfolio, on='week', how='left')
temp_df.ffill(inplace=True) # Forward-fill weights for any gaps

# Set the week as the index and shift weights by 1 to prevent lookahead bias
macd_daily_weights = temp_df[TICKERS]
macd_daily_weights = macd_daily_weights.shift(1).fillna(0)

weekly_prices['MACD Daily Return'] = (asset_returns * macd_daily_weights).sum(axis=1)
weekly_prices['Cumulative MACD Daily Return'] = (1 + weekly_prices['MACD Daily Return'].fillna(0)).cumprod() - 1

# === 7. Nur Wochen mit Portfolio-Daten verwenden ===
filtered_benchmark = weekly_benchmark[
    weekly_benchmark["week"].isin(merged["Effective_Week"])
]

# === 8. Finales Merge für Plot ===
merged_compare = pd.merge(
    merged[["Effective_Week", "Cumulative Return", "Portfolio Return"]],
    filtered_benchmark[["week", "Cumulative Benchmark Return", "Benchmark Return"]],
    left_on="Effective_Week",
    right_on="week",
    how="inner"
)

# Add benchmark strategy data to the comparison
merged_compare = pd.merge(
    merged_compare,
    weekly_prices[[
        "week",
        "Cumulative B&H NVDA Return", "B&H NVDA Return",
        "Cumulative B&H Equal-Weight Return", "B&H Equal-Weight Return",
        "Cumulative MACD Daily Return", "MACD Daily Return"
    ]],
    on="week",
    how="inner"
)

red = (1.0, 0.4, 0.4)
blue = (0.4, 0.6, 1.0)
green = (0.4, 0.8, 0.4)
purple = (0.6, 0.4, 0.8)
orange = (1.0, 0.6, 0.2) # New color for Equal-Weight B&H

# === NEW: ROLLING VOLATILITY PLOT ===
window = 4 # 4 weeks ~ 1 month
plt.figure()

vol_df = merged_compare.set_index('Effective_Week')

vol_df['Portfolio Return'].rolling(window).std().plot(label='Portfolio Volatility', color=red, linewidth=1.5)
vol_df['Benchmark Return'].rolling(window).std().plot(label='NASDAQ 100 Volatility', color=blue, linewidth=1.5, linestyle='--')
vol_df['B&H Equal-Weight Return'].rolling(window).std().plot(label='B&H Equal-Weight Volatility', color=orange, linewidth=1.5, linestyle=':')
vol_df['B&H NVDA Return'].rolling(window).std().plot(label='B&H NVDA Volatility', color=green, linewidth=1.5, linestyle=':')
vol_df['MACD Daily Return'].rolling(window).std().plot(label='MACD Daily Volatility', color=purple, linewidth=1.5, linestyle='-.')

plt.title(f'{window}-Week Rolling Weekly Volatility')
plt.ylabel("Weekly Volatility")
plt.xlabel("")
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
plt.legend(loc="upper left", frameon=False)
plt.tight_layout()
plt.savefig("rolling_volatility.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === 1. CUMULATIVE RETURN COMPARISON ===
plt.figure()
plt.plot(merged_compare["Effective_Week"], merged_compare["Cumulative Return"],
         label="Portfolio", linewidth=1.5, marker='o', markersize=2, color=red)
plt.plot(merged_compare["Effective_Week"], merged_compare["Cumulative Benchmark Return"],
         label="NASDAQ 100", linewidth=1.5, marker='s', markersize=2, color=blue)
plt.plot(merged_compare["Effective_Week"], merged_compare["Cumulative B&H Equal-Weight Return"],
         label="B&H (Equal Weight)", linewidth=1.5, marker='d', markersize=2, color=orange)
plt.plot(merged_compare["Effective_Week"], merged_compare["Cumulative B&H NVDA Return"],
         label="B&H (100% NVDA)", linewidth=1.5, marker='^', markersize=2, color=green)
plt.plot(merged_compare["Effective_Week"], merged_compare["Cumulative MACD Daily Return"],
         label="MACD Strategy", linewidth=1.5, marker='p', markersize=2, color=purple)
plt.xlabel("Date")
plt.ylabel("Cumulative Return")
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
plt.legend(loc="upper left", frameon=False)
plt.tight_layout()
plt.savefig("cumulative_returns.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === 2. DRAWDOWN CHART ===

def calculate_drawdown_pct(cumulative_percent_return_series):
    """
    Calculates the percentage drawdown for a cumulative return series 
    that starts at 0.
    """
    # Find the peak of the cumulative percentage returns
    high_water_mark = cumulative_percent_return_series.cummax()
    
    # Calculate the drawdown percentage. 
    # The (1 + high_water_mark) correctly converts the peak percentage 
    # back to a portfolio value factor (e.g., 200% -> 3.0) for the calculation.
    drawdown_pct = (cumulative_percent_return_series - high_water_mark) / (1 + high_water_mark)
    
    return drawdown_pct

# --- Apply the corrected function to all your columns ---
merged_compare["Portfolio Drawdown"] = calculate_drawdown_pct(merged_compare["Cumulative Return"])
merged_compare["NASDAQ100 Drawdown"] = calculate_drawdown_pct(merged_compare["Cumulative Benchmark Return"])
merged_compare["B&H Equal-Weight Drawdown"] = calculate_drawdown_pct(merged_compare["Cumulative B&H Equal-Weight Return"])
merged_compare["B&H NVDA Drawdown"] = calculate_drawdown_pct(merged_compare["Cumulative B&H NVDA Return"])
merged_compare["MACD Daily Drawdown"] = calculate_drawdown_pct(merged_compare["Cumulative MACD Daily Return"])

# You no longer need the intermediate 'Max' columns, the function handles it.

plt.figure(figsize=(12, 6))

# Portfolio drawdown
plt.fill_between(merged_compare["Effective_Week"], 
                 merged_compare["Portfolio Drawdown"], 
                 color='red', alpha=0.3, label='Portfolio Drawdown')

# NASDAQ 100 drawdown
plt.fill_between(merged_compare["Effective_Week"], 
                 merged_compare["NASDAQ100 Drawdown"], 
                 color='blue', alpha=0.2, label='NASDAQ 100 Drawdown')
# B&H NVDA drawdown
plt.fill_between(merged_compare["Effective_Week"],
                 merged_compare["B&H NVDA Drawdown"],
                 color='green', alpha=0.2, label='B&H (100% NVDA) Drawdown')
# B&H Equal-Weight drawdown
plt.fill_between(merged_compare["Effective_Week"],
                 merged_compare["B&H Equal-Weight Drawdown"],
                 color=orange, alpha=0.2, label='B&H (Equal Weight) Drawdown')
# MACD Daily drawdown
plt.fill_between(merged_compare["Effective_Week"],
                 merged_compare["MACD Daily Drawdown"],
                 color='purple', alpha=0.2, label='MACD Daily Drawdown')


plt.title("Drawdown Comparison")
plt.ylabel("Drawdown")
plt.legend(loc="lower left")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("drawdown_comparison.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)


# === 3. ROLLING SHARPE RATIO ===
window = 4  # 4 weeks ~ monthly
returns = merged["Portfolio Return"].fillna(0)
rolling_sharpe = returns.rolling(window).mean() / returns.rolling(window).std()

plt.figure()
plt.plot(merged["Effective_Week"], rolling_sharpe, color='purple', linewidth=1.5)
plt.title("4-Week Rolling Sharpe Ratio (Portfolio)")
plt.axhline(0, linestyle='--', color='gray', linewidth=0.8)
plt.xlabel("Date")
plt.ylabel("Sharpe Ratio")
plt.tight_layout()
plt.savefig("rolling_sharpe.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === 4. TOTAL RETURN BAR CHART & PERFORMANCE METRICS TABLE ===
num_weeks = len(merged_compare)
risk_free_rate = 0.0 # Assuming 0 for simplicity

metrics_data = {
    'Portfolio': {
        'Cumulative Return': merged_compare["Cumulative Return"].iloc[-1],
        'Annualized Volatility': merged_compare['Portfolio Return'].std() * np.sqrt(52),
        'Max Drawdown': merged_compare['Portfolio Drawdown'].min(),
    },
    'NASDAQ 100': {
        'Cumulative Return': merged_compare["Cumulative Benchmark Return"].iloc[-1],
        'Annualized Volatility': merged_compare['Benchmark Return'].std() * np.sqrt(52),
        'Max Drawdown': merged_compare['NASDAQ100 Drawdown'].min(),
    },
     'B&H (Equal Weight)': {
        'Cumulative Return': merged_compare["Cumulative B&H Equal-Weight Return"].iloc[-1],
        'Annualized Volatility': merged_compare['B&H Equal-Weight Return'].std() * np.sqrt(52),
        'Max Drawdown': merged_compare['B&H Equal-Weight Drawdown'].min(),
    },
    'B&H (100% NVDA)': {
        'Cumulative Return': merged_compare["Cumulative B&H NVDA Return"].iloc[-1],
        'Annualized Volatility': merged_compare['B&H NVDA Return'].std() * np.sqrt(52),
        'Max Drawdown': merged_compare['B&H NVDA Drawdown'].min(),
    },
    'MACD Daily': {
        'Cumulative Return': merged_compare["Cumulative MACD Daily Return"].iloc[-1],
        'Annualized Volatility': merged_compare['MACD Daily Return'].std() * np.sqrt(52),
        'Max Drawdown': merged_compare['MACD Daily Drawdown'].min(),
    }
}

# Map strategy names to their return columns for easier access
return_cols = {
    'Portfolio': 'Portfolio Return',
    'NASDAQ 100': 'Benchmark Return',
    'B&H (Equal Weight)': 'B&H Equal-Weight Return',
    'B&H (100% NVDA)': 'B&H NVDA Return',
    'MACD Daily': 'MACD Daily Return',
}
benchmark_returns = merged_compare[return_cols['NASDAQ 100']].fillna(0)

# Calculate Annualized Return, Sharpe, Alpha, and Beta
for strat, data in metrics_data.items():
    # Annualized Return
    data['Annualized Return'] = (1 + data['Cumulative Return'])**(52/num_weeks) - 1 if num_weeks > 0 else 0
    # Sharpe Ratio
    data['Sharpe Ratio'] = (data['Annualized Return'] - risk_free_rate) / data['Annualized Volatility'] if data['Annualized Volatility'] > 0 else np.nan
    
    # Alpha and Beta calculation
    if strat == 'NASDAQ 100':
        data['Alpha'] = 0.0
        data['Beta'] = 1.0
    else:
        strategy_returns = merged_compare[return_cols[strat]].fillna(0)
        if len(strategy_returns) > 1 and len(benchmark_returns) > 1:
            # Regression: strategy_returns = beta * benchmark_returns + weekly_alpha
            beta, weekly_alpha = np.polyfit(benchmark_returns, strategy_returns, deg=1)
            # Annualize Alpha: (1 + weekly_alpha)^52 - 1
            data['Alpha'] = (1 + weekly_alpha)**52 - 1
            data['Beta'] = beta
        else:
            data['Alpha'] = np.nan
            data['Beta'] = np.nan

metrics_df = pd.DataFrame(metrics_data).T # Transpose to have strategies as rows

# Print the metrics table
print("\n" + "="*50)
print("PERFORMANCE METRICS SUMMARY")
print("="*50)
print(metrics_df.to_string(formatters={
    'Cumulative Return': '{:,.2%}'.format,
    'Annualized Return': '{:,.2%}'.format,
    'Annualized Volatility': '{:,.2%}'.format,
    'Sharpe Ratio': '{:,.2f}'.format,
    'Max Drawdown': '{:,.2%}'.format,
    'Alpha': '{:,.2%}'.format,
    'Beta': '{:,.2f}'.format,
}))
print("="*50 + "\n")

plt.figure(figsize=(8, 5))
plt.bar(metrics_df.index, metrics_df['Cumulative Return'] * 100, color=[red, blue, green, orange, purple])
plt.ylabel("Total Return (%)")
plt.title("Total Return Comparison (2024)")
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig("total_return_comparison.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === 5. RETURN CORRELATION SCATTER ===
# Use weekly returns for correlation, not cumulative returns, as it's more statistically sound.
x = merged_compare["Benchmark Return"]
y = merged_compare["Portfolio Return"]

# Linear regression (1st-degree polynomial fit)
slope, intercept = np.polyfit(x.fillna(0), y.fillna(0), deg=1) # fillna for safety
regression_line = slope * x + intercept

correlation = x.corr(y)
print(f"Correlation of weekly returns with NASDAQ 100: {correlation:.2f}")

plt.figure(figsize=(6, 4))
plt.scatter(x, y, alpha=0.6, color='gray', label='Weekly Observations')
plt.plot(x, regression_line, color='red', linewidth=2, label=f'Linear Fit (slope = {slope:.2f})')
plt.xlabel("NASDAQ 100 Weekly Return")
plt.ylabel("Portfolio Weekly Return")
plt.title("Weekly Return Correlation Scatter (2024)")
plt.legend(frameon=False)
plt.grid(True, linestyle='--', alpha=0.5)
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
plt.tight_layout()
plt.savefig("correlation_scatter.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === 6. WEEKLY RETURNS HEATMAP (using Matplotlib) ===
plt.figure(figsize=(18, 6))
ax = plt.gca()

# Prepare data for heatmap (Portfolio only)
heatmap_data = merged_compare[['Portfolio Return']].copy()
heatmap_data.columns = ['Portfolio']
heatmap_data_T = heatmap_data.T # Transpose for plotting

# Get labels and data
y_labels = heatmap_data_T.index
x_labels = merged_compare['Effective_Week'].dt.strftime('%Y-%m-%d')
data = heatmap_data_T.to_numpy()

# Determine color range, centered at 0
vmax = np.abs(data).max() if np.abs(data).max() > 0 else 0.10 # Avoid error if all returns are zero
vmin = -vmax

# Create the heatmap image
im = ax.imshow(data, cmap="RdYlGn", vmin=vmin, vmax=vmax, aspect='auto')

# Create colorbar
cbar = ax.figure.colorbar(im, ax=ax, location='bottom', pad=0.2, shrink=0.3, format=mtick.PercentFormatter(xmax=1.0))
cbar.set_label("Weekly Return", fontsize=12)

# Set ticks and labels
ax.set_xticks(np.arange(len(x_labels)))
ax.set_yticks(np.arange(len(y_labels)))
ax.set_xticklabels(x_labels)
ax.set_yticklabels(y_labels)

# Rotate the tick labels and set their alignment
plt.setp(ax.get_xticklabels(), rotation=90, ha="right", rotation_mode="anchor")
plt.tight_layout()
plt.savefig("weekly_returns_heatmap.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# Define formatter
percent_fmt = mtick.PercentFormatter(xmax=1.0)

# === NVDA Plot ===
nvda_weekly = weekly_prices[["week", "NVDA"]].copy()
nvda_weekly["Cumulative Return"] = nvda_weekly["NVDA"] / nvda_weekly["NVDA"].iloc[0]
nvda_plot = pd.merge(portfolio[["Week", "NVDA"]], nvda_weekly, left_on="Week", right_on="week", how="inner")

fig, ax1 = plt.subplots()
ax1.set_xlabel("Date")
ax1.set_ylabel("Allocation Weight", color="black")
ax1.plot(nvda_plot["Week"], nvda_plot["NVDA_x"], color="black", linewidth=1.5)

ax1.tick_params(axis='y', labelcolor="black")
ax1.set_ylim(-1.0, 1.0)
ax1.yaxis.set_major_formatter(percent_fmt)

plt.title("NVDA Allocation")
fig.tight_layout()
plt.savefig("nvda_alloc_vs_return.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === MSFT Plot ===
msft_weekly = weekly_prices[["week", "MSFT"]].copy()
msft_weekly["Cumulative Return"] = msft_weekly["MSFT"] / msft_weekly["MSFT"].iloc[0]
msft_plot = pd.merge(portfolio[["Week", "MSFT"]], msft_weekly, left_on="Week", right_on="week", how="inner")

fig, ax1 = plt.subplots()
ax1.set_xlabel("Date")
ax1.set_ylabel("Allocation Weight", color="black")
ax1.plot(msft_plot["Week"], msft_plot["MSFT_x"], color="black", linewidth=1.5)

ax1.tick_params(axis='y', labelcolor="black")
ax1.set_ylim(-1.0, 1.0)
ax1.yaxis.set_major_formatter(percent_fmt)

plt.title("MSFT Allocation")
fig.tight_layout()
plt.savefig("msft_alloc_vs_return.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === AAPL Plot ===
aapl_weekly = weekly_prices[["week", "AAPL"]].copy()
aapl_weekly["Cumulative Return"] = aapl_weekly["AAPL"] / aapl_weekly["AAPL"].iloc[0]
aapl_plot = pd.merge(portfolio[["Week", "AAPL"]], aapl_weekly, left_on="Week", right_on="week", how="inner")

fig, ax1 = plt.subplots()
ax1.set_xlabel("Date")
ax1.set_ylabel("Allocation Weight", color="black")
ax1.plot(aapl_plot["Week"], aapl_plot["AAPL_x"], color="black", linewidth=1.5)

ax1.tick_params(axis='y', labelcolor="black")
ax1.set_ylim(-1.0, 1.0)
ax1.yaxis.set_major_formatter(percent_fmt)

plt.title("AAPL Allocation")
fig.tight_layout()
plt.savefig("aapl_alloc_vs_return.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

# === CASH Plot ===
plt.figure()
plt.plot(portfolio["Week"], portfolio["CASH"], color='black', linewidth=1.5)
plt.xlabel("Date")
plt.ylabel("Weight")
plt.ylim(-2.2, 2.2)
plt.gca().yaxis.set_major_formatter(percent_fmt)
plt.title("Cash Allocation")
plt.tight_layout()
plt.savefig("allocation_cash.png", dpi=300)
plt.show(block=False)
plt.pause(0.5)

input("Press Enter to close all plots...")