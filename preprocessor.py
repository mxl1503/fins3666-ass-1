import pandas as pd

def filter_data(input_file_path, output_file_path):
    df = pd.read_csv(input_file_path)

    df['Date-Time'] = pd.to_datetime(df['Date-Time'])

    market_open = df['Date-Time'].dt.normalize() + pd.Timedelta(hours=10)
    market_close = df['Date-Time'].dt.normalize() + pd.Timedelta(hours=16)

    time_filtered_df = df[(df['Date-Time'] >= market_open) & (df['Date-Time'] <= market_close)]
    time_filtered_df.to_csv(output_file_path, index=False)

def main():
    filter_data('PXA.X 20250115 Market Depth Legacy x 10.csv', 'PXA.X_Filtered_Market_Depth.csv')
    filter_data('PXA.X 20250115 Stocks Trades.csv', 'PXA.X_Filtered_Trades.csv')

if __name__ == '__main__':
    main()
