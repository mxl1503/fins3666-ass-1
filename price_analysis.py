import pandas as pd

def calculate_trade_vwap(trade_file):
    df_trades = pd.read_csv(trade_file)
    df_trades['Date-Time'] = pd.to_datetime(df_trades['Date-Time'], errors='coerce')
    df_trades.dropna(subset=['Date-Time'], inplace=True)
    
    # Compute VWAP for each 5-minute interval
    df_trades.set_index('Date-Time', inplace=True)
    df_trades = df_trades.sort_index()
    
    trade_vwap = df_trades.groupby(pd.Grouper(freq='5min')).apply(
        lambda x: (x['Price'] * x['Volume']).sum() / x['Volume'].sum() if x['Volume'].sum() > 0 else None
    ).rename('Trade VWAP')
    
    trade_volume = df_trades['Volume'].resample('5min').sum().rename('Cumulative Trade Volume')
    
    return pd.concat([trade_vwap, trade_volume], axis=1).reset_index()

def calculate_order_book_metrics(order_book_file):
    df_order_book = pd.read_csv(order_book_file)
    df_order_book['Date-Time'] = pd.to_datetime(df_order_book['Date-Time'], errors='coerce')
    df_order_book.dropna(subset=['Date-Time'], inplace=True)
    
    df_order_book.set_index('Date-Time', inplace=True)
    df_order_book = df_order_book.sort_index()
    
    # Compute Mid Price
    df_order_book['Mid Price'] = (df_order_book['L1-BidPrice'] + df_order_book['L1-AskPrice']) / 2
    
    # Compute Spread Crossing Volume Weighted Mid Price
    df_order_book['Spread Crossing Volume'] = df_order_book['L1-BidSize'] + df_order_book['L1-AskSize']
    df_order_book['SCVWMP'] = (df_order_book['Mid Price'] * df_order_book['Spread Crossing Volume']).cumsum() / df_order_book['Spread Crossing Volume'].cumsum()
    
    # Compute Volume Weighted Mid Price
    df_order_book['VWMP'] = (df_order_book['Mid Price'] * df_order_book['Spread Crossing Volume']).cumsum() / df_order_book['Spread Crossing Volume'].cumsum()
    
    # Resample to 5-minute intervals
    order_book_resampled = df_order_book[['SCVWMP', 'VWMP', 'Mid Price']].resample('5min').mean().reset_index()
    
    return order_book_resampled

def main():
    trade_vwap_df = calculate_trade_vwap('PXA.X_Filtered_Trades.csv')
    order_book_metrics_df = calculate_order_book_metrics('PXA.X_Filtered_Market_Depth.csv')
    
    # Merge trade VWAP with order book metrics
    merged_df = pd.merge(trade_vwap_df, order_book_metrics_df, on='Date-Time', how='inner')
    
    # Compute absolute difference measures
    merged_df['Abs(VWAP - SCVWMP)'] = abs(merged_df['Trade VWAP'] - merged_df['SCVWMP'])
    merged_df['Abs(VWAP - VWMP)'] = abs(merged_df['Trade VWAP'] - merged_df['VWMP'])
    merged_df['Abs(VWAP - Mid Price)'] = abs(merged_df['Trade VWAP'] - merged_df['Mid Price'])
    
    # Compute total absolute error for each measure
    total_errors = merged_df[['Abs(VWAP - SCVWMP)', 'Abs(VWAP - VWMP)', 'Abs(VWAP - Mid Price)']].sum()
    closest_measure = total_errors.idxmin()
    
    print("Total Absolute Differences:")
    print(total_errors)
    print(f"The measure closest to VWAP is: {closest_measure}")
    
    # Save results
    output_file_path = 'VWAP_Comparison_5min.csv'
    merged_df.to_csv(output_file_path, index=False)
    print(f"Comparison data saved to {output_file_path}")

if __name__ == '__main__':
    main()
