import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

def load_config(config_path="config/config.json"):
    """Loads the configuration file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def load_data(data_path):
    """Loads data from a Parquet or CSV file."""
    df = None
    if os.path.exists(data_path):
        try:
            df = pd.read_parquet(data_path)
            print(f"Loaded data from parquet: {data_path}")
        except Exception as e:
            print(f"Error loading parquet file: {e}")
            # Fallback to CSV if parquet fails
            csv_path = data_path.replace('.parquet', '.csv')
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                    print(f"Loaded data from CSV: {csv_path}")
                except Exception as e_csv:
                    print(f"Error loading CSV file: {e_csv}")
                    raise
    if df is None:
        raise FileNotFoundError(f"No data file found at {data_path}")
    return df

def map_data(df, mapping):
    """Maps raw data to field names based on the mapping configuration."""
    mapping_df = pd.DataFrame(mapping)
    mapping_df = mapping_df.rename(columns={
        "ItemNumber": "item_number",
        "ColumnNumber": "column_number",
        "Name": "field_name",
        "Hirearchy": "hierarchy",
        "Classification": "classification"
    })

    # Ensure data types are consistent for merging
    df['item_number'] = df['item_number'].astype(str)
    df['column_number'] = df['column_number'].astype(str)
    mapping_df['item_number'] = mapping_df['item_number'].astype(str)
    mapping_df['column_number'] = mapping_df['column_number'].astype(str)

    # Merge to map data
    extracted_df = pd.merge(
        df, 
        mapping_df, 
        on=['item_number', 'column_number']
    )
    
    print(f"Extracted {len(extracted_df)} mapped records")
    return extracted_df

def pivot_data(df):
    """Pivots the data to have fields as columns."""
    if df.empty:
        return pd.DataFrame()
    
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    pivoted_df = df.pivot_table(
        values='value', 
        index=['period', 'institution_name'],
        columns='field_name',
        aggfunc='first'
    ).reset_index()
    
    print("Created pivot table.")
    return pivoted_df

def calculate_aggregates(pivoted_df, extracted_df, config):
    """Calculates aggregate metrics based on the configuration."""
    loan_aggregation_config = config.get("loan_aggregation", {})
    
    for agg_name, agg_rules in loan_aggregation_config.items():
        classification = agg_rules["classification"]
        hierarchy_prefix = agg_rules["hierarchy_prefix"]
        
        # Filter items for this aggregation
        agg_items = extracted_df[
            (extracted_df['classification'] == classification) & 
            (extracted_df['hierarchy'].str.startswith(hierarchy_prefix))
        ]
        
        if not agg_items.empty:
            # Sum by period and institution
            agg_sum = agg_items.groupby(['period', 'institution_name'])['value'].sum().reset_index()
            agg_sum.rename(columns={'value': agg_name}, inplace=True)
            
            # Merge with the pivoted dataframe
            pivoted_df = pd.merge(pivoted_df, agg_sum, on=['period', 'institution_name'], how='left')
            print(f"Calculated aggregate: {agg_name}")
            
    return pivoted_df

def calculate_analytics(pivoted_df):
    """Calculates time-series analytics for each institution."""
    analytics_dfs = []
    institutions = pivoted_df['institution_name'].unique()
    
    # Separate total data for market share calculation
    total_data = pivoted_df[pivoted_df['institution_name'] == 'Total'].set_index('period')
    
    for institution in institutions:
        inst_data = pivoted_df[pivoted_df['institution_name'] == institution].copy()
        if inst_data.empty:
            continue

        inst_data = inst_data.sort_values('period').set_index('period')
        value_columns = [col for col in inst_data.columns if col not in ['institution_name']]

        for col in value_columns:
            if inst_data[col].isna().all():
                continue

            inst_data[f'{col}_mom_pct'] = inst_data[col].pct_change() * 100
            inst_data[f'{col}_yoy_pct'] = inst_data[col].pct_change(periods=12) * 100
            inst_data[f'{col}_yoy_3m_roll'] = inst_data[f'{col}_yoy_pct'].rolling(window=3, min_periods=1).mean()

            if institution != 'Total' and not total_data.empty and col in total_data.columns:
                # Align total data with institution data for market share calculation
                market_share = 100 * inst_data[col] / total_data[col]
                inst_data[f'{col}_market_share'] = market_share
                inst_data[f'{col}_market_share_yoy_bps'] = (inst_data[f'{col}_market_share'] - inst_data[f'{col}_market_share'].shift(12)) * 100

        analytics_dfs.append(inst_data.reset_index())

    analytics_df = pd.concat(analytics_dfs, ignore_index=True)
    print("Calculated analytics.")
    return analytics_df

def save_excel_report(pivoted_df, analytics_df, file_path):
    """Saves the analytics data to a multi-sheet Excel report."""
    report_sheets = {
        'Raw Values': (pivoted_df, None),
        'MoM % Change': (analytics_df, '_mom_pct'),
        'YoY % Change': (analytics_df, '_yoy_pct'),
        '3M Avg YoY %': (analytics_df, '_yoy_3m_roll'),
        'Market Share %': (analytics_df, '_market_share'),
        'Market Share YoY Change (bps)': (analytics_df, '_market_share_yoy_bps')
    }

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        for sheet_name, (df, suffix) in report_sheets.items():
            if suffix:
                cols = ['period', 'institution_name'] + [c for c in df.columns if c.endswith(suffix)]
                if len(cols) <= 2:
                    continue
                sheet_df = df[cols].copy()
                sheet_df.columns = [c.replace(suffix, '') for c in sheet_df.columns]
                if 'Market Share' in sheet_name:
                    sheet_df = sheet_df[sheet_df['institution_name'] != 'Total']
            else:
                sheet_df = df.copy()

            if not sheet_df.empty:
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"Analytics report saved to {file_path}")

def generate_analytics_summary(pivoted_df, analytics_df, mapping, file_path):
    """Generate a summary text file for analytics processing."""
    with open(file_path, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("SARB BA 900 ANALYTICS SUMMARY\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("PROCESSING OVERVIEW:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Mapping Items Used: {len(mapping)}\n")
        f.write(f"Raw Records Processed: {len(pivoted_df)}\n")
        f.write(f"Analytics Records Generated: {len(analytics_df)}\n")
        if not pivoted_df.empty:
            f.write(f"Date Range: {pivoted_df['period'].min()} to {pivoted_df['period'].max()}\n\n")
        
        f.write("INSTITUTIONS ANALYZED:\n")
        f.write("-" * 20 + "\n")
        institutions = pivoted_df['institution_name'].unique()
        for inst in sorted(institutions):
            inst_records = len(pivoted_df[pivoted_df['institution_name'] == inst])
            f.write(f"  {inst}: {inst_records} periods\n")
        f.write("\n")
        
        f.write("MAPPED FIELDS:\n")
        f.write("-" * 20 + "\n")
        field_columns = [col for col in pivoted_df.columns if col not in ['period', 'institution_name']]
        for field in sorted(field_columns):
            non_null_count = pivoted_df[field].notna().sum()
            f.write(f"  {field}: {non_null_count} non-null values\n")
        f.write("\n")

        f.write("ANALYTICS METRICS GENERATED:\n")
        f.write("-" * 30 + "\n")
        metric_types = {
            'Month-on-Month %': '_mom_pct',
            'Year-on-Year %': '_yoy_pct',
            '3M Avg YoY %': '_yoy_3m_roll',
            'Market Share %': '_market_share',
            'Market Share YoY (bps)': '_market_share_yoy_bps'
        }
        for name, suffix in metric_types.items():
            count = sum(1 for col in analytics_df.columns if col.endswith(suffix))
            f.write(f"  {name}: {count} metrics\n")
        f.write("\n")

        f.write("=" * 60 + "\n")
        f.write("End of Analytics Summary\n")
        f.write("=" * 60 + "\n")
    
    print(f"Analytics summary saved to {file_path}")

def main():
    """Main function to run the data processing pipeline."""
    config = load_config()
    
    data_dir = config["data_dir"]
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Load data
    combined_file_path = os.path.join(data_dir, config["combined_data_parquet"])
    df = load_data(combined_file_path)
    df['period'] = pd.to_datetime(df['period'])

    # Load mapping
    with open(config["mapping_file"], 'r') as f:
        mapping = json.load(f)

    # Process data
    extracted_df = map_data(df, mapping)
    pivoted_df = pivot_data(extracted_df)
    
    if pivoted_df.empty:
        print("No mapped data found. Check mapping configuration.")
        return

    pivoted_df = calculate_aggregates(pivoted_df, extracted_df, config)
    analytics_df = calculate_analytics(pivoted_df)

    # Save results
    pivoted_parquet_path = os.path.join(data_dir, config["pivoted_data_parquet"])
    pivoted_df.to_parquet(pivoted_parquet_path, index=False)
    print(f"Pivoted mapped data saved to parquet: {pivoted_parquet_path}")

    analytics_parquet_path = os.path.join(data_dir, config["analytics_data_parquet"])
    analytics_df.to_parquet(analytics_parquet_path, index=False)
    print(f"Analytics data saved to parquet: {analytics_parquet_path}")

    excel_file_path = os.path.join(data_dir, config["analytics_report_excel"])
    save_excel_report(pivoted_df, analytics_df, excel_file_path)

    summary_file_path = os.path.join(data_dir, config["analytics_summary_txt"])
    generate_analytics_summary(pivoted_df, analytics_df, mapping, summary_file_path)

    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()
