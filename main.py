import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, Date, Float, Text, Boolean

try:
    db = psycopg2.connect(
        host="internaldb.c0rz9kyyn4jp.us-east-2.rds.amazonaws.com",
        user="postgres",
        password="Kwantx11!!",
        database="internaldb"
    )
    print("PostgreSQL connection is succssful!")
except (psycopg2.Error, Exception) as error:
    print(f"DB connection is error, {error}")

    cursor = db.cursor()    
    db.set_session(autocommit=True)

engine = create_engine('postgresql://postgres:Kwantx11!!@internaldb.c0rz9kyyn4jp.us-east-2.rds.amazonaws.com/internaldb')

    # Fetch data from the database
earning_calendar_df = pd.read_sql_table('earning_calendar', engine)
index_symbols_df = pd.read_sql_table('index_symbols', engine)
financial_ratios_df = pd.read_sql_table('financial_ratios', engine)
market_cap_df = pd.read_sql_table('market_cap_data', engine)
company_profile_df = pd.read_sql_table('company_profile', engine)
stock_history_df = pd.read_sql_table('stock_history', engine)

# 1. Creating a DataFrame with every trading day from 1/1/2000 to 9/13/2023
date_range = pd.date_range(start='2000-01-01', end='2023-09-13', freq='B')
base_df = pd.DataFrame({'date': date_range})

# Initialize index columns in base_df
base_df['symbol in s&p500'] = False
base_df['symbol in dow30'] = False

for _, row in index_symbols_df.iterrows():
   if row['addedSecurity']:  # If a symbol was added
        mask_added = (base_df['date'] >= row['dateAdded'])
        if row['index'] == "S&P500":
            base_df.loc[mask_added, 'symbol in s&p500'] = True
        elif row['index'] == "Dow30":
            base_df.loc[mask_added, 'symbol in dow30'] = True
   
   if row['removedTicker']:  # If a symbol was removed
        mask_removed = (base_df['date'] >= row['dateAdded'])
        if row['index'] == "S&P500":
            base_df.loc[mask_removed, 'symbol in s&p500'] = False
        elif row['index'] == "Dow30":
            base_df.loc[mask_removed, 'symbol in dow30'] = False

# 1. Creating a DataFrame with every trading day from 1/1/2000 to 9/13/2023
date_range = pd.date_range(start='2000-01-01', end='2023-09-13', freq='B')
base_df = pd.DataFrame({'date': date_range})

# 2. Merging the earnings_calendar data
master_df = base_df.merge(earning_calendar_df, on='date', how='left')

# 3. Merging the market_cap data
master_df = master_df.merge(market_cap_df, left_on=['date', 'symbol'], right_on=['datetime', 'symbol'], how='left')

# 4. Merging the financial_ratios data using fiscalDateEnding for merging
master_df = master_df.merge(financial_ratios_df, left_on=['fiscalDateEnding', 'symbol'], right_on=['date', 'symbol'], how='left')

# 6. Merging the company_profile data
master_df = master_df.merge(company_profile_df, on='symbol', how='left', suffixes=('_master', '_profile'))
# print(master_df.columns)
# Forward fill data for each symbol
master_df = master_df.sort_values(by=['symbol', 'date_x'])
master_df.groupby('symbol').fillna(method='ffill', inplace=True)


# Merge the stock_history data using 'symbol' and 'date' for merging
master_df = master_df.merge(stock_history_df[['open', 'high', 'low', 'close', 'adjClose', 'volume', 'unadjustedVolume', 'change', 'changePercent', 'vwap', 'symbol', 'date']], left_on=['date_x', 'symbol'], right_on=['date', 'symbol'], how='left')

# Define table schema and insert data
columns = []
for col_name, dtype in master_df.dtypes.items():
    print(col_name, dtype)
    if "int" in str(dtype):
        columns.append(Column(col_name, Integer))
    elif "float" in str(dtype):
        columns.append(Column(col_name, Float))
    elif "date" in str(dtype):
        columns.append(Column(col_name, Date))
    elif "bool" in str(dtype):
        columns.append(Column(col_name, Boolean))
    else:
        columns.append(Column(col_name, Text))

# Create a new master_data table
metadata = MetaData()
master_data = Table('master_data', metadata, *columns)
master_data.drop(engine, checkfirst=True)  # Drop the table if it exists
master_data.create(engine, checkfirst=True)  # Create the table, checking first if it exists

# Insert the master_df DataFrame into the master_data table
master_df.to_sql('master_data', engine, if_exists='append', index=False)

# print(base_df)


