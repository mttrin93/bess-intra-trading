import random
from datetime import datetime, timedelta
from psycopg2.extensions import cursor
from psycopg2.extensions import connection as PgConnection
import pandas as pd
import numpy as np

import pytz
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

# --- 1. CONFIGURATION (Moved to default arguments or environment variables) ---

# Set the Europe/Berlin timezone globally
BERLIN_TZ = pytz.timezone('Europe/Berlin')

# --- 2. CORE UTILITY FUNCTIONS (Refactored for modularity) ---

def round_to_full_hour(dt: datetime) -> datetime:
    """Rounds a datetime object to the nearest full hour."""
    if dt.minute >= 30:
        dt = dt + timedelta(hours=1)

    return dt.replace(minute=0, second=0, microsecond=0)


def random_time_in_2022() -> datetime:
    """Generates a random timezone-aware timestamp in 2022, rounded to the nearest full hour."""
    # TODO: fix this hard-coded implementation
    start = BERLIN_TZ.localize(datetime(2022, 1, 1))
    # end = BERLIN_TZ.localize(datetime(2023, 1, 1))
    end = BERLIN_TZ.localize(datetime(2022, 3, 1))

    # Calculate total hours (instead of total seconds for finer control)
    total_hours = int((end - start).total_seconds() // 3600)
    random_hours = random.randint(0, total_hours)
    random_time = start + timedelta(hours=random_hours)

    return round_to_full_hour(random_time)


def random_deliverystart(executiontime: datetime) -> datetime:
    """Generates a deliverystart within the next 16 hours, ensuring it is at a full hour."""
    # Start checking from the full hour of execution time
    rounded_executiontime = round_to_full_hour(executiontime)

    # Add a random number of full hours between 0 and 15 (inclusive)
    random_hours = random.randint(0, 15)

    deliverystart = rounded_executiontime + timedelta(hours=random_hours)

    return deliverystart  # Already at a full hour due to the way it's calculated


def connect_db(db_config: dict) -> psycopg2.connect:
    """Establishes and returns a PostgreSQL database connection."""
    conn = psycopg2.connect(**db_config)
    print("Connected to the database successfully!")
    return conn

def setup_table(cur: cursor):
    """Drops and creates the transactions_intraday_de table."""
    create_table_query = """
    DROP TABLE IF EXISTS transactions_intraday_de;

    CREATE TABLE IF NOT EXISTS transactions_intraday_de (
        id SERIAL PRIMARY KEY,
        executiontime TIMESTAMP WITH TIME ZONE NOT NULL,
        deliverystart TIMESTAMP WITH TIME ZONE NOT NULL,
        deliveryend TIMESTAMP WITH TIME ZONE NOT NULL,
        price REAL NOT NULL,
        volume REAL NOT NULL,
        side VARCHAR(4) NOT NULL,
        product VARCHAR(50) NOT NULL
    );
    """
    try:
        cur.execute(create_table_query)
        print("Table 'transactions_intraday_de' created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise


def generate_and_insert_fake_transactions(cur: cursor, conn: PgConnection, num_transactions: int):
    """Generates and inserts fake transactions into the database."""
    print(f"Generating and inserting {num_transactions:,} fake transactions...")
    count = 0
    # Use a list to batch the inserts for efficiency
    insert_data = []

    while count < num_transactions:
        base_executiontime = random_time_in_2022()

        # Insert the base transaction + 5 more transactions within the next 5 minutes
        for i in range(6):
            if i == 0:
                executiontime = base_executiontime
            else:
                # Add a random number of minutes and round the executiontime to the nearest minute
                executiontime = (base_executiontime + timedelta(minutes=i)).replace(second=0, microsecond=0)

            # Ensure executiontime is rounded to a full hour (as per original logic, though this might need review)
            executiontime = round_to_full_hour(executiontime)

            deliverystart = random_deliverystart(executiontime)
            deliveryend = deliverystart + timedelta(minutes=60)

            price = round(random.uniform(20, 100), 2)
            volume = round(random.uniform(1, 10), 2)
            side = random.choice(['BUY', 'SELL'])
            product = random.choice(['XBID_Hour_Power', 'Intraday_Hour_Power'])

            # Insert the transaction into the transactions_intraday_de table
            cur.execute(
                sql.SQL(
                    "INSERT INTO transactions_intraday_de (executiontime, deliverystart, deliveryend, price, volume, side, product) VALUES (%s, %s, %s, %s, %s, %s, %s);"),
                [executiontime, deliverystart, deliveryend, price, volume, side, product]
            )
            count += 1

        # Commit the transaction
        conn.commit()

    print(f"{num_transactions:,} fake transactions inserted successfully!")

def load_external_data(cur: cursor, file_path: str, table_name: str = 'transactions_intraday_de'):
    """
    Loads data from an external CSV file into the specified PostgreSQL table
    using a row-by-row INSERT loop (suitable for small datasets).

    Args:
        cur (PgCursor): Open database cursor object.
        file_path (str): Path to the external CSV file.
        table_name (str): The name of the target database table.
    """
    print(f"Loading data from external file: {file_path}...")

    # 1. Read the CSV file into a Pandas DataFrame
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at path: {file_path}")
        return

    # 2. Preprocessing and Type Conversion (CRITICAL)
    BERLIN_TZ = pytz.timezone('Europe/Berlin')

    try:
        # Convert necessary columns to timezone-aware datetime objects
        datetime_cols = ['executiontime', 'deliverystart', 'deliveryend']
        for col in datetime_cols:
            # Assume data is in the local timezone or convert it correctly
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(BERLIN_TZ)

        # Ensure price and volume are numeric
        df['price'] = pd.to_numeric(df['price'])
        df['volume'] = pd.to_numeric(df['volume'])

    except Exception as e:
        print(f"Error during data type preprocessing. Check CSV format: {e}")
        return

    # Define the order of columns to align with the database schema
    columns_to_insert = [
        'executiontime',
        'deliverystart',
        'deliveryend',
        'price',
        'volume',
        'side',
        'product'
    ]

    # Select and reorder columns
    df_clean = df[columns_to_insert]

    # 3. Execute the Row-by-Row INSERT Loop

    insert_count = 0
    # Create the SQL template once using sql.SQL for safety
    insert_query_template = sql.SQL(
        "INSERT INTO {} (executiontime, deliverystart, deliveryend, price, volume, side, product) VALUES (%s, %s, %s, %s, %s, %s, %s);"
    ).format(sql.Identifier(table_name))

    print(f"Starting row-by-row insertion into {table_name}...")

    # Iterate through each row of the clean DataFrame
    for index, row in df_clean.iterrows():
        try:
            # Prepare the values as a tuple
            values = tuple(row)

            # Execute the query
            cur.execute(insert_query_template, values)
            insert_count += 1

        except psycopg2.Error as e:
            print(f"Error inserting row {index}: {e}")
            # Continue to the next row or break based on desired error handling

    # Note: The final conn.commit() is still handled by the calling main() function.
    print(f"✅ Successfully inserted {insert_count} out of {len(df)} rows.")

def get_average_prices(
        conn: PgConnection,
        side: str,
        execution_time_start: pd.Timestamp,
        execution_time_end: pd.Timestamp,
        target_delivery_date: pd.Timestamp,
        min_trades: int = 1
) -> pd.DataFrame:
    """
    Calculates the historical Volume-Weighted Average Price (VWAP) for a
    specific delivery day based on transactions executed within a historical window.
    """
    start_of_day = pd.to_datetime(target_delivery_date) - pd.Timedelta(hours=2)

    # set hour and minute to 0 (europe/berlin time)
    start_of_day = start_of_day.replace(hour=0, minute=0)

    end_of_day = start_of_day

    end_of_day = end_of_day.replace(hour=23, minute=45)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT
        deliverystart,
        SUM(price*volume)/SUM(volume) AS weighted_avg_price
        FROM
        transactions_intraday_de
        WHERE
        (executiontime BETWEEN '{execution_time_start}' AND '{execution_time_end}')
        AND (product ='XBID_Hour_Power' or product = 'Intraday_Hour_Power') 
        AND side='{side}' 
        AND deliverystart < '{target_delivery_date}' 
        AND deliverystart >= '{start_of_day}' 
        GROUP BY
        deliverystart
        HAVING
        COUNT(*) >= {min_trades};
        """)
    result = cursor.fetchall()

    df = pd.DataFrame(result, columns=["product", "price"])

    # set index to product
    df.set_index("product", inplace=True)

    # set index to be all 15 minute intervals from start_of_day to end_of_day, filling missing values with NaN
    df.index = pd.DatetimeIndex(df.index)

    # Remove timezone if present
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df = df.reindex(pd.date_range(start_of_day, end_of_day, freq="60min"))

    return df

