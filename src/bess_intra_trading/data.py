import random
from datetime import datetime, timedelta
from psycopg2.extensions import cursor
import pandas as pd

import pytz
import psycopg2
from psycopg2 import sql

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
    start = BERLIN_TZ.localize(datetime(2022, 1, 1))
    end = BERLIN_TZ.localize(datetime(2023, 1, 1))

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


def generate_and_insert_fake_transactions(cur: cursor, num_transactions: int):
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

            insert_data.append(
                (executiontime, deliverystart, deliveryend, price, volume, side, product)
            )
            count += 1
            if count >= num_transactions:
                break

    # Execute the batched inserts
    insert_query = sql.SQL(
        "INSERT INTO transactions_intraday_de (executiontime, deliverystart, deliveryend, price, volume, side, product) VALUES (%s, %s, %s, %s, %s, %s, %s);")
    cur.executemany(insert_query, insert_data)

    print(f"{num_transactions:,} fake transactions inserted successfully!")


def load_external_data(cur: cursor, file_path: str):
    """
    Placeholder function to load external data (e.g., from CSV or JSON) into the DB.

    NOTE: This would require specific implementation based on the external file format.
    """
    print(f"Loading data from external file: {file_path}...")

    profits = pd.read_csv(file_path)
    # # Preprocess and insert data...

    # For now, we print a confirmation and skip actual insertion
    print("External data loading functionality is a placeholder and was skipped.")