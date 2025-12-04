from psycopg2.extensions import connection as PgConnection
import pandas as pd

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
