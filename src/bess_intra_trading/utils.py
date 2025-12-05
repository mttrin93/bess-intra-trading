from psycopg2.extensions import connection as PgConnection
import pandas as pd
import logging
import socket
from typing import Optional


def setup_logger(
    name: str = 'bess-intra-trading',
    level: int = logging.INFO,
    format_str: Optional[str] = None
) -> logging.Logger:
    hostname = socket.gethostname()
    if format_str is None:
        log_format = f'%(asctime)s %(levelname).1s - ({hostname}) - %(message)s'
    else:
        log_format = format_str

    if not logging.getLogger(name).handlers:
        logging.basicConfig(
            level=level,
            format=log_format,
            datefmt="%Y/%m/%d %H:%M:%S"
        )

    return logging.getLogger(name)


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


def get_net_trades(trades: pd.DataFrame, end_date: pd.Timestamp):

    # create a new empty dataframe with the columns "net_buy" and "net_sell"
    net_trades = pd.DataFrame(
        columns=["sum_buy", "sum_sell", "net_buy", "net_sell", "product"]
    )

    # based on trades, calculate the net buy and net sell for each product
    for product in trades["product"].unique():
        product_trades = trades[trades["product"] == product]
        sum_buy = product_trades[product_trades["side"] == "buy"]["quantity"].sum()
        sum_sell = product_trades[product_trades["side"] == "sell"]["quantity"].sum()
        # add to net_trades using concat
        net_trades = pd.concat(
            [
                net_trades,
                pd.DataFrame(
                    [[sum_buy, sum_sell, product]],
                    columns=["sum_buy", "sum_sell", "product"],
                ),
            ],
            ignore_index=True,
        )

    # add the columns "net_buy" and "net_sell" to net_trades, net_buy = sum_buy - sum_sell (if > 0), net_sell = sum_sell - sum_buy (if > 0)
    net_trades["net_buy"] = net_trades["sum_buy"] - net_trades["sum_sell"]
    net_trades["net_sell"] = net_trades["sum_sell"] - net_trades["sum_buy"]

    # remove values < 0 for net_buy and net_sell
    net_trades.loc[net_trades["net_buy"] < 0, "net_buy"] = 0
    net_trades.loc[net_trades["net_sell"] < 0, "net_sell"] = 0

    # set column product to index
    net_trades = net_trades.set_index("product")

    # set start_of_day to end_date minus 1 day
    start_of_day = pd.to_datetime(end_date) - pd.Timedelta(hours=2)

    # set hour and minute to 0 (europe/berlin time)
    start_of_day = start_of_day.replace(hour=0, minute=0)
    end_of_day = start_of_day
    end_of_day = end_of_day.replace(hour=23, minute=45)

    net_trades = net_trades.reindex(
        pd.date_range(start_of_day, end_of_day, freq="60min")
    )

    # fill NaN values with 0
    net_trades = net_trades.astype(float)
    net_trades = net_trades.fillna(0)

    # set index to datetime
    net_trades.index = pd.to_datetime(net_trades.index)

    # return the net_trades dataframe
    return net_trades
