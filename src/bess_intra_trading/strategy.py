import pandas as pd
from bess_intra_trading.utils import get_average_prices, get_net_trades, setup_logger
from bess_intra_trading.model import solve_intrinsic_problem
from psycopg2.extensions import connection as PgConnection
import socket
import getpass
import os


hostname = socket.gethostname()
username = getpass.getuser()
log = setup_logger()


class RollingIntrinsicStrategy:
    """
    Implements the Rolling Intrinsic (RI) BESS trading strategy.
    """

    def __init__(self, bess_params: dict, horizon_h: float = 1.0):
        """
        Initializes the strategy with BESS parameters.
        Args:
            bess_params (dict): Battery specs (capacity, min_soc, max_power_mw, etc.).
            horizon_h (float): The fixed look-ahead horizon for the optimization in hours.
        """
        self.params = bess_params
        self.dt = self.params.get('time_step_h', 15)  # Default 15 min step

    def simulate(
            self,
            conn: PgConnection,
            start_date: pd.Timestamp,
            end_date: pd.Timestamp,
            initial_soc: float
    ):
            # -> pd.DataFrame:
        """
        Runs the rolling simulation over the market data.

        Args:
            initial_soc (float): Starting State of Charge [MWh].

        Returns:
            pd.DataFrame: A log of all trading decisions and BESS states.
        """
        # set path as ./ma_results/threshold
        path = os.path.join(
            "output",
            "hourly",
            "bs"
            + str(self.dt)
            + "cr"
            + str(self.params['c_rate'])
            + "rto"
            + str(self.params['efficiency'])
            + "mc"
            + str(self.params['max_cycles'])
            + "mt"
            + str(self.params['min_trades'])
        )
        tradepath = os.path.join(path, "trades")

        profits = pd.DataFrame(columns=["day", "profit", "cycles"])

        # create directory if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)

        if not os.path.exists(tradepath):
            os.makedirs(tradepath)

        current_day = start_date
        current_cycles = 0
        net_trades = pd.DataFrame(
            columns=["sum_buy", "sum_sell", "net_buy", "net_sell", "product"]
        )

        while current_day < end_date:

            all_trades = pd.DataFrame(
                columns=["execution_time", "side", "quantity", "price", "product", "profit"]
            )

            current_day = current_day.replace(hour=0, minute=0, second=0, microsecond=0)
            current_day = current_day + pd.Timedelta(days=1)

            # introduce a hard-coded lookback window
            trading_start = current_day - pd.Timedelta(hours=8)
            trading_end = current_day + pd.Timedelta(days=1)

            execution_time_start = trading_start
            execution_time_end = trading_start + pd.Timedelta(minutes=self.dt)

            days_left = (end_date - current_day).days

            allowed_cycles = self.params['max_cycles'] / 365 + (
                    (self.params['max_cycles'] / 365 * (365 - days_left)) - current_cycles
            )

            while execution_time_end < trading_end:
                volume_weighted_average_price = get_average_prices(
                    conn=conn,
                    side='BUY',
                    execution_time_start=execution_time_start,
                    execution_time_end=execution_time_end,
                    target_delivery_date=trading_end,
                    min_trades=self.params['min_trades'],
                )

                net_trades = get_net_trades(all_trades, trading_end)

                if volume_weighted_average_price["price"].isnull().all():
                    log.info("No trades in this quarter hour")
                    execution_time_start = execution_time_end
                    execution_time_end = execution_time_end + pd.Timedelta(
                        minutes=self.dt
                    )
                    continue
                else:
                    try:
                        results, trades, profit = solve_intrinsic_problem(
                                prices_qh=volume_weighted_average_price,
                                execution_time=execution_time_start,
                                cap=1,
                                c_rate=self.params['c_rate'],
                                roundtrip_eff=self.params['efficiency'],
                                max_cycles=allowed_cycles,
                                threshold=self.params['threshold'],
                                threshold_abs_min=self.params['threshold_abs_min'],
                                discount_rate=self.params['discount_rate'],
                                prev_net_trades=net_trades,
                        )
                        # append trades to all_trades using concat
                        all_trades = pd.concat([all_trades, trades])
                    except ValueError:
                        log.info("Error in optimization")
                        log.info("execution_time_start: {}".format(execution_time_start))
                        execution_time_start = execution_time_end
                        execution_time_end = execution_time_start + pd.Timedelta(
                            minutes=self.dt
                        )
                        continue

                execution_time_start = execution_time_end
                execution_time_end = execution_time_end + pd.Timedelta(
                    minutes=self.dt
                )

            # calculate daily_profit as sum of all_trades["profit"]
            daily_profit = all_trades["profit"].sum()
            current_cycles += net_trades["net_buy"].sum() / 1.0 * self.params['efficiency'] ** 0.5

            # save trades
            all_trades.to_csv(
                os.path.join(tradepath,
                             "trades_" + current_day.strftime("%Y-%m-%d") + ".csv"),
                index=False,
            )

            # append daily_profit to profits.csv using concat
            profits = pd.concat(
                [
                    profits,
                    pd.DataFrame(
                        [[current_day, daily_profit, current_cycles]],
                        columns=["day", "profit", "cycles"],
                    ),
                ]
            )

            profits_db = pd.DataFrame(
                [
                    [
                        current_day,
                        daily_profit,
                        net_trades["net_buy"].sum() / 1.0 * self.params['efficiency'] ** 0.5,
                    ]
                ],
                columns=["day", "profit", "cycles"],
            )

            # add column threshold, threshold_abs and discount_rate to profits_db
            profits_db["type_freq"] = "H"
            profits_db["max_cycles"] = self.params['max_cycles']
            profits_db["bucket_size"] = self.dt
            profits_db["rto"] = self.params['efficiency']
            profits_db["c_rate"] = self.params['c_rate']
            profits_db["min_trades"] = self.params['min_trades']

            # save profits_db to database
            # TODO: this should be fixed
            # profits_db.to_sql(
            #     "revenues",
            #     conn_alchemy,
            #     if_exists="append",
            #     index=False,
            # )

            # save profits.csv
            profits.to_csv(os.path.join(path, "profit.csv"), index=False)

            # set current day to current_day plus 1 day
            current_day = current_day + pd.Timedelta(days=1) + pd.Timedelta(hours=2)
