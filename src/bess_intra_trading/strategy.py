import pandas as pd
from bess_intra_trading.data import get_average_prices
from psycopg2.extensions import connection as PgConnection


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
        current_soc = initial_soc
        log = []

        current_day = start_date
        while current_day < end_date:

            current_day = current_day.replace(hour=0, minute=0, second=0, microsecond=0)
            current_day = current_day + pd.Timedelta(days=1)

            # introduce a hard-coded lookback window
            trading_start = current_day - pd.Timedelta(hours=8)
            trading_end = current_day + pd.Timedelta(days=1)

            execution_time_start = trading_start
            execution_time_end = trading_start + pd.Timedelta(minutes=self.dt)

            days_left = (end_date - current_day).days

            while execution_time_end < trading_end:
                volume_weighted_average_price_buy = get_average_prices(
                    conn=conn,
                    side='SELL',
                    execution_time_start=execution_time_start,
                    execution_time_end=execution_time_end,
                    target_delivery_date=trading_end
                )
                # print(volume_weighted_average_price_buy)

                break
                # execution_time_end = execution_time_end + pd.Timedelta(minutes=self.dt)


        # # Iterate over all possible decision points (index of market_data)
        # for i in range(len(market_data)):
        #
        #     # --- 1. Define the Rolling Lookahead Window ---
        #     # The intrinsic model looks from time 'i' up to i + T_horizon
        #     lookahead_prices = market_data.iloc[i: i + T_horizon]
        #
        #     # If the lookahead window is incomplete, stop trading
        #     if len(lookahead_prices) < T_horizon:
        #         break
        #
        #     # --- 2. Solve Optimization for the Current Step ---
        #     results = solve_intrinsic_problem(
        #         prices=lookahead_prices,
        #         soc_initial=current_soc,
        #         bess_params=self.params
        #     )
        #
        #     # Optimal action (Charge: negative MW, Discharge: positive MW)
        #     action_mw = results['optimal_action_mw']
        #
        #     # --- 3. Update BESS State (Only for the first time step, dt) ---
        #
        #     # Calculate power flow for the next dt (15 min)
        #     power_flow = action_mw * self.dt  # Energy traded in MWh
        #
        #     # Apply efficiency to the actual action (charging uses 1/eff, discharging uses eff)
        #     eff = self.params['efficiency']
        #     if power_flow > 0:  # Discharge (Selling)
        #         energy_delivered = power_flow
        #         energy_removed = power_flow / eff
        #     elif power_flow < 0:  # Charge (Buying)
        #         energy_delivered = power_flow
        #         energy_added = power_flow * eff  # power_flow is negative here
        #         energy_removed = power_flow
        #     else:
        #         energy_delivered = 0
        #         energy_removed = 0
        #
        #     # Calculate the revenue generated in this step
        #     price = lookahead_prices.iloc[0]['bid'] if action_mw > 0 else lookahead_prices.iloc[0]['ask']
        #     revenue_dt = abs(action_mw) * price * self.dt
        #
        #     # Update SoC (Energy in MWh)
        #     # current_soc += (action_mw * eff) * self.dt if action_mw < 0 else (action_mw / eff) * self.dt
        #     # A simpler way using the optimized energy change:
        #     soc_change = (action_mw * eff * self.dt) if action_mw < 0 else (-action_mw / eff * self.dt)
        #
        #     # Ensure SoC stays within bounds after update
        #     next_soc = current_soc + soc_change
        #
        #     # Log the step
        #     log.append({
        #         'time': market_data.index[i],
        #         'soc_start_mwh': current_soc,
        #         'optimal_action_mw': action_mw,
        #         'soc_end_mwh': np.clip(next_soc, self.params['min_soc'], self.params['max_soc']),
        #         'revenue_dt': revenue_dt,
        #         'status': results.get('status', 'Optimal')
        #     })
        #
        #     # Update state for the next iteration
        #     current_soc = log[-1]['soc_end_mwh']
        #
        # return pd.DataFrame(log).set_index('time')
