# bess_intra_trading/bin/run_optimization.py

import argparse
import pandas as pd
import pytz
import numpy as np

# Absolute imports for the core package components
from bess_intra_trading.strategy import RollingIntrinsicStrategy
from bess_intra_trading.data import fetch_processed_market_data  # Placeholder function
from bess_intra_trading.data import connect_db


# --- Placeholder for Data Fetching (You will need to implement this!) ---
def fetch_processed_market_data(conn, start_date, end_date):
    """
    Fetch the aggregated and processed market data (bid/ask prices)
    required by the strategy from the database.

    NOTE: This is a critical step that requires implementing the
    data aggregation logic (e.g., aggregating transactions by 15-min product
    and calculating bid/ask spreads) in a separate file (e.g., bess_intra_trading/data.py).
    """
    # Example placeholder: replace this with actual DB query and processing
    print(f"Fetching data from DB between {start_date} and {end_date}...")

    # Dummy data structure required by RollingIntrinsicStrategy.simulate:
    idx = pd.to_datetime(pd.date_range(start_date, end_date, freq='15min', inclusive='left'))
    data = {
        'bid': np.random.uniform(50, 150, size=len(idx)),
        'ask': np.random.uniform(55, 155, size=len(idx)),
    }
    return pd.DataFrame(data, index=idx).tz_localize('Europe/Berlin')  # IMPORTANT: Must be timezone aware


# --- MAIN EXECUTION LOGIC ---
def main():
    parser = argparse.ArgumentParser(
        description="Runs the Rolling Intrinsic BESS Optimization Strategy."
    )

    # --- Date/Time Arguments ---
    parser.add_argument(
        '--start-date',
        type=str,
        default='2022-01-01',
        help='Start date for simulation (YYYY-MM-DD).'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        default='2022-01-02',
        help='End date for simulation (YYYY-MM-DD).'
    )

    # --- BESS Parameters ---
    parser.add_argument(
        '--capacity',
        type=float,
        default=10.0,
        help='BESS capacity [MWh].'
    )

    parser.add_argument(
        '--efficiency',
        type=float,
        default=0.86,
        help='BESS roundtrip efficiency (0 to 1).'
    )

    parser.add_argument(
        '--power',
        type=float,
        default=5.0,
        help='Maximum charge/discharge power [MW].'
    )

    parser.add_argument(
        '--init-soc',
        type=float,
        default=5.0,
        help='Initial State of Charge [MWh].'
    )

    # --- DB Connection Arguments (Reused from create_data) ---
    parser.add_argument(
        '--db-name',
        default='intradaydb',
        help='PostgreSQL database name.'
    )
    # ... (Other DB args omitted for brevity, but should be included)

    args = parser.parse_args()

    # --- 1. Setup BESS and Strategy ---
    bess_params = {
        'capacity_mwh': args.capacity,
        'min_soc': args.capacity * 0.1,  # Assuming 10% minimum SoC
        'max_soc': args.capacity * 0.9,  # Assuming 90% maximum SoC
        'max_power_mw': args.power,
        'efficiency': args.efficiency,
        'time_step_h': 0.25,  # 15 minute time step (0.25 hours)
    }

    strategy = RollingIntrinsicStrategy(bess_params=bess_params, horizon_h=1.0)

    # --- 2. Database Connection and Data Fetch ---
    db_config = {'dbname': args.db_name, 'user': 'leloq', 'password': '123', 'host': 'localhost', 'port': '5432'}

    try:
        with connect_db(db_config) as conn:
            # Data aggregation/processing happens here
            market_data = fetch_processed_market_data(conn, args.start_date, args.end_date)

            if market_data.empty:
                print("Error: No market data fetched. Cannot run simulation.")
                return

        # --- 3. Run Simulation ---
        print("\n--- Starting Rolling Intrinsic Simulation ---")
        results_df = strategy.simulate(market_data, initial_soc=args.init_soc)

        # --- 4. Report Results ---
        total_revenue = results_df['revenue_dt'].sum()
        cycles = results_df['optimal_action_mw'].abs().sum() * bess_params['time_step_h'] / (2 * args.capacity)

        print("\n--- Optimization Results ---")
        print(f"Time Period: {args.start_date} to {args.end_date}")
        print(f"Total Gross Revenue: {total_revenue:,.2f} €")
        print(f"Total Equivalent Cycles: {cycles:,.2f}")

        # Optional: Save results to CSV
        results_df.to_csv('ribess_optimization_results.csv')
        print("Results saved to ribess_optimization_results.csv")

    except Exception as e:
        print(f"\nOptimization failed due to an error: {e}")
        # Note: connect_db context manager handles connection closing/rollback


if __name__ == "__main__":
    main()