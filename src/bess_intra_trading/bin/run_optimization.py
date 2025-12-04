import argparse

from bess_intra_trading.strategy import RollingIntrinsicStrategy
from bess_intra_trading.data import connect_db
import pandas as pd

def main():
    parser = argparse.ArgumentParser(
        description="Runs the Rolling Intrinsic BESS Optimization Strategy."
    )

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

    parser.add_argument(
        '--db-name',
        default='intradaydb',
        help='PostgreSQL database name.'
    )

    args = parser.parse_args()

    # Setup BESS and Strategy
    bess_params = {
        'capacity_mwh': args.capacity,
        'min_soc': args.capacity * 0.1,  # Assuming 10% minimum SoC
        'max_soc': args.capacity * 0.9,  # Assuming 90% maximum SoC
        'max_power_mw': args.power,
        'efficiency': args.efficiency,
        'time_step_h': 15,  # 15 minutes
    }

    strategy = RollingIntrinsicStrategy(bess_params=bess_params)

    # Database Connection and Data Fetch
    db_config = {'dbname': args.db_name, 'user': 'leloq', 'password': '123', 'host': 'localhost', 'port': '5432'}

    # CONNECTION_ALCHEMY = f"postgresql://leloq{password_for_url}@127.0.0.1/intradaydb"
    # conn_alchemy = create_engine(CONNECTION_ALCHEMY)

    try:
        with connect_db(db_config) as conn:

            df = pd.read_sql("SELECT * FROM transactions_intraday_de;", conn)
            print(df)

            # Run Simulation
            print("\n--- Starting Rolling Intrinsic Simulation ---")
            strategy.simulate(
                conn=conn,
                start_date=pd.to_datetime(args.start_date),
                end_date=pd.to_datetime(args.end_date),
                initial_soc=args.init_soc
            )

    #     # --- 4. Report Results ---
    #     total_revenue = results_df['revenue_dt'].sum()
    #     cycles = results_df['optimal_action_mw'].abs().sum() * bess_params['time_step_h'] / (2 * args.capacity)
    #
    #     print("\n--- Optimization Results ---")
    #     print(f"Time Period: {args.start_date} to {args.end_date}")
    #     print(f"Total Gross Revenue: {total_revenue:,.2f} €")
    #     print(f"Total Equivalent Cycles: {cycles:,.2f}")
    #
    #     # Optional: Save results to CSV
    #     results_df.to_csv('ribess_optimization_results.csv')
    #     print("Results saved to ribess_optimization_results.csv")

    except Exception as e:
        print(f"\nOptimization failed due to an error: {e}")
        # Note: connect_db context manager handles connection closing/rollback


if __name__ == "__main__":
    main()
