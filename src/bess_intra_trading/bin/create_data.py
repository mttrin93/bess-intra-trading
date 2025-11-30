import argparse
from bess_intra_trading.data import (
    connect_db,
    setup_table,
    generate_and_insert_fake_transactions,
    load_external_data
)

def main():
    """
    Main entry point for the run_generator command-line script.
    Handles CLI arguments, database connection, and data generation/loading.
    """
    parser = argparse.ArgumentParser(
        description="Utility to generate fake intraday trading data or load external data into the PostgreSQL database."
    )

    # --- Database Connection Arguments ---
    parser.add_argument('--db-name', default='intradaydb', help='PostgreSQL database name.')
    parser.add_argument('--db-user', default='leloq', help='PostgreSQL user.')
    parser.add_argument('--db-password', default='123', help='PostgreSQL password.')
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host.')
    parser.add_argument('--db-port', default='5432', help='PostgreSQL port.')

    # --- Data Generation/Loading Arguments ---
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--num-rows', type=int,
        help='Number of fake transactions to generate (e.g., 1000000).'
    )
    group.add_argument(
        '--file-path', type=str,
        help='Path to an external data file (e.g., CSV) to load instead of generating fake data.'
    )

    args = parser.parse_args()

    # --- Database Connection Setup ---
    db_config = {
        'dbname': args.db_name,
        'user': args.db_user,
        'password': args.db_password,
        'host': args.db_host,
        'port': args.db_port
    }

    try:
        with connect_db(db_config) as conn:
            with conn.cursor() as cur:

                setup_table(cur)

                if args.num_rows is not None:
                    generate_and_insert_fake_transactions(cur, args.num_rows)
                elif args.file_path is not None:
                    load_external_data(cur, args.file_path)

            print("Database transaction committed successfully and connection closed.")

    except Exception as e:
        print(f"\nAn unhandled error occurred during execution: {e}")

if __name__ == "__main__":
    main()