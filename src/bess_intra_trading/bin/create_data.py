import argparse
import sys
from bess_intra_trading.data import (
    connect_db,
    setup_table,
    generate_and_insert_fake_transactions,
    load_external_data
)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Utility to generate fake intraday trading data or load external data into the PostgreSQL database."
    )

    # --- Database Connection Arguments ---
    parser.add_argument(
        '--db-name',
        default='intradaydb',
        help='PostgreSQL database name.'
    )

    parser.add_argument(
        '--db-user',
        default='leloq',
        help='PostgreSQL user.'
    )

    parser.add_argument(
        '--db-password',
        default='123',
        help='PostgreSQL password.'
    )

    parser.add_argument(
        '--db-host',
        default='localhost',
        help='PostgreSQL host.'
    )

    parser.add_argument(
        '--db-port',
        default='5432',
        help='PostgreSQL port.'
    )

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

    args_parse = parser.parse_args(args)

    # --- Database Connection Setup ---
    db_config = {
        'dbname': args_parse.db_name,
        'user': args_parse.db_user,
        'password': args_parse.db_password,
        'host': args_parse.db_host,
        'port': args_parse.db_port
    }

    try:
        with connect_db(db_config) as conn:
            with conn.cursor() as cur:

                setup_table(cur)

                if args_parse.num_rows is not None:
                    generate_and_insert_fake_transactions(cur, args_parse.num_rows)
                elif args_parse.file_path is not None:
                    load_external_data(cur, args_parse.file_path)

            print("Database transaction committed successfully and connection closed.")

    except Exception as e:
        print(f"\nAn unhandled error occurred during execution: {e}")


if __name__ == "__main__":
    main(sys.argv[1:])
