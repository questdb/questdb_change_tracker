import psycopg2
import time
import sys
import argparse

def main(table_name, columns, dbname='qdb', user='admin', host='127.0.0.1', port=8812, password='quest', row_threshold=1000, check_interval=30, timestamp_column='timestamp'):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port,
        password=password
    )
    cur = conn.cursor()
    
    # Initial query to get the latest transaction ID and structure version
    cur.execute(f"SELECT sequencerTxn, structureVersion FROM wal_transactions('{table_name}') ORDER BY sequencerTxn DESC LIMIT 1")
    latest_txn_id, latest_structure_version = cur.fetchone()
    print(f"Starting from transaction ID: {latest_txn_id} with structure version: {latest_structure_version}")

    while True:
        time.sleep(check_interval)
        
        # Query to get new transactions
        cur.execute(f"""
            SELECT sequencerTxn, minTimestamp, maxTimestamp, rowCount, structureVersion 
            FROM wal_transactions('{table_name}') 
            WHERE sequencerTxn > {latest_txn_id}
        """)
        new_transactions = cur.fetchall()
        
        if not new_transactions:
            continue

        # Check for structure version changes
        for txn in new_transactions:
            if txn[4] != latest_structure_version:
                print(f"Structure version changed from {latest_structure_version} to {txn[4]} on transaction {txn[0]}")
                latest_structure_version = txn[4]

        # Aggregate the number of rows in new transactions, ignoring None values
        total_new_rows = sum(txn[3] for txn in new_transactions if txn[3] is not None)
        
        if total_new_rows < row_threshold:
            continue

        # Find the min and max timestamps from new transactions, ignoring None values
        min_timestamp = min((txn[1] for txn in new_transactions if txn[1] is not None), default=None)
        max_timestamp = max((txn[2] for txn in new_transactions if txn[2] is not None), default=None)

        if min_timestamp is None or max_timestamp is None:
            continue

        # Construct the aggregation query
        column_list = columns.split(',')
        aggregations = ', '.join([
            f"first({col}) AS {col}_first, last({col}) AS {col}_last, min({col}) AS {col}_min, max({col}) AS {col}_max, avg({col}) AS {col}_avg"
            for col in column_list
        ])
        query = f"""
        SELECT {aggregations}
        FROM {table_name}
        WHERE {timestamp_column} BETWEEN '{min_timestamp}' AND '{max_timestamp}'
        """

        # Execute the aggregation query
        cur.execute(query)
        results = cur.fetchone()
        
        # Output the results
        print(f"Aggregated results from {min_timestamp} to {max_timestamp}:")
        print(f"Included Transactions: {new_transactions[0][0]} to {new_transactions[-1][0]}")
        print(f"Total Rows: {total_new_rows}")

        # Print column headers
        headers = [f"{col}_first" for col in column_list] + \
                  [f"{col}_last" for col in column_list] + \
                  [f"{col}_min" for col in column_list] + \
                  [f"{col}_max" for col in column_list] + \
                  [f"{col}_avg" for col in column_list]
        print(", ".join(headers))
        
        # Print the results
        print(", ".join(map(str, results)))
        
        # Update the latest transaction ID
        latest_txn_id = new_transactions[-1][0]

    cur.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitor and aggregate changes in a QuestDB table.')
    parser.add_argument('--table_name', required=True, help='The name of the table to monitor.')
    parser.add_argument('--row_threshold', type=int, default=1000, help='The number of rows to trigger aggregation.')
    parser.add_argument('--check_interval', type=int, default=30, help='The interval (in seconds) to check for new transactions.')
    parser.add_argument('--columns', required=True, help='Comma-separated list of columns to aggregate.')
    parser.add_argument('--timestamp_column', default='timestamp', help='The name of the timestamp column.')
    parser.add_argument('--dbname', default='qdb', help='The name of the database.')
    parser.add_argument('--user', default='admin', help='The database user.')
    parser.add_argument('--host', default='127.0.0.1', help='The database host.')
    parser.add_argument('--port', type=int, default=8812, help='The database port.')
    parser.add_argument('--password', default='quest', help='The database password.')

    args = parser.parse_args()

    main(args.table_name, args.columns, args.dbname, args.user, args.host, args.port, args.password, args.row_threshold, args.check_interval, args.timestamp_column)

