import psycopg2
import time
import sys
import argparse
import base64

def main(table_names, transaction_threshold, sql_template_path, check_interval, timestamp_columns, lookback_seconds, dbname='qdb', user='admin', host='127.0.0.1', port=8812, password='quest', tracking_table=None, tracking_id=None):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port,
        password=password
    )
    cur = conn.cursor()
    
    table_info = {}
    
    # Read the SQL template and encode it in base64
    with open(sql_template_path, 'r') as file:
        sql_template = file.read()
    template64 = base64.b64encode(sql_template.encode('utf-8')).decode('utf-8')
    
    # Initialize tracking state from tracking table if provided
    if tracking_table and tracking_id:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tracking_table} (
                timestamp TIMESTAMP,
                trackingId SYMBOL,
                tableName SYMBOL,
                sequencerTxn LONG,
                template64 VARCHAR
            ) timestamp (timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS (timestamp, trackingId, tableName);
        """)
        conn.commit()
        
        for table in table_names:
            cur.execute(f"""
                SELECT sequencerTxn 
                FROM {tracking_table} 
                WHERE trackingId = '{tracking_id}' AND tableName = '{table}' 
                ORDER BY timestamp DESC LIMIT 1;
            """)
            result = cur.fetchone()
            latest_txn_id = result[0] if result else None
            table_info[table] = {
                'latest_txn_id': latest_txn_id,
                'transaction_count': 0
            }
            print(f"Starting from transaction ID: {latest_txn_id} for table {table}")
    else:
        # Initial query to get the latest transaction ID for each table
        for table in table_names:
            cur.execute(f"SELECT sequencerTxn FROM wal_transactions('{table}') ORDER BY sequencerTxn DESC LIMIT 1")
            latest_txn_id = cur.fetchone()[0]
            table_info[table] = {
                'latest_txn_id': latest_txn_id,
                'transaction_count': 0
            }
            print(f"Starting from transaction ID: {latest_txn_id} for table {table}")

    # Ensure all tables have their starting transaction ID initialized
    for table in table_names:
        if table_info[table]['latest_txn_id'] is None:
            cur.execute(f"SELECT sequencerTxn FROM wal_transactions('{table}') ORDER BY sequencerTxn DESC LIMIT 1")
            latest_txn_id = cur.fetchone()[0]
            table_info[table]['latest_txn_id'] = latest_txn_id
            print(f"Initialized starting transaction ID: {latest_txn_id} for table {table}")

    while True:
        time.sleep(check_interval)
        
        for table, timestamp_col in zip(table_names, timestamp_columns):
            latest_txn_id = table_info[table]['latest_txn_id']
            
            cur.execute(f"""
                SELECT sequencerTxn, timestamp 
                FROM wal_transactions('{table}') 
                WHERE sequencerTxn > {latest_txn_id}
            """)
            new_transactions = cur.fetchall()
            
            if not new_transactions:
                continue

            # Update the number of new transactions
            table_info[table]['transaction_count'] += len(new_transactions)
            
            if table_info[table]['transaction_count'] < transaction_threshold:
                continue

            # Find the earliest transaction timestamp
            earliest_txn_timestamp = min(txn[1] for txn in new_transactions)

            # Replace {timestamp_txn_filter} with the appropriate filters for each table
            timestamp_filters = " AND ".join(
                f"{col} >= dateadd('s', -{lookback_seconds}, '{earliest_txn_timestamp}')"
                for col in timestamp_columns
            )
            
            sql_query = sql_template.replace("{timestamp_txn_filter}", timestamp_filters)
            
            # Execute the query
            cur.execute(sql_query)
            conn.commit()
            print("Executed query:")
            print(sql_query)
        
            # Update the latest transaction IDs and reset the transaction count
            table_info[table]['latest_txn_id'] = new_transactions[-1][0]
            table_info[table]['transaction_count'] = 0
            
            # Update the tracking table
            if tracking_table and tracking_id:
                cur.execute(f"""
                    INSERT INTO {tracking_table} (timestamp, trackingId, tableName, sequencerTxn, template64) 
                    VALUES (NOW(), '{tracking_id}', '{table}', {table_info[table]['latest_txn_id']}, '{template64}');
                """)
                conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitor and aggregate changes in QuestDB tables.')
    parser.add_argument('--table_names', required=True, help='Comma-separated list of table names to monitor.')
    parser.add_argument('--transaction_threshold', type=int, required=True, help='Number of transactions to trigger the materialize query.')
    parser.add_argument('--sql_template_path', required=True, help='Path to the file containing the SQL template.')
    parser.add_argument('--check_interval', type=int, default=30, help='The interval (in seconds) to check for new transactions.')
    parser.add_argument('--timestamp_columns', required=True, help='Comma-separated list of timestamp columns corresponding to each table (format: table_name.column_name).')
    parser.add_argument('--lookback_seconds', type=int, default=5, help='Number of seconds to look back from the earliest transaction timestamp in the batch.')
    parser.add_argument('--tracking_table', help='Name of the tracking table to keep track of processed transactions.')
    parser.add_argument('--tracking_id', help='Tracking ID for this run.')
    parser.add_argument('--dbname', default='qdb', help='The name of the database.')
    parser.add_argument('--user', default='admin', help='The database user.')
    parser.add_argument('--host', default='127.0.0.1', help='The database host.')
    parser.add_argument('--port', type=int, default=8812, help='The database port.')
    parser.add_argument('--password', default='quest', help='The database password.')

    args = parser.parse_args()

    table_names = args.table_names.split(',')
    timestamp_columns = args.timestamp_columns.split(',')

    main(table_names, args.transaction_threshold, args.sql_template_path, args.check_interval, timestamp_columns, args.lookback_seconds, args.dbname, args.user, args.host, args.port, args.password, args.tracking_table, args.tracking_id)

