import psycopg2
import time
import sys
import argparse
import base64

def main(table_names, thresholds, sql_template_path, check_interval, timestamp_columns, dbname='qdb', user='admin', host='127.0.0.1', port=8812, password='quest', tracking_table=None, tracking_id=None):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port,
        password=password
    )
    cur = conn.cursor()
    
    table_info = {}
    
    if tracking_table and tracking_id:
        # Create tracking table if it does not exist
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tracking_table} (
                timestamp TIMESTAMP,
                trackingId SYMBOL,
                tableName SYMBOL,
                sequencerTxn LONG,
                template64 VARCHAR
            ) timestamp (timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS(timestamp, trackingId, tableName);
        """)
        conn.commit()
        
        # Get the latest transaction ID from tracking table
        cur.execute(f"""
            SELECT tableName, sequencerTxn
            FROM {tracking_table}
            WHERE trackingId = '{tracking_id}'
            LATEST ON timestamp
            PARTITION BY tableName;
        """)
        latest_transactions = cur.fetchall()
        
        for table in table_names:
            # Initialize with the latest transaction from tracking table if available
            latest_txn_id = next((txn[1] for txn in latest_transactions if txn[0] == table), None)
            if latest_txn_id:
                cur.execute(f"SELECT structureVersion FROM wal_transactions('{table}') WHERE sequencerTxn = {latest_txn_id} LIMIT 1")
                latest_structure_version = cur.fetchone()[0]
            else:
                cur.execute(f"SELECT sequencerTxn, structureVersion FROM wal_transactions('{table}') ORDER BY sequencerTxn DESC LIMIT 1")
                latest_txn_id, latest_structure_version = cur.fetchone()
            table_info[table] = {
                'latest_txn_id': latest_txn_id,
                'latest_structure_version': latest_structure_version,
                'total_new_rows': 0,
                'min_timestamp': None,
                'max_timestamp': None
            }
            print(f"Starting from transaction ID: {latest_txn_id} with structure version: {latest_structure_version} for table {table}")
    else:
        for table in table_names:
            cur.execute(f"SELECT sequencerTxn, structureVersion FROM wal_transactions('{table}') ORDER BY sequencerTxn DESC LIMIT 1")
            latest_txn_id, latest_structure_version = cur.fetchone()
            table_info[table] = {
                'latest_txn_id': latest_txn_id,
                'latest_structure_version': latest_structure_version,
                'total_new_rows': 0,
                'min_timestamp': None,
                'max_timestamp': None
            }
            print(f"Starting from transaction ID: {latest_txn_id} with structure version: {latest_structure_version} for table {table}")

    while True:
        time.sleep(check_interval)
        
        # Reset new rows count and timestamps for each table
        for table in table_names:
            table_info[table]['total_new_rows'] = 0
            table_info[table]['min_timestamp'] = None
            table_info[table]['max_timestamp'] = None
        
        # Query to get new transactions for each table
        for table, threshold in zip(table_names, thresholds):
            cur.execute(f"""
                SELECT sequencerTxn, minTimestamp, maxTimestamp, rowCount, structureVersion 
                FROM wal_transactions('{table}') 
                WHERE sequencerTxn > {table_info[table]['latest_txn_id']}
            """)
            new_transactions = cur.fetchall()
            
            if not new_transactions:
                continue

            # Check for structure version changes
            for txn in new_transactions:
                if txn[4] != table_info[table]['latest_structure_version']:
                    print(f"Structure version changed from {table_info[table]['latest_structure_version']} to {txn[4]} on transaction {txn[0]} for table {table}")
                    table_info[table]['latest_structure_version'] = txn[4]

            # Aggregate the number of rows in new transactions, ignoring None values
            table_info[table]['total_new_rows'] = sum(txn[3] for txn in new_transactions if txn[3] is not None)
            
            if table_info[table]['total_new_rows'] < threshold:
                continue

            # Find the min and max timestamps from new transactions, ignoring None values
            table_info[table]['min_timestamp'] = min((txn[1] for txn in new_transactions if txn[1] is not None), default=None)
            table_info[table]['max_timestamp'] = max((txn[2] for txn in new_transactions if txn[2] is not None), default=None)

        # Check if any table met the threshold
        if any(table_info[table]['total_new_rows'] >= threshold for table, threshold in zip(table_names, thresholds)):
            # Load the SQL template
            with open(sql_template_path, 'r') as file:
                sql_template = file.read()
            
            # Replace {timestamp_txn_filter} with the appropriate filters for each table
            timestamp_filters = " AND ".join(
                f"{col} >= '{table_info[table]['min_timestamp']}' AND {col} <= '{table_info[table]['max_timestamp']}'"
                for table, col in zip(table_names, timestamp_columns)
                if table_info[table]['min_timestamp'] is not None and table_info[table]['max_timestamp'] is not None
            )
            
            sql_query = sql_template.replace("{timestamp_txn_filter}", timestamp_filters)
            
            # Execute the query
            cur.execute(sql_query)
            conn.commit()
            print("Executed query:")
            print(sql_query)

            # Update tracking table with the latest transactions
            if tracking_table and tracking_id:
                timestamp_now = time.strftime('%Y-%m-%dT%H:%M:%S')
                template64 = base64.b64encode(sql_template.encode()).decode()
                for table in table_names:
                    cur.execute(f"""
                        INSERT INTO {tracking_table} (timestamp, trackingId, tableName, sequencerTxn, template64)
                        VALUES ('{timestamp_now}', '{tracking_id}', '{table}', {table_info[table]['latest_txn_id']}, '{template64}')
                    """)
                conn.commit()
        
        # Update the latest transaction IDs
        for table in table_names:
            cur.execute(f"SELECT MAX(sequencerTxn) FROM wal_transactions('{table}')")
            table_info[table]['latest_txn_id'] = cur.fetchone()[0]

    cur.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitor and aggregate changes in QuestDB tables.')
    parser.add_argument('--table_names', required=True, help='Comma-separated list of table names to monitor.')
    parser.add_argument('--thresholds', required=True, help='Comma-separated list of row thresholds corresponding to each table.')
    parser.add_argument('--sql_template_path', required=True, help='Path to the file containing the SQL template.')
    parser.add_argument('--check_interval', type=int, default=30, help='The interval (in seconds) to check for new transactions.')
    parser.add_argument('--timestamp_columns', required=True, help='Comma-separated list of timestamp columns corresponding to each table (format: table_name.column_name).')
    parser.add_argument('--dbname', default='qdb', help='The name of the database.')
    parser.add_argument('--user', default='admin', help='The database user.')
    parser.add_argument('--host', default='127.0.0.1', help='The database host.')
    parser.add_argument('--port', type=int, default=8812, help='The database port.')
    parser.add_argument('--password', default='quest', help='The database password.')
    parser.add_argument('--tracking_table', help='The name of the tracking table.')
    parser.add_argument('--tracking_id', help='The tracking ID for this run.')

    args = parser.parse_args()

    table_names = args.table_names.split(',')
    thresholds = list(map(int, args.thresholds.split(',')))
    timestamp_columns = args.timestamp_columns.split(',')

    main(table_names, thresholds, args.sql_template_path, args.check_interval, timestamp_columns, args.dbname, args.user, args.host, args.port, args.password, args.tracking_table, args.tracking_id)

