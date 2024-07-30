
# QuestDB Change Tracker

QuestDB Change Tracker is a Python script designed to monitor and aggregate changes in QuestDB tables. The script leverages the undocumented `wal_transactions` function to track transaction history, detect structural changes, and perform aggregations on specified columns or insert data into a materialized view based on a threshold of new rows.

## Features
- Monitors specified QuestDB tables for changes.
- Aggregates data from new transactions based on specified columns.
- Detects and reports structural changes in the table.
- Outputs aggregated results to stdout.
- Inserts data into a materialized view based on a user-defined SQL template.
- Tracks processing status using a specified QuestDB table to ensure continuity between script runs.

## Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/javier/questdb_change_tracker.git
    cd questdb_change_tracker
    ```

2. Create and activate a virtual environment (optional but recommended):
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. Install the required dependencies:
    ```sh
    pip install psycopg2
    ```

## Change Tracker

### Usage
```sh
python change_tracker.py --table_name <table_name> --columns <columns> [--row_threshold <row_threshold>] [--check_interval <check_interval>] [--timestamp_column <timestamp_column>] [--tracking_table <tracking_table>] [--tracking_id <tracking_id>] [--dbname <dbname>] [--user <user>] [--host <host>] [--port <port>] [--password <password>]
```

### Example
```sh
python change_tracker.py --table_name smart_meters --columns frequency,voltage --row_threshold 100 --check_interval 30 --timestamp_column timestamp --tracking_table materialize_tracker --tracking_id meter_logger
```

### Parameters
- `--table_name`: The name of the table to monitor (required).
- `--columns`: Comma-separated list of columns to aggregate (required).
- `--row_threshold`: The number of rows to trigger aggregation (default: 1000).
- `--check_interval`: The interval (in seconds) to check for new transactions (default: 30).
- `--timestamp_column`: The name of the timestamp column (default: 'timestamp').
- `--tracking_table`: The name of the tracking table (optional).
- `--tracking_id`: The tracking ID for this run (optional).
- `--dbname`: The name of the database (default: 'qdb').
- `--user`: The database user (default: 'admin').
- `--host`: The database host (default: '127.0.0.1').
- `--port`: The database port (default: 8812).
- `--password`: The database password (default: 'quest').

### Output
The script provides the following output:
- Initial transaction ID and structure version.
- Notifications of structure version changes.
- Aggregated results including transaction IDs, total rows, and specified column statistics (first, last, min, max, avg).

### Example Output
```
Starting from transaction ID: 125 with structure version: 1
Structure version changed from 1 to 2 on transaction 127
Aggregated results from 2024-07-29 11:03:03.102658 to 2024-07-29 11:03:33.002031:
Included Transactions: 126 to 129
Total Rows: 300
frequency_first, voltage_first, frequency_last, voltage_last, frequency_min, voltage_min, frequency_max, voltage_max, frequency_avg, voltage_avg
50, 60, 50, 60, 54.6, 216.50205993652344, 132.5439910888672, 110.09369659423828, 239.7596435546875, 176.34308303833006
```

## Materialize View

The Materialize View script allows monitoring one or more tables, and when the specified row thresholds are met, it executes a SQL template provided by the user. The script replaces the `{timestamp_txn_filter}` placeholder in the SQL template with the appropriate timestamp filters based on the transactions. Note that this script is not production-ready as the state is reset upon restart.

### Usage
```sh
python materialize_view.py --table_names <table_names> --thresholds <thresholds> --sql_template_path <sql_template_path> [--check_interval <check_interval>] --timestamp_columns <timestamp_columns> [--tracking_table <tracking_table>] [--tracking_id <tracking_id>] [--dbname <dbname>] [--user <user>] [--host <host>] [--port <port>] [--password <password>]
```

### Example
```sh
python materialize_view.py --table_names smart_meters,trades --thresholds 100,50 --sql_template_path materialize.sql --check_interval 5 --timestamp_columns smart_meters.timestamp,trades.timestamp --tracking_table materialize_tracker --tracking_id meters_and_trades
```

### Parameters
- `--table_names`: Comma-separated list of table names to monitor (required).
- `--thresholds`: Comma-separated list of row thresholds corresponding to each table (required).
- `--sql_template_path`: Path to the file containing the SQL template (required).
- `--check_interval`: The interval (in seconds) to check for new transactions (default: 30).
- `--timestamp_columns`: Comma-separated list of timestamp columns corresponding to each table (format: `table_name.column_name`) (required).
- `--tracking_table`: The name of the tracking table (optional).
- `--tracking_id`: The tracking ID for this run (optional).
- `--dbname`: The name of the database (default: 'qdb').
- `--user`: The database user (default: 'admin').
- `--host`: The database host (default: '127.0.0.1').
- `--port`: The database port (default: 8812).
- `--password`: The database password (default: 'quest').

### SQL Template Example
```sql
INSERT INTO sampled_meters(
  timestamp, device_id, mark_model, 
  first_status, last_status, frequency, energy_consumption, voltage, current, power_factor,
  price
  )
SELECT smart_meters.timestamp, device_id, mark_model, 
      first(status), last(status), 
      avg(frequency), avg(energy_consumption), avg(voltage), avg(current), 
      avg(power_factor), avg(price)
FROM smart_meters ASOF JOIN trades 
WHERE {timestamp_txn_filter} 
SAMPLE BY 10m; 
```

### Example Command Line
```bash
python materialize_view.py --table_names smart_meters,trades --thresholds 100,50 --sql_template_path materialize.sql --check_interval 5 --timestamp_columns smart_meters.timestamp,trades.timestamp --tracking_table materialize_tracker --tracking_id meters_and_trades
```

### Example Output
```
python materialize_view.py --table_names smart_meters,trades --thresholds 100,50 --sql_template_path materialize.sql --check_interval 5 --timestamp_columns smart_meters.timestamp,trades.timestamp --tracking_table materialize_tracker --tracking_id meters_and_trades

Starting from transaction ID: 308 with structure version: 3 for table smart_meters
Starting from transaction ID: 3728 with structure version: 0 for table trades
Executed query:
INSERT INTO sampled_meters(
  timestamp, device_id, mark_model,
  first_status, last_status, frequency, energy_consumption, voltage, current, power_factor,
  price
  )
SELECT smart_meters.timestamp, device_id, mark_model,
      first(status), last(status),
      avg(frequency), avg(energy_consumption), avg(voltage), avg(current),
      avg(power_factor), avg(price)
FROM smart_meters ASOF JOIN trades
WHERE smart_meters.timestamp >= '2024-07-29 14:51:34.144738' AND smart_meters.timestamp <= '2024-07-29 14:52:04.044696' AND trades.timestamp >= '2024-07-29 14:51:37.107452' AND trades.timestamp <= '2024-07-29 14:52:06.804897'
SAMPLE BY 10m;

Executed query:
INSERT INTO sampled_meters(
  timestamp, device_id, mark_model,
  first_status, last_status, frequency, energy_consumption, voltage, current, power_factor,
  price
  )
SELECT smart_meters.timestamp, device_id, mark_model,
      first(status), last(status),
      avg(frequency), avg(energy_consumption), avg(voltage), avg(current),
      avg(power_factor), avg(price)
FROM smart_meters ASOF JOIN trades
WHERE smart_meters.timestamp >= '2024-07-29 14:52:04.142463' AND smart_meters.timestamp <= '2024-07-29 14:52:23.047161' AND trades.timestamp >= '2024-07-29 14:52:06.905866' AND trades.timestamp <= '2024-07-29 14:52:36.885151'
SAMPLE BY 10m;
```

## License
This project is licensed under the Apache License 2.0.
