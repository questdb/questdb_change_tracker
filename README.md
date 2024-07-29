
# QuestDB Change Tracker

QuestDB Change Tracker is a Python script designed to monitor and aggregate changes in a QuestDB table. The script leverages the undocumented `wal_transactions` function to track transaction history, detect structural changes, and perform aggregations on specified columns once a threshold of new rows is met.

## Features
- Monitors specified QuestDB table for changes.
- Aggregates data from new transactions based on specified columns.
- Detects and reports structural changes in the table.
- Outputs aggregated results to stdout.

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

## Usage
```sh
python change_tracker.py --table_name <table_name> --columns <columns> [--row_threshold <row_threshold>] [--check_interval <check_interval>] [--timestamp_column <timestamp_column>] [--dbname <dbname>] [--user <user>] [--host <host>] [--port <port>] [--password <password>]
```

### Example
```sh
python change_tracker.py --table_name smart_meters --columns frequency,voltage --row_threshold 100 --check_interval 30 --timestamp_column timestamp
```

### Parameters
- `--table_name`: The name of the table to monitor (required).
- `--columns`: Comma-separated list of columns to aggregate (required).
- `--row_threshold`: The number of rows to trigger aggregation (default: 1000).
- `--check_interval`: The interval (in seconds) to check for new transactions (default: 30).
- `--timestamp_column`: The name of the timestamp column (default: 'timestamp').
- `--dbname`: The name of the database (default: 'qdb').
- `--user`: The database user (default: 'admin').
- `--host`: The database host (default: '127.0.0.1').
- `--port`: The database port (default: 8812).
- `--password`: The database password (default: 'quest').

## Output
The script provides the following output:
- Initial transaction ID and structure version.
- Notifications of structure version changes.
- Aggregated results including transaction IDs, total rows, and specified column statistics (first, last, min, max, avg).

## Example
### Example command line
```bash
python change_tracker.py --table_name smart_meters --columns frequency,voltage --row_threshold 100 --check_interval 30 --timestamp_column timestamp
```

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

## License
This project is licensed under the Apache License 2.0.
