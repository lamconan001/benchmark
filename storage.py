# Storage manager module

import sqlite3
from datetime import datetime

def create_benchmark_run(sqlite_path, db_type, test_type, num_threads, duration_secs, table_schema):
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS benchmark_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        db_type TEXT,
        test_type TEXT,
        num_threads INTEGER,
        duration_secs INTEGER,
        table_schema TEXT,
        start_time DATETIME,
        end_time DATETIME
    )''')
    start_time = datetime.now()
    c.execute('''INSERT INTO benchmark_runs (db_type, test_type, num_threads, duration_secs, table_schema, start_time)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (db_type, test_type, num_threads, duration_secs, table_schema, start_time))
    run_id = c.lastrowid
    conn.commit()
    conn.close()
    return run_id, start_time

def update_benchmark_run_end(sqlite_path, run_id, end_time):
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('UPDATE benchmark_runs SET end_time = ? WHERE id = ?', (end_time, run_id))
    conn.commit()
    conn.close()

def create_benchmark_snapshots_table(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS benchmark_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        snapshot_time DATETIME,
        total_queries INTEGER,
        avg_qps REAL,
        min_latency_ms REAL,
        max_latency_ms REAL,
        avg_latency_ms REAL,
        p25_latency_ms REAL,
        p50_latency_ms REAL,
        p75_latency_ms REAL,
        stddev_queries_per_thread REAL,
        FOREIGN KEY(run_id) REFERENCES benchmark_runs(id)
    )''')
    conn.commit()
    conn.close()

def insert_benchmark_snapshot(sqlite_path, run_id, now, total_queries, qps, min_latency, max_latency, avg_latency, p25, p50, p75, stddev_qpt):
    create_benchmark_snapshots_table(sqlite_path)
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('''INSERT INTO benchmark_snapshots (
        run_id, snapshot_time, total_queries, avg_qps, min_latency_ms, max_latency_ms, avg_latency_ms,
        p25_latency_ms, p50_latency_ms, p75_latency_ms, stddev_queries_per_thread
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (run_id, now, total_queries, qps, min_latency, max_latency, avg_latency, p25, p50, p75, stddev_qpt))
    conn.commit()
    conn.close()
