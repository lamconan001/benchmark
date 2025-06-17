from generator import prepare_data
from db import DBExecutor
from metrics import MetricsCollector
from storage import create_benchmark_run, update_benchmark_run_end
from visualizer import visualize as do_visualize
from datetime import datetime
import threading
from runner import run_benchmark

# Benchmark coordinator module
class BenchmarkCoordinator:
    def __init__(self, sqlite_path='results.db'):
        self.sqlite_path = sqlite_path

    def prepare(self, schema, rows, db_type, db_host, db_port, db_user, db_password, db_name):
        return prepare_data(schema, rows, db_type, db_host, db_port, db_user, db_password, db_name)

    def run(self, threads, duration, test_type, report_interval, db_type, db_host, db_port, db_user, db_password, db_name, schema, is_cluster=False, cluster_nodes=None):
        if is_cluster:
            # Chỉ cho phép truy vấn đọc
            if test_type not in ['read', 'readpk', 'readfull']:
                raise ValueError('Chỉ hỗ trợ test_type là truy vấn đọc khi kiểm thử cụm database.')
            # Tạo danh sách DBExecutor cho từng node
            executors = []
            for node in cluster_nodes:
                host, port = node.split(':')
                if db_type == 'mysql':
                    engine_url = f'mysql+pymysql://{db_user}:{db_password}@{host}:{port}/{db_name}'
                else:
                    engine_url = f'postgresql+psycopg2://{db_user}:{db_password}@{host}:{port}/{db_name}'
                executors.append(DBExecutor(engine_url))
            # Phân phối round-robin theo thread
            def get_rows_func(table, pk_col):
                # Lấy pk từ node đầu tiên
                return executors[0].get_pk_values(table, pk_col)
            def make_exec_func(idx):
                def exec_func(sql, params=None):
                    executors[idx % len(executors)].exec_query(sql, params)
                return exec_func
            run_id, start_time = create_benchmark_run(self.sqlite_path, db_type+"-cluster", test_type, threads, duration, schema)
            metrics = MetricsCollector(run_id, self.sqlite_path, snapshot_interval=report_interval, thread_count=threads)
            # Tạo list exec_func cho từng thread
            exec_funcs = [make_exec_func(i) for i in range(threads)]
            def print_snapshot_loop():
                last_printed = 0
                while not getattr(metrics, '_stop_event', threading.Event()).is_set():
                    import time
                    time.sleep(report_interval)
                    if metrics.snapshots and len(metrics.snapshots) > last_printed:
                        snap = metrics.snapshots[-1]
                        print(f"\n--- Snapshot tại {snap['time']} ---")
                        print(f"Tổng truy vấn: {snap['total_queries']}")
                        print(f"QPS trung bình: {snap['avg_qps']:.2f}")
                        print(f"Latency (ms): min={snap['min_latency']:.2f}, max={snap['max_latency']:.2f}, avg={snap['avg_latency']:.2f}")
                        print(f"Percentile: p25={snap['p25']:.2f}, p50={snap['p50']:.2f}, p75={snap['p75']:.2f}")
                        print(f"Độ lệch chuẩn queries/thread: {snap['stddev_qpt']:.2f}")
                        last_printed = len(metrics.snapshots)
            printer_thread = threading.Thread(target=print_snapshot_loop, daemon=True)
            printer_thread.start()
            # Chạy benchmark với exec_func riêng cho từng thread
            from runner import run_benchmark_cluster
            run_benchmark_cluster(schema, get_rows_func, exec_funcs, test_type, threads, duration, metrics)
            metrics._stop_event.set()
            printer_thread.join(timeout=2)
            update_benchmark_run_end(self.sqlite_path, run_id, datetime.now())
            if metrics.snapshots:
                last = metrics.snapshots[-1]
                print(f'\n--- Kết quả snapshot cuối cùng ---')
                print(f"Tổng truy vấn: {last['total_queries']}")
                print(f"QPS trung bình: {last['avg_qps']:.2f}")
                print(f"Latency (ms): min={last['min_latency']:.2f}, max={last['max_latency']:.2f}, avg={last['avg_latency']:.2f}")
                print(f"Percentile: p25={last['p25']:.2f}, p50={last['p50']:.2f}, p75={last['p75']:.2f}")
                print(f"Độ lệch chuẩn queries/thread: {last['stddev_qpt']:.2f}")
            return run_id
        else:
            if db_type == 'mysql':
                engine_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
            else:
                engine_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
            run_id, start_time = create_benchmark_run(self.sqlite_path, db_type, test_type, threads, duration, schema)
            metrics = MetricsCollector(run_id, self.sqlite_path, snapshot_interval=report_interval, thread_count=threads)
            db_executor = DBExecutor(engine_url)
            get_rows_func = db_executor.get_pk_values
            exec_func = db_executor.exec_query
            def print_snapshot_loop():
                last_printed = 0
                while not getattr(metrics, '_stop_event', threading.Event()).is_set():
                    import time
                    time.sleep(report_interval)
                    if metrics.snapshots and len(metrics.snapshots) > last_printed:
                        snap = metrics.snapshots[-1]
                        print(f"\n--- Snapshot tại {snap['time']} ---")
                        print(f"Tổng truy vấn: {snap['total_queries']}")
                        print(f"QPS trung bình: {snap['avg_qps']:.2f}")
                        print(f"Latency (ms): min={snap['min_latency']:.2f}, max={snap['max_latency']:.2f}, avg={snap['avg_latency']:.2f}")
                        print(f"Percentile: p25={snap['p25']:.2f}, p50={snap['p50']:.2f}, p75={snap['p75']:.2f}")
                        print(f"Độ lệch chuẩn queries/thread: {snap['stddev_qpt']:.2f}")
                        last_printed = len(metrics.snapshots)
            printer_thread = threading.Thread(target=print_snapshot_loop, daemon=True)
            printer_thread.start()
            run_benchmark(schema, get_rows_func, exec_func, test_type, threads, duration, metrics)
            metrics._stop_event.set()
            printer_thread.join(timeout=2)
            update_benchmark_run_end(self.sqlite_path, run_id, datetime.now())
            if metrics.snapshots:
                last = metrics.snapshots[-1]
                print(f'\n--- Kết quả snapshot cuối cùng ---')
                print(f"Tổng truy vấn: {last['total_queries']}")
                print(f"QPS trung bình: {last['avg_qps']:.2f}")
                print(f"Latency (ms): min={last['min_latency']:.2f}, max={last['max_latency']:.2f}, avg={last['avg_latency']:.2f}")
                print(f"Percentile: p25={last['p25']:.2f}, p50={last['p50']:.2f}, p75={last['p75']:.2f}")
                print(f"Độ lệch chuẩn queries/thread: {last['stddev_qpt']:.2f}")
            return run_id

    def visualize(self, run_ids, sqlite_path=None, run_names=None):
        path = sqlite_path if sqlite_path else self.sqlite_path
        do_visualize(path, run_ids, run_names=run_names)
