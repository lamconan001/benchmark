# Metrics collector module

import threading
import time
import numpy as np
from datetime import datetime
from storage import insert_benchmark_snapshot

class MetricsCollector:
    def __init__(self, run_id, sqlite_path, snapshot_interval=5, thread_count=1):
        self.run_id = run_id
        self.sqlite_path = sqlite_path
        self.snapshot_interval = snapshot_interval
        self.thread_count = thread_count
        self.latencies = []  # (thread_id, latency)
        self.thread_queries = {}  # thread_id: count
        self.lock = threading.Lock()
        self.snapshots = []
        self._stop_event = threading.Event()
        self._timer = None

    def record(self, latency):
        thread_id = threading.get_ident()
        with self.lock:
            self.latencies.append((thread_id, latency))
            self.thread_queries[thread_id] = self.thread_queries.get(thread_id, 0) + 1

    def start_snapshot_timer(self):
        self._timer = threading.Thread(target=self._snapshot_loop)
        self._timer.start()

    def _snapshot_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.snapshot_interval)
            self.snapshot()

    def stop(self):
        self._stop_event.set()
        if self._timer:
            self._timer.join()
        self.snapshot(final=True)

    def snapshot(self, final=False):
        with self.lock:
            if not self.latencies:
                return
            now = datetime.now()
            total_queries = len(self.latencies)
            latencies_ms = np.array([l[1] * 1000 for l in self.latencies])
            min_latency = float(np.min(latencies_ms))
            max_latency = float(np.max(latencies_ms))
            avg_latency = float(np.mean(latencies_ms))
            p25 = float(np.percentile(latencies_ms, 25))
            p50 = float(np.percentile(latencies_ms, 50))
            p75 = float(np.percentile(latencies_ms, 75))
            qps = total_queries / (self.snapshot_interval if not final else max(1, (self.latencies[-1][1] if self.latencies else 1)))
            queries_per_thread = list(self.thread_queries.values())
            stddev_qpt = float(np.std(queries_per_thread)) if queries_per_thread else 0.0
            # Lưu vào SQLite qua storage
            insert_benchmark_snapshot(self.sqlite_path, self.run_id, now, total_queries, qps, min_latency, max_latency, avg_latency, p25, p50, p75, stddev_qpt)
            # Ghi nhận snapshot vào bộ nhớ (tuỳ chọn)
            self.snapshots.append({
                'time': now,
                'total_queries': total_queries,
                'avg_qps': qps,
                'min_latency': min_latency,
                'max_latency': max_latency,
                'avg_latency': avg_latency,
                'p25': p25,
                'p50': p50,
                'p75': p75,
                'stddev_qpt': stddev_qpt
            })
            # Reset nếu không phải snapshot cuối
            if not final:
                self.latencies.clear()
                self.thread_queries.clear()
