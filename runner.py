# Test runner module

import re
import threading
import time
import random


def parse_columns(schema_sql):
    """Phân tích lệnh CREATE TABLE và trả về danh sách cột và kiểu dữ liệu."""
    columns = []
    pattern = r'\((.*)\)'  # Lấy phần trong ngoặc
    match = re.search(pattern, schema_sql, re.DOTALL)
    if not match:
        raise ValueError('Không tìm thấy định nghĩa cột trong CREATE TABLE')
    cols_def = match.group(1)
    for line in cols_def.split(','):
        line = line.strip()
        if not line or line.upper().startswith(('PRIMARY', 'UNIQUE', 'KEY', 'CONSTRAINT')):
            continue
        col_match = re.match(r'([`\w]+)\s+([A-Z\w\(\)]+)', line, re.IGNORECASE)
        if col_match:
            col_name, col_type = col_match.groups()
            if 'AUTO_INCREMENT' in line.upper() or 'SERIAL' in line.upper():
                continue
            columns.append((col_name.strip('`'), col_type.upper()))
    return columns

def get_primary_key(schema_sql):
    """
    Trả về tên cột primary key đầu tiên (ưu tiên inline, nếu không có thì lấy table-level).
    Nếu là composite key, trả về tên cột đầu tiên.
    """
    sql = schema_sql.strip()
    start = sql.find('(')
    end = sql.rfind(')')
    if start < 0 or end < 0 or end <= start:
        return None
    body = sql[start+1:end]
    # 1) inline PRIMARY KEY
    inline_pattern = re.compile(
        r'\b([A-Za-z_][A-Za-z0-9_]*)\b'      # tên cột
        r'\s+[^,]*?'                         # mọi thứ đến dấu phẩy (nhưng không qua dấu phẩy đầu tiên)
        r'\s+PRIMARY\s+KEY\b', 
        re.IGNORECASE
    )
    inline_keys = inline_pattern.findall(body)
    # 2) table-level PRIMARY KEY(k1, k2, …)
    table_pattern = re.compile(
        r'PRIMARY\s+KEY\s*\(\s*([A-Za-z0-9_,\s]+?)\s*\)',
        re.IGNORECASE
    )
    table_defs = table_pattern.findall(body)
    table_keys = []
    for grp in table_defs:
        cols = [c.strip() for c in grp.split(',') if c.strip()]
        table_keys.extend(cols)
    # hợp nhất kết quả, giữ thứ tự xuất hiện
    seen = set()
    result = []
    for col in inline_keys + table_keys:
        col_norm = col.strip()
        if col_norm.upper() not in seen:
            seen.add(col_norm.upper())
            result.append(col_norm)
    return result[0] if result else None

def get_pk_range(get_rows_func, table, pk_col):
    if pk_col is None:
        return []
    # get_rows_func là hàm do db.py cung cấp, trả về list giá trị pk
    return get_rows_func(table, pk_col)

def run_select_pk(exec_func, table, pk_col, pk_range):
    pk_val = random.choice(pk_range)
    sql = f"SELECT * FROM {table} WHERE {pk_col} = :pk"
    params = {'pk': pk_val}
    start = time.perf_counter()
    exec_func(sql, params)
    latency = time.perf_counter() - start
    return latency

def run_select_full(exec_func, table):
    sql = f"SELECT * FROM {table}"
    start = time.perf_counter()
    exec_func(sql)
    latency = time.perf_counter() - start
    return latency

def run_insert(exec_func, table, columns):
    values = []
    for _, t in columns:
        t_upper = t.upper()
        if 'INT' in t_upper:
            values.append(random.randint(1, 100000))
        elif 'CHAR' in t_upper or 'TEXT' in t_upper or 'CLOB' in t_upper:
            values.append('randstr_' + str(random.randint(1, 100000)))
        elif 'FLOAT' in t_upper or 'DOUBLE' in t_upper or 'REAL' in t_upper or 'DECIMAL' in t_upper or 'NUMERIC' in t_upper:
            values.append(random.uniform(1, 100000))
        elif 'DATE' in t_upper or 'TIME' in t_upper or 'DATETIME' in t_upper or 'TIMESTAMP' in t_upper:
            day = random.randint(1, 28)
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            values.append(f'2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d}')
        elif 'BOOL' in t_upper:
            values.append(random.choice([True, False]))
        else:
            values.append(None)
    col_names = ','.join([c for c, _ in columns])
    placeholders = ','.join([f':{i}' for i in range(len(columns))])
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
    params = {str(i): v for i, v in enumerate(values)}
    start = time.perf_counter()
    exec_func(sql, params)
    latency = time.perf_counter() - start
    return latency

def run_update(exec_func, table, pk_col, columns, pk_range):
    pk_val = random.choice(pk_range)
    # Chọn cột không phải PK để update
    for c, _ in columns:
        if c != pk_col:
            update_col = c
            break
    else:
        return 0
    sql = f"UPDATE {table} SET {update_col} = :val WHERE {pk_col} = :pk"
    params = {'val': random.randint(1, 100000), 'pk': pk_val}
    start = time.perf_counter()
    exec_func(sql, params)
    latency = time.perf_counter() - start
    return latency

def worker(exec_func, table, columns, pk_col, pk_range, test_type, duration, metrics_collector):
    end_time = time.time() + duration
    while time.time() < end_time:
        if test_type == 'readpk':
            latency = run_select_pk(exec_func, table, pk_col, pk_range)
        elif test_type == 'readfull':
            latency = run_select_full(exec_func, table)
        elif test_type == 'read':
            if random.random() < 0.5:
                latency = run_select_full(exec_func, table)
            else:
                latency = run_select_pk(exec_func, table, pk_col, pk_range)
        elif test_type == 'insert':
            latency = run_insert(exec_func, table, columns)
        elif test_type == 'update':
            latency = run_update(exec_func, table, pk_col, columns, pk_range)
        elif test_type == 'write':
            if random.random() < 0.5:
                latency = run_insert(exec_func, table, columns)
            else:
                latency = run_update(exec_func, table, pk_col, columns, pk_range)
        elif test_type == 'mix':
            r = random.random()
            if r < 0.1:
                latency = run_insert(exec_func, table, columns)
            elif r < 0.2:
                latency = run_update(exec_func, table, pk_col, columns, pk_range)
            elif r < 0.6:
                latency = run_select_full(exec_func, table)
            else:
                latency = run_select_pk(exec_func, table, pk_col, pk_range)
        else:
            print(f"Unknown test type: {test_type}")
        metrics_collector.record(latency)

def run_benchmark(schema_sql, get_rows_func, exec_func, test_type, threads, duration, metrics_collector):
    columns = parse_columns(schema_sql)
    table = re.search(r'CREATE TABLE\s+([`\w]+)', schema_sql, re.IGNORECASE).group(1).strip('`')
    pk_col = get_primary_key(schema_sql)
    pk_range = get_pk_range(get_rows_func, table, pk_col) if pk_col else []
    thread_list = []
    metrics_collector.start_snapshot_timer()
    for _ in range(threads):
        t = threading.Thread(target=worker, args=(exec_func, table, columns, pk_col, pk_range, test_type, duration, metrics_collector))
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()
    metrics_collector.stop()
