# Data generator module

import re
import random
import string
import datetime
import numpy as np
from sqlalchemy import create_engine, text

def parse_create_table(schema_sql):
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

def random_value(col_type):
    if 'INT' in col_type:
        return random.randint(1, 100000)
    if 'CHAR' in col_type or 'TEXT' in col_type:
        return ''.join(random.choices(string.ascii_letters, k=10))
    if 'TIMESTAMP' in col_type or 'DATE' in col_type:
        return datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))
    if 'FLOAT' in col_type or 'DOUBLE' in col_type or 'REAL' in col_type:
        return float(np.random.uniform(1, 100000))
    return None

def generate_data(columns, n_rows):
    data = []
    for _ in range(n_rows):
        row = [random_value(col_type) for _, col_type in columns]
        data.append(row)
    return data

def insert_data(engine_url, table_name, columns, data):
    engine = create_engine(engine_url)
    col_names = ','.join([col[0] for col in columns])
    placeholders = ','.join([':{}'.format(i) for i in range(len(columns))])
    insert_sql = f'INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})'
    with engine.begin() as conn:
        for batch_start in range(0, len(data), 1000):
            batch = data[batch_start:batch_start+1000]
            params = [{str(i): v for i, v in enumerate(row)} for row in batch]
            conn.execute(text(insert_sql), params)

def prepare_data(schema_sql, n_rows, db_type, db_host, db_port, db_user, db_password, db_name):
    columns = parse_create_table(schema_sql)
    data = generate_data(columns, n_rows)
    table_name = re.search(r'CREATE TABLE\s+([`\w]+)', schema_sql, re.IGNORECASE).group(1).strip('`')
    if db_type == 'mysql':
        engine_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    else:
        engine_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    # Tạo bảng
    engine = create_engine(engine_url)
    with engine.begin() as conn:
        try:
            conn.execute(text(schema_sql))
        except Exception as e:
            if 'already exists' in str(e):
                pass  # Bảng đã tồn tại, bỏ qua tạo bảng
            else:
                raise
    # Insert dữ liệu
    insert_data(engine_url, table_name, columns, data)
    return table_name, len(data)
