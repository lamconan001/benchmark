# CLI interface module

import click
import time
import threading
from datetime import datetime
from storage import create_benchmark_run, update_benchmark_run_end
from coordinator import BenchmarkCoordinator

@click.group()
def cli():
    """Benchmark CLI for PostgreSQL/MySQL."""
    pass

@cli.command()
@click.option('--schema', prompt='CREATE TABLE statement', help='Câu lệnh CREATE TABLE.')
@click.option('--rows', prompt='Số dòng dữ liệu', type=int, help='Số dòng dữ liệu cần sinh.')
@click.option('--db-type', prompt='Loại DB (mysql/postgres)', type=click.Choice(['mysql', 'postgres']), help='Chọn loại DB.')
@click.option('--db-host', prompt='DB Host', help='Địa chỉ host của DB.')
@click.option('--db-port', prompt='DB Port', type=int, help='Cổng DB.')
@click.option('--db-user', prompt='DB User', help='Tên user DB.')
@click.option('--db-password', prompt=True, hide_input=True, help='Mật khẩu DB.')
@click.option('--db-name', prompt='Tên database', help='Tên database.')
def prepare(schema, rows, db_type, db_host, db_port, db_user, db_password, db_name):
    """Chuẩn bị dữ liệu kiểm thử."""
    click.echo('Chuẩn bị dữ liệu...')
    coordinator = BenchmarkCoordinator()
    try:
        table_name, inserted = coordinator.prepare(schema, rows, db_type, db_host, db_port, db_user, db_password, db_name)
        click.echo(f'Đã tạo bảng {table_name} và sinh {inserted} dòng dữ liệu.')
    except Exception as e:
        click.echo(f'Lỗi khi chuẩn bị dữ liệu: {e}', err=True)

@cli.command()
@click.option('--threads', prompt='Số luồng', type=int, help='Số luồng kiểm thử.')
@click.option('--duration', prompt='Thời gian chạy (giây)', type=int, help='Thời gian chạy benchmark.')
@click.option('--test-type', prompt='Loại bài kiểm thử', type=click.Choice(['read', 'readpk', 'readfull', 'write', 'insert', 'update', 'mix']), help='Chọn loại bài kiểm thử.')
@click.option('--report-interval', prompt='Thời gian xuất báo cáo (giây)', type=int, help='Thời gian xuất báo cáo định kỳ.')
@click.option('--db-type', prompt='Loại DB (mysql/postgres)', type=click.Choice(['mysql', 'postgres']), help='Chọn loại DB.')
@click.option('--db-host', prompt='DB Host', help='Địa chỉ host của DB.')
@click.option('--db-port', prompt='DB Port', type=int, help='Cổng DB.')
@click.option('--db-user', prompt='DB User', help='Tên user DB.')
@click.option('--db-password', prompt=True, hide_input=True, help='Mật khẩu DB.')
@click.option('--db-name', prompt='Tên database', help='Tên database.')
@click.option('--schema', prompt='CREATE TABLE statement', help='Câu lệnh CREATE TABLE.')
@click.option('--sqlite-path', default='results.db', help='Đường dẫn file SQLite để lưu metrics.')
@click.option('--is-cluster', is_flag=True, default=False, help='Kiểm thử trên cụm database.')
@click.option('--cluster-nodes', default='', help='Danh sách các node trong cụm (ip:port, cách nhau bởi dấu phẩy).')
def run(threads, duration, test_type, report_interval, db_type, db_host, db_port, db_user, db_password, db_name, schema, sqlite_path, is_cluster, cluster_nodes):
    """Chạy benchmark."""
    if is_cluster:
        if test_type not in ['read', 'readpk', 'readfull']:
            click.echo('Chỉ hỗ trợ test_type là truy vấn đọc khi kiểm thử cụm database.', err=True)
            return
        if not cluster_nodes.strip():
            click.echo('Vui lòng nhập --cluster-nodes khi kiểm thử cụm mà không dùng load-balancer.', err=True)
            return
        node_list = [n.strip() for n in cluster_nodes.split(',') if n.strip()]
    else:
        node_list = None
    click.echo('Bắt đầu benchmark...')
    coordinator = BenchmarkCoordinator(sqlite_path)
    run_id = coordinator.run(threads, duration, test_type, report_interval, db_type, db_host, db_port, db_user, db_password, db_name, schema, is_cluster=is_cluster, cluster_nodes=node_list)
    click.echo(f'Run ID của bài kiểm thử này là: {run_id}')
    click.echo('Benchmark đã hoàn thành. Các snapshot đã được lưu vào SQLite.')

@cli.command()
@click.option('--sqlite-path', default='results.db', help='Đường dẫn file SQLite.')
@click.option('--run-ids', default='', help='Chọn một hoặc nhiều run_id để visualize (cách nhau bởi dấu phẩy, để trống để liệt kê tất cả).')
@click.option('--names', default='', help='Tên chú thích cho từng run, cách nhau bởi dấu phẩy, thứ tự tương ứng với run-ids.')
def visualize(sqlite_path, run_ids, names):
    """Trực quan hóa kết quả benchmark."""
    import sqlite3
    import pandas as pd
    coordinator = BenchmarkCoordinator(sqlite_path)
    if not run_ids.strip():
        # Lấy tất cả run_id và thông tin trong bảng benchmark_runs, chỉ hiển thị bảng
        conn = sqlite3.connect(sqlite_path)
        df = pd.read_sql_query('SELECT id, db_type, test_type, num_threads, duration_secs, start_time, end_time FROM benchmark_runs ORDER BY id', conn)
        conn.close()
        if df.empty:
            click.echo('Không có run_id nào trong database.')
            return
        click.echo('\nDanh sách các bài benchmark trong database:')
        click.echo(df.to_string(index=False))
        return
    run_id_list = [int(i.strip()) for i in run_ids.split(',') if i.strip().isdigit()]
    name_list = [n.strip() for n in names.split(',')] if names.strip() else None
    coordinator.visualize(run_id_list, sqlite_path=sqlite_path, run_names=name_list)

if __name__ == '__main__':
    cli()




