# Visualization module

import sqlite3
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

def load_runs(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    df = pd.read_sql_query('SELECT * FROM benchmark_runs', conn)
    conn.close()
    return df

def load_snapshots(sqlite_path, run_ids):
    conn = sqlite3.connect(sqlite_path)
    ids = ','.join(str(i) for i in run_ids)
    df = pd.read_sql_query(f'SELECT * FROM benchmark_snapshots WHERE run_id IN ({ids})', conn)
    conn.close()
    return df

def visualize(sqlite_path, run_ids):
    df_runs = load_runs(sqlite_path)
    df_snapshots = load_snapshots(sqlite_path, run_ids)

    # Tính toán thông tin cho từng run
    run_infos = []
    avg_latency = []
    avg_qps = []
    labels = []
    color_map = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    for idx, run_id in enumerate(run_ids):
        df = df_snapshots[df_snapshots['run_id'] == run_id]
        run_info = df_runs[df_runs['id'] == run_id].iloc[0]
        label = f"Run {run_id} | {run_info['db_type']} | {run_info['test_type']} | Threads:{run_info['num_threads']}"
        labels.append(label)
        avg_latency.append(df['avg_latency_ms'].mean())
        duration = run_info['duration_secs']
        total_queries = df['total_queries'].sum()
        avg_qps.append(total_queries / duration if duration > 0 else 0)
        run_infos.append([
            run_id,
            run_info['db_type'],
            run_info['test_type'],
            run_info['num_threads'],
            run_info['duration_secs']
        ])

    # Tạo bảng thông tin các run
    import plotly.figure_factory as ff
    table_fig = ff.create_table(
        [['Run ID', 'DB', 'Test', 'Threads', 'Duration (s)']] + run_infos,
        height_constant=30
    )
    table_fig.update_layout(
        margin=dict(t=30, l=60, r=30, b=10),
        width=900,
        height=40 + 30 * (len(run_ids) + 1),
        font=dict(size=14)
    )
    table_fig.show()

    # Vẽ biểu đồ cột so sánh Avg Latency và QPS
    import plotly.graph_objs as go
    from plotly.subplots import make_subplots
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Avg Latency (ms)", "Avg QPS"),
        horizontal_spacing=0.15
    )
    fig.add_trace(
        go.Bar(
            x=labels,
            y=avg_latency,
            marker_color=color_map[:len(run_ids)],
            name='Avg Latency (ms)',
            text=[f"{v:.2f}" for v in avg_latency],
            textposition='auto',
            hovertemplate='<b>%{x}</b><br>Avg Latency: %{y:.2f} ms'
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(
            x=labels,
            y=avg_qps,
            marker_color=color_map[:len(run_ids)],
            name='Avg QPS',
            text=[f"{v:.2f}" for v in avg_qps],
            textposition='auto',
            hovertemplate='<b>%{x}</b><br>Avg QPS: %{y:.2f}'
        ),
        row=1, col=2
    )
    fig.update_layout(
        title_text='<b>Benchmark Dashboard</b>',
        height=600,
        width=1200,
        legend=dict(orientation='h', yanchor='bottom', y=1.08, xanchor='right', x=1, font=dict(size=13)),
        font=dict(family='Arial', size=13),
        plot_bgcolor='#f9f9f9',
        paper_bgcolor='#fff',
        margin=dict(t=100, l=60, r=30, b=60),
        barmode='group',
        showlegend=False
    )
    fig.update_xaxes(title_text='Run', showgrid=True, gridcolor='#e5e5e5', tickangle=20)
    fig.update_yaxes(showgrid=True, gridcolor='#e5e5e5')
    fig.show()

    # --- Vẽ biểu đồ QPS và Latency theo thời gian (bắt đầu từ mốc 0) ---
    import plotly.graph_objs as go
    time_fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.10,
        subplot_titles=("QPS theo thời gian", "Avg Latency (ms) theo thời gian")
    )
    for idx, run_id in enumerate(run_ids):
        df = df_snapshots[df_snapshots['run_id'] == run_id].copy()
        if df.empty:
            continue
        # Chuyển snapshot_time sang giây từ mốc 0
        df = df.sort_values('snapshot_time')
        t0 = pd.to_datetime(df['snapshot_time'].iloc[0])
        df['seconds'] = (pd.to_datetime(df['snapshot_time']) - t0).dt.total_seconds()
        label = labels[idx]
        color = color_map[idx % len(color_map)]
        # QPS
        time_fig.add_trace(
            go.Scatter(
                x=df['seconds'],
                y=df['avg_qps'],
                mode='lines+markers',
                name=f"QPS | {label}",
                line=dict(color=color),
                showlegend=True
            ),
            row=1, col=1
        )
        # Latency
        time_fig.add_trace(
            go.Scatter(
                x=df['seconds'],
                y=df['avg_latency_ms'],
                mode='lines+markers',
                name=f"Latency | {label}",
                line=dict(color=color, dash='dot'),
                showlegend=True
            ),
            row=2, col=1
        )
    time_fig.update_layout(
        # Xóa title
        height=700,
        width=1100,
        font=dict(family='Arial', size=13),
        plot_bgcolor='#f9f9f9',
        paper_bgcolor='#fff',
        margin=dict(t=100, l=60, r=30, b=60),
        legend=dict(orientation='h', yanchor='bottom', y=1.08, xanchor='right', x=1, font=dict(size=13)),
    )
    time_fig.update_xaxes(title_text='Thời gian (giây)', showgrid=True, gridcolor='#e5e5e5')
    time_fig.update_yaxes(title_text='QPS', showgrid=True, gridcolor='#e5e5e5', row=1, col=1)
    time_fig.update_yaxes(title_text='Avg Latency (ms)', showgrid=True, gridcolor='#e5e5e5', row=2, col=1)
    time_fig.show()
