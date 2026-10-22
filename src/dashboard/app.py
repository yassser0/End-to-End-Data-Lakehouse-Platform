"""
Lakehouse Analytics & Data Quality Dashboard.
Interactive Streamlit application powered by the Gold analytical layer & PostgreSQL.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

from src.utils.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    get_postgres_url,
)

# ─── Page Configuration ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lakehouse Platform Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e222d 0%, #252a36 100%);
        border: 1px solid #2e3546;
        border-radius: 10px;
        padding: 18px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.25);
    }
    .metric-value {
        font-size: 26px;
        font-weight: 700;
        color: #00d4aa;
        margin-top: 4px;
    }
    .metric-title {
        font-size: 13px;
        font-weight: 500;
        color: #9ba1b0;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_db_engine():
    """Create a persistent SQLAlchemy engine."""
    url = get_postgres_url()
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=60)
def load_table(table_name: str) -> pd.DataFrame:
    """Load an entire table from PostgreSQL."""
    engine = get_db_engine()
    query = text(f"SELECT * FROM {table_name}")
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


# ─── Header & Sidebar ─────────────────────────────────────────────────────────
st.sidebar.title("⚡ Lakehouse Platform")
st.sidebar.caption("Medallion Architecture (Bronze ➔ Silver ➔ Gold)")

nav = st.sidebar.radio(
    "Navigation",
    ["📊 Business Overview", "📈 Sales & Revenue", "🛍️ Product Insights", "👥 Customer Analytics", "🛡️ Data Quality & Observability"],
)

st.sidebar.divider()
st.sidebar.markdown(
    f"""
    **Environment Status:**
    - **PostgreSQL**: `{POSTGRES_HOST}:{POSTGRES_PORT}`
    - **Database**: `{POSTGRES_DB}`
    - **Airflow UI**: [localhost:8080](http://localhost:8080)
    """
)

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ─── Data Loading ─────────────────────────────────────────────────────────────
try:
    df_daily = load_table("gold_daily_sales")
    df_products = load_table("gold_product_performance")
    df_customers = load_table("gold_customer_analytics")
    df_status = load_table("gold_order_status_summary")
    df_quality = load_table("gold_data_quality_metrics")
except Exception as e:
    st.error(f"Failed to connect to PostgreSQL: {e}")
    st.info("Make sure PostgreSQL is running via Docker: `docker compose up -d postgres`")
    st.stop()


# ─── 1. Business Overview ─────────────────────────────────────────────────────
if nav == "📊 Business Overview":
    st.title("📊 E-Commerce Lakehouse Overview")
    st.markdown("Real-time aggregated KPIs from the **Gold analytical layer**.")

    # High level KPIs
    total_revenue = df_daily["total_revenue"].sum()
    total_paid_orders = df_daily["paid_order_count"].sum()
    avg_order_value = total_revenue / total_paid_orders if total_paid_orders else 0
    total_customers = len(df_customers)
    overall_quality = df_quality["quality_pct"].mean() if not df_quality.empty else 100.0

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    with kpi1:
        st.markdown(
            f"""<div class="metric-card">
                <div class="metric-title">Total Paid Revenue</div>
                <div class="metric-value">${total_revenue:,.2f}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with kpi2:
        st.markdown(
            f"""<div class="metric-card">
                <div class="metric-title">Paid Orders</div>
                <div class="metric-value">{total_paid_orders:,}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with kpi3:
        st.markdown(
            f"""<div class="metric-card">
                <div class="metric-title">Avg Order Value (AOV)</div>
                <div class="metric-value">${avg_order_value:.2f}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with kpi4:
        st.markdown(
            f"""<div class="metric-card">
                <div class="metric-title">Unique Customers</div>
                <div class="metric-value">{total_customers:,}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with kpi5:
        st.markdown(
            f"""<div class="metric-card">
                <div class="metric-title">Data Quality Score</div>
                <div class="metric-value">{overall_quality:.1f}%</div>
            </div>""",
            unsafe_allow_html=True,
        )

    st.write("")
    st.write("")

    col_chart1, col_chart2 = st.columns([3, 2])

    with col_chart1:
        st.subheader("Daily Revenue Trend")
        df_daily_sorted = df_daily.sort_values("order_date").tail(30)
        fig_rev = px.line(
            df_daily_sorted,
            x="order_date",
            y="total_revenue",
            markers=True,
            line_shape="spline",
            title="Revenue Trend (Last 30 Active Days)",
            labels={"order_date": "Date", "total_revenue": "Revenue ($)"},
            color_discrete_sequence=["#00d4aa"],
        )
        fig_rev.update_layout(template="plotly_dark", height=380, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_rev, use_container_width=True)

    with col_chart2:
        st.subheader("Order Distribution by Status")
        fig_pie = px.pie(
            df_status,
            names="order_status",
            values="order_count",
            hole=0.45,
            title="Orders by Lifecycle Status",
            color_discrete_sequence=["#00d4aa", "#38bdf8", "#f43f5e", "#fbbf24"],
        )
        fig_pie.update_layout(template="plotly_dark", height=380, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_pie, use_container_width=True)


# ─── 2. Sales & Revenue ───────────────────────────────────────────────────────
elif nav == "📈 Sales & Revenue":
    st.title("📈 Sales & Revenue Deep-Dive")

    df_daily["order_date"] = pd.to_datetime(df_daily["order_date"])
    df_sorted = df_daily.sort_values("order_date")

    # Date range filter
    min_date = df_sorted["order_date"].min().date()
    max_date = df_sorted["order_date"].max().date()
    date_range = st.slider("Select Date Range", min_value=min_date, max_value=max_date, value=(min_date, max_date))

    mask = (df_sorted["order_date"].dt.date >= date_range[0]) & (df_sorted["order_date"].dt.date <= date_range[1])
    filtered_daily = df_sorted[mask]

    # Bar + Line combo
    fig_combo = go.Figure()
    fig_combo.add_trace(
        go.Bar(
            x=filtered_daily["order_date"],
            y=filtered_daily["total_revenue"],
            name="Daily Revenue ($)",
            marker_color="#0ea5e9",
            opacity=0.8,
        )
    )
    fig_combo.add_trace(
        go.Scatter(
            x=filtered_daily["order_date"],
            y=filtered_daily["avg_order_value"],
            name="Average Order Value ($)",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="#f59e0b", width=2),
        )
    )
    fig_combo.update_layout(
        title="Daily Revenue vs Average Order Value",
        yaxis=dict(title="Revenue ($)"),
        yaxis2=dict(title="AOV ($)", overlaying="y", side="right"),
        template="plotly_dark",
        height=450,
        legend=dict(orientation="h", y=1.1, x=0),
    )
    st.plotly_chart(fig_combo, use_container_width=True)

    st.subheader("Raw Daily Sales Table")
    st.dataframe(
        filtered_daily.sort_values("order_date", ascending=False).style.format(
            {"total_revenue": "${:,.2f}", "avg_order_value": "${:,.2f}", "paid_order_count": "{:,}"}
        ),
        use_container_width=True,
    )


# ─── 3. Product Insights ──────────────────────────────────────────────────────
elif nav == "🛍️ Product Insights":
    st.title("🛍️ Product Performance & Catalogue")

    col_cat1, col_cat2 = st.columns([1, 1])

    with col_cat1:
        st.subheader("Revenue by Category")
        cat_perf = (
            df_products.groupby("category", as_index=False)
            .agg({"total_revenue": "sum", "total_quantity": "sum", "product_id": "count"})
            .sort_values("total_revenue", ascending=False)
        )
        fig_cat = px.bar(
            cat_perf,
            x="category",
            y="total_revenue",
            color="total_revenue",
            color_continuous_scale="Viridis",
            labels={"category": "Category", "total_revenue": "Total Revenue ($)"},
        )
        fig_cat.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig_cat, use_container_width=True)

    with col_cat2:
        st.subheader("Units Sold vs Revenue")
        fig_scatter = px.scatter(
            df_products,
            x="total_quantity",
            y="total_revenue",
            size="order_count",
            color="category",
            hover_name="product_id",
            labels={"total_quantity": "Units Sold", "total_revenue": "Revenue ($)"},
        )
        fig_scatter.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.subheader("Top 10 Revenue Generating Products")
    top_prods = df_products.sort_values("total_revenue", ascending=False).head(10)
    fig_top = px.bar(
        top_prods,
        x="total_revenue",
        y="product_id",
        orientation="h",
        color="category",
        title="Top 10 Products by Total Revenue",
        labels={"total_revenue": "Revenue ($)", "product_id": "Product ID"},
    )
    fig_top.update_layout(template="plotly_dark", yaxis=dict(autorange="reversed"), height=420)
    st.plotly_chart(fig_top, use_container_width=True)


# ─── 4. Customer Analytics ────────────────────────────────────────────────────
elif nav == "👥 Customer Analytics":
    st.title("👥 Customer Segmentation & LTV")

    col_seg1, col_seg2 = st.columns([1, 1])

    with col_seg1:
        st.subheader("Spending by Customer Segment")
        seg_summary = (
            df_customers.groupby("segment", as_index=False)
            .agg({"total_spent": "sum", "customer_id": "count", "avg_order_value": "mean"})
            .sort_values("total_spent", ascending=False)
        )
        fig_seg = px.pie(
            seg_summary,
            names="segment",
            values="total_spent",
            hole=0.4,
            title="Total Revenue Share by Segment",
            color_discrete_sequence=["#eab308", "#94a3b8", "#b45309"],
        )
        fig_seg.update_layout(template="plotly_dark", height=380)
        st.plotly_chart(fig_seg, use_container_width=True)

    with col_seg2:
        st.subheader("Average Order Value by Segment")
        fig_aov = px.bar(
            seg_summary,
            x="segment",
            y="avg_order_value",
            color="segment",
            labels={"avg_order_value": "Average AOV ($)", "segment": "Segment"},
            color_discrete_sequence=["#eab308", "#94a3b8", "#b45309"],
        )
        fig_aov.update_layout(template="plotly_dark", height=380)
        st.plotly_chart(fig_aov, use_container_width=True)

    st.subheader("Top 10 High-Value Customers Leaderboard")
    top_cust = df_customers.sort_values("total_spent", ascending=False).head(10)
    st.dataframe(
        top_cust[["customer_id", "first_name", "last_name", "segment", "total_orders", "paid_orders", "total_spent", "avg_order_value"]]
        .style.format({"total_spent": "${:,.2f}", "avg_order_value": "${:,.2f}"}),
        use_container_width=True,
    )


# ─── 5. Data Quality & Observability ──────────────────────────────────────────
elif nav == "🛡️ Data Quality & Observability":
    st.title("🛡️ Data Quality & Quarantine Observability")
    st.markdown("Automated quality gates applied during Bronze ➔ Silver transformation.")

    col_q1, col_q2 = st.columns([1, 1])

    with col_q1:
        st.subheader("Quality Pass Rate by Entity (%)")
        fig_bar_q = px.bar(
            df_quality,
            x="entity",
            y="quality_pct",
            color="quality_pct",
            text="quality_pct",
            color_continuous_scale=["#f43f5e", "#fbbf24", "#00d4aa"],
            range_y=[0, 105],
            labels={"entity": "Entity", "quality_pct": "Quality Pass Rate (%)"},
        )
        fig_bar_q.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_bar_q.update_layout(template="plotly_dark", height=380)
        st.plotly_chart(fig_bar_q, use_container_width=True)

    with col_q2:
        st.subheader("Ingested vs Quarantined Records")
        fig_stack = go.Figure()
        fig_stack.add_trace(go.Bar(x=df_quality["entity"], y=df_quality["valid_count"], name="Valid (Silver)", marker_color="#00d4aa"))
        fig_stack.add_trace(go.Bar(x=df_quality["entity"], y=df_quality["rejected_count"], name="Quarantined", marker_color="#f43f5e"))
        fig_stack.update_layout(barmode="stack", template="plotly_dark", height=380)
        st.plotly_chart(fig_stack, use_container_width=True)

    st.subheader("Batch Execution Quality Log")
    st.dataframe(
        df_quality.style.format({"quality_pct": "{:.2f}%", "ingested_count": "{:,}", "valid_count": "{:,}", "rejected_count": "{:,}"}),
        use_container_width=True,
    )
