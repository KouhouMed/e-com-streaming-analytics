"""
Day 6 — Real-time Streamlit analytics dashboard.

Pulls from two PostgreSQL tables written by the PySpark foreachBatch sink:

    ecom.hourly_revenue    → live revenue line chart  (1-min tumbling windows)
    ecom.category_metrics  → category bar chart       (5-min sliding windows)

Refresh is driven by streamlit-autorefresh, which injects a JavaScript timer
so the page reruns every DASHBOARD_REFRESH_MS milliseconds without blocking
the server thread.  Query results are cached with st.cache_data(ttl=...) so
each database round-trip happens at most once per refresh cycle.
"""

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ── Configuration ──────────────────────────────────────────────────────────────
PG_HOST = os.getenv("POSTGRES_HOST",     "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB",       "ecom_db")
PG_USER = os.getenv("POSTGRES_USER",     "ecom_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "ecom_pass")

REFRESH_MS       = int(os.getenv("DASHBOARD_REFRESH_MS",       "3000"))
LOOKBACK_MINUTES = int(os.getenv("DASHBOARD_LOOKBACK_MINUTES", "60"))
CATEGORY_WINDOW  = int(os.getenv("DASHBOARD_CATEGORY_WINDOW",  "30"))

# ── Design tokens (dark terminal palette) ──────────────────────────────────────
AMBER  = "#E8B84B"
BLUE   = "#4493F8"
GREEN  = "#3FB950"
PURPLE = "#BC8CFF"
MUTED  = "#8B949E"
BG     = "#0D1117"
SURF   = "#161B22"
BORDER = "#30363D"

_MONO = "'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace"

# Shared Plotly layout applied to every figure
_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=SURF,
    font=dict(family=_MONO, size=12, color=MUTED),
    margin=dict(l=4, r=4, t=40, b=4),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor=BORDER,
        borderwidth=1,
        font=dict(size=11),
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right",  x=1,
    ),
    xaxis=dict(gridcolor=BORDER, linecolor=BORDER, zerolinecolor="rgba(0,0,0,0)", tickfont=dict(size=11)),
    yaxis=dict(gridcolor=BORDER, linecolor=BORDER, zerolinecolor="rgba(0,0,0,0)", tickfont=dict(size=11)),
    hoverlabel=dict(bgcolor=SURF, bordercolor=BORDER, font=dict(family=_MONO, size=12)),
)

# ── Page config (must be first Streamlit call) ─────────────────────────────────
st.set_page_config(
    page_title="E-Com Live Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Additional CSS — the theme in config.toml handles global colors;
# these patches fix Streamlit-specific layout quirks
st.markdown("""
<style>
  .block-container { padding-top: 1.25rem; padding-bottom: 1rem; }
  [data-testid="stMetricValue"] { font-size: 1.6rem; letter-spacing: -0.02em; }
  [data-testid="stMetricLabel"] { font-size: 0.72rem; letter-spacing: 0.06em;
                                   text-transform: uppercase; }
  hr { border-color: #30363D !important; margin: 0.6rem 0 !important; }
  .refresh-tag {
    font-family: 'Cascadia Code', monospace;
    font-size: 11px;
    letter-spacing: .06em;
    text-transform: uppercase;
    color: #6E7681;
    text-align: right;
    padding-top: 1.5rem;
    line-height: 1.9;
  }
</style>
""", unsafe_allow_html=True)

# ── Auto-refresh ───────────────────────────────────────────────────────────────
# st_autorefresh injects a JS setInterval that triggers st.rerun() every N ms.
# The return value is the number of refreshes since the page loaded.
_tick = st_autorefresh(interval=REFRESH_MS, key="live_dashboard")

# ── Database ───────────────────────────────────────────────────────────────────
def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        connect_timeout=5,
        options="-c timezone=UTC",
    )


def _query(sql: str) -> pd.DataFrame:
    """
    Run sql, return DataFrame.  Opens and closes a fresh connection each call
    so stale connections don't accumulate between auto-refresh cycles.
    Returns an empty DataFrame on any error (displayed via the no-data state).
    """
    conn = None
    try:
        conn = _connect()
        return pd.read_sql_query(sql, conn)
    except Exception as exc:
        # Surface the error in the sidebar so the main layout stays clean
        st.sidebar.error(f"DB error: {exc}")
        return pd.DataFrame()
    finally:
        if conn and not conn.closed:
            conn.close()


# ttl aligns cache expiry with the refresh interval so each cycle gets fresh data
_TTL = max(1, REFRESH_MS // 1000)


@st.cache_data(ttl=_TTL, show_spinner=False)
def load_revenue(lookback_min: int) -> pd.DataFrame:
    return _query(f"""
        SELECT
            window_start,
            window_end,
            ROUND(total_revenue::numeric, 2)  AS total_revenue,
            purchase_count
        FROM ecom.hourly_revenue
        WHERE window_start >= NOW() - INTERVAL '{lookback_min} minutes'
        ORDER BY window_start ASC
    """)


@st.cache_data(ttl=_TTL, show_spinner=False)
def load_categories(window_min: int) -> pd.DataFrame:
    return _query(f"""
        SELECT
            category,
            action,
            SUM(event_count) AS total_events
        FROM ecom.category_metrics
        WHERE window_start >= NOW() - INTERVAL '{window_min} minutes'
          AND action IN ('view', 'purchase')
        GROUP BY category, action
        ORDER BY total_events DESC
    """)


# ── Chart builders ─────────────────────────────────────────────────────────────
def _empty_fig(message: str) -> go.Figure:
    """Placeholder figure shown when no data is available yet."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False,
        font=dict(color=MUTED, size=13, family=_MONO),
    )
    fig.update_layout(**_LAYOUT)
    return fig


def chart_revenue(df: pd.DataFrame) -> go.Figure:
    """
    Dual-axis line chart:
      • Left  Y — total revenue ($), amber area fill
      • Right Y — purchase count,   blue dotted line

    Hovering on either trace shows the window start time and value.
    """
    if df.empty:
        return _empty_fig(
            "No revenue windows yet — check WATERMARK_DURATION in .env<br>"
            "(default 10 min means first rows appear ~11 min after the producer starts)"
        )

    fig = go.Figure()

    # ── Revenue area ──────────────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=df["window_start"],
        y=df["total_revenue"],
        name="Revenue ($)",
        mode="lines+markers",
        fill="tozeroy",
        fillcolor="rgba(232,184,75,0.10)",
        line=dict(color=AMBER, width=2.5),
        marker=dict(color=AMBER, size=5, line=dict(color=BG, width=1.5)),
        hovertemplate="<b>%{x|%H:%M}</b><br>Revenue: <b>$%{y:,.2f}</b><extra></extra>",
    ))

    # ── Purchase count (right axis) ───────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=df["window_start"],
        y=df["purchase_count"],
        name="Purchases",
        mode="lines+markers",
        line=dict(color=BLUE, width=1.5, dash="dot"),
        marker=dict(color=BLUE, size=4),
        yaxis="y2",
        hovertemplate="<b>%{x|%H:%M}</b><br>Purchases: <b>%{y}</b><extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text=f"Revenue — 1-min tumbling windows  (last {LOOKBACK_MINUTES} min)",
            font=dict(size=13, color=MUTED, family=_MONO), x=0,
        ),
        yaxis=dict(
            title="Revenue ($)",
            title_font=dict(color=AMBER, size=11),
            tickprefix="$",
            tickfont=dict(color=AMBER, size=11),
            **{k: v for k, v in _LAYOUT["yaxis"].items() if k != "tickfont"},
        ),
        yaxis2=dict(
            title="Purchases",
            title_font=dict(color=BLUE, size=11),
            overlaying="y",
            side="right",
            showgrid=False,
            tickfont=dict(color=BLUE, size=11),
            linecolor=BORDER,
            zerolinecolor="rgba(0,0,0,0)",
        ),
        **_LAYOUT,
    )
    return fig


def chart_categories(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal stacked bar chart — one bar per category, coloured by action.
    Categories sorted by total activity so the busiest sits at the top.
    """
    if df.empty:
        return _empty_fig(f"No category data in the last {CATEGORY_WINDOW} min yet")

    pivot = (
        df.pivot_table(
            index="category", columns="action",
            values="total_events", aggfunc="sum", fill_value=0,
        )
        .reset_index()
    )
    pivot["_total"] = pivot.select_dtypes("number").sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=True).drop(columns="_total")

    fig = go.Figure()

    if "view" in pivot.columns:
        fig.add_trace(go.Bar(
            y=pivot["category"],
            x=pivot["view"],
            name="Views",
            orientation="h",
            marker=dict(color=BLUE, opacity=0.85),
            hovertemplate="<b>%{y}</b><br>Views: <b>%{x:,}</b><extra></extra>",
        ))
    if "purchase" in pivot.columns:
        fig.add_trace(go.Bar(
            y=pivot["category"],
            x=pivot["purchase"],
            name="Purchases",
            orientation="h",
            marker=dict(color=GREEN, opacity=0.85),
            hovertemplate="<b>%{y}</b><br>Purchases: <b>%{x:,}</b><extra></extra>",
        ))

    fig.update_layout(
        title=dict(
            text=f"Category Activity — last {CATEGORY_WINDOW} min",
            font=dict(size=13, color=MUTED, family=_MONO), x=0,
        ),
        barmode="stack",
        bargap=0.22,
        xaxis_title="Events",
        yaxis_title=None,
        yaxis=dict(tickfont=dict(size=12), **{k: v for k, v in _LAYOUT["yaxis"].items() if k != "tickfont"}),
        **_LAYOUT,
    )
    return fig


def chart_action_donut(df: pd.DataFrame) -> go.Figure:
    """
    Donut showing the view / purchase split across all categories.
    Gives an at-a-glance conversion ratio for the selected window.
    """
    if df.empty:
        return _empty_fig("No data yet")

    totals = df.groupby("action")["total_events"].sum().reset_index()
    color_map = {"view": BLUE, "purchase": GREEN}
    colors = [color_map.get(a, MUTED) for a in totals["action"]]

    fig = go.Figure(go.Pie(
        labels=totals["action"].str.title(),
        values=totals["total_events"],
        hole=0.6,
        marker=dict(colors=colors, line=dict(color=BG, width=3)),
        textfont=dict(size=12, family=_MONO),
        hovertemplate="<b>%{label}</b><br>%{value:,} events (%{percent})<extra></extra>",
        direction="clockwise",
        sort=True,
    ))

    fig.update_layout(
        title=dict(
            text="Action split",
            font=dict(size=13, color=MUTED, family=_MONO), x=0,
        ),
        paper_bgcolor=BG,
        font=dict(family=_MONO, size=12, color=MUTED),
        margin=dict(l=4, r=4, t=40, b=4),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11),
            orientation="h",
            yanchor="bottom", y=-0.15,
            xanchor="center",  x=0.5,
        ),
        hoverlabel=dict(bgcolor=SURF, bordercolor=BORDER, font=dict(family=_MONO, size=12)),
    )
    return fig


# ── App layout ─────────────────────────────────────────────────────────────────
def main() -> None:

    # ── Header ────────────────────────────────────────────────────────────────
    col_title, col_meta = st.columns([4, 1])
    with col_title:
        st.title("E-Commerce Live Analytics")
    with col_meta:
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        st.markdown(
            f'<div class="refresh-tag">'
            f'⚡ {REFRESH_MS / 1000:.0f}s refresh<br>{now_utc}'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Load data ─────────────────────────────────────────────────────────────
    revenue_df  = load_revenue(LOOKBACK_MINUTES)
    category_df = load_categories(CATEGORY_WINDOW)

    # ── KPI metrics ───────────────────────────────────────────────────────────
    total_revenue   = float(revenue_df["total_revenue"].sum())   if not revenue_df.empty  else 0.0
    total_purchases = int(revenue_df["purchase_count"].sum())    if not revenue_df.empty  else 0
    n_windows       = len(revenue_df)

    # Window-over-window revenue delta for the metric card
    rev_delta = None
    if len(revenue_df) >= 2:
        prev = float(revenue_df.iloc[-2]["total_revenue"])
        curr = float(revenue_df.iloc[-1]["total_revenue"])
        rev_delta = f"${curr - prev:+.2f} vs prev window"

    # Top category by combined event volume
    top_cat = (
        category_df.groupby("category")["total_events"].sum().idxmax()
        if not category_df.empty else "—"
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(f"Revenue (last {LOOKBACK_MINUTES}m)", f"${total_revenue:,.2f}", delta=rev_delta)
    m2.metric("Purchases",    f"{total_purchases:,}")
    m3.metric("Top Category", top_cat)
    m4.metric("Closed Windows", n_windows,
              help="1-minute windows with finalised revenue in the lookback period")

    st.divider()

    # ── Revenue line chart — full width ───────────────────────────────────────
    st.plotly_chart(
        chart_revenue(revenue_df),
        use_container_width=True,
        config={"displayModeBar": False},
    )

    # ── Category bar + action donut ───────────────────────────────────────────
    col_bar, col_donut = st.columns([2, 1])
    with col_bar:
        st.plotly_chart(
            chart_categories(category_df),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with col_donut:
        st.plotly_chart(
            chart_action_donut(category_df),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Raw data table (collapsed by default) ─────────────────────────────────
    with st.expander("Raw — ecom.hourly_revenue", expanded=False):
        if revenue_df.empty:
            st.info(
                "No rows yet. With the default 10-minute watermark, the first row "
                "appears ~11 minutes after the producer starts sending purchase events. "
                "Set WATERMARK_DURATION=1 minute in .env for faster feedback."
            )
        else:
            display_df = (
                revenue_df[["window_start", "window_end", "total_revenue", "purchase_count"]]
                .sort_values("window_start", ascending=False)
                .reset_index(drop=True)
            )
            st.dataframe(
                display_df.style.format({
                    "total_revenue":  "${:,.2f}",
                    "purchase_count": "{:,}",
                }),
                use_container_width=True,
                hide_index=True,
            )


if __name__ == "__main__":
    main()
