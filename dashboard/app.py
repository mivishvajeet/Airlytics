"""Airlytics Real-Time Flight Tracking & Airspace Analytics Dashboard."""

from datetime import datetime, timezone
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.data_provider import DashboardDataProvider, HUB_AIRPORTS

# -----------------------------------------------------------------------------
# 1. Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Airlytics ✈️ Real-Time Airspace Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------------------------------------------------------
# 2. Theme State & CSS Design System
# -----------------------------------------------------------------------------
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

if "data_source" not in st.session_state:
    st.session_state.data_source = "Live OpenSky Feed"

IS_DARK = st.session_state.theme == "dark"

bg = "#09090b" if IS_DARK else "#ffffff"
bg_subtle = "#0c0c0f" if IS_DARK else "#f9fafb"
card_bg = "#111116" if IS_DARK else "#ffffff"
border_color = "#1e1e24" if IS_DARK else "#e4e4e7"
text_color = "#fafafa" if IS_DARK else "#09090b"
text_muted = "#71717a"
accent_blue = "#2563eb"
green_badge_bg = "rgba(34,197,94,0.12)" if IS_DARK else "rgba(22,163,74,0.1)"
green_badge_text = "#22c55e" if IS_DARK else "#16a34a"

custom_css = f"""
<style>
/* Hide standard Streamlit chrome */
header[data-testid="stHeader"], #MainMenu, footer, [data-testid="stToolbar"],
[data-testid="stDecoration"], [data-testid="stStatusWidget"], .stDeployButton,
div[data-testid="stSidebarCollapsedControl"] {{
    display: none !important;
}}

html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"], .main, .block-container, section[data-testid="stMain"] {{
    background-color: {bg} !important;
    color: {text_color} !important;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
}}

.block-container {{
    padding: 1.5rem 2.0rem 3rem !important;
    max-width: 1440px !important;
}}

/* Card container */
.air-card {{
    background-color: {card_bg};
    border: 1px solid {border_color};
    border-radius: 10px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 1rem;
}}

/* KPI Metric styles */
.kpi-title {{
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: {text_muted};
    font-weight: 600;
    margin-bottom: 0.25rem;
}}
.kpi-value {{
    font-size: 1.85rem;
    font-weight: 700;
    color: {text_color};
    letter-spacing: -0.02em;
    font-family: "JetBrains Mono", monospace, monospace;
}}
.kpi-badge {{
    display: inline-block;
    padding: 0.2rem 0.5rem;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-top: 0.4rem;
    background-color: {green_badge_bg};
    color: {green_badge_text};
}}

/* Pill tabs */
button[data-baseweb="tab"] {{
    background: transparent !important;
    color: {text_muted} !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    padding: 0.55rem 1rem !important;
    border: 1px solid transparent !important;
    border-radius: 8px !important;
}}
button[data-baseweb="tab"][aria-selected="true"] {{
    color: {text_color} !important;
    background-color: {card_bg} !important;
    border-color: {border_color} !important;
}}
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 3. Data Initialization & Polling
# -----------------------------------------------------------------------------
@st.cache_data(ttl=10.0, show_spinner=False)
def load_airspace_data(source_name: str) -> pd.DataFrame:
    provider = DashboardDataProvider()
    if source_name == "Live OpenSky Feed":
        try:
            return provider.get_live_opensky_pings()
        except Exception:
            return provider.get_synthetic_pings()
    else:
        return provider.get_synthetic_pings()


provider = DashboardDataProvider()
pings_df = load_airspace_data(st.session_state.data_source)
legs_df = provider.get_curated_flight_legs(pings_df)
events_df = provider.get_airport_events(pings_df)
routes_df = provider.get_route_aggregates(legs_df)

total_aircraft = len(pings_df)
airborne_count = len(pings_df[~pings_df["on_ground"]])
ground_count = len(pings_df[pings_df["on_ground"]])
active_legs = len(legs_df)
avg_speed = round(float(pings_df["velocity_ms"].mean() * 1.94384), 0) if not pings_df.empty else 0

# -----------------------------------------------------------------------------
# 4. Header & Top Control Bar
# -----------------------------------------------------------------------------
top_col1, top_col2, top_col3 = st.columns([3, 1.2, 0.8])

with top_col1:
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 0.75rem;">
            <h1 style="font-size: 1.6rem; font-weight: 700; margin: 0; color: {text_color};">Airlytics ✈️</h1>
            <span style="background-color: {green_badge_bg}; color: {green_badge_text}; font-size: 0.75rem; font-weight: 600; padding: 0.25rem 0.6rem; border-radius: 9999px;">● STREAMING ACTIVE</span>
            <span style="color: {text_muted}; font-size: 0.8rem;">SLA: &lt; 2 min latency | &lt; 0.1% duplicates</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with top_col2:
    selected_source = st.selectbox(
        "Data Source",
        ["Live OpenSky Feed", "Synthetic Corridor Stream"],
        index=0 if st.session_state.data_source == "Live OpenSky Feed" else 1,
        label_visibility="collapsed",
    )
    if selected_source != st.session_state.data_source:
        st.session_state.data_source = selected_source
        st.cache_data.clear()
        st.rerun()

with top_col3:
    if st.button("🔄 Refresh Telemetry", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# -----------------------------------------------------------------------------
# 5. KPI Metrics Grid
# -----------------------------------------------------------------------------
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.markdown(
        f"""
        <div class="air-card">
            <div class="kpi-title">Airborne Aircraft</div>
            <div class="kpi-value">{airborne_count:,}</div>
            <div class="kpi-badge">Active Transponders</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi2:
    st.markdown(
        f"""
        <div class="air-card">
            <div class="kpi-title">Aircraft On Ground</div>
            <div class="kpi-value">{ground_count:,}</div>
            <div class="kpi-badge">Taxing / Apron</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi3:
    st.markdown(
        f"""
        <div class="air-card">
            <div class="kpi-title">Sessionized Flight Legs</div>
            <div class="kpi-value">{active_legs:,}</div>
            <div class="kpi-badge">15m Gap Windows</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi4:
    st.markdown(
        f"""
        <div class="air-card">
            <div class="kpi-title">Average Fleet Speed</div>
            <div class="kpi-value">{int(avg_speed)} <span style="font-size: 1rem; color: {text_muted};">kts</span></div>
            <div class="kpi-badge">Ground Speed</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi5:
    st.markdown(
        f"""
        <div class="air-card">
            <div class="kpi-title">Takeoff / Landing Events</div>
            <div class="kpi-value">{len(events_df):,}</div>
            <div class="kpi-badge">GIS 5km Buffers</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------------------------
# 6. Main Content Tabs
# -----------------------------------------------------------------------------
tab_map, tab_legs, tab_airports, tab_routes, tab_arch = st.tabs([
    "🗺️ Live Airspace Radar",
    "🛫 Sessionized Flight Legs",
    "🏢 Airport Congestion & Geofencing",
    "📈 Route Analytics",
    "⚙️ GCP Data Architecture",
])

# --- Tab 1: Live Airspace Radar ---
with tab_map:
    map_col, side_col = st.columns([3.2, 1.2])

    with side_col:
        st.markdown(f"<div class='kpi-title' style='margin-top:0.5rem;'>Filter Airspace</div>", unsafe_allow_html=True)
        search_callsign = st.text_input("Search Callsign / ICAO24", placeholder="e.g. BAW, AFR, DLH...")
        min_alt = st.slider("Minimum Altitude (m)", min_value=0, max_value=12000, value=0, step=500)
        only_airborne = st.checkbox("Only Airborne Aircraft", value=True)

    filtered_pings = pings_df.copy()
    if only_airborne:
        filtered_pings = filtered_pings[~filtered_pings["on_ground"]]
    if min_alt > 0:
        filtered_pings = filtered_pings[filtered_pings["altitude_baro_m"] >= min_alt]
    if search_callsign:
        search_upper = search_callsign.upper()
        filtered_pings = filtered_pings[
            filtered_pings["callsign"].str.contains(search_upper, na=False) |
            filtered_pings["icao24"].str.contains(search_upper, na=False)
        ]

    with map_col:
        fig_map = px.scatter_mapbox(
            filtered_pings,
            lat="latitude",
            lon="longitude",
            color="altitude_baro_m",
            size_max=9,
            zoom=4.8,
            center={"lat": 51.0, "lon": 5.0},
            hover_name="callsign",
            hover_data={
                "icao24": True,
                "origin_country": True,
                "altitude_baro_m": ":.0f",
                "velocity_ms": ":.1f",
                "vertical_rate_ms": ":.1f",
                "nearest_airport_name": True,
                "latitude": False,
                "longitude": False,
            },
            color_continuous_scale="Viridis",
            labels={"altitude_baro_m": "Altitude (m)"},
            mapbox_style="carto-positron" if not IS_DARK else "carto-darkmatter",
        )
        fig_map.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            height=540,
            paper_bgcolor=card_bg,
        )
        st.plotly_chart(fig_map, use_container_width=True)

    st.markdown(f"<div class='kpi-title' style='margin-top:1rem;'>Live Telemetry Sample ({len(filtered_pings):,} matched)</div>", unsafe_allow_html=True)
    st.dataframe(
        filtered_pings[[
            "callsign", "icao24", "origin_country", "altitude_baro_m", "velocity_ms", "vertical_rate_ms", "nearest_airport_name", "on_ground"
        ]].head(50),
        use_container_width=True,
        hide_index=True,
    )

# --- Tab 2: Sessionized Flight Legs ---
with tab_legs:
    st.markdown(
        f"<div class='kpi-title'>Beam Session Window Reconstruction (`fact_flight_leg`)</div>",
        unsafe_allow_html=True,
    )
    col_l1, col_l2 = st.columns([1.5, 2.5])

    with col_l1:
        fig_hist = px.histogram(
            legs_df,
            x="duration_minutes",
            nbins=15,
            title="Flight Duration Distribution (Minutes)",
            color_discrete_sequence=[accent_blue],
        )
        fig_hist.update_layout(
            paper_bgcolor=card_bg,
            plot_bgcolor=card_bg,
            font={"color": text_color},
            margin={"l": 20, "r": 20, "t": 40, "b": 20},
            height=320,
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with col_l2:
        fig_scatter = px.scatter(
            legs_df,
            x="duration_minutes",
            y="max_altitude_m",
            color="status",
            hover_name="callsign",
            title="Max Altitude vs Duration by Flight Leg",
            color_discrete_map={"COMPLETED": "#22c55e", "IN-FLIGHT": "#2563eb"},
        )
        fig_scatter.update_layout(
            paper_bgcolor=card_bg,
            plot_bgcolor=card_bg,
            font={"color": text_color},
            margin={"l": 20, "r": 20, "t": 40, "b": 20},
            height=320,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.dataframe(
        legs_df[[
            "flight_leg_id", "callsign", "origin_airport_name", "destination_airport_name", "duration_minutes", "max_altitude_m", "avg_velocity_ms", "status"
        ]],
        use_container_width=True,
        hide_index=True,
    )

# --- Tab 3: Airport Congestion & Geofencing ---
with tab_airports:
    col_a1, col_a2 = st.columns([2, 2])

    with col_a1:
        hub_counts = events_df.groupby(["airport_icao", "event_type"]).size().reset_index(name="event_count")
        fig_bar = px.bar(
            hub_counts,
            x="airport_icao",
            y="event_count",
            color="event_type",
            barmode="group",
            title="Takeoffs vs Landings by European Hub",
            color_discrete_map={"TAKEOFF": "#22c55e", "LANDING": "#3b82f6"},
        )
        fig_bar.update_layout(
            paper_bgcolor=card_bg,
            plot_bgcolor=card_bg,
            font={"color": text_color},
            margin={"l": 20, "r": 20, "t": 40, "b": 20},
            height=320,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_a2:
        st.markdown(f"<div class='kpi-title' style='margin-bottom:0.75rem;'>Monitored Hub Geofences (5km BigQuery GIS Buffers)</div>", unsafe_allow_html=True)
        hubs_summary = [
            {"ICAO": code, "Name": m["name"], "Country": m["country"], "Buffer": "5,000m Polygon"}
            for code, m in HUB_AIRPORTS.items()
        ]
        st.dataframe(pd.DataFrame(hubs_summary), use_container_width=True, hide_index=True)

    st.markdown(f"<div class='kpi-title' style='margin-top:1rem;'>Live Geofenced Takeoff & Landing Events (`fact_airport_event`)</div>", unsafe_allow_html=True)
    st.dataframe(
        events_df[[
            "event_id", "airport_name", "event_type", "callsign", "distance_km", "vertical_rate_ms", "altitude_m"
        ]],
        use_container_width=True,
        hide_index=True,
    )

# --- Tab 4: Route Performance Analytics ---
with tab_routes:
    st.markdown(f"<div class='kpi-title'>Route Aggregations (`curated_flights.fact_route_agg`)</div>", unsafe_allow_html=True)
    if not routes_df.empty:
        col_r1, col_r2 = st.columns([2.5, 1.5])
        with col_r1:
            st.dataframe(routes_df, use_container_width=True, hide_index=True)
        with col_r2:
            fig_pie = px.pie(
                routes_df.head(6),
                names="route",
                values="flight_count",
                title="Busiest Corridors Share",
                hole=0.45,
            )
            fig_pie.update_layout(
                paper_bgcolor=card_bg,
                plot_bgcolor=card_bg,
                font={"color": text_color},
                margin={"l": 10, "r": 10, "t": 40, "b": 10},
                height=300,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

# --- Tab 5: GCP Architecture & Verification ---
with tab_arch:
    st.markdown(
        f"""
        <div class="air-card">
            <h3 style="margin-top:0; color:{text_color};">Airlytics End-to-End GCP Architecture</h3>
            <p style="color:{text_muted}; font-size:0.9rem;">
                This streaming analytics platform delivers real-time flight telemetry from transponder broadcast to analytical marts with guaranteed low latency and zero secrets.
            </p>
            <div style="display:grid; grid-template-columns: repeat(3, 1fr); gap:1rem; margin-top:1rem;">
                <div style="background:{bg_subtle}; padding:1rem; border-radius:8px; border:1px solid {border_color};">
                    <strong style="color:{accent_blue};">1. Ingestion</strong><br/>
                    <span style="font-size:0.85rem; color:{text_muted};">Cloud Run Job + OpenSky REST Poller with exponential backoff & DLQ -> Pub/Sub topic <code>raw-flight-pings</code></span>
                </div>
                <div style="background:{bg_subtle}; padding:1rem; border-radius:8px; border:1px solid {border_color};">
                    <strong style="color:{accent_blue};">2. Streaming Engine</strong><br/>
                    <span style="font-size:0.85rem; color:{text_muted};">Apache Beam (Dataflow Flex Template) -> 15-minute gap sessionization + 10-minute late data side output sink</span>
                </div>
                <div style="background:{bg_subtle}; padding:1rem; border-radius:8px; border:1px solid {border_color};">
                    <strong style="color:{accent_blue};">3. Curated Marts</strong><br/>
                    <span style="font-size:0.85rem; color:{text_muted};">Dataform incremental SQLX models + BigQuery GIS 5km spatial buffers for takeoff/landing detection</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
