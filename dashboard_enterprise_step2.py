import sqlite3
import pandas as pd
import streamlit as st
import altair as alt
from datetime import date

st.set_page_config(layout="wide")

DB_NAME = "nodehub.db"

# -----------------------------------------
# Load data from SQLite
# -----------------------------------------
@st.cache_data

def load_peopleflow():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM peopleflowtotals", conn)
    conn.close()
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df

def load_login_camera():
    conn = sqlite3.connect(DB_NAME)
    df1 = pd.read_sql_query("SELECT * FROM login_camera", conn)
    conn.close()
    df1["pong_ts"] = pd.to_datetime(df1["pong_ts"])
    df1["pong_ts_last_fail"] = pd.to_datetime(df1["pong_ts_last_fail"])
    return df1

df = load_peopleflow()

if df.empty:
    st.error("No data found in peopleflowtotals.")
    st.stop()

df1=load_login_camera()

if df1.empty:
    st.error("No data found in login_camera.")
    st.stop()


# -----------------------------------------
# Sidebar – filters
# -----------------------------------------
st.sidebar.title("Filters")

min_date = df["created_at"].dt.date.min()
max_date = df["created_at"].dt.date.max()

date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Normalize date_range
if isinstance(date_range, date):
    start_date = end_date = date_range
else:
    if len(date_range) == 1:
        start_date = end_date = date_range[0]
    else:
        start_date, end_date = date_range

mask_date = (df["created_at"].dt.date >= start_date) & (df["created_at"].dt.date <= end_date)
df = df[mask_date]

if df.empty:
    st.warning("No data for the selected date range.")
    st.stop()

# Camera filter
camera_ids = sorted(df["camera_id"].unique())
selected_cameras = st.sidebar.multiselect(
    "Select cameras",
    options=camera_ids,
    default=camera_ids
)

if not selected_cameras:
    st.warning("Please select at least one camera.")
    st.stop()

df = df[df["camera_id"].isin(selected_cameras)]

# Valid filter
only_valid = st.sidebar.checkbox("Only valid records (valid = 1)", value=True)
if only_valid:
    df = df[df["valid"] == 1]

if df.empty:
    st.warning("No data after applying filters.")
    st.stop()

# -----------------------------------------
# Feature engineering
# -----------------------------------------
df["date"] = df["created_at"].dt.date
df["hour"] = df["created_at"].dt.hour

# -----------------------------------------
# Layout – title
# -----------------------------------------
st.title("📊 People Flow Enterprise Dashboard — Step 2")

st.caption(
    f"Date range: **{start_date}** to **{end_date}** | "
    f"Cameras: {', '.join(str(c) for c in selected_cameras)}"
)

# -----------------------------------------
# Summary metrics
# -----------------------------------------
total_inside = int(df["total_inside"].sum())
total_outside = int(df["total_outside"].sum())
num_cameras = df["camera_id"].nunique()
num_days = df["date"].nunique()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total inside", total_inside)
col2.metric("Total outside", total_outside)
col3.metric("Cameras selected", num_cameras)
col4.metric("Days in range", num_days)

# -----------------------------------------
# Detailed per-camera hourly chart
# -----------------------------------------
st.subheader("Hourly flow per camera")

camera_for_detail = st.selectbox(
    "Select camera for detailed view",
    sorted(df["camera_id"].unique()),
    index=0
)

df_detail = df[df["camera_id"] == camera_for_detail].copy()
df_detail["hour_label"] = df_detail["hour"].astype(str).str.zfill(2) + ":00 (" + df_detail["date"].astype(str) + ")"

melt_detail = df_detail.melt(
    id_vars=["hour_label", "hour", "date"],
    value_vars=["total_inside", "total_outside"],
    var_name="direction",
    value_name="count"
)

chart_detail = (
    alt.Chart(melt_detail)
    .mark_bar()
    .encode(
        x=alt.X("hour_label:N", title="Date & hour", sort=None),
        y=alt.Y("count:Q", title="People count"),
        color=alt.Color("direction:N", title="Direction"),
        tooltip=["date", "hour", "direction", "count"]
    )
    .properties(height=400)
)

st.altair_chart(chart_detail, width="stretch")

# -----------------------------------------
# Combined hourly flow (all cameras)
# -----------------------------------------
st.subheader("Hourly flow (all selected cameras combined)")

grouped_all = (
    df.groupby(["date", "hour"])
    .agg(
        total_inside=("total_inside", "sum"),
        total_outside=("total_outside", "sum")
    )
    .reset_index()
)

grouped_all["hour_label"] = grouped_all["hour"].astype(str).str.zfill(2) + ":00 (" + grouped_all["date"].astype(str) + ")"

melt_all = grouped_all.melt(
    id_vars=["hour_label", "hour", "date"],
    value_vars=["total_inside", "total_outside"],
    var_name="direction",
    value_name="count"
)

chart_all = (
    alt.Chart(melt_all)
    .mark_bar()
    .encode(
        x=alt.X("hour_label:N", title="Date & hour", sort=None),
        y=alt.Y("count:Q", title="People count"),
        color=alt.Color("direction:N", title="Direction"),
        tooltip=["date", "hour", "direction", "count"]
    )
    .properties(height=400)
)

st.altair_chart(chart_all, width="stretch")

# -----------------------------------------
# HEATMAP — Camera × Hour
# -----------------------------------------
st.subheader("Heatmap — Flow intensity by camera and hour")

heatmap_df = (
    df.groupby(["camera_id", "hour"])
    .agg(total_flow=("total_inside", "sum"))
    .reset_index()
)

heatmap_chart = (
    alt.Chart(heatmap_df)
    .mark_rect()
    .encode(
        x=alt.X("hour:O", title="Hour of Day"),
        y=alt.Y("camera_id:O", title="Camera ID"),
        color=alt.Color("total_flow:Q", title="Flow", scale=alt.Scale(scheme="blues")),
        tooltip=["camera_id", "hour", "total_flow"]
    )
    .properties(height=400)
)

st.altair_chart(heatmap_chart, width="stretch")

# -----------------------------------------
# DAILY TOTALS LINE CHART
# -----------------------------------------
st.subheader("Daily totals — Inside vs Outside")

daily_df = (
    df.groupby("date")
    .agg(
        total_inside=("total_inside", "sum"),
        total_outside=("total_outside", "sum")
    )
    .reset_index()
)

daily_melt = daily_df.melt(
    id_vars="date",
    value_vars=["total_inside", "total_outside"],
    var_name="direction",
    value_name="count"
)

daily_chart = (
    alt.Chart(daily_melt)
    .mark_line(point=True)
    .encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("count:Q", title="People Count"),
        color="direction:N",
        tooltip=["date", "direction", "count"]
    )
    .properties(height=350)
)

st.altair_chart(daily_chart, width="stretch")

# -----------------------------------------
# PEAK HOUR DETECTION
# -----------------------------------------
st.subheader("Peak hour per camera")

peak_df = (
    df.groupby(["camera_id", "hour"])
    .agg(total_flow=("total_inside", "sum"))
    .reset_index()
)

peak_idx = peak_df.groupby("camera_id")["total_flow"].idxmax()
peak_hours = peak_df.loc[peak_idx].sort_values("camera_id")

st.dataframe(peak_hours)

# -----------------------------------------
# MULTI-CAMERA COMPARISON CHART
# -----------------------------------------
st.subheader("Camera comparison — Total flow per hour")

compare_df = (
    df.groupby(["camera_id", "hour"])
    .agg(total_flow=("total_inside", "sum"))
    .reset_index()
)

compare_chart = (
    alt.Chart(compare_df)
    .mark_line(point=True)
    .encode(
        x=alt.X("hour:O", title="Hour of Day"),
        y=alt.Y("total_flow:Q", title="Flow"),
        color=alt.Color("camera_id:N", title="Camera ID"),
        tooltip=["camera_id", "hour", "total_flow"]
    )
    .properties(height=350)
)

st.altair_chart(compare_chart, width="stretch")

# -----------------------------------------
# RAW DATA VIEW
# -----------------------------------------
with st.expander("Show aggregated data (per camera, per date, per hour)"):
    grouped = (
        df.groupby(["camera_id", "date", "hour"])
        .agg(
            total_inside=("total_inside", "sum"),
            total_outside=("total_outside", "sum")
        )
        .reset_index()
    )
    st.dataframe(grouped.sort_values(["camera_id", "date", "hour"]))
    
    
with st.expander("Show login_camera data)"):
    st.dataframe(df1[["id", "location", "pong_ts", "pong_ts_last_fail"]])
