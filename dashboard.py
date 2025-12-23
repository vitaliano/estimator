import sqlite3
import pandas as pd
import streamlit as st
import altair as alt

DB_NAME = "nodehub.db"

# -----------------------------------------
# Load data from SQLite
# -----------------------------------------
@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM peopleflowtotals", conn)
    conn.close()
    return df

df = load_data()

# Convert created_at to datetime
df["created_at"] = pd.to_datetime(df["created_at"])

# -----------------------------------------
# Sidebar filters
# -----------------------------------------
st.sidebar.header("Filters")

# Date selector
selected_date = st.sidebar.date_input(
    "Select a date",
    value=df["created_at"].dt.date.min()
)

# Filter by selected date
df_day = df[df["created_at"].dt.date == selected_date]

# Camera selector
camera_list = sorted(df_day["camera_id"].unique())
selected_camera = st.sidebar.selectbox("Select a camera", camera_list)

# Filter by camera
df_cam = df_day[df_day["camera_id"] == selected_camera].copy()

# -----------------------------------------
# Aggregate per hour
# -----------------------------------------
df_cam["hour"] = df_cam["created_at"].dt.hour

df_hourly = df_cam.groupby("hour").agg({
    "total_inside": "sum",
    "total_outside": "sum"
}).reset_index()

# Melt for chart
df_melt = df_hourly.melt(
    id_vars="hour",
    value_vars=["total_inside", "total_outside"],
    var_name="direction",
    value_name="count"
)

# -----------------------------------------
# Dashboard Title
# -----------------------------------------
st.title("📊 People Flow Dashboard")
st.subheader(f"Camera {selected_camera} — {selected_date}")

# -----------------------------------------
# Bar Chart
# -----------------------------------------

chart = (
    alt.Chart(df_melt)
    .mark_bar()
    .encode(
        x=alt.X("hour:O", title="Hour of Day"),
        y=alt.Y("count:Q", title="People Count"),
        color="direction:N",
        tooltip=["hour", "direction", "count"]
    )
    .properties(height=400)
)

st.altair_chart(chart, width="stretch")


# -----------------------------------------
# Show raw data (optional)
# -----------------------------------------
with st.expander("Show raw hourly data"):
    st.dataframe(df_hourly)