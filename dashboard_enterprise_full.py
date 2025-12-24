import sqlite3
import pandas as pd
import streamlit as st
import altair as alt
from datetime import date
from sklearn.cluster import KMeans
import subprocess
import os
import sys

st.set_page_config(layout="wide")

DB_NAME = "nodehub.db"

# -------------------------------------------------------------------
# Simple CSS for nicer KPI metrics
# -------------------------------------------------------------------
st.markdown("""
<style>
    .big-metric {
        font-size: 32px;
        font-weight: bold;
        color: #4CAF50;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------------------
# Load peopleflowtotals
# -------------------------------------------------------------------
@st.cache_data
def load_peopleflow():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM peopleflowtotals", conn)
    conn.close()
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df

df_people = load_peopleflow()

# -------------------------------------------------------------------
# Load login_camera
# -------------------------------------------------------------------
@st.cache_data
def load_login_camera():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM login_camera", conn)
    conn.close()
    df["pong_ts"] = pd.to_datetime(df["pong_ts"], errors="coerce")
    df["pong_ts_last_fail"] = pd.to_datetime(df["pong_ts_last_fail"], errors="coerce")
    return df

df_login = load_login_camera()

# -------------------------------------------------------------------
# Tabs
# -------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5= st.tabs(["📊 Analytics", "🛠 Camera Health", "📄 Raw Data", "📊 Forecast", "🛠 Simulation Tool"])

# ===================================================================
# 📊 TAB 1 — ANALYTICS (Steps 1, 2, 4)
# ===================================================================
with tab1:

    df = df_people.copy()

    if df.empty:
        st.error("No data found in peopleflowtotals.")
        st.stop()

    # Sidebar filters
    st.sidebar.title("Filters")

    min_date = df["created_at"].dt.date.min()
    max_date = df["created_at"].dt.date.max()

    date_range = st.sidebar.date_input(
        "Select date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

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

    only_valid = st.sidebar.checkbox("Only valid records (valid = 1)", value=True)
    if only_valid:
        df = df[df["valid"] == 1]

    if df.empty:
        st.warning("No data after applying filters.")
        st.stop()

    df["date"] = df["created_at"].dt.date
    df["hour"] = df["created_at"].dt.hour

    st.title("📊 People Flow Analytics")

    st.caption(
        f"Date range: **{start_date}** to **{end_date}** | "
        f"Cameras: {', '.join(str(c) for c in selected_cameras)}"
    )

    # ----------------------------------------------------------------
    # Summary metrics (with KPI style)
    # ----------------------------------------------------------------
    total_inside = int(df["total_inside"].sum())
    total_outside = int(df["total_outside"].sum())
    num_cameras = df["camera_id"].nunique()
    num_days = df["date"].nunique()

    col1, col2, col3, col4 = st.columns(4)
    col1.markdown(f"<div class='big-metric'>📥 {total_inside}</div><br>Inside", unsafe_allow_html=True)
    col2.markdown(f"<div class='big-metric'>📤 {total_outside}</div><br>Outside", unsafe_allow_html=True)
    col3.markdown(f"<div class='big-metric'>🎥 {num_cameras}</div><br>Cameras", unsafe_allow_html=True)
    col4.markdown(f"<div class='big-metric'>📅 {num_days}</div><br>Days", unsafe_allow_html=True)

    # ----------------------------------------------------------------
    # Detailed per-camera hourly chart
    # ----------------------------------------------------------------
    st.subheader("Hourly flow per camera")

    camera_for_detail = st.selectbox(
        "Select camera for detailed view",
        sorted(df["camera_id"].unique()),
        index=0
    )

    df_detail = df[df["camera_id"] == camera_for_detail].copy()
    df_detail["hour_label"] = (
        df_detail["hour"].astype(str).str.zfill(2)
        + ":00 (" + df_detail["date"].astype(str) + ")"
    )

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

    # ----------------------------------------------------------------
    # Combined hourly flow (all selected cameras)
    # ----------------------------------------------------------------
    st.subheader("Hourly flow (all selected cameras combined)")

    grouped_all = (
        df.groupby(["date", "hour"])
        .agg(
            total_inside=("total_inside", "sum"),
            total_outside=("total_outside", "sum")
        )
        .reset_index()
    )

    grouped_all["hour_label"] = (
        grouped_all["hour"].astype(str).str.zfill(2)
        + ":00 (" + grouped_all["date"].astype(str) + ")"
    )

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

    # ----------------------------------------------------------------
    # Heatmap — camera x hour
    # ----------------------------------------------------------------
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

    # ----------------------------------------------------------------
    # Daily totals — Inside vs Outside
    # ----------------------------------------------------------------
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

    # ----------------------------------------------------------------
    # Peak hour per camera
    # ----------------------------------------------------------------
    st.subheader("Peak hour per camera")

    peak_df = (
        df.groupby(["camera_id", "hour"])
        .agg(total_flow=("total_inside", "sum"))
        .reset_index()
    )

    peak_idx = peak_df.groupby("camera_id")["total_flow"].idxmax()
    peak_hours = peak_df.loc[peak_idx].sort_values("camera_id")

    st.dataframe(peak_hours)

    # ----------------------------------------------------------------
    # Multi-camera comparison
    # ----------------------------------------------------------------
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

    # ----------------------------------------------------------------
    # STEP 4 — Anomaly detection (z-score on daily totals)
    # ----------------------------------------------------------------
    st.subheader("⚠️ Anomaly Detection (Daily Inside Flow)")

    if daily_df["total_inside"].std() == 0 or len(daily_df) < 5:
        st.info("Not enough variation in data to detect anomalies.")
    else:
        daily_df["zscore"] = (
            (daily_df["total_inside"] - daily_df["total_inside"].mean())
            / daily_df["total_inside"].std()
        )

        anomalies = daily_df[daily_df["zscore"].abs() > 2]

        if anomalies.empty:
            st.success("No anomalies detected in the selected period.")
        else:
            st.error(f"{len(anomalies)} anomaly day(s) detected")
            st.dataframe(anomalies[["date", "total_inside", "zscore"]])

    # ----------------------------------------------------------------
    # STEP 4 — Camera behavior clustering (K-Means)
    # ----------------------------------------------------------------
    st.subheader("🎯 Camera Behavior Clustering (Hourly Pattern)")

    cluster_df = (
        df.groupby(["camera_id", "hour"])
        .agg(total_flow=("total_inside", "sum"))
        .reset_index()
    )

    if cluster_df["camera_id"].nunique() >= 3:
        pivot = cluster_df.pivot(
            index="camera_id",
            columns="hour",
            values="total_flow"
        ).fillna(0)

        # Limit number of clusters to number of cameras
        n_clusters = min(3, pivot.shape[0])

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init="auto")
        pivot["cluster"] = kmeans.fit_predict(pivot)

        st.dataframe(pivot[["cluster"]].sort_values("cluster"))
    else:
        st.info("Need at least 3 cameras to perform clustering.")

# ===================================================================
# 🛠 TAB 2 — CAMERA HEALTH (Step 3, with tz fix)
# ===================================================================
with tab2:

    st.title("🛠 Camera Health Monitor")

    # Make 'now' timezone-naive to match DB timestamps
    now = pd.Timestamp.utcnow().tz_localize(None)
    freshness_minutes = 5
    threshold = now - pd.Timedelta(minutes=freshness_minutes)

    df_login_display = df_login.copy()

    df_login_display["status"] = df_login_display["pong_ts"].apply(
        lambda ts: "🟢 Online" if pd.notnull(ts) and ts >= threshold else "🔴 Offline"
    )

    df_login_display["last_seen"] = df_login_display["pong_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df_login_display["last_fail"] = df_login_display["pong_ts_last_fail"].dt.strftime("%Y-%m-%d %H:%M:%S")

    st.subheader("Camera Status Overview")

    st.dataframe(
        df_login_display[["id", "location", "status", "last_seen", "last_fail"]]
        .sort_values("id"),
        width="stretch"
    )

    offline = df_login_display[df_login_display["status"] == "🔴 Offline"]

    if not offline.empty:
        st.error(f"{len(offline)} camera(s) offline")
        st.dataframe(offline[["id", "location", "last_seen"]])

    st.download_button(
        "Download Camera Health CSV",
        df_login_display.to_csv(index=False),
        "camera_health.csv",
        "text/csv"
    )

# ===================================================================
# 📄 TAB 3 — RAW DATA (INCLUDING login_camera)
# ===================================================================
with tab3:

    st.title("📄 Raw Data Explorer")

    st.subheader("Peopleflow Aggregated Data (per camera, date, hour)")

    df = df_people.copy()
    df["date"] = df["created_at"].dt.date
    df["hour"] = df["created_at"].dt.hour

    grouped = (
        df.groupby(["camera_id", "date", "hour"])
        .agg(
            total_inside=("total_inside", "sum"),
            total_outside=("total_outside", "sum")
        )
        .reset_index()
    )

    st.dataframe(grouped.sort_values(["camera_id", "date", "hour"]))

    st.subheader("Login Camera Table (raw)")
    st.dataframe(df_login)


# ===================================================================
# 📄 TAB 4 — 7 days forecast
# ===================================================================
with tab4:

    # ----------------------------------------------------------------
    # STEP 4 — Forecasting (moving average)
    # ----------------------------------------------------------------
    st.subheader("📈 Forecast — Next 7 Days (Moving Average) must be improved")
    
    forecast_df = (
        df.groupby("date")
        .agg(total_flow=("total_inside", "sum"))
        .reset_index()
    ).sort_values("date")

    if len(forecast_df) >= 3:
        forecast_df["ma3"] = forecast_df["total_flow"].rolling(window=3).mean()

        last_date = forecast_df["date"].max()
        future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=7)

        last_ma = forecast_df["ma3"].iloc[-1]
        future_values = [last_ma] * 7

        future_df = pd.DataFrame({
            "date": future_dates,
            "forecast": future_values
        })

        chart_forecast = (
            alt.Chart(forecast_df)
            .mark_line(point=True)
            .encode(
                x="date:T",
                y="total_flow:Q",
                tooltip=["date", "total_flow"]
            )
            +
            alt.Chart(future_df)
            .mark_line(point=True, strokeDash=[5, 5], color="orange")
            .encode(
                x="date:T",
                y="forecast:Q",
                tooltip=["date", "forecast"]
            )
        ).properties(height=350)

        st.altair_chart(chart_forecast, width="stretch")
    else:
        st.info("Not enough data points to compute a 3-day moving average forecast.")




# ===================================================================
# 📄 TAB 5 — cameras fail simulation
# ===================================================================

with tab5:

    st.header("Simulation Tools")
    st.write("Use these tools to simulate database errors and test system resilience.")

    import subprocess
    import sys
    import os

    def run_script(script_name, args=None):
        """Run a Python script inside the simulate/ folder with optional arguments."""
        script_path = os.path.join("simulate", script_name)

        cmd = [sys.executable, script_path]

        if args:
            cmd.extend(args)

        result = subprocess.run(cmd, capture_output=True, text=True)

        st.subheader("Output")
        if result.stdout:
            st.code(result.stdout)

        if result.stderr:
            st.error(result.stderr)

    # ---------------------------------------------------------
    # 1. SIMULATE ANOMALY
    # ---------------------------------------------------------
    with st.expander("Simulate Anomaly"):
        st.write("Create a sudden spike or drop in people count for a specific camera.")

        camera_id = st.text_input("Camera ID", key="anomaly_camera_id")
        date = st.date_input("Date", key="anomaly_date")
        anomaly_type = st.selectbox("Anomaly Type", ["drop", "spike"], key="anomaly_type")
        magnitude = st.number_input("Magnitude (%)", min_value=1.0, step=1.0, key="anomaly_magnitude")

        if st.button("Run Anomaly Simulation", key="run_anomaly"):
            if not camera_id:
                st.error("Camera ID is required.")
            else:
                args = [
                    "--camera-id", camera_id,
                    "--date", date.strftime("%Y-%m-%d"),
                    "--anomaly-type", anomaly_type,
                    "--magnitude", str(magnitude)
                ]
                run_script("simulate_anomaly.py", args)

    # ---------------------------------------------------------
    # 2. SIMULATE CAMERA FAILURE
    # ---------------------------------------------------------
    with st.expander("Simulate Camera Failure"):
        st.write("Force a camera to go offline in the database.")

        camera_fail_id = st.text_input("Camera ID to fail", key="camera_fail_id")

        if st.button("Trigger Camera Failure", key="run_camera_fail"):
            if not camera_fail_id:
                st.error("Camera ID is required.")
            else:
                run_script("simulate_camera_fail.py", ["--camera-id", camera_fail_id])

    # ---------------------------------------------------------
    # 3. SIMULATE MISSING DATA
    # ---------------------------------------------------------
    with st.expander("Simulate Missing Data"):
        st.write("Remove or blank out data for a specific date or camera.")

        missing_camera_id = st.text_input("Camera ID", key="missing_camera_id")
        missing_date = st.date_input("Date", key="missing_date")

        if st.button("Simulate Missing Data", key="run_missing"):
            if not missing_camera_id:
                st.error("Camera ID is required.")
            else:
                args = [
                    "--camera-id", missing_camera_id,
                    "--date", missing_date.strftime("%Y-%m-%d")
                ]
                run_script("simulate_missing_data.py", args)

    # ---------------------------------------------------------
    # 4. SIMULATE WRONG TOTALS
    # ---------------------------------------------------------
    with st.expander("Simulate Wrong Totals"):
        st.write("Inject incorrect totals into the peopleflow table.")

        wrong_camera_id = st.text_input("Camera ID", key="wrong_camera_id")
        wrong_date = st.date_input("Date", key="wrong_date")
        wrong_value = st.number_input("Wrong Total Value", min_value=0, step=1, key="wrong_value")

        if st.button("Inject Wrong Totals", key="run_wrong_totals"):
            if not wrong_camera_id:
                st.error("Camera ID is required.")
            else:
                args = [
                    "--camera-id", wrong_camera_id,
                    "--date", wrong_date.strftime("%Y-%m-%d"),
                    "--value", str(wrong_value)
                ]
                run_script("simulate_wrong_totals.py", args)

    # ---------------------------------------------------------
    # 5. SIMULATE DATA CAME DELAYED
    # ---------------------------------------------------------
    with st.expander("Simulate Data Came Delayed"):
        st.write("Upload a CSV file representing delayed data to be inserted into the database.")

        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"], key="delayed_csv")

        if uploaded_file is not None:
            st.success("File uploaded successfully.")

            # Save uploaded file temporarily
            temp_path = os.path.join("simulate", "temp_delayed_data.csv")
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            st.write("Ready to simulate delayed data.")

            if st.button("Run Delayed Data Simulation", key="run_delayed_data"):
                args = ["--file", temp_path]
                run_script("simulate_data_came_delayed.py", args)