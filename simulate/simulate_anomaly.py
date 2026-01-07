"""
Simulate anomalies in the database at time when  valid = 1 compressed
anomalie occur when there is a sudden drop or spike in the number of people detected by the cameras.
the variation must be greater than 30% compared to the mean of the same weekday in the last 8 weeks
parameters to this simulation:
camera_id, date, hour,anomaly_type (drop or spike), magnitude (percentage of variation)
"""

import argparse
import sqlite3
from datetime import datetime


def simulate_camera_failure(camera_id, target_date,target_hour, anomaly_type, magnitude):
    db_path = "nodehub.db"

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        print(f"Simulating anomaly for camera_id={camera_id} at {target_date} at hour = {target_hour}")

        # ---------------------------------------------------------
        # 1. Update the rows
        # ---------------------------------------------------------
        strSQL="""
            UPDATE  peopleflowtotals
            SET total_inside = total_inside * (1 + ?/100.0) ,
                total_outside = total_outside * (1 + ?/100.0)
            WHERE camera_id = ?   
                AND created_at  = ?
                AND DATEPART(HOUR,created_at) = ?
                AND valid = 1
            """, (f"{int(magnitude):02d}", f"{int(magnitude):02d}", camera_id,{target_date}, f"{int(target_hour):02d}"))
        cur.execute(strSQL)
        updated_rows = cur.rowcount
        print(f"UPDATED {updated_rows} row from peopleflowtotals.")
        conn.commit()
        conn.close()    
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate camera failure")
    parser.add_argument("--camera-id", required=True, help="Camera ID to fail")
    parser.add_argument("--target-date", required=True, help="date to fail")
    parser.add_argument("--hour", required=True, help="hour at which the anomaly will occur ")
    parser.add_argument("--anomaly-type", required=True, help="anomaly type (drop or spike)")
    parser.add_argument("--magnitude", type=float, required=True, help="magnitude (percentage of variation)")   
    args = parser.parse_args()

    simulate_camera_failure(args.camera_id, args.hour)
    print("Anomaly finished successfully.")
