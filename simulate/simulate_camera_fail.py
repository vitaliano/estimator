"""
Simulate camera failure in the database 
from the datetime indicated to the end of the day
camera_id, date, hour
"""

import argparse
import sqlite3
from datetime import datetime


def simulate_camera_fail(camera_id,target_date, target_hour):
    db_path = "nodehub.db"

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        print(f"Simulating camera failure for camera_id={camera_id} from date {target_date} after hour={target_hour}")


        # ---------------------------------------------------------
        # 1. Delete the rowS
        # ---------------------------------------------------------
        cur.execute("""
            DELETE FROM peopleflowtotals
            WHERE camera_id = ?
                AND strftime('%Y-%m-%d', created_at) = ?
                AND strftime('%H', created_at) >= ?
                AND valid = 1
        """, (camera_id, target_date, f"{int(target_hour):02d}"))

        deleted_rows = cur.rowcount
        print(f"Deleted {deleted_rows} rows from peopleflowtotals.")

        # ---------------------------------------------------------
        # 2. Update login_camera.pong_ts to oldest deleted timestamp
        # ---------------------------------------------------------
        lastpong_detestring=target_date+f" {int(target_hour):02d}:00:00"
        cur.execute("""
            UPDATE login_camera
            SET pong_ts = ? 
            WHERE id = ?
        """, (lastpong_detestring, camera_id))
        if cur.rowcount == 0:
            print("WARNING: No login_camera record found for this camera_id.")
        else:
            print(f"Updated login_camera.pong_ts to {lastpong_detestring}")
        conn.commit()
        conn.close()

        print("Camera failure simulation completed successfully.")

    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate camera failure")
    parser.add_argument("--camera-id", required=True, help="Camera ID to fail")
    parser.add_argument("--target-date", required=True, help="Date for deletion")
    parser.add_argument("--target-hour", required=True, help="Hour threshold for deletion")

    args = parser.parse_args()

    simulate_camera_fail(args.camera_id, args.target_date, args.target_hour)