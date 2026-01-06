"""
Simulate camera failure by:
1. Finding the most recent day with valid=0 for the selected camera
2. Deleting all rows for that day where created_at.hour >= selected hour AND valid=0
3. Updating login_camera.pong_ts to the oldest deleted created_at timestamp
"""

import argparse
import sqlite3
from datetime import datetime


def simulate_camera_failure(camera_id, hour):
    db_path = "nodehub.db"

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        print(f"Simulating camera failure for camera_id={camera_id} from hour={hour}")

        # ---------------------------------------------------------
        # 1. Find the most recent day with valid=0 for this camera
        # ---------------------------------------------------------
        cur.execute("""
            SELECT DATE(created_at)
            FROM peopleflowtotals
            WHERE camera_id = ?
              AND valid = 0
            ORDER BY DATE(created_at) DESC
            LIMIT 1
        """, (camera_id,))

        row = cur.fetchone()
        if not row:
            print("No records with valid=0 found for this camera. Nothing to delete.")
            return

        target_date = row[0]  # e.g. "2025-01-11"
        print(f"Last day with valid=0: {target_date}")

        # ---------------------------------------------------------
        # 2. Find the oldest record that will be deleted
        # ---------------------------------------------------------
        cur.execute("""
            SELECT MIN(created_at)
            FROM peopleflowtotals
            WHERE camera_id = ?
              AND DATE(created_at) = ?
              AND strftime('%H', created_at) >= ?
              AND valid = 0
        """, (camera_id, target_date, f"{int(hour):02d}"))

        oldest_deleted = cur.fetchone()[0]

        if not oldest_deleted:
            print("No rows match the deletion criteria. Nothing to delete.")
            return

        print(f"Oldest record to delete: {oldest_deleted}")

        # ---------------------------------------------------------
        # 3. Delete the rows
        # ---------------------------------------------------------
        cur.execute("""
            DELETE FROM peopleflowtotals
            WHERE camera_id = ?
              AND DATE(created_at) = ?
              AND strftime('%H', created_at) >= ?
              AND valid = 0
        """, (camera_id, target_date, f"{int(hour):02d}"))

        deleted_rows = cur.rowcount
        print(f"Deleted {deleted_rows} rows from peopleflowtotals.")

        # ---------------------------------------------------------
        # 4. Update login_camera.pong_ts to oldest deleted timestamp
        # ---------------------------------------------------------
        cur.execute("""
            UPDATE login_camera
            SET pong_ts = ? 
            WHERE id = ?
        """, (oldest_deleted, camera_id))

        if cur.rowcount == 0:
            print("WARNING: No login_camera record found for this camera_id.")
        else:
            print(f"Updated login_camera.pong_ts to {oldest_deleted}")

        conn.commit()
        conn.close()

        print("Camera failure simulation completed successfully.")

    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate camera failure")
    parser.add_argument("--camera-id", required=True, help="Camera ID to fail")
    parser.add_argument("--hour", required=True, help="Hour threshold for deletion")

    args = parser.parse_args()

    simulate_camera_failure(args.camera_id, args.hour)