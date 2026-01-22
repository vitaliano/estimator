"""
Simulate date arrive late in the database 
1- simulate camera fail 
3- insert with valid=0 the rows that were deleted in the camera fail simulation 

"""

import argparse
import sqlite3
from datetime import datetime
from simulate_camera_fail import simulate_camera_fail



def simulate_data_arrive_late(camera_id,target_date, target_hour):
    db_path = "nodehub.db"

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        print(f"Simulating date arrive late for camera_id={camera_id} from date {target_date} after hour={target_hour}")


        # ---------------------------------------------------------
        # 1. Find the rows to be deleted
        # ---------------------------------------------------------
        cur.execute("""
            SELECT camera_id,created_at, total_inside,total_outside FROM peopleflowtotals
            WHERE camera_id = ?
                AND strftime('%Y-%m-%d', created_at) = ?
                AND strftime('%H', created_at) >= ?
        """, (camera_id, target_date, f"{int(target_hour):02d}")) 
        rows_to_reinsert = cur.fetchall()
        print(f"Found {len(rows_to_reinsert)} rows to re-insert later with valid=0.")
        conn.commit()
        conn.close()

        # ---------------------------------------------------------
        # 2. Simulate camera fail (delete the rows reinserted with valid=0
        # ---------------------------------------------------------
        simulate_camera_fail(camera_id,target_date, target_hour)
        # ---------------------------------------------------------

        # ---------------------------------------------------------
        # 3. Re-insert the rows with valid=0
        # ---------------------------------------------------------
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        for row in rows_to_reinsert:
            camera_id, created_at, total_inside, total_outside = row
            cur.execute("""
                INSERT INTO peopleflowtotals (camera_id, created_at, total_inside, total_outside, valid)
                VALUES (?, ?, ?, ?, 0)
            """, (camera_id, created_at, total_inside, total_outside))
        conn.commit()
        print(f"Re-inserted {len(rows_to_reinsert)} rows with valid=0.")
        conn.close()    
        
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate camera failure")
    parser.add_argument("--camera-id", required=True, help="Camera ID to fail")
    parser.add_argument("--target-date", required=True, help="Date for deletion")
    parser.add_argument("--target-hour", required=True, help="Hour threshold for deletion")

    args = parser.parse_args()

    simulate_data_arrive_late(args.camera_id, args.target_date, args.target_hour)