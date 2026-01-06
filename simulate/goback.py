"""
ends the simulation going back to the originall db the oe in camera_data.csv
"""
import sys
import os

# Add parent folder to Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from setup_camera_data_db import setup_camera_data_db

def main():
    print("Running goback...")
    setup_camera_data_db()

if __name__ == "__main__":
    main()
