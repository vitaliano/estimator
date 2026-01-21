import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
import warnings
warnings.filterwarnings('ignore')

class CameraDataImputer:
    def __init__(self, db_path: str, target_client_locations: List[Tuple[str, str]] = None):
        """
        Initialize with SQLite database path.
        
        Args:
            db_path: Path to SQLite database
            target_client_locations: List of (client, location) tuples to process.
                                    If None, process all client-location pairs.
        """
        self.db_path = db_path
        self.target_client_locations = target_client_locations
        self.conn = None
        self.cameras_df = None
        self.flow_df = None
        self.weekday_columns = {
            0: ('counting_hour_monday', 'counting_hour_monday_qtd'),    # Monday
            1: ('counting_hour_tuesday', 'counting_hour_tuesday_qtd'),  # Tuesday
            2: ('counting_hour_wednesday', 'counting_hour_wednesday_qtd'),  # Wednesday
            3: ('counting_hour_thursday', 'counting_hour_thursday_qtd'),    # Thursday
            4: ('counting_hour_fryday', 'counting_hour_fryday_qtd'),        # Friday
            5: ('counting_hour_saturday', 'counting_hour_saturday_qtd'),    # Saturday
            6: ('counting_hour_sunday', 'counting_hour_sunday_qtd'),        # Sunday
        }
        
    def connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path)
        
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            
    def get_client_location_list(self) -> List[Tuple[str, str]]:
        """
        Get list of client-location pairs to process.
        If target_client_locations is specified, use that.
        Otherwise, get all client-location pairs from database.
        """
        if self.target_client_locations:
            return self.target_client_locations
            
        # If no target specified, get all client-location pairs from database
        self.connect()
        query = """
            SELECT DISTINCT client, location 
            FROM login_camera 
            WHERE client IS NOT NULL AND location IS NOT NULL
            ORDER BY client, location
        """
        df = pd.read_sql_query(query, self.conn)
        self.disconnect()
        
        client_locations = list(df.itertuples(index=False, name=None))
        print(f"Found {len(client_locations)} client-location pairs in database")
        return client_locations
    
    def load_data_for_client_location(self, client: str, location: str, days_back: int = 30) -> bool:
        """
        Load camera and peopleflow data for a specific client-location.
        
        Args:
            client: Client name
            location: Location name
            days_back: Number of days to load historical data
            
        Returns:
            True if data loaded successfully, False otherwise
        """
        print(f"\n{'='*60}")
        print(f"Processing: {client} - {location}")
        print(f"{'='*60}")
        
        self.connect()
        
        # Load cameras for this specific client-location
        camera_query = """
            SELECT 
                id,
                client,
                location,
                pong_ts,
                counting_hour_sunday,
                counting_hour_sunday_qtd,
                counting_hour_monday,
                counting_hour_monday_qtd,
                counting_hour_tuesday,
                counting_hour_tuesday_qtd,
                counting_hour_wednesday,
                counting_hour_wednesday_qtd,
                counting_hour_thursday,
                counting_hour_thursday_qtd,
                counting_hour_fryday,
                counting_hour_fryday_qtd,
                counting_hour_saturday,
                counting_hour_saturday_qtd,
                counting_hour_holiday,
                counting_hour_holiday_qtd
            FROM login_camera
            WHERE client = ? AND location = ?
        """
        
        self.cameras_df = pd.read_sql_query(camera_query, self.conn, params=[client, location])
        
        if self.cameras_df.empty:
            print(f"No cameras found for {client} - {location}")
            self.disconnect()
            return False
            
        print(f"Loaded {len(self.cameras_df)} cameras for {client} - {location}")
        
        # Get camera IDs for this client-location
        target_camera_ids = self.cameras_df['id'].unique()
        
        # Calculate cutoff date based on days_back parameter
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Load peopleflow totals for the last N days, only for target cameras
        placeholders = ','.join(['?'] * len(target_camera_ids))
        
        peopleflow_query = f"""
            SELECT id, created_at, camera_id, total_inside, total_outside, valid 
            FROM peopleflowtotals 
            WHERE created_at >= ? 
            AND camera_id IN ({placeholders})
            AND valid = 1
        """
        
        # Prepare parameters
        peopleflow_params = [cutoff_date] + target_camera_ids.tolist()
        
        self.flow_df = pd.read_sql_query(
            peopleflow_query, 
            self.conn, 
            params=peopleflow_params
        )
        
        # Convert datetime columns
        if not self.flow_df.empty:
            self.flow_df['created_at'] = pd.to_datetime(self.flow_df['created_at'])
            self.flow_df['date'] = self.flow_df['created_at'].dt.date
            self.flow_df['hour'] = self.flow_df['created_at'].dt.hour
            self.flow_df['weekday'] = self.flow_df['created_at'].dt.weekday
            
            # Calculate actual date range loaded
            min_date = self.flow_df['date'].min()
            max_date = self.flow_df['date'].max()
            date_range_days = (max_date - min_date).days + 1 if max_date != min_date else 1
            print(f"Loaded {len(self.flow_df)} peopleflow records from {min_date} to {max_date} ({date_range_days} days)")
        else:
            print(f"No peopleflow data found for {client} - {location}")
            # Don't disconnect yet, we might need connection for inserting data
            return False
            
        return True
    
    def get_camera_active_hours(self, camera_id: int, weekday: int) -> Tuple[int, int]:
        """
        Get active hour range for a specific camera and weekday.
        
        Args:
            camera_id: Camera ID
            weekday: Weekday (0=Monday, 6=Sunday)
            
        Returns:
            Tuple of (start_hour, end_hour)
        """
        if camera_id not in self.cameras_df['id'].values:
            return (0, 23)  # Default to all hours if camera not found
            
        camera_row = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
        
        if weekday in self.weekday_columns:
            start_col, end_col = self.weekday_columns[weekday]
            start_hour = camera_row[start_col]
            end_hour = camera_row[end_col]
            
            # Handle None/NaN values
            if pd.isna(start_hour) or pd.isna(end_hour):
                return (0, 23)
                
            # Ensure valid range
            start_hour = max(0, min(23, int(start_hour)))
            end_hour = max(0, min(23, int(end_hour)))
            
            return (start_hour, end_hour)
        else:
            return (0, 23)  # Default to all hours
        
    def get_last_valid_day(self) -> Optional[datetime]:
        """
        Get the last day in the loaded data that has valid=1 data.
        
        Returns:
            datetime object for the last valid day, or None if no valid data
        """
        if self.flow_df.empty:
            print("No valid data available in the loaded dataset.")
            return None
            
        # Get the most recent date from valid data
        last_date = self.flow_df['date'].max()
        last_datetime = datetime.combine(last_date, datetime.min.time())
        
        print(f"Last valid day for this client-location: {last_date}")
        
        return last_datetime
    
    def identify_failing_cameras(self, target_date: datetime = None) -> Dict[int, List[int]]:
        """
        Identify failing cameras for the current client-location.
        
        Args:
            target_date: Date to check (default: last valid day in loaded data)
            
        Returns:
            Dictionary with failing cameras and missing hours
        """
        if target_date is None:
            target_date = self.get_last_valid_day()
            
        if target_date is None:
            print("No target date available. Cannot identify failing cameras.")
            return {}
            
        target_date_str = target_date.strftime('%Y-%m-%d')
        target_weekday = target_date.weekday()
        print(f"\nChecking for failing cameras on {target_date_str} (weekday: {target_weekday})")
        
        # Get all camera IDs for current client-location
        camera_ids = self.cameras_df['id'].tolist()
        
        # Get data for target date
        target_data = self.flow_df[
            (self.flow_df['camera_id'].isin(camera_ids)) &
            (self.flow_df['date'] == target_date.date()) & 
            (self.flow_df['valid'] == 1)
        ]
        
        failing_cameras = {}
        
        for camera_id in camera_ids:
            # Get active hour range for this camera and weekday
            start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
            active_hours = list(range(start_hour, end_hour + 1))
            
            if not active_hours:
                continue
                
            camera_failed_hours = []
            
            for hour in active_hours:
                hour_data = target_data[
                    (target_data['camera_id'] == camera_id) &
                    (target_data['hour'] == hour)
                ]
                if camera_id==155266:
                    print(hour_data)




                if hour_data.empty:
                    # Camera has no data for this active hour
                    camera_failed_hours.append(hour)
                else:
                    # Check for anomalously low counts
                    row = hour_data.iloc[0]
                    current_count = row['total_inside'] + row['total_outside']
                    
                    # Get historical average for comparison
                    hist_avg = self._get_historical_average(camera_id, hour, target_weekday)
                    
                    # Mark as failed if count is less than 20% of historical average
                    if hist_avg > 10 and current_count < (hist_avg * 0.2):
                        camera_failed_hours.append(hour)
                        print(f"  Camera {camera_id} hour {hour}: Low count ({current_count} vs avg {hist_avg:.1f})")
            
            if camera_failed_hours:
                failing_cameras[camera_id] = camera_failed_hours
        
        # Print summary
        print(f"\nFound {len(failing_cameras)} failing cameras:")
        for camera_id, hours in failing_cameras.items():
            camera_info = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
            start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
            print(f"  Camera {camera_id}: Active {start_hour}-{end_hour}, Missing hours {hours}")
            
        return failing_cameras
    
    def _get_historical_average(self, camera_id: int, hour: int, weekday: int, 
                               weeks_back: int = 4) -> float:
        """Get historical average counts for specific camera, hour, and weekday."""
        # Get data from previous weeks (same weekday, same hour)
        mask = (
            (self.flow_df['camera_id'] == camera_id) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        )
        
        historical_data = self.flow_df[mask]
        
        if len(historical_data) == 0:
            return 0
            
        # Calculate average total traffic
        total_traffic = historical_data['total_inside'] + historical_data['total_outside']
        return total_traffic.mean()
    
    def _get_camera_relationships(self, target_weekday: int) -> Dict[int, Dict[int, float]]:
        """
        Calculate proportional relationships between cameras within the current client-location.
        
        Args:
            target_weekday: Weekday to calculate relationships for
            
        Returns:
            Dictionary mapping camera_id to dict of other camera ratios
        """
        print(f"\nCalculating camera relationships for weekday {target_weekday}...")
        
        camera_relationships = {}
        camera_ids = self.cameras_df['id'].tolist()
        
        # Calculate daily totals for each camera for the target weekday
        daily_totals = {}
        for camera_id in camera_ids:
            camera_data = self.flow_df[
                (self.flow_df['camera_id'] == camera_id) &
                (self.flow_df['weekday'] == target_weekday)
            ]
            
            if len(camera_data) > 0:
                daily_totals[camera_id] = {}
                for date in camera_data['date'].unique():
                    date_data = camera_data[camera_data['date'] == date]
                    daily_total = (date_data['total_inside'].sum() + 
                                  date_data['total_outside'].sum())
                    daily_totals[camera_id][date] = daily_total
        
        # Calculate ratios between cameras
        for camera_id in camera_ids:
            camera_relationships[camera_id] = {}
            
            if camera_id not in daily_totals:
                continue
                
            for other_id in camera_ids:
                if other_id == camera_id or other_id not in daily_totals:
                    continue
                    
                # Find common dates
                common_dates = set(daily_totals[camera_id].keys()) & set(daily_totals[other_id].keys())
                if len(common_dates) >= 2:  # Need at least 2 common dates for reliable ratio
                    ratios = []
                    for date in common_dates:
                        if daily_totals[camera_id][date] > 0:
                            ratio = daily_totals[other_id][date] / daily_totals[camera_id][date]
                            ratios.append(ratio)
                    
                    if ratios:
                        # Use median for robustness against outliers
                        camera_relationships[camera_id][other_id] = np.median(ratios)
        
        # Print relationship summary
        cameras_with_relationships = len([c for c in camera_relationships if camera_relationships[c]])
        print(f"Calculated relationships for {cameras_with_relationships} cameras")
        
        return camera_relationships
    
    def _get_weekday_patterns(self, camera_id: int) -> Dict[int, float]:
        """
        Get weekday patterns for a camera.
        
        Returns:
            Dictionary mapping weekday (0-6) to relative factor
        """
        camera_data = self.flow_df[self.flow_df['camera_id'] == camera_id]
        
        if len(camera_data) == 0:
            return {i: 1.0 for i in range(7)}
        
        # Calculate average traffic per weekday
        weekday_totals = {}
        weekday_counts = {}
        
        for weekday in range(7):
            weekday_data = camera_data[camera_data['weekday'] == weekday]
            if len(weekday_data) > 0:
                # Only count active hours for each day
                total_traffic = 0
                hour_count = 0
                
                for hour in range(24):
                    hour_data = weekday_data[weekday_data['hour'] == hour]
                    if not hour_data.empty:
                        # Get active hours for this camera and weekday
                        start_hour, end_hour = self.get_camera_active_hours(camera_id, weekday)
                        if start_hour <= hour <= end_hour:
                            total_traffic += hour_data['total_inside'].sum() + hour_data['total_outside'].sum()
                            hour_count += 1
                
                if hour_count > 0:
                    weekday_totals[weekday] = total_traffic
                    weekday_counts[weekday] = hour_count
        
        # Normalize to get relative factors
        if weekday_totals:
            # Calculate average traffic per active hour for each weekday
            weekday_avg_per_hour = {wd: weekday_totals[wd]/weekday_counts[wd] 
                                   for wd in weekday_totals if weekday_counts[wd] > 0}
            
            if weekday_avg_per_hour:
                overall_avg = np.mean(list(weekday_avg_per_hour.values()))
                weekday_factors = {wd: avg/overall_avg for wd, avg in weekday_avg_per_hour.items()}
                
                # Fill missing weekdays with nearest available
                for wd in range(7):
                    if wd not in weekday_factors:
                        # Find nearest weekday with data
                        distances = [(abs(wd - other_wd), other_wd) 
                                    for other_wd in weekday_factors.keys()]
                        _, nearest_wd = min(distances)
                        weekday_factors[wd] = weekday_factors[nearest_wd]
                        
                return weekday_factors
        
        return {i: 1.0 for i in range(7)}
    
    def estimate_missing_data(self, failing_cameras: Dict[int, List[int]], 
                             target_date: datetime) -> pd.DataFrame:
        """
        Estimate missing data for failing cameras.
        
        Args:
            failing_cameras: Dictionary of failing cameras and hours
            target_date: Target date for estimation
            
        Returns:
            DataFrame with estimated records
        """
        print(f"\nEstimating missing data for {target_date.date()}...")
        
        target_weekday = target_date.weekday()
        camera_relationships = self._get_camera_relationships(target_weekday)
        
        estimated_records = []
        camera_ids = list(failing_cameras.keys())
        
        for camera_id in camera_ids:
            missing_hours = failing_cameras[camera_id]
            
            # Get active hour range for validation
            start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
            active_hours = set(range(start_hour, end_hour + 1))
            
            # Filter missing hours to only include active hours
            missing_hours = [h for h in missing_hours if h in active_hours]
            
            if not missing_hours:
                continue
                
            print(f"\nProcessing Camera {camera_id}: {len(missing_hours)} missing/low hours (active: {start_hour}-{end_hour})")
            
            # Get weekday pattern for this camera
            weekday_factors = self._get_weekday_patterns(camera_id)
            target_factor = weekday_factors[target_weekday]
            
            # Find working cameras in the same client-location related to this one
            related_cameras = []
            if camera_id in camera_relationships:
                for other_id, ratio in camera_relationships[camera_id].items():
                    # Only consider cameras that are not failing
                    if other_id in failing_cameras:
                        continue  # Skip other failing cameras
                    
                    # Check if other camera has data for the missing hours
                    other_data = self.flow_df[
                        (self.flow_df['camera_id'] == other_id) &
                        (self.flow_df['date'] == target_date.date()) &
                        (self.flow_df['valid'] == 1)
                    ]
                    if len(other_data) > 0:
                        # Also check if other camera should be active at these hours
                        other_start, other_end = self.get_camera_active_hours(other_id, target_weekday)
                        other_active_hours = set(range(other_start, other_end + 1))
                        
                        # Check if other camera has data for at least some of the missing hours
                        common_hours = other_active_hours.intersection(set(missing_hours))
                        if common_hours:
                            related_cameras.append((other_id, ratio, other_active_hours))
            
            if not related_cameras:
                print(f"  No related working cameras found for Camera {camera_id}")
                
                # Try to use camera's own historical data
                self._estimate_from_own_history(camera_id, missing_hours, target_date, 
                                               target_factor, estimated_records)
                continue
                
            print(f"  Found {len(related_cameras)} related working cameras")
            
            for hour in missing_hours:
                # Get estimates from each related camera
                estimates_inside = []
                estimates_outside = []
                
                for other_id, base_ratio, other_active_hours in related_cameras:
                    if hour not in other_active_hours:
                        continue  # Other camera is not active at this hour
                    
                    other_hour_data = self.flow_df[
                        (self.flow_df['camera_id'] == other_id) &
                        (self.flow_df['date'] == target_date.date()) &
                        (self.flow_df['hour'] == hour) &
                        (self.flow_df['valid'] == 1)
                    ]
                    
                    if len(other_hour_data) > 0:
                        # Get historical ratio for this specific hour and weekday
                        hist_ratio = self._get_hourly_ratio(camera_id, other_id, hour, target_weekday)
                        
                        if hist_ratio > 0:
                            other_row = other_hour_data.iloc[0]
                            # Adjust ratio by weekday factor
                            adjusted_ratio = hist_ratio * target_factor
                            
                            # Estimate counts
                            estimated_inside = int(other_row['total_inside'] * adjusted_ratio)
                            estimated_outside = int(other_row['total_outside'] * adjusted_ratio)
                            
                            estimates_inside.append(estimated_inside)
                            estimates_outside.append(estimated_outside)
                
                if estimates_inside:
                    # Use median of all estimates
                    estimated_inside = int(np.median(estimates_inside))
                    estimated_outside = int(np.median(estimates_outside))
                    
                    # Ensure non-negative values
                    estimated_inside = max(0, estimated_inside)
                    estimated_outside = max(0, estimated_outside)
                    
                    # Create timestamp for this hour
                    hour_timestamp = datetime(
                        target_date.year, target_date.month, target_date.day, hour
                    )
                    
                    record = {
                        'created_at': hour_timestamp,
                        'camera_id': camera_id,
                        'total_inside': estimated_inside,
                        'total_outside': estimated_outside,
                        'valid': 1,
                        'estimated': 1,
                        'client': self.cameras_df[self.cameras_df['id'] == camera_id]['client'].iloc[0],
                        'location': self.cameras_df[self.cameras_df['id'] == camera_id]['location'].iloc[0]
                    }
                    
                    estimated_records.append(record)
                    print(f"  Hour {hour:02d}: Estimated {estimated_inside} in, {estimated_outside} out")
                else:
                    # Fall back to camera's own historical data
                    self._estimate_hour_from_history(camera_id, hour, target_date, 
                                                    target_factor, estimated_records)
        
        return pd.DataFrame(estimated_records)
    
    def _estimate_from_own_history(self, camera_id: int, missing_hours: List[int], 
                                  target_date: datetime, target_factor: float,
                                  estimated_records: List[Dict]):
        """Estimate data using camera's own historical patterns."""
        print(f"  Using historical patterns for Camera {camera_id}")
        
        target_weekday = target_date.weekday()
        camera_info = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
        client = camera_info['client']
        location = camera_info['location']
        
        for hour in missing_hours:
            # Get historical average for this camera, hour, and weekday
            hist_data = self.flow_df[
                (self.flow_df['camera_id'] == camera_id) &
                (self.flow_df['hour'] == hour) &
                (self.flow_df['weekday'] == target_weekday)
            ]
            
            if len(hist_data) >= 2:  # Need at least 2 historical points
                # Calculate average in/out
                avg_inside = hist_data['total_inside'].mean()
                avg_outside = hist_data['total_outside'].mean()
                
                # Adjust by weekday factor
                estimated_inside = int(avg_inside * target_factor)
                estimated_outside = int(avg_outside * target_factor)
                
                # Ensure non-negative
                estimated_inside = max(0, estimated_inside)
                estimated_outside = max(0, estimated_outside)
                
                # Create timestamp
                hour_timestamp = datetime(
                    target_date.year, target_date.month, target_date.day, hour
                )
                
                record = {
                    'created_at': hour_timestamp,
                    'camera_id': camera_id,
                    'total_inside': estimated_inside,
                    'total_outside': estimated_outside,
                    'valid': 1,
                    'estimated': 1,
                    'client': client,
                    'location': location
                }
                
                estimated_records.append(record)
                print(f"  Hour {hour:02d}: Historical estimate {estimated_inside} in, {estimated_outside} out")
            else:
                print(f"  Hour {hour:02d}: Insufficient historical data")
    
    def _estimate_hour_from_history(self, camera_id: int, hour: int, 
                                   target_date: datetime, target_factor: float,
                                   estimated_records: List[Dict]):
        """Estimate single hour from camera's own history."""
        target_weekday = target_date.weekday()
        camera_info = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
        client = camera_info['client']
        location = camera_info['location']
        
        # Get historical average for this camera, hour, and weekday
        hist_data = self.flow_df[
            (self.flow_df['camera_id'] == camera_id) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == target_weekday)
        ]
        
        if len(hist_data) >= 2:
            avg_inside = hist_data['total_inside'].mean()
            avg_outside = hist_data['total_outside'].mean()
            
            estimated_inside = int(avg_inside * target_factor)
            estimated_outside = int(avg_outside * target_factor)
            
            # Ensure non-negative
            estimated_inside = max(0, estimated_inside)
            estimated_outside = max(0, estimated_outside)
            
            hour_timestamp = datetime(
                target_date.year, target_date.month, target_date.day, hour
            )
            
            record = {
                'created_at': hour_timestamp,
                'camera_id': camera_id,
                'total_inside': estimated_inside,
                'total_outside': estimated_outside,
                'valid': 1,
                'estimated': 1,
                'client': client,
                'location': location
            }
            
            estimated_records.append(record)
            print(f"  Hour {hour:02d}: Historical fallback {estimated_inside} in, {estimated_outside} out")
        else:
            print(f"  Hour {hour:02d}: No data available for estimation")
    
    def _get_hourly_ratio(self, camera_a: int, camera_b: int, hour: int, weekday: int) -> float:
        """Get historical ratio between two cameras for specific hour and weekday."""
        # Get historical data for both cameras
        data_a = self.flow_df[
            (self.flow_df['camera_id'] == camera_a) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        data_b = self.flow_df[
            (self.flow_df['camera_id'] == camera_b) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        if len(data_a) == 0 or len(data_b) == 0:
            return 0
        
        # Find common dates
        dates_a = set(data_a['date'].unique())
        dates_b = set(data_b['date'].unique())
        common_dates = dates_a & dates_b
        
        if len(common_dates) < 2:
            return 0
        
        # Calculate ratios for common dates
        ratios = []
        for date in common_dates:
            total_a = (data_a[data_a['date'] == date]['total_inside'].sum() + 
                      data_a[data_a['date'] == date]['total_outside'].sum())
            total_b = (data_b[data_b['date'] == date]['total_inside'].sum() + 
                      data_b[data_b['date'] == date]['total_outside'].sum())
            
            if total_a > 0:
                ratios.append(total_b / total_a)
        
        return np.median(ratios) if ratios else 0
    
    def insert_estimated_data(self, estimated_df: pd.DataFrame) -> Tuple[int, int]:
        """
        Insert estimated data into the database.
        
        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if estimated_df.empty:
            print("\nNo estimated data to insert.")
            return 0, 0
        
        cursor = self.conn.cursor()
        
        inserted_count = 0
        updated_count = 0
        
        for _, row in estimated_df.iterrows():
            created_at_str = row['created_at'].strftime('%Y-%m-%d %H:%M:%S') 
            camera_id= row['camera_id'] 
            # Check if record already exists
            cursor.execute("""
                SELECT id, valid FROM peopleflowtotals 
                WHERE camera_id = ? AND created_at = ?
            """, (camera_id, created_at_str))
            
            existing = cursor.fetchone()
            
            if existing is None:
                # Insert new record
                cursor.execute("""
                    INSERT INTO peopleflowtotals 
                    (created_at, camera_id, total_inside, total_outside, valid)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    created_at_str,
                    camera_id,
                    row['total_inside'],
                    row['total_outside'],
                    1  # Mark as valid
                ))
                inserted_count += 1
            else:
                existing_id, existing_valid = existing
                if existing_valid == 0:
                    # Update existing invalid record
                    cursor.execute("""
                        UPDATE peopleflowtotals 
                        SET total_inside = ?, total_outside = ?, valid = 1
                        WHERE id = ?
                    """, (
                        row['total_inside'],
                        row['total_outside'],
                        existing_id
                    ))
                    updated_count += 1
        
        self.conn.commit()
        print(f"\nInserted {inserted_count} new records and updated {updated_count} existing records.")
        
        return inserted_count, updated_count
    
    def create_imputation_log(self, client: str, location: str, target_date: datetime,
                            estimated_df: pd.DataFrame, inserted: int, updated: int):
        """Create log entry for the imputation process."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_imputation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    imputation_date TIMESTAMP,
                    client TEXT,
                    location TEXT,
                    target_date DATE,
                    target_weekday INTEGER,
                    cameras_affected INTEGER,
                    hours_estimated INTEGER,
                    records_inserted INTEGER,
                    records_updated INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cameras_affected = len(estimated_df['camera_id'].unique())
            hours_estimated = len(estimated_df)
            
            notes = f"Imputed data for {cameras_affected} cameras, {hours_estimated} hours"
            
            cursor.execute("""
                INSERT INTO data_imputation_log 
                (imputation_date, client, location, target_date, target_weekday,
                 cameras_affected, hours_estimated, records_inserted, records_updated, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                client,
                location,
                target_date.strftime('%Y-%m-%d'),
                target_date.weekday(),
                cameras_affected,
                hours_estimated,
                inserted,
                updated,
                notes
            ))
            
            self.conn.commit()
            print("Imputation log created successfully.")
        except Exception as e:
            print(f"Note: Could not create imputation log: {e}")
    
    def process_client_location(self, client: str, location: str, days_back: int = 45) -> Dict:
        """
        Process a single client-location pair.
        
        Args:
            client: Client name
            location: Location name
            days_back: Number of days to use for historical data
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'client': client,
            'location': location,
            'success': False,
            'cameras_loaded': 0,
            'failing_cameras': 0,
            'hours_estimated': 0,
            'records_inserted': 0,
            'records_updated': 0
        }
        
        # Load data for this client-location
        if not self.load_data_for_client_location(client, location, days_back):
            return results
            
        results['cameras_loaded'] = len(self.cameras_df)
        
        # Get the last valid day from loaded data
        target_date = self.get_last_valid_day()
        
        if target_date is None:
            print("No valid data found. Skipping this client-location.")
            self.disconnect()
            return results
        
        # Identify failing cameras
        failing_cameras = self.identify_failing_cameras(target_date)
        results['failing_cameras'] = len(failing_cameras)
        
        if not failing_cameras:
            print("\nNo failing cameras detected. Nothing to do.")
            self.disconnect()
            results['success'] = True
            return results
        
        # Estimate missing data
        estimated_data = self.estimate_missing_data(failing_cameras, target_date)
        results['hours_estimated'] = len(estimated_data)
        
        if estimated_data.empty:
            print("\nCould not estimate any missing data.")
            self.disconnect()
            results['success'] = True
            return results
        
        # Insert estimated data
        inserted, updated = self.insert_estimated_data(estimated_data)
        results['records_inserted'] = inserted
        results['records_updated'] = updated
        
        # Create imputation log
        if inserted > 0 or updated > 0:
            self.create_imputation_log(client, location, target_date, estimated_data, inserted, updated)
        
        # Clean up
        self.disconnect()
        results['success'] = True
        
        return results
    
    def run_imputation(self, days_back: int = 45):
        """
        Main method to run the complete imputation process for all client-locations.
        
        Args:
            days_back: Number of days to use for historical data
        """
        print("=" * 60)
        print("CAMERA DATA IMPUTATION SYSTEM")
        print("=" * 60)
        
        # Get list of client-locations to process
        client_locations = self.get_client_location_list()
        
        if not client_locations:
            print("No client-locations to process.")
            return
        
        print(f"\nFound {len(client_locations)} client-location pairs to process")
        
        all_results = []
        successful_count = 0
        
        # Process each client-location sequentially
        for i, (client, location) in enumerate(client_locations, 1):
            print(f"\n{'='*60}")
            print(f"Processing {i}/{len(client_locations)}: {client} - {location}")
            print(f"{'='*60}")
            
            try:
                # Process this client-location
                result = self.process_client_location(client, location, days_back)
                all_results.append(result)
                
                if result['success']:
                    successful_count += 1
                    print(f"\n✓ Successfully processed {client} - {location}")
                else:
                    print(f"\n✗ Failed to process {client} - {location}")
                
                # Print summary for this client-location
                print(f"\nSummary for {client} - {location}:")
                print(f"  Cameras loaded: {result['cameras_loaded']}")
                print(f"  Failing cameras: {result['failing_cameras']}")
                print(f"  Hours estimated: {result['hours_estimated']}")
                print(f"  Records inserted: {result['records_inserted']}")
                print(f"  Records updated: {result['records_updated']}")
                
            except Exception as e:
                print(f"\n✗ Error processing {client} - {location}: {e}")
                import traceback
                traceback.print_exc()
                # Add error result
                all_results.append({
                    'client': client,
                    'location': location,
                    'success': False,
                    'error': str(e)
                })
        
        # Print final summary
        print("\n" + "=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)
        
        total_cameras = sum(r.get('cameras_loaded', 0) for r in all_results)
        total_failing = sum(r.get('failing_cameras', 0) for r in all_results)
        total_hours = sum(r.get('hours_estimated', 0) for r in all_results)
        total_inserted = sum(r.get('records_inserted', 0) for r in all_results)
        total_updated = sum(r.get('records_updated', 0) for r in all_results)
        
        print(f"\nOverall Summary:")
        print(f"  Client-locations processed: {len(client_locations)}")
        print(f"  Successfully processed: {successful_count}")
        print(f"  Total cameras loaded: {total_cameras}")
        print(f"  Total failing cameras: {total_failing}")
        print(f"  Total hours estimated: {total_hours}")
        print(f"  Total records inserted: {total_inserted}")
        print(f"  Total records updated: {total_updated}")
        
        # Print detailed results
        print(f"\nDetailed Results:")
        for result in all_results:
            status = "✓ SUCCESS" if result.get('success', False) else "✗ FAILED"
            print(f"  {result['client']} - {result['location']}: {status}")
            if 'error' in result:
                print(f"    Error: {result['error']}")


def main():
    """Main execution function."""
    # Configuration
    DB_PATH = "nodehub.db"  # Update with your database path
    
    # Define which client-location pairs to process
    # Example: [('ClientA', 'Store1'), ('ClientB', 'Store2')]
    # If None, all client-location pairs will be processed
    TARGET_CLIENT_LOCATIONS = [
        ('net3rcorp', 'teste'),
        # Add more as needed
    ]
    
    # Create imputer
    imputer = CameraDataImputer(DB_PATH, TARGET_CLIENT_LOCATIONS)
    
    try:
        # Run imputation for all client-locations
        imputer.run_imputation(
            days_back=45  # Use 45 days of historical data
        )
    except Exception as e:
        print(f"\n✗ Fatal error during imputation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure connection is closed
        try:
            imputer.disconnect()
        except:
            pass


if __name__ == "__main__":
    main()