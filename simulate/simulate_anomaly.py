"""
Simulate anomalies in the database.
anomalie occur when there is a sudden drop or spike in the number of people detected by the cameras.
the variation must be greater than 30% compared to the mean of the same weekday in the last 8 weeks
parameters to this simulation:
camera_id, date, anomaly_type (drop or spike), magnitude (percentage of variation)
"""


print("Anomaly")
