"""
This program executes the following tasks:

a) create the sqlite db nodehub for tables 
    1)peopleflowtotals
    2)ogin_camera
    
2) reads the csvfiles in google drive to load these tables
    1)peopleflowtotals.csv 
    2)login_camera.csv 

3) compact records with valid=0

4) calculate the average and sigma for the last 8  weekday and hour for each camera 
    if there is no data enough for the average put min estimated data in login camera 
    
5) store these values in login_camera_aux

6) find failed camera - hours  records
    1) hours later than pong_ts register in the login_camera
    2) hours with total_inside and total_outside smaller than average+-2*Sigma 

7) Calculate the ratio =
    sum good cameraas today  /
    sum good camras in login_camera_aux
    if no good cameras ratio=1
 
 8) Replace failed cameras values with 
    (average in login_camera_aux)*ratio


quando detect fa



"""
