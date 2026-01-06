#Cria um arquivo csv simulando 63 dias de contagem de pessoas em um shopping com 6 portas
#a contagem começa 63 dias atras e vem até o dia de ontem
#os dados de todos os dias são por hora, exceptuando-se os de ontem que sao a cada 10seg

import csv
import random
from datetime import datetime, timedelta


#variáveis da simulação
camera_ids = [148782, 155266, 155310, 155325, 155542, 155681]

location = [
    'EntradaPrincipal', 'PracaDeAlimentacao', 'CorredorCentral',
    'CorredorLateral', 'LojaAncora', 'Estacionamnto']
camera_array=[2.0,1.6,1.4,0.6,1.4,0.5]

weekdays=['seg','ter','qua','qui','sex','sab','dom']
weekday_array=[0.6,0.7,0.8,0.9,1.2,1.6,1.5]

hours=[(0,9),(9,11),(11,14),(14,17),(17,20),(20,23),(23,24)]
hours_array=[0,0.5,1.2,1.0,1.5,1.1,0]
 
#yesterday = sem compressao de hora
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
#simulacao termina yesterday last hour
end_time = datetime.now().replace(hour=23, minute=59, second=50, microsecond=0) - timedelta(days=1)
#simulação começa primeira hora de 63 dias atras
start_time= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=63)
#percntagem max após as 19hs
max_inside_percentage = 0.05 



#função para calcular os totais dependendo da camerea, dia da semana e horário
def totals_i_o(cam_index,t):
    std_ent=6
    weekday_index=t.weekday()
    i=0
    for s,e in hours:
        if t.hour >= s and t.hour  < e:
            hour_index=i
            break
        i+=1
        
    f=camera_array[cam_index]*weekday_array[weekday_index]*hours_array[hour_index]
    input=round(random.randint(0,std_ent)*f)
    output=round(random.randint(0, std_ent)*f)
    return input,output

#INÍCIO DA CARGA DOS DASDOS SIMULADOS
day_total_inside=0
day_total_outside=0            
hour_total_inside=0
hour_total_outside=0
current_time = start_time
rows = []
while current_time <= end_time:
    if current_time.minute==0 and current_time.second==0:
        hour_total_inside=[0,0,0,0,0,0,0,0]
        hour_total_outside=[0,0,0,0,0,0,0,0]
    cam_idx=0
    for cam in camera_ids:     
        if current_time.hour<=8 or current_time.hour>=23 :
            continue 
        total_inside,total_outside=totals_i_o(cam_idx,current_time)
        #total_inside = random.randint(0, 2)
        #total_outside = random.randint(0, 2)
        #nao pode ser negatio o numero de pessoas dentro
        if (day_total_inside-day_total_outside+total_inside-total_outside)<0:
            total_outside=0
        # após as 20hs o número de pessoas dentro não pode ser muito alto
        if current_time.hour>=19 :
            max_inside= max_inside_percentage*day_total_inside
            inside_aux = day_total_inside-day_total_outside+total_inside-total_outside
            if inside_aux>max_inside:
                total_inside=0       
        day_total_inside+=total_inside
        day_total_outside+=total_outside     
        inside=day_total_inside-day_total_outside   
        cam_loc=location[cam_idx]
        wday=weekdays[current_time.weekday()]    
        if current_time<yesterday:
            hour_total_inside[cam_idx]+=total_inside
            hour_total_outside[cam_idx]+=total_outside
            if current_time.minute==59 and current_time.second==50:
                hti=hour_total_inside[cam_idx]
                hto=hour_total_outside[cam_idx]
                rows.append([cam, cam_loc, current_time, wday, hti, hto, 1, day_total_inside, day_total_outside,inside])                    
            else:
                stop=True
        else:           
            rows.append([cam, cam_loc, current_time, wday, total_inside, total_outside, 0, day_total_inside, day_total_outside,inside])
        cam_idx+=1
    current_time_aux = current_time + timedelta(seconds=10)
    if current_time_aux.day!=current_time.day:
        day_total_inside=0
        day_total_outside=0               
        inside=0
    
    current_time = current_time_aux

# escreve CSV
with open("camera_data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["camera_id", "location", "created_at", "weekday", "total_inside", "total_outside", "valid","day_total_inside", "day_total_outside","inside"])
    writer.writerows(rows)

print("CSV gerado: camera_data_yesterday.csv com registros a cada 10 segundos")
