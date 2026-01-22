"""

-------1 SÓ VEZ
0-ler da azure a lista dos cient-locations login_camera.comment=c or cc
1-carregar em db o login_camera e o peopleflowtotals (utimos 63 dias)da azure)

2-para cada client_location da lista 
    2-0 ler a lista das cameras do client_location (cameras) 
    2-1 importar da azure os dias mais recentes com valid=1 que ainda nao constam do db 
    2-2 ler a indicaao de non negative inside (nn_inside)
    2-3 para cada camera em cameras
        2-3-0 detetar falhas horarias do ultimo dia (failures)
    2-4 para cada item failure de failures
        2-4-1 corrigir a falha (correction) add to  corrections
        2-4-2 logar as falhas e as correcoes 
    2-5 detetar e corrigir horas negativas  (nn_totals_corrections)
    2-6 para cada camera em db.login_camera
        2.6.1 se camera.comments==cc 
            2.6.1.1 aplicar corrections em azure
        2.6.2 se client_location.nn_inside
            2.6.2.1 aplicar nn_totals_corrections em azure
            

    
"""