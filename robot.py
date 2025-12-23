#DURANTE O DIA ANTERIOR CHEGARAM REGISTROS DA CAMERA 
#ESSES REGISTROS TANTO SAO DO DIA ANTERIOR COMO DOS DIAS PASSADOS
#OS DADOS D DIA ANTERIOR 
#       SAO INSERIDOS EM peopleflowtotals COM valid=0
#OS DADOS DE DIAS PASSADOS ATE 3 DIAS ATRASO
#       COM equivalentes camera e hora com valid=1 em peopleflowtotals
#           SOMADOS AOS VALORES EXISTENTES
#       COM equivalentes camera e hora com valid=2 em peopleflowfails
#           SOMADOS AOS VALORES EXISTENTES
#Os DADOS COM ATRASO DE MAIS DE 3 DIAS
#       SAO IGNORADOS

#este é o robot que deve rodar todo dia de madrugada com as funçoes
#-0 compara dados de mesma camera e horario em pftotals com valid=2 (estimados)  pffails
#      se pffails maior, atualiza os valores em pftotals com valid=1 e deleta de pffails
#-1 deleta de pffails dados com mais de 3 dias de atraso
#-3 comprime os dados de ontem em peopleflowtotals (valid=0 --> valid=1)
#-4 marca os registros com falhas dos dados de ontem (valid=1-->valid=2)
#-5 os envia para peopleflowfails deletando-os de peopleflowtotals
#-6 estima valores para esses registros, insere-os em peopleflowtotals com vaid=3
#-7 compara valores estiados de ontem em pffuture com os reais em pftotals com valid=1
#       gera um fator de previsao a partir dessa comparacao
#-7 estima os dados da próxima semana e os insere em pffuture usando o fator de previsao 

import os
import sqlite3