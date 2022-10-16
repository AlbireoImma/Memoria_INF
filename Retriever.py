# -*- coding: utf-8 -*-
"""
Created on Sun Oct 24 17:08:42 2021

@author: Francisco Abarca
"""
import Scrapperv2
import sys


ARGS = sys.argv

try:
    P_MES = ARGS[1]
    P_AGNO = int(ARGS[2])
    R_Flag = ARGS[3]
except:
    print("Argumentos inváidos (es mes año con el formato MM YYYY)")


FLAG_SLEEP=True
WDIR = "D:/Memoria/"
WDIR = "./Data/"
QUERY_LISTADO = "https://apis.mercadopublico.cl/OCDS/data/listaAñoMes/{Agno}/{Mes}/{offset}/{limit}"
PARAM_QUERY = {'Agno':P_AGNO, 'Mes':P_MES, 'offset':0, 'limit':5}

Total_R, Queries = Scrapperv2.obtener_cantidad_licitaciones(QUERY_LISTADO, PARAM_QUERY)
Enlaces = Scrapperv2.obtener_enlaces_licitaciones(QUERY_LISTADO, PARAM_QUERY, Queries, Total_R)
f_TD = open("./Links/LIC/{}{}.txt".format(P_AGNO, P_MES), "w")
for e in Enlaces:
    print(e)
    try:
        f_TD.write(e['urlAward'] + "\n")
    except:
        pass
f_TD.close()

TENDERS, AWARDS = Scrapperv2.separar_tipo_enlace(Enlaces)

if R_Flag == "Y":
    Scrapperv2.poblate_data(AWARDS, PARAM_QUERY, WDIR, FLAG_SLEEP)
