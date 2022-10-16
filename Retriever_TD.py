# -*- coding: utf-8 -*-
"""
Created on Thu Nov 11 00:24:43 2021

@author: Francisco
"""
import Scrapperv2
import sys

ARGS = sys.argv
# ARGS = ['Retriever.py', '01', '2019']

try:
    P_MES = ARGS[1]
    P_AGNO = int(ARGS[2])
    FLAG_TD = ARGS[3]
    FLAG_CM = ARGS[4]
except:
    sys.exit("Argumentos inváidos (es mes año con el formato MM YYYY)")

# API solo tiene resultados del periodo 2019 01 en adelante

FLAG_SLEEP = True

QUERY_LISTADO_TD = "https://api.mercadopublico.cl/APISOCDS/OCDS/listaOCDSAgnoMesTratoDirecto/{Agno}/{Mes}/{offset}/{limit}"
QUERY_LISTADO_CM = "https://api.mercadopublico.cl/APISOCDS/OCDS/listaOCDSAgnoMesConvenio/{Agno}/{Mes}/{offset}/{limit}"
PARAM_QUERY = {'Agno':P_AGNO, 'Mes':P_MES, 'offset':0, 'limit':5}

def RUN_RETRIEVER(FLAG_TD, FLAG_CM, PARAM_QUERY):
    P_MES = PARAM_QUERY['Mes']
    P_AGNO = PARAM_QUERY['Agno']
    if FLAG_TD == "Y":
        PARAM_QUERY['offset'] = 0
        PARAM_QUERY['limit'] = 5
        Total_R, Queries = Scrapperv2.obtener_cantidad_licitaciones(QUERY_LISTADO_TD, PARAM_QUERY)
        Enlaces_TD = Scrapperv2.obtener_enlaces_licitaciones(QUERY_LISTADO_TD, PARAM_QUERY, Queries, Total_R)
        f_TD = open("./Links/TD/{}{}.txt".format(P_AGNO, P_MES), "w")
        for e in Enlaces_TD:
            f_TD.write(e['urlAward'] + "\n")
        f_TD.close()
    if FLAG_CM == "Y":
        PARAM_QUERY['offset'] = 0
        PARAM_QUERY['limit'] = 5
        Total_R, Queries = Scrapperv2.obtener_cantidad_licitaciones(QUERY_LISTADO_CM, PARAM_QUERY)
        Enlaces_CM = Scrapperv2.obtener_enlaces_licitaciones(QUERY_LISTADO_CM, PARAM_QUERY, Queries, Total_R)  
        f_CM = open("./Links/CM/{}{}.txt".format(P_AGNO, P_MES), "w")
        for e in Enlaces_CM:
            f_CM.write(e['urlAward'] + "\n")
        f_CM.close()
    print("Retriever periodo: ", str(P_AGNO), P_MES," completado.")
    
PARAM_QUERY = {'Agno':P_AGNO, 'Mes':P_MES, 'offset':0, 'limit':5}
RUN_RETRIEVER(FLAG_TD, FLAG_CM, PARAM_QUERY)