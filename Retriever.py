# -*- coding: utf-8 -*-
"""
Created on Sun Oct 24 17:08:42 2021

@author: Francisco
"""
import Scrapperv2
import sys


ARGS = sys.argv
ARGS = ['Retriever.py', '08', '2019']

try:
    P_MES = ARGS[1]
    P_AGNO = int(ARGS[2])
except:
    print("Argumentos inváidos (es mes año con el formato MM YYYY)")


FLAG_SLEEP=True
WDIR = "D:/Memoria/"
WDIR = "./Data/"
QUERY_LISTADO = "https://apis.mercadopublico.cl/OCDS/data/listaAñoMes/{Agno}/{Mes}/{offset}/{limit}"
PARAM_QUERY = {'Agno':P_AGNO, 'Mes':P_MES, 'offset':0, 'limit':5}

Total_R, Queries = Scrapperv2.obtener_cantidad_licitaciones(QUERY_LISTADO, PARAM_QUERY)
Enlaces = Scrapperv2.obtener_enlaces_licitaciones(QUERY_LISTADO, PARAM_QUERY, Queries, Total_R)
TENDERS, AWARDS = Scrapperv2.separar_tipo_enlace(Enlaces)

Scrapperv2.poblate_data(AWARDS, PARAM_QUERY, WDIR, FLAG_SLEEP)
