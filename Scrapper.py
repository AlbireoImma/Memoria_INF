# -*- coding: utf-8 -*-
"""
Created on Thu Sep 23 15:39:00 2021

@author: Francisco Abarca
"""

import requests, pprint
import math
import pandas as pd
import time

url = "https://apis.mercadopublico.cl/OCDS/data/listaAñoMes/{Agno}/{Mes}/{offset}/{limit}"
params = {'Agno':2021, 'Mes':' 02', 'offset':0, 'limit':5}
response = requests.get(url.format(**params))
print(response)
RESP_MES = response.json()
pprint.pprint(RESP_MES)
Total_R = RESP_MES['pagination']['total']
Queries = math.ceil(Total_R / 1000)
print(Queries)

# Obtención Enlaces
RESPON = []
params['offset'] = 0
params['limit'] = Total_R

for i in range(int(Queries)):
    params['offset'] = i * 1000
    response = requests.get(url.format(**params))
    while response.status_code != 200:
        response = requests.get(url.format(**params))
    RESPON.append(response.json())
    print(i, params['offset'], len(RESPON[i]['data']))

#len(RESPON)

enlaces = []
for d in RESPON:
    enlaces = [*enlaces, *d['data']]
len(enlaces)

RESP_MES['data'] = enlaces

print("== Query ==")
print("Query URL:", url)
print("Params:", params)
print("Fecha Query:", RESP_MES['creationDate'])
print("Cantidad resultados:", RESP_MES['pagination']['total'])
a = []
for x in RESP_MES['data']:
    a.append(x['ocid'])
if len(a) < 20:
    print("OCIDs:", a)

print(len(RESP_MES['data']))

Tenders = []
Awards = []
for x in RESP_MES['data']:
    try:
        Tenders.append(x['urlTender'])
        Awards.append(x['urlAward'])
    except:
        Tenders.append(x['urlTender'])

print("Awards:",len(Awards), "\nTenders:", len(Tenders))

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            if isinstance(x, str):
                x = x.replace("\'","")
                x = x.replace("\r\n","\n")
                x = x.replace("\n","")
                out[name[:-1]] = x
            else:
                out[name[:-1]] = x
    flatten(y)
    return out

def clean(response):
    cleaned = dict()
    try:
        cleaned['URI'] = response['uri']
    except:
        return -1
    try:
        cleaned['FechaPublicacion'] = response['publishedDate']
    except:
        cleaned['FechaPublicacion'] = "NA"
    if len(response['releases']) < 1:
        return cleaned
    cleaned['OCID'] = response['releases'][0]['ocid']
    cleaned['Titulo'] = response['releases'][0]['awards'][0]['title']
    cleaned['Descripcion'] = response['releases'][0]['awards'][0]['description']
    cleaned['FechaLic'] = response['releases'][0]['date']
    cleaned['Estado'] = response['releases'][0]['awards'][0]['status']
    participantes = response['releases'][0]['parties']
    procuradores = []
    proveedores = []
    if cleaned['Estado'] == 'active':
        montos = response['releases'][0]['awards'][0]['value']
        cleaned['Montos'] = montos
    for p in participantes:
        if 'procuringEntity' in p['roles']:
            data = dict()
            data['Nombre'] = p['name']
            try:
                data['NombreLegal'] = p['identifier']['legalName']
            except:
                pass
            data['Region'] = p['address']['region']
            data['Pais'] = p['address']['countryName']
            procuradores.append(data)
        elif 'supplier' in p['roles'] and cleaned['Estado'] == 'active':
            data = dict()
            data['Nombre'] = p['name']
            try:
                data['NombreLegal'] = p['identifier']['legalName']
            except:
                pass
            #data['Region'] = p['address']['region']
            #data['Pais'] = p['address']['countryName']
            proveedores.append(data)
    cleaned['Procuradores'] = procuradores
    if cleaned['Estado'] == 'active':    
        cleaned['Proveedores'] = proveedores
    return cleaned

#import json

A = pd.DataFrame()
i = 0
e = []
t = []
R_aw = []
T_inicial = time.time()
for a in Awards:
    # clear_output(wait=True)
    time.sleep(1)
    secondsSinceEpoch = time.time()
    timeObj = time.localtime(secondsSinceEpoch)
    s = 0
    r = 0
    #print(i, "de", len(Awards))
    while s != 200:
        #print(s, r)
        try:
            aw_response = requests.get(a, timeout=5)
            s = aw_response.status_code
        except:
            s = -1
            r = r + 1
        if r > 3:
            break
    if r < 3:
        d = aw_response.json()
        R_aw.append(d)
        d = clean(d)
        if d == -1:
            e.append(d)
            print("error con índice:", i,", cantidad de errores:", len(e))
        else:
            #d = str(d)
            #d = d.replace("\'", "\"")
            try:
                #jsonObj = json.loads(d)
                flat = flatten_json(d)
                df = pd.json_normalize(flat)
                A = pd.concat([A, df], ignore_index=True)
            except:
                e.append(a)
                print("error con índice:", i,", cantidad de errores:", len(e))
    else:
        t.append(a)
        print("Timeout con índice:", i,", cantidad de timeouts:", len(t))
    if i % 100 == 0:
        print("Indice:", i, "Errores:", len(e), 'Hora: %d/%d/%d %d:%d:%d' % (timeObj.tm_mday, timeObj.tm_mon, timeObj.tm_year, timeObj.tm_hour, timeObj.tm_min, timeObj.tm_sec))
        A.to_csv("D:/Memoria/" + str(params['Agno']) + params['Mes'] + ".csv", index=False, encoding='utf-8')
    i = i + 1
A.to_csv("D:/Memoria/" + str(params['Agno']) + params['Mes'] + ".csv", index=False, encoding='utf-8')
print("Done!")
