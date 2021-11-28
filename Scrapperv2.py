# -*- coding: utf-8 -*-
"""
Created on Thu Sep 23 15:39:00 2021

@author: Francisco Abarca
"""
import requests
import math
import pandas as pd
import time


def obtener_cantidad_licitaciones(Query, Params):
    print("Obteniendo cantidad de licitaciones para el periodo " + str(Params['Agno']) + Params['Mes'])
    s = 0
    while s != 200:
        response = requests.get(Query.format(**Params))
        s = response.status_code
        print("Response code:", s)
    Resultados_Mes = response.json()
    Total_Resultados = Resultados_Mes['pagination']['total']
    Queries_a_Realizar = math.ceil(Total_Resultados / 1000)
    print("Cantidad de resultados: " + str(Total_Resultados))
    return (Total_Resultados, Queries_a_Realizar)

def obtener_enlaces_licitaciones(Query, Params, N_Queries, Limit):
    print("Obteniendo enlaces...")
    print(str(int(N_Queries)) + " consultas, cada una recibiendo 1000 resultados.")
    Enlaces = []
    Params['offset'] = 0
    Params['limit'] = Limit
    for i in range(int(N_Queries)):
        Params['offset'] = i * 1000
        response = requests.get(Query.format(**Params))
        while response.status_code != 200:
            response = requests.get(Query.format(**Params))
        Enlaces.append(response.json())
        print("Consulta: " + str(i + 1) + " Registro inicial: " + str(Params['offset']) + " Registros obtenidos: " + str(len(Enlaces[i]['data'])))
    unpack = []
    for d in Enlaces:
        unpack = [*unpack, *d['data']]
    return unpack

def separar_tipo_enlace(unpack):
    print("Separando tipo de enlaces...")
    Tenders = []
    Awards = []
    for data in unpack:
        try:
            Tenders.append(data['urlTender'])
            Awards.append(data['urlAward'])
        except:
            Tenders.append(data['urlTender'])
    print("Awards: ",len(Awards), " - Tenders: ", len(Tenders))
    return(Tenders, Awards)
    
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
                x = x.split("|")[0].strip()
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
    prov_limit = 15
    if cleaned['Estado'] == 'active':
        try:
            montos = response['releases'][0]['awards'][0]['value']
        except:
            montos = {}
            montos['currency'] = "NA"
            montos['amount'] = "NA"
        cleaned['Montos'] = montos
    for p in participantes:
        if 'procuringEntity' in p['roles']:
            data = dict()
            data['Nombre'] = p['name']
            try:
                data['NombreLegal'] = p['identifier']['legalName']
            except:
                data['NombreLegal'] = "NA"
            try:
                data['Region'] = p['address']['region']
            except:
                data['Region'] = "NA"
            try:
                data['Pais'] = p['address']['countryName']
            except:
                data['Pais'] = "NA"
            procuradores.append(data)
        elif 'supplier' in p['roles'] and cleaned['Estado'] == 'active' and len(proveedores) < prov_limit:
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

def poblate_data(ENLACES, PARAM_QUERY, WDIR, FLAG_SLEEP=True):
    A = pd.DataFrame()
    i = 0
    e = []
    t = []
    R_aw = []
    #T_inicial = time.time()
    for enlace in ENLACES:
        # clear_output(wait=True)
        if FLAG_SLEEP:
            time.sleep(1)
        secondsSinceEpoch = time.time()
        #elapsed = time.localtime(secondsSinceEpoch - T_inicial)
        timeObj = time.localtime(secondsSinceEpoch)
        s = 0
        r = 0
        #print(i, "de", len(Awards))
        while s != 200:
            #print(s, r)
            try:
                aw_response = requests.get(enlace, timeout=5)
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
                e.append(enlace)
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
                    e.append(enlace)
                    print("error con índice:", i,", cantidad de errores:", len(e))
        else:
            e.append(enlace)
            print("Timeout con índice:", i,", cantidad de timeouts:", len(t))
        if i % 100 == 0:
            #print("Tiempo en ejecución:", '%d:%d:%d' % (elapsed.tm_hour, elapsed.tm_min, elapsed.tm_sec))
            print("Indice:", i, "Errores:", len(e), 'Hora: %d/%d/%d %d:%d:%d' % (timeObj.tm_mday, timeObj.tm_mon, timeObj.tm_year, timeObj.tm_hour, timeObj.tm_min, timeObj.tm_sec))
            A.to_csv(WDIR + str(PARAM_QUERY['Agno']) + PARAM_QUERY['Mes'] + ".csv", index=False, encoding='utf-8')
        i = i + 1
    A.to_csv(WDIR + str(PARAM_QUERY['Agno']) + PARAM_QUERY['Mes'] + ".csv", index=False, encoding='utf-8')
    with open("ERRORES_" + str(PARAM_QUERY['Agno']) + PARAM_QUERY['Mes'] + ".txt", 'w') as f:
        for item in e:
            f.write("%s\n" % item)
    print("Done!")


'''
WDIR = "D:/Memoria/"
QUERY_LISTADO = "https://apis.mercadopublico.cl/OCDS/data/listaAñoMes/{Agno}/{Mes}/{offset}/{limit}"
PARAM_QUERY = {'Agno':2021, 'Mes':' 02', 'offset':0, 'limit':5}


Total_R, Queries = obtener_cantidad_licitaciones(QUERY_LISTADO, PARAM_QUERY)
Enlaces = obtener_enlaces_licitaciones(QUERY_LISTADO, PARAM_QUERY, Queries, Total_R)
TENDERS, AWARDS = separar_tipo_enlace(Enlaces)

poblate_data(AWARDS, PARAM_QUERY, WDIR, FLAG_SLEEP=True)


A = pd.DataFrame()
i = 0
e = []
t = []
R_aw = []
T_inicial = time.time()
for a in AWARDS[:50]:
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
        A.to_csv("D:/Memoria/" + str(PARAM_QUERY['Agno']) + PARAM_QUERY['Mes'] + ".csv", index=False, encoding='utf-8')
    i = i + 1
A.to_csv("D:/Memoria/" + str(PARAM_QUERY['Agno']) + PARAM_QUERY['Mes'] + ".csv", index=False, encoding='utf-8')
print("Done!")
'''