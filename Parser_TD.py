# -*- coding: utf-8 -*-
"""
Created on Thu Nov 11 23:24:34 2021

@author: Francisco
"""

from os import listdir
from os.path import isfile
from os import rename
import pandas as pd
import json


def cambiar_ext(FILES, ext=".json"):
    NEW_NAMES = []
    for f in FILES:
        rename(f, f + ext)
        NEW_NAMES.append(f + ext)
    return NEW_NAMES

import sys

ARGS = sys.argv

try:
    DATA_MES = ARGS[1]
    DATA_AGNO = ARGS[2]
except:
    print("Argumentos inváidos")
    sys.exit()

DATADIR = "./Data/TD/JSON/{}{}/"
SAVE_DIR = "./Data/TD/Consolidado/"
FLAG_CHANGE_EXT = False

DATADIR = DATADIR.format(DATA_AGNO, DATA_MES)
JSON_FILES = []

for f in listdir(DATADIR):
    if isfile(DATADIR + f):
        JSON_FILES.append(DATADIR + f)


if FLAG_CHANGE_EXT:
    JSON_FILES = cambiar_ext(JSON_FILES, ext=".json")

json_data = open(JSON_FILES[100], encoding='utf-8')
data = json.load(json_data)
json_data = open(JSON_FILES[200], encoding='utf-8')
data2 = json.load(json_data)

#print(data)

'''
flat = flatten_json(data)
df = pd.json_normalize(flat)
print(df)
'''

# Estandarizar json

LICITACIONES = pd.DataFrame()
PARTIES = pd.DataFrame()
PARTY_LIC = pd.DataFrame()

i = 0
errores = 0
invalid_f = list()

DATA_J = list()

for f in JSON_FILES:
    file = open(f, encoding='utf-8')
    try:
        data = json.load(file)
        DATA_J.append(data)
    except:
        invalid_f.append(f)
    file.close()

for data in DATA_J:
    i = i + 1
    #file = open(f, encoding='utf-8')
    #data = json.load(file)
    #file.close()
    try:
        if data['status'] == 500:
            errores = errores + 1
            continue
        if data['status'] == 404:
            errores = errores + 1
            continue
    except:
        Lic = dict()
        Lic['OCID'] = data['releases'][0]['ocid']
        tender = data['releases'][0]['tender']
        Lic['Estado_Tender'] = "NA"
        Lic['Tipo'] = "NA"
        Lic['Metodo'] = tender['procurementMethod']
        Lic['Date_tender_I'] = "NA"
        Lic['Date_tender_F'] = "NA"
        Lic['Date_tender_D'] = "NA"
        Lic['Date_enquiry_I'] = "NA"
        Lic['Date_enquiry_F'] = "NA"
        Lic['Date_enquiry_D'] = "NA"
        Lic['enquiries'] = tender['hasEnquiries']
        if 'awards' in data['releases'][0].keys():
            award = data['releases'][0]['awards'][0]
            Lic['Estado_Award'] = award['status']
            Lic['Title'] = award['title']
            if Lic['Estado_Award'] not in ['cancelled', 'unsuccessful', 'pending']:
                if 'value' in award.keys():
                    Lic['Amount'] = award['value']['amountGross']
                    if 'currency' in award['value'].keys():
                        Lic['Currency'] = award['value']['currency']
                    elif 'unitOfAccount' in award['value'].keys():
                        Lic['Currency'] = award['value']['unitOfAccount']
                    else:
                        Lic['Currency'] = 'NA'
                if 'date' in award.keys():
                    Lic['Date_Award'] = award['date']
        listado_parties = data['releases'][0]['parties']
        for p in listado_parties:
            Lic_Party_Dummy = dict()
            Lic_Party_Dummy['OCID'] = Lic['OCID']
            Lic_Party_Dummy['Party_ID'] = p['id']
            Lic_Party_Dummy['Tenderer'] = 1 if 'tenderer' in p['roles'] else 0
            Lic_Party_Dummy['Supplier'] = 1 if 'supplier' in p['roles'] else 0
            Lic_Party_Dummy['Buyer'] = 1 if 'buyer' in p['roles'] else 0
            Lic_Party_Dummy['Procurer'] = 1 if 'procuringEntity' in p['roles'] else 0
            Lic_Party_Dummy = pd.DataFrame(Lic_Party_Dummy, index=[0])
            PARTY_LIC = pd.concat([PARTY_LIC, Lic_Party_Dummy], ignore_index=True)
            #print(Lic_Party_Dummy)
            Party = dict()
            Party['ID'] = p['id']
            if 'name' in p.keys():
                Party['Name'] = p['name'].split("|")[0].strip()
            else:
                if 'identifier' in p.keys():
                    Party['Name'] = p['identifier']['legalName'].split("|")[0].strip()
                else:
                    Party['Name'] = 'NA'
            #Party['Name2'] = p['name'].split("|")[1].strip() if len(p['name'].split("|")) > 1 else "NA"
            try:
                Party['Region'] = p['address']['region'].strip()
            except:
                Party['Region'] = "NA"
            Party = pd.DataFrame(Party, index=[0])
            PARTIES = pd.concat([PARTIES, Party], ignore_index=True)
        Lic = pd.DataFrame(Lic, index=[0])
        LICITACIONES = pd.concat([LICITACIONES, Lic], ignore_index=True)
        PARTIES = PARTIES.drop_duplicates()
        PARTY_LIC = PARTY_LIC.drop_duplicates()

#Party = dict()
#Lic_Party = dict()
PARTIES = PARTIES.drop_duplicates()
PARTY_LIC = PARTY_LIC.drop_duplicates()

LICITACIONES.to_csv(SAVE_DIR + DATA_AGNO + DATA_MES + "_TD.csv", index=False, encoding='utf-8')
PARTIES.to_csv(SAVE_DIR + DATA_AGNO + DATA_MES + "_PARTIES.csv", index=False, encoding='utf-8')
PARTY_LIC.to_csv(SAVE_DIR + DATA_AGNO + DATA_MES + "_PARTIES_TD.csv", index=False, encoding='utf-8')



