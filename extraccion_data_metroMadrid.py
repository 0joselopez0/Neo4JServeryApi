#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 12:39:19 2019

"""

import Connector_to_neo4j as nj_connect
from neo4j import GraphDatabase 
import json 
import pandas as pd 
from bs4 import BeautifulSoup as Soup

def tiempo_velocidad_tiempo(velocidad,distancia):
    #pasar verlocidad de km a metros 
    tiempo=distancia/(velocidad*16.6666666667)
    return tiempo

def switch_lines(linea,zona,data):
    aux={"num_orden":data.NUMEROORDEN,"zona":zona,"data":data,"nombre":data.DENOMINACION}
    if(linea=='7' or linea=='10' or linea =='9'):
        lines_json['linea_'+ str(linea)+zona][data.NUMEROORDEN]=aux
    else:
        lines_json['linea_'+ str(linea)][data.NUMEROORDEN]=aux


#lectura de los datos
estaciones_pd=pd.read_csv("./data/M4_Estaciones.csv", encoding='utf-8')
tramos_pd=pd.read_csv("./data/M4_Tramos.csv", encoding='utf-8')
kml_soup= ""
with open('./data/M4_Estaciones.kml' , encoding='utf-8') as data:
    kml_soup = Soup(data, 'lxml-xml') # Parse as XML



estaciones_pd.head()

#trasformacion de los datos 
estacion_clean_pd=estaciones_pd.drop(["ENCUESTAAFOROS","FECHAFIN","CALIFICADORPORTAL","ENCUESTADOMICILIARIA","SITUACIONCALLE","DENOMINACION_SAE","INTERURBANOS_CODIGOEMT_CRTM","INTERURBANOS_CODIGOEMT_EMPRESA","DENOMINACIONABREVIADA","CARRETERA"],axis=1)
tramos_clean_pd=tramos_pd.drop(["CODIGOPOSTE","FECHAFIN","CODPROV_LINEA","CODMUN_LINEA","CODIGOOBSERVACION","CODIGOSUBLINEA","DENOMINACION_SAE"],axis=1)

cordinate_id=[]
cordinate = kml_soup.find_all('coordinates')
for i in cordinate:
    cordinate_id.append(i.get_text(strip=True).split(','))
estaciones_all = kml_soup.find_all(class_="CODIGOESTACION")[1:]
estaciones_id=[]
for i in estaciones_all:
    estaciones_id.append(i.get_text(strip=True))

estacionesID_LatLong=[]
for i,e in enumerate(estaciones_id):
    estacionesID_LatLong.append([int(e),cordinate_id[i][1],cordinate_id[i][0]])

df_estacionesID_LatLong=pd.DataFrame(estacionesID_LatLong,columns=['id_estacion','lat','long'])
df_estacionesID_LatLong.id_estacion.astype(int)
estacion_clean_pd.CODIGOESTACION.astype(int)
df_merge_estaciones=pd.merge(left=estacion_clean_pd,right=df_estacionesID_LatLong,left_on='CODIGOESTACION',right_on='id_estacion',how='inner')

lines_json={"linea_1":{},"linea_2":{},"linea_3":{},"linea_4":{},"linea_5":{},"linea_6": {},"linea_7A":{},"linea_7B":{},"linea_8":{},"linea_9A":{},"linea_9B":{},"linea_10A":{},"linea_10B":{},"linea_11": {},"linea_12":{},"linea_R":{}}

    #lines_json['linea_'+ str(linea)].append(aux)


for i in tramos_clean_pd.iterrows():
    x=i[1].NUMEROLINEAUSUARIO
    if (len(x.split('a'))>1):
        if(i[1].SENTIDO==1):
            switch_lines(x.split('a')[0],'A',i[1])
    elif(len(x.split('A'))>1):
        if(i[1].SENTIDO==1):
            switch_lines(x.split('A')[0],'A',i[1])
    elif(len(x.split('b'))>1): 
        if(i[1].SENTIDO==1):
            switch_lines(x.split('b')[0],'B',i[1])
    elif(len(x.split('B'))>1):
        if(i[1].SENTIDO==1):
            switch_lines(x.split('B')[0],'B',i[1])
    elif(len(x.split('-'))>1): 
        if(x.split('-')[1]=='1'):
            switch_lines(x.split('-')[0],'B',i[1])
    else:
        if(i[1].SENTIDO==1):
            switch_lines(x,'A',i[1])     

#print(lines_json['linea_1'].index())
#print(sorted(lines_json['linea_1']))

#Connector to neo4j             
driver = GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","1234"))

#Ingest in neo4j all station
data=json.loads(df_merge_estaciones.to_json(orient='index'))
nj_connect.create_graph_estaciones(driver, data)

#Ingest in neo4j all lines 
lines_1zone=["linea_1","linea_2","linea_3","linea_4","linea_5","linea_6","linea_8","linea_11","linea_12","linea_7A","linea_7B","linea_10A","linea_10B","linea_9A","linea_9B"]

with driver.session() as session:
    for line in lines_1zone:
        for i,e in enumerate(sorted(lines_json[line])):
            if(len(lines_json[line])>=e+1):
                session.write_transaction(nj_connect.add_lines_relationships,[lines_json[line][e]['data'].CODIGOESTACION,lines_json[line][e+1]['data'].CODIGOESTACION],line,tiempo_velocidad_tiempo(lines_json[line][e]['data'].VELOCIDADTRAMOANTERIOR,lines_json[line][e]['data'].LONGITUDTRAMOANTERIOR))
                
    
   
    #Merge all nodes by name
    session.write_transaction(nj_connect.merge_all_nodes_by)
  

result_node_df,result_relationship_df=nj_connect.print_info_graph(driver)
nj_connect.ploty_data_barchart(result_relationship_df,"LINEAS & NUM_PARADAS")
driver.close()
#print(result_node_df,result_relationship_df)