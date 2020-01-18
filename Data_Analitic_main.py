#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 20:39:21 2020

@author: adriangallego
"""

import Connector_to_neo4j as nj_connect
from neo4j import GraphDatabase

def request_intercambiadores_to_Json(driver):
    request=nj_connect.all_intercambiadores(driver)
    
    json_intercambiadores=[]
    json_start={'name':'','lat_long':'','lineas':[]}
    for a in request[0].data(): 
        if(json_start['name']==''):
            for k,v in a.items():
                if(k!='lineas'):
                    json_start[k]=v
                else:
                    if not(v in json_start[k]):
                        json_start[k].append(v)
        else:
            for k,v in a.items():
                if(k=="name"):
                    if(json_start[k]!=v):
                        json_intercambiadores.append(json_start)
                        json_start={'name':'','lat_long':'','lineas':[]}
                        json_start[k]=v
                else:
                    if(k!='lineas'):
                        json_start[k]=v
                    else: 
                        if not (v in json_start[k]):
                            json_start[k].append(v)                
    return json_intercambiadores
def request_Lines_to_Json(i):
    start=[]
    end=[]
    if (i =="linea_7" or i=="linea_10" or i=="linea_9"):
        request=nj_connect.all_lines_7or_9or_10(driver,i)[0].data()
        for a in request:
            json_start={'name':'','lat':'','long':'','id_estacion':''}
            json_end={'name':'','lat':'','long':'','id_estacion':''}
            for k, v in a['start'].items():
                json_start[k]=v
            start.append(json_start)
            for k,v in a['end'].items():
                json_end[k]=v
            end.append(json_end)
    else:
        request=nj_connect.all_lines(driver,i)[0].data()
        for a in request:
            json_start={'name':'','lat':'','long':'','id_estacion':''}
            json_end={'name':'','lat':'','long':'','id_estacion':''}
            for k, v in a['start'].items():
                json_start[k]=v
            start.append(json_start)
            for k,v in a['end'].items():
                json_end[k]=v
            end.append(json_end)
        
    return start,end
def array_listStop_to_key_value(element,listOfElements):
    indexPosList = []
    indexPos = 0
    while True:
        try:
            # Search for item in list from indexPos to the end of list
            indexPos = listOfElements.index(element, indexPos)
            # Add the index position in list
            indexPosList.append(indexPos)
            indexPos += 1
        except ValueError as e:
            break
    return indexPosList

#Connector to neo4j             
driver = GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","1234"))
lines=["linea_1","linea_2","linea_3","linea_4","linea_5","linea_6","linea_8","linea_11","linea_12","linea_7","linea_10","linea_9"]
line_json={"linea_1":{},"linea_2":{},"linea_3":{},"linea_4":{},"linea_5":{},"linea_6":{},"linea_8":{},"linea_11":{},"linea_12":{},"linea_7":{},"linea_10":{},"linea_9":{}}
aaaa=request_intercambiadores_to_Json(driver)
for i in lines:
    aux=request_Lines_to_Json(i)    
    line_json[i]['start']=aux[0]
    line_json[i]['end']=aux[1]


#result_node_df,result_relationship_df=nj_connect.print_info_graph(driver)
#nj_connect.ploty_data_barchart(result_relationship_df,"Carlitos Sale Bien")
reqestaciones=nj_connect.all_estaciones(driver)
estaciones=[]
for est in reqestaciones[0].data():
    estaciones.append(est['estacion']['name'])
    
    
print(estaciones,reqestaciones[1])
'''
req=nj_connect.find_sort_path_v2(driver,'SOL','CHAMARTIN')
sort_path_recomendation=req[0]
stops_lines_for_recomender_trip=[]
for a,s in enumerate(sort_path_recomendation['lines_intercambiador']):
    aux_linetype={}
    for i,e in enumerate(sort_path_recomendation['lines_intercambiador'][a]):
        #check if the key exist
        try:
            (aux_linetype[e])
        except KeyError:
           aux_linetype[e]={'stops':array_listStop_to_key_value(e,sort_path_recomendation['lines_intercambiador'][a])}
    
    stops_lines_for_recomender_trip.append(aux_linetype)      
sort_path_recomendation['stops_lines']=stops_lines_for_recomender_trip
lat_long=[]
for k,v in enumerate (sort_path_recomendation.lat):
    l_l=[]
    for c,b in enumerate (sort_path_recomendation.long[k]):
        l_l.append((v[c],b))
    lat_long.append(l_l)
sort_path_recomendation['lat_long'] = lat_long

for (c,rec) in sort_path_recomendation['stops_lines'].items():
    for k,v in rec.items():
        cost_min=0
        cost_per=[]
        for q,stp in enumerate(v['stops']):
            print(q,v['stops'])
            cost_min+=sort_path_recomendation['costs'][c][q]
            cost_per.append(sort_path_recomendation['costs'][c][q])
        sort_path_recomendation['stops_lines'][c][k]['cost_all_stops']=cost_min
        sort_path_recomendation['stops_lines'][c][k]['cost_per_stops']=cost_per
        
recomend= sort_path_recomendation[['places','lat_long','stops_lines']]
print(sort_path_recomendation[['places','lat_long','stops_lines']].to_json())
print(req[1])
driver.close()
#check lineas camino mas corto
'''
'''
if(len(lines_json[line])>e+1):
    session.write_transaction(nj_connect.add_lines_relationships,)
'''
#print(aux['id_estacion'])
#aux_linetype = nj_connect.tracking_line_number(driver,35,34)['line'][0]

#print(aux.path[0].relationships[0])
#print(aux['path'][8])