#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 18:12:02 2020

@author: adriangallego
"""
import Connector_to_neo4j as nj_connect
from neo4j import GraphDatabase
from flask import Blueprint
main = Blueprint('main', __name__)

import json
#from engine import RecommendationEngine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, render_template, request, session, redirect, url_for, Response, send_file

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
def request_Lines_to_Json(i):
    start=[]
    end=[]
    if (i =="linea_7" or i=="linea_10" or i=="linea_9"):
        request=nj_connect.all_lines_7or_9or_10(driver,i)
        for a in request[0].data():
            json_start={'name':'','lat':'','long':'','id_estacion':''}
            json_end={'name':'','lat':'','long':'','id_estacion':''}
            for k, v in a['start'].items():
                json_start[k]=v
            start.append(json_start)
            for k,v in a['end'].items():
                json_end[k]=v
            end.append(json_end)
    else:
        request=nj_connect.all_lines(driver,i)
        for a in request[0].data():
            json_start={'name':'','lat':'','long':'','id_estacion':''}
            json_end={'name':'','lat':'','long':'','id_estacion':''}
            for k, v in a['start'].items():
                json_start[k]=v
            start.append(json_start)
            for k,v in a['end'].items():
                json_end[k]=v
            end.append(json_end)
        
    return start,end,request[1]
'''
&&& APP &&&
/lines/<int:line> -> 
/all_lines-> 
/all_Estaciones -> 
/<string:start>/rutas_Mas_Cortas/<string:end> ->
/intercambiadores ->  

'''
@main.route('/',methods=['GET', 'POST'])
def index():
    return "METRO API"

@main.route("/lines/<int:line>")
def return_Lines(line):
    linea="linea_"+str(line)
    line_json={linea:{}}
    aux=request_Lines_to_Json(linea)
    line_json[linea]['start']=aux[0]
    line_json[linea]['end']=aux[1]
    line_json['query']=aux[2]
    return line_json
@main.route("/all_lines")
def return_all_Lines():
    lines=["linea_1","linea_2","linea_3","linea_4","linea_5","linea_6","linea_8","linea_11","linea_12","linea_7","linea_10","linea_9"]
    line_json={"linea_1":{},"linea_2":{},"linea_3":{},"linea_4":{},"linea_5":{},"linea_6":{},"linea_7":{},"linea_8":{},"linea_9":{},"linea_10":{},"linea_11":{},"linea_12":{}}
    for i in lines:
        aux=request_Lines_to_Json(i)
        line_json[i]['start']=aux[0]
        line_json[i]['end']=aux[1]
        line_json[i]['query']=aux[2]
    return json.dumps(line_json)
@main.route("/all_Estaciones")
def return_all_Estaciones():
    reqestaciones=nj_connect.all_estaciones(driver)
    estaciones=[]
    for est in reqestaciones[0].data():
        estaciones.append(est['estacion']['name'])
    json_={}
    json_['query']=reqestaciones[1]
    json_['contenido']=estaciones  
    return json_                     
@main.route("/<string:start>/rutas_Mas_Cortas/<string:end>")
def rutas_Mas_Cortas(start,end):
    req=nj_connect.find_sort_path_v2(driver,start,end)
    
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
                cost_min+=sort_path_recomendation['costs'][c][q]
                cost_per.append(sort_path_recomendation['costs'][c][q])
            sort_path_recomendation['stops_lines'][c][k]['cost_all_stops']=cost_min
            sort_path_recomendation['stops_lines'][c][k]['cost_per_stops']=cost_per
            
    json_={}
    json_['query']=req[1]
    json_['contenido']=sort_path_recomendation[['places','lat_long','stops_lines','totalCost']].to_json()
    return json_
    #return sort_path_recomendation[['places','lat_long','stops_lines','totalCost']].to_json()
    
@main.route("/intercambiadores")
def request_intercambiadores_to_Json():
    json_intercambiadores=[]
    json_start={'name':'','lat_long':'','lineas':[]}
    request=nj_connect.all_intercambiadores(driver)
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
    json_={}
    aux=json.dumps(json_intercambiadores)
    json_['query']=request[1]
    json_['contenido']=aux
    return json_

def create_app():
    #Connector to neo4j
    global driver
    try:
        driver= GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","1234"))
    except:
        print("BBDD shut down")
    #recommendation_engine = RecommendationEngine(spark_context, dataset_path)
    
    app = Flask(__name__, static_url_path='/static')
    app.register_blueprint(main)
    return app