#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 15:30:55 2019

"""
import pandas as pd 
import matplotlib.pyplot as plt

plt.style.use('fivethirtyeight')
pd.set_option('display.float_format',lambda x:'%.3f' % x)
#show ploty density
def plot_density(data):
    fig1,ax1=plt.subplots()
    keys=data.keys()
    ax1.hist(pd.Series(data[keys[0]].dropna()),1250,density=True, facecolor='g',alpha=0.75)
    plt.tight_layout()
    plt.show()
#show data in barchart 
def ploty_data_barchart(data,title):
    keys=data.keys()
    data.plot(kind='bar',x=keys[0],y=keys[1], legend=None, title=title)
    #plt.yscale("log")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def all_lines_7or_9or_10(driver,line):
    with driver.session() as session:
        query="match (start)-[r]->(end) where type(r)='"+line+'A'+"' or type(r)='"+line+'B'+"' return  DISTINCT start,end"
        line = session.run(query)
        return line,query
def all_lines(driver,line):
    with driver.session() as session:
        query="match (start)-[r]->(end) where type(r)='"+line+"' return  DISTINCT start,end"
        line = session.run(query,line=line)
        return line,query
def all_intercambiadores(driver):
    with driver.session() as session:
        query="match(n)-[r:Intercambiador]-(e)-[a]-() where not(type(a)='Intercambiador') return  distinct n.name as name ,[n.lat,n.long] as lat_long ,type(a) as lineas order by n.name"
        intercambiadores = session.run(query)
        return intercambiadores,query 
#Connect to neo4j 
def print_info_graph(driver):
    result_node = {"label": [], "count": []}
    result = {"relType": [], "count": []}
    with driver.session() as session:
         labels = [row["label"] for row in session.run("CALL db.labels()")]
         for label in labels:
             query = f"MATCH (:`{label}`) RETURN count(*) as count"
             count = session.run(query).single()["count"]
             result_node["label"].append(label)
             result_node["count"].append(count)
         rel_types = [row["relationshipType"] for row in session.run("CALL db.relationshipTypes()")]
         for rel_type in rel_types:
             query = f"MATCH ()-[:`{rel_type}`]->() RETURN count(*) as count"
             count = session.run(query).single()["count"]
             result["relType"].append(rel_type)
             result["count"].append(count)
    result_node_df=pd.DataFrame(data=result_node)
    result_relationship_df=pd.DataFrame(data=result)
    return result_node_df,result_relationship_df 

def tracking_line_number(driver,id_estacion_start,id_estacion_end):
    with driver.session() as session:
        line_type=pd.DataFrame([dict(record) for record in session.run("MATCH (:Estacion{id_estacion:$id_estacion_start})-[r]-(:Estacion{id_estacion:$id_estacion_end}) " 
            +"return type(r) as line",id_estacion_start=id_estacion_start,id_estacion_end=id_estacion_end)])
        return line_type
    
def all_estaciones(driver):
    with driver.session() as session:
        query="match (n:Estacion) return distinct n as estacion order by n.name"
        estaciones= session.run(query)        
        return estaciones, query

def find_sort_path_v1(driver):
    with driver.session() as session:
        sort_path_recomendation = pd.DataFrame([dict(record) for record in session.run("MATCH (start:Estacion{name:'SOL'}), (end:Estacion{name:'EL CARMEN'})"
            +"CALL algo.kShortestPaths.stream(start, end, 3, 'cost' ,{path:true})"
            +"YIELD index, nodeIds, costs, path "
            +"RETURN DISTINCT [node in algo.getNodesById(nodeIds) | node.name] AS places,"
            +"costs, reduce(acc = 0.0, cost in costs | acc + cost) AS totalCost, path ORDER BY totalCost")])
        return sort_path_recomendation
    
def find_sort_path_v2(driver,station_start,station_end):
    with driver.session() as session:
        query="MATCH (start:Estacion{name:'"+station_start+"'}), (end:Estacion{name:'"+station_end+"'}) CALL algo.kShortestPaths.stream(start, end, 3, 'cost') YIELD index, nodeIds, costs RETURN DISTINCT [node in algo.getNodesById(nodeIds) | node.name] AS places, [node in algo.getNodesById(nodeIds) | node.id_estacion] AS id_estacion, costs, [node in algo.getNodesById(nodeIds) | node.lat] as lat, [node in algo.getNodesById(nodeIds) | node.long] as long, reduce(acc = 0.0, cost in costs | acc + cost) AS totalCost ORDER BY totalCost LIMIT 4"

        sort_path_recomendation = pd.DataFrame([dict(record) for record in session.run(query)])
        ##### add lines type ######
        linetype_array=[]
        for a,s in enumerate(sort_path_recomendation['id_estacion']):
            aux_linetype=[]
            for i,e in enumerate(sort_path_recomendation['id_estacion'][a]):
                if(len(sort_path_recomendation['id_estacion'][a])>i+1):
                    if(len(tracking_line_number(driver,e,sort_path_recomendation['id_estacion'][a][i+1])['line'][0].split('_'))>1):
                        aux_linetype.append(tracking_line_number(driver,e,sort_path_recomendation['id_estacion'][a][i+1])['line'][0].split('_')[1])
                    else :
                        aux_linetype.append(tracking_line_number(driver,e,sort_path_recomendation['id_estacion'][a][i+1])['line'][0])
                    
            linetype_array.append(aux_linetype)
        sort_path_recomendation['lines_intercambiador']=linetype_array
        return sort_path_recomendation,query
    
def add_lines_relationships(tx,id_estacion,tipe_realation,data):
    tx.run("MATCH(a:Estacion),(b:Estacion) WHERE a.id_estacion = $id_estacion1 and b.id_estacion = $id_estacion2 Create (a)-[r: "+tipe_realation+"{cost:$data}]->(b)",
       id_estacion1=id_estacion[0],id_estacion2=id_estacion[1],tipe_realation=tipe_realation,data=data)
def merge_all_nodes_by(tx):
    tx.run("MATCH (n:Estacion),(a:Estacion) WHERE n.name=a.name and not(id(n)=id(a)) Create (n)-[r:Intercambiador{cost:3}]->(a)")
    #tx.run("MATCH (n1:Estacion),(n2:Estacion) WHERE n1.name = n2.name and id(n1) < id(n2) WITH [n1,n2] as ns CALL apoc.refactor.mergeNodes(ns) YIELD node RETURN node")
def add_estacion(tx,estacion):
    tx.run("CREATE(a:Estacion{name:$estacion.DENOMINACION,id_estacion:$estacion.id_estacion,lat:$estacion.lat,long:$estacion.long})",estacion=estacion[1])

def create_graph_estaciones(driver,data):
    with driver.session() as session:
        for i in data.items():
            session.write_transaction(add_estacion,i)
            

'''
########### SHORTES PATH FROM Station to all##############

MATCH (n:Estacion {name:'LISTA'})
CALL algo.shortestPath.deltaStepping.stream(n, 'cost', 3.0)
YIELD nodeId, distance

RETURN algo.asNode(nodeId).name AS destination, distance

########### Disktra##############

MATCH (start:Estacion{name:'LISTA'}), (end:Estacion{name:'GOYA'})
CALL algo.shortestPath.stream(start, end, 'cost')
YIELD nodeId, cost
RETURN algo.asNode(nodeId).name AS name, cost


########### MAYOR TRAyectoria ##############

CALL algo.allShortestPaths.stream('cost',{nodeQuery:'Estacion',defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance
WITH sourceNodeId, targetNodeId, distance
WHERE algo.isFinite(distance) = true

MATCH (source:Estacion) WHERE id(source) = sourceNodeId
MATCH (target:Estacion) WHERE id(target) = targetNodeId
WITH source, target, distance WHERE source <> target

RETURN source.name AS source, target.name AS target, distance
ORDER BY distance DESC
LIMIT 100


#####https://neo4j.com/docs/graph-algorithms/current/labs-algorithms/yen-s-k-shortest-path/####

MODEL 1__Mostramos los resultados en formato de tabla 

    MATCH (start:Estacion{name:'SOL'}), (end:Estacion{name:'EL CARMEN'})
    CALL algo.kShortestPaths.stream(start, end, 3, 'cost' ,{})
    
    YIELD index, nodeIds, costs
    RETURN DISTINCT [node in algo.getNodesById(nodeIds) | node.name] AS places,
           costs,
           reduce(acc = 0.0, cost in costs | acc + cost) AS totalCost
    ORDER BY totalCost

MODEL 2__Mostramos los resultados con la trayectoria (error no mostraba la mejor trayectoria)

    MATCH (start:Estacion{name:'SOL'}), (end:Estacion{name:'EL CARMEN'})
    CALL algo.kShortestPaths.stream(start, end, 3, 'cost', {path: true})
    YIELD path
    RETURN path
    LIMIT 3      

MODEL 3__Mostramos los resultados con la trayectoria menor( ULTIMA VERSION- VERSION MEJORADA)

    Version_1      
        MATCH (start:Estacion{name:'SOL'}), (end:Estacion{name:'EL CARMEN'})
        CALL algo.kShortestPaths.stream(start, end, 3, 'cost' ,{path:true})
        
        YIELD index, nodeIds, costs, path 
        RETURN DISTINCT [node in algo.getNodesById(nodeIds) | node.name] AS places,
               costs,
               reduce(acc = 0.0, cost in costs | acc + cost) AS totalCost,
               path
        ORDER BY totalCost
        
    Version_2
        MATCH (start:Estacion{name:'SOL'}), (end:Estacion{name:'EL CARMEN'})
        CALL algo.kShortestPaths.stream(start, end, 3, 'cost' )
        YIELD index, nodeIds, costs
        RETURN DISTINCT [node in algo.getNodesById(nodeIds) | node.name] AS places,
        [node in algo.getNodesById(nodeIds) | node.id_estacion] as id_estacion,
        costs,
        reduce(acc = 0.0, cost in costs | acc + cost) AS totalCost
        ORDER BY totalCost
############ MERGE INTERCAMBIADOR BY NAME id!= ################3
    ULTIMO
    match (n:Estacion),(a:Estacion) where n.name=a.name and not(id(n)=id(a)) Create (n)-[r:Intercambiador{cost:1000}]->(a)
    
    VERSIONES ANTERIORES    
    match (n:Estacion),(a:Estacion) where n.name=a.name and (id(n)<id(a) or id(n)>id(a)) Create (n)-[r:Intercambiador{cost:1000}]->(a)

###### Query to find all line not filter by zone #######
    match (n)-[r]-() where type(r)="linea_10A" or type(r)="linea_10B" return n,r

'''
   
