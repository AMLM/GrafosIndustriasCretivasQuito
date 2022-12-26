# -*- coding: utf-8 -*-
"""
Created on Sat Mar 30 16:06:26 2019

@author: kruger
"""

#importa conf database
from py2neo import Graph
from py2neo import Node, Relationship

import pandas as pd
import numpy as np
import uuid

#configuracion base de datos
uri = "bolt://localhost:7687"
graph = Graph(uri, auth=("neo4j", "admin"))

#Leer el archivo csv
nombreArchivo = "origen_data.csv"
origenFile = pd.read_csv(nombreArchivo, sep=',', encoding='utf-8')

#Para reemplazar los valores nan 1
origenFile = origenFile.replace({np.nan:None})

for index, col in origenFile.iterrows():


    #Para Barrio VIVE_EN
        
    barrio = Node("Barrio",cod=col[0],nombre= col[1],barrioTradicional=col[2])
    graph.merge(barrio,"Barrio",'nombre')    
    #Para persona 
    person = Node("Persona",idPersona=uuid.uuid4(), rolFamiliar = col[4], edad = col[5],grupoEdad = col[6], sexo = col[7])
    graph.merge(person,"Persona","idPersona")
    
    
    #Para Pais  PAIS_ORIGEN
    pais = Node ("Pais",nombre = col[9])
    graph.merge(pais,"Pais","nombre")
    
    #Relationships
    personaBarrio = Relationship(person,"VIVE_EN",barrio)
    graph.merge(personaBarrio)
    
    if col[8]!="OTRO PAIS":
        #Para Provincia PROVINCIA_ORIGEN
        provincia = Node("Provincia",nombre= col[8])
        graph.merge(provincia, "Provincia","nombre")   
        
        barrioProvincia = Relationship(person,"PROVINCIA_ORIGEN",provincia)
        graph.merge(barrioProvincia)
                
        provinciaPais = Relationship(person,"PAIS_ORIGEN",pais)
        graph.merge(provinciaPais)        
    else:
        
        provinciaPais = Relationship(person,"PAIS_ORIGEN",pais)
        graph.merge(provinciaPais)            
        




    
    
    
    
    