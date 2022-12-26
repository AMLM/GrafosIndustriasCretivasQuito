# -*- coding: utf-8 -*-
"""
Created on Sun Mar 24 17:42:16 2019

@author: Alexis Bautista

Seño
https://docs.google.com/spreadsheets/d/1qJbBGw7RuIqiOP3QNkkqCV8R3i2-f0KkNTLqE1bK27w/edit#gid=1173813847
Alexis
https://docs.google.com/spreadsheets/d/1XsdfcX5TOyfQGMl3HLkCKfNFqBk3jXhUpWgi8dATj5w/edit#gid=1173813847
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
nombreArchivo = "diseno_datav4.csv"
disenoFile = pd.read_csv(nombreArchivo, sep=',', encoding='utf-8')
#Elimino 
del disenoFile['Unnamed: 21']
#Para reemplazar los valores nan 1
disenoFile = disenoFile.replace({np.nan:None})



motEmpr = ['Creación de fuentes de trabajo',
           'Desarrollo local y/o comunitario',
           'Negocio/empresa','Fomento cultural',
           'Formativo/Educativo','Alternativas de entretenimiento y/o uso de tiempo libre',
           'Difusión','Causa o mensaje social','Otra, ¿cuál?']

espaciosFisicosCirculacionExhibicion =['Espacios al aire libre no pagados',
                                           'Espacios privados','Espacios cubiertos no pagados',
                                           'Medios de comunicación públicos','Medios de comunicación privados']
espaciosDigitalesCirculacionExhibicion =['Internet','Medios de comunicación públicos',
                                                     'Medios de comunicación privados','Redes sociales'] 

espaciosFisicoDistribucion = ['Festivales y concursos','Organizaciones de promoción cultural',
                                 'Distribuidores privados',
                                 'Ferias',
                                 'Distribuidores públicos']

espaciosDigitalDistribucion = ['Internet','Canales de televisión','Redes Sociales']

	
espaciosFisicosCirculacionExhibicion =['Espacios al aire libre no pagados',
                                           'Espacios privados','Espacios cubiertos no pagados',
                                           'Medios de comunicación públicos','Medios de comunicación privados']
espaciosDigitalesCirculacionExhibicion =['Internet','Medios de comunicación públicos','Medios digitales (internet)',
                                                     'Medios de comunicación privados','Redes sociales']    
    

   
def telefonos(campoTelefonos):
    if str(campoTelefonos)=="S/R" or str(campoTelefonos) == "S/N" or str(campoTelefonos)=="N/S":
        return None
    else: 
        return campoTelefonos
    

def rucToNumber(ruc):
    if ruc=="S/N":
        return None
        
    if ruc!=None and ruc!=" ":
        ruc=ruc.replace(".","")
        return int(float(ruc))
    else:
        return ruc
    
    
def changeNone(string):
    if string != " " or string != None:
        return string
    else:
        return None
    
def replaceCharacter(dato):
    return dato.replace("?", "")
    
def tipoBeneficio(tipoBeneficio):
    if tipoBeneficio=="No sabe/no responde":
        return "NO DEFINIDO"
    return tipoBeneficio    

def utilString (string):
    if string ==" " or string == "" or string== "No sabe/ No responde":
        return None
    else:
        return string

def deleteCharacter(string):
    if string=="No sabe/no responde":
        return "NO_DEFINIDO"
    else:
        return str(string).replace("?","")
    
    
def delteCirculacionName(string):
    if string=="NO RESPONDE" or string=="N/S" or string =="NS/NR" or string =="NO ESPECIFICA":
        return None
    else:
        return string
    
def sectorRename(sector):
    if sector=='ARTES ESCÉNICAS':
        sector='ARTES_ESCENICAS'
    if sector =='FONOGRÁFICO':
        sector='FONOGRAFICO'
    if sector == 'ARTES PLÁSICAS':
        sector='ARTES_PLASICAS'
    if sector == 'DISEÑO ':
        sector= 'DISEÑO'
    return  str(sector).upper()

def renameRedesSociales(redes):
    if redes == "Redes Sociales":
        return "Redes sociales"
    else:
        return redes
    
def noneToNotDefine(nombre):
    if nombre== None:
        return "NO_DEFINIDO"
    else:
        return nombre
    
for index, col in disenoFile.iterrows():

    #Para ubicación Geográfica Provincia
    provincia = Node("Provincia", nombreProvincia = col[1])
    graph.merge(provincia,"Provincia", "nombreProvincia")
    
    #Para ubicacion geografica Canton
    ubicacionG = Node("Canton", divPolAdministrativa = col[0],nombre = col[2],ciudad = col[3])
    graph.merge(ubicacionG,"Canton", "divPolAdministrativa")
    
    #Relación entre Provincia y Cantón
    cantonProvincia = Relationship(ubicacionG, "PERTENECE_PROVINCIA",provincia) 
    graph.merge(cantonProvincia)
    
    #Para crear el Barrio como no tenemos esto pues entonces se crea una con NO_DEFINIDO
    if col[138]!=None:
        barrio = Node("Barrio",nombre= col[138])
        graph.merge(barrio,"Barrio",'nombre')
    else:
        barrio = Node("Barrio",nombre= 'NO_DEFINIDO')
        graph.merge(barrio,"Barrio",'nombre')
        
    #Relación entre bARRIO y Cantón
    barrioCanton = Relationship(barrio, "PERTENECE_CANTON",ubicacionG)
    graph.merge(barrioCanton)
    
    #Para la parte deIndustriaCreativa
    industriaCreativa = Node("IndustriaCreativa",sector="DISEÑO", nombre = col[7], telConvencional = telefonos(col[8]), telCelular = telefonos(col[9]), ruc = rucToNumber(col[10]), correo = telefonos(col[11]),tiempoFuncionamientoAnios = col[19],tiempoFuncionamientoMeses = col[20], personal = col[21]  , callePrincipal = col[4], no = col[5], calleSecundaria = col[6] )
    graph.merge(industriaCreativa,"IndustriaCreativa", "nombre")    
    
    #Relacion entre  IndustriaCreativa - Barrio = UBICADAS
    industriaUbica = Relationship(industriaCreativa, "UBICADA_EN",barrio) 
    graph.merge(industriaUbica)
    
    #Espacio Digital
    if col[12]!=None and col[12]!="NO TIENE" and col[12]!="N/T":
        if str(col[12]).lower().startswith("fb") or str(col[12]).lower().startswith("facebook") :
            espacioDigital = Node("EspacioDigital", nombre= "FACEBOOK")
            graph.merge(espacioDigital,"EspacioDigital", "nombre")
            #Relacion entre  EspacioDigital - Ubicación Geografica = UBICADAS
            industriaUbica = Relationship(industriaCreativa, "TIENEN",espacioDigital,nombre=col[12]) 
            graph.merge(industriaUbica)   
            
        elif str(col[12]).lower().startswith("www."):
            espacioDigital = Node("EspacioDigital", nombre= "PAGINA_WEB")
            graph.merge(espacioDigital,"EspacioDigital", "nombre")
            #Relacion entre  EspacioDigital - Ubicación Geografica = TIENEN
            industriaUbica = Relationship(industriaCreativa, "TIENEN",espacioDigital,nombre=col[12]) 
            graph.merge(industriaUbica)
            
        elif str(col[12]).lower().startswith(" @"):
            espacioDigital = Node("EspacioDigital", nombre= "TWITTER")
            graph.merge(espacioDigital,"EspacioDigital", "nombre")
            #Relacion entre  EspacioDigital - Ubicación Geografica = TIENEN
            industriaUbica = Relationship(industriaCreativa, "TIENEN",espacioDigital,nombre=col[12]) 
            graph.merge(industriaUbica)            
            
        else:
            espacioDigital = Node("EspacioDigital", nombre= "PAGINA_WEB")
            graph.merge(espacioDigital,"EspacioDigital", "nombre")
            #Relacion entre  EspacioDigital - Ubicación Geografica = TIENEN
            industriaUbica = Relationship(industriaCreativa, "TIENEN",espacioDigital,nombre=col[12]) 
            graph.merge(industriaUbica)           
    
    #Para persona
    personaNode = Node("Persona",idPersona=str(uuid.uuid4()),nombre = col[13],sexo = col[14],nivelEstudio= col[15],nombreTituloUniversitario = col[16],tipoFormacion= col[17])
    graph.merge(personaNode,"Persona", "idPersona")
        #Relación persona A CARGO DE
    personaIndustria = Relationship(personaNode, "A_CARGO_DE",industriaCreativa,aniosExperiencia=col[18]) 
    graph.merge(personaIndustria)    


    #Para Actividades Cadena Productiva
            #Para Actividades Cadena Productiva
    #Produccion
    if str(col[22]).lower() == "si":
        #Hay una relacion entre Produccion y la industria Creativa
        actividadProd = Node("ActividadesCadenaProductiva",nombre = 'PRODUCCION')
        graph.merge(actividadProd,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "DESARROLLA", actividadProd)
        graph.merge(industriaProd)
    #Distribucion
    if str(col[23]).lower() == "si":
        #Hay una relacion entre Distribucion y la industria Creativa
        actividadDistr = Node("ActividadesCadenaProductiva",nombre = 'DISTRIBUCION')
        graph.merge(actividadDistr,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "DESARROLLA", actividadDistr)
        graph.merge(industriaProd)
    #Conceptualización
    if str(col[24]).lower() == "si":
        #Hay una relacion entre Circulacion y la industria Creativa
        actividadCircu = Node("ActividadesCadenaProductiva",nombre = 'CONCEPTUALIZACION')
        graph.merge(actividadCircu,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "DESARROLLA", actividadCircu) 
        graph.merge(industriaProd)



    # Para motivo emprendimiento
    #Se toma de la lista que se ha generado en motivosEmprendimiento()
    #Para motivo 1 en la parte de motivo emprendimiento
    if str(col[25]) in motEmpr:
        motivo1 = Node("MotivoEmprendimiento",categoria = col[25])
        graph.merge(motivo1,"MotivoEmprendimiento", "categoria")
        motiv1_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo1) 
        graph.merge(motiv1_industria)
        
    #Para motivo 2 en la parte de motivo emprendimiento
    if str(col[26]) in motEmpr:
        if str(col[26]) =="Otra, ¿cuál?":
            motivo2 = Node("MotivoEmprendimiento",categoria = 'Otra')
            graph.merge(motivo2,"MotivoEmprendimiento", "categoria")
            motiv2_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo2,nombre=col[28]) 
            graph.merge(motiv2_industria)
            
        else:
            motivo2 = Node("MotivoEmprendimiento",categoria = col[26])
            graph.merge(motivo2,"MotivoEmprendimiento", "categoria")
            motiv2_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo2) 
            graph.merge(motiv2_industria)            
        
    #Para motivo 3 en la parte de motivo emprendimiento
    if str(col[27]) in motEmpr:
        if str(col[27]) =="Otra, ¿cuál?":
            motivo3 = Node("MotivoEmprendimiento",categoria = 'Otra')
            graph.merge(motivo3,"MotivoEmprendimiento", "categoria")
            motiv3_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo3,nombre=col[28])
            graph.merge(motiv3_industria)
        else:             
            motivo3 = Node("MotivoEmprendimiento",categoria = col[27])
            graph.merge(motivo3,"MotivoEmprendimiento", "categoria")
            motiv3_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo3) 
            graph.merge(motiv3_industria)        
            
            
            
    #Para  29    
    motivo4 = Node("MotivoEmprendimiento",categoria = col[29])
    graph.merge(motivo4,"MotivoEmprendimiento", "categoria")
    motiv4_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo4) 
    graph.merge(motiv4_industria)               
            
    
    #Para Productos
    #Producen u Ofrecen
    #Para Productos y Servicios

    if col[31]!=None and col[31]!="":
        listaProductos = col[31].split(",")
        for i in listaProductos:
            productoServicio = Node("ProductosYServicios",categoria = i)
            graph.merge(productoServicio,"ProductosYServicios","categoria")  
            prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio) 
            graph.merge(prod_servicio)
            


    #Para taller
    if str(col[36]).lower() == "si":
        productoServicio = Node("ProductosYServicios",categoria = "Taller")
        #graph.merge(productoServicio,"ProductosYServicios", "categoria")
        graph.create(productoServicio)
        prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio) 
        graph.merge(prod_servicio)        

        #Para Espacios Físicos Publico al Aire Libre
        if str(col[37])=='SI' or str(col[37])=='si' :
            espacioFisico = Node("EspacioFisico",nombre = 'Publico al Aire Libre')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico,nombre= col[38]) 
            graph.merge(produc_espacioFis)
        #Para Espacios Cubierto No Pagados
        if str(col[39])=='SI' or str(col[39])=='si':
            espacioFisico = Node("EspacioFisico",nombre = 'Publico No Cubierto')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico,nombre= col[40]) 
            graph.merge(produc_espacioFis)
        #Privado
        if str(col[41])=='SI' or str(col[41])=='si':
            espacioFisico = Node("EspacioFisico",nombre = 'Privado')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico,nombre= col[42]) 
            graph.merge(produc_espacioFis)
        #Para Espacio Fisico Propio        
        if str(col[45])=='SI' or str(col[45])=='si':
            espacioFisico = Node("EspacioFisico",nombre = 'Propio')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico) 
            graph.merge(produc_espacioFis)                 
            
        #Para temática 47-49
        if isinstance(col[46], str)and col[46]!='0' and col[46]!='19':
            listaTematicas = col[46].split(",")
            for i in listaTematicas:
                tematica = Node("Tematica",nombre = i)
                graph.merge(tematica,"Tematica","nombre")  
                produc_temati = Relationship(productoServicio, "CONTIENEN", tematica) 
                graph.merge(produc_temati)
        if (isinstance(col[47],str) and col[47]!='0'and col[47]!='19'):
            listaTematicas = col[47].split(",")
            for i in listaTematicas:
                tematica = Node("Tematica",nombre = i)
                graph.merge(tematica,"Tematica","nombre")  
                produc_temati = Relationship(productoServicio, "CONTIENEN", tematica) 
                graph.merge(produc_temati)      
                
        if (isinstance(col[48],str) and col[48]!='0'and col[48]!='19' and col[48]!= None):
            listaTematicas = col[48].split(",")
            for i in listaTematicas:
                tematica = Node("Tematica",nombre = i)
                graph.merge(tematica,"Tematica","nombre")  
                produc_temati = Relationship(productoServicio, "CONTIENEN", tematica) 
                graph.merge(produc_temati)
                
        #Consumidores
        #Solo si es taller- niños
        if str(col[49]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'niños')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)        
        
        #Para consumidores- JOVENES
        if str(col[50]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'jovenes')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)
        #Para consumidores-Adultos 
        if str(col[51]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'adultos')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)
        #Para consumidores- Solo si es taller Adultos Mayores
        if str(col[52]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Adultos Mayores')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)
        #Para consumidores- Poblacion vulnerable 
        if str(col[53]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Población Vulnerable')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)    
        #Para consumidores- Público en General
        if str(col[54]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Público en General')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)  
        #Para consumidores- Profesionales del Sector 
        if str(col[55]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Profesionales del Sector')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)    
            
            
        #Para tipo de Registro que ahora es Derechos de Autor---60-63-65
    if( str(col[57]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Obras artísticas')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)            

    if( str(col[58]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Programas y software')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      
        
        
    if( str(col[59]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Patentes de invencion')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      


    if( str(col[60]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Patente Modelos de utilidad')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      


    if( str(col[61]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Patente Diseños Industriales')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      
        
        
    if( str(col[62]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Marcas')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      


    if( str(col[63]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Creative Commons')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      


    if( str(col[64]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Creative Commons')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      

    if( str(col[65]).lower() == "si"):
        derechosAutor = Node("DerechosAutor",nombre = 'Copyleft')
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)      


    #Para PrensaEscrita 77
    if str(col[68]).lower()=='si':
        espacioFisico = Node("EspacioFisico",nombre = 'Prensa Escrita')
        graph.merge(espacioFisico,"EspacioFisico", "nombre")
        produc_espacioFis = Relationship(industriaCreativa, "PUBLICITAN", espacioFisico) 
        graph.merge(produc_espacioFis)
        
   #Para espacio Digital
       #Television Digital
    if str(col[69]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Televisión')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)      
       
       #Radio Digital
    if str(col[70]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Radio Digital')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)      
          
       #Internet
    if str(col[71]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Internet')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)       
       #Redes Sociales     
    if str(col[72]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Redes sociales')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)  
        
        
#Para Alcance (85-90)
        
    #Para alcance Internacional
    if str(col[75]).lower()=="internacional" :
        alcance = Node("Alcance",nombre = 'Internacional')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)           
        #Para alcance Costa
    if str(col[76])=="SI":
        alcance = Node("Alcance",nombre = 'Costa')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)
    #Para alcance Sierra
    if str(col[77])=="SI":
        alcance = Node("Alcance",nombre = 'Sierra')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)                   
    #Para alcance Amazonía
    if str(col[78])=="SI":
        alcance = Node("Alcance",nombre = 'Amazonía')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)   
    #Para alcance Insular
    if str(col[79])=="SI":
        alcance = Node("Alcance",nombre = 'Insular')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)   

    #Relacion SELIMITA Entre IndustriaCreativa Y factores de Dificultad

    #Factores Alcance Nacional
    if col[81]!= None and str(col[81])!="No aplica":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Nacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad ,nombreDificultad =deleteCharacter(col[84])) 
        graph.merge(alcanceFactores)   
    
    if col[82]!= None and str(col[82])!="No aplica" and str(col[82])!="No sabe/no responde":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Nacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad,nombreDificultad = deleteCharacter(col[85])) 
        graph.merge(alcanceFactores)  
    
    #Factores Alcance Internacional
    if col[83]!= None and str(col[83])!="No aplica":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Internacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad,nombreDificultad = deleteCharacter(col[86]))
        graph.merge(alcanceFactores)   
    
    if col[84]!= None and str(col[84])!="No aplica" and str(col[84])!="No sabe/no responde":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Internacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad,nombreDificultad = deleteCharacter(col[87]))
        graph.merge(alcanceFactores)   
        
        
        
#95            
	#Para digitales
    if str(col[85]) in espaciosDigitalDistribucion:
        espacioDigital = Node("EspacioDigital",nombre = renameRedesSociales(col[85]))
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        cadeProd_espaDig = Relationship(industriaCreativa, "DISTRIBUYEN", espacioDigital) 
        graph.merge(cadeProd_espaDig)                
	#Para fisicos
    if str(col[85]) in espaciosFisicoDistribucion :
        espacioFisico = Node("EspacioFisico",nombre = col[85])
        graph.merge(espacioFisico,"EspacioFisico","nombre")  
        cadeProd_espacioFis = Relationship(industriaCreativa, "DISTRIBUYEN", espacioFisico) 
        graph.merge(cadeProd_espacioFis)   
        
    
#89
	#Para digitales
    if str(col[86]) in espaciosDigitalDistribucion:
        espacioDigital = Node("EspacioDigital",nombre = renameRedesSociales(col[86]))
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        cadeProd_espaDig = Relationship(industriaCreativa, "DISTRIBUYEN", espacioDigital) 
        graph.merge(cadeProd_espaDig)                
	#Para fisicos
    if str(col[86]) in espaciosFisicoDistribucion :
        espacioFisico = Node("EspacioFisico",nombre = col[86])
        graph.merge(espacioFisico,"EspacioFisico","nombre")  
        cadeProd_espacioFis = Relationship(industriaCreativa, "DISTRIBUYEN", espacioFisico) 
        graph.merge(cadeProd_espacioFis)          
        

#Tipo Consumidores- Relaciòn SIRVE entre INDUSTRIAC Y CONSUMIDOR
        
#Tipo de Consumidor - Relación CONSUMEN 
    #Con columna 101
    if str(col[87])!="No sabe/no responde" and col[87]!=None and col[87]!="No aplica":
        tipoConsumidor = Node("Consumidor",tipoConsumidor = str(col[87]))
        graph.merge(tipoConsumidor,"Consumidor","tipoConsumidor")  
        consum_prod = Relationship(industriaCreativa, "SIRVE", tipoConsumidor) 
        graph.merge(consum_prod)
        
    #Con la columna 102
    if str(col[88])!="No sabe/no responde" and col[88]!=None and col[88]!="No aplica":        
        tipoConsumidor102 = Node("Consumidor",tipoConsumidor = str(col[88]))
        graph.merge(tipoConsumidor102,"Consumidor","tipoConsumidor")  
        consum_prod = Relationship(industriaCreativa, "SIRVE", tipoConsumidor102) 
        graph.merge(consum_prod)     


#INTERCAMBIONOMONETARIO Se forma de acuerdo a la relaciòn tal 
    #Produccion
    if str(col[90]).lower() == "si":
        #Hay una relacion entre Produccion y la industria Creativa
        actividadProd = Node("ActividadesCadenaProductiva",nombre = 'CONCEPTUALIZACION')
        graph.merge(actividadProd,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "INTERCAMBIO", actividadProd)
        graph.merge(industriaProd)
    #Distribucion
    if str(col[91]).lower() == "si":
        #Hay una relacion entre Distribucion y la industria Creativa
        actividadDistr = Node("ActividadesCadenaProductiva",nombre = 'PRODUCCION')
        graph.merge(actividadDistr,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "INTERCAMBIO", actividadDistr)
        graph.merge(industriaProd)
    #Circulacion
    if str(col[92]).lower() == "si":
        #Hay una relacion entre Circulacion y la industria Creativa
        actividadCircu = Node("ActividadesCadenaProductiva",nombre = 'DISTRIBUCION')
        graph.merge(actividadCircu,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "INTERCAMBIO", actividadCircu) 
        graph.merge(industriaProd)



# ORGANIZACIONES
    #Primero debe estar un nodo tipo Beneficio
    #119
    if col[94]!=" " and col[94]!= None :
        nodeGremio = Node("Organizacion",tipoOrganizacion="Gremio",nombre=col[94])
        graph.merge(nodeGremio,"Organizacion","nombre")
        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeGremio) 
        graph.merge(industriaOrganizacion)

        if col[104]!=" " and col[104]!=None and col[104]!="No sabe/no responde" :
            nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[104]))
            graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")    
            gremioTipo = Relationship(nodeGremio, "PRESTAN", nodeTipoBeneficio) 
            graph.merge(gremioTipo)
        
        if col[105]!=" " and col[105]!= None and col[105]!="No sabe/no responde" :
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=col[105])
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeGremio, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)
            
    #109
    if col[96]!=" " and col[96]!= None :
        nodeGremio = Node("Organizacion",tipoOrganizacion="Colectivo",nombre=col[96])
        graph.merge(nodeGremio,"Organizacion","nombre")
        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeGremio) 
        graph.merge(industriaOrganizacion)

        if col[104]!=" " and col[104]!=None and col[104]!="No sabe/no responde" :
            nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[104]))
            graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")    
            gremioTipo = Relationship(nodeGremio, "PRESTAN", nodeTipoBeneficio) 
            graph.merge(gremioTipo)
        
        if col[105]!=" " and col[105]!= None and col[105]!="No sabe/no responde" :
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=col[105])
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeGremio, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)
            
        
    if col[98]!=" " and col[98]!= None:
        nodeAsociacion = Node("Organizacion",tipoOrganizacion="Asociacion",nombre=col[98])
        graph.merge(nodeAsociacion,"Organizacion","nombre")
        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeAsociacion) 
        graph.merge(industriaOrganizacion)

        if col[104]!=" " and col[104]!=None and col[104]!="No sabe/no responde" :
            nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[104]))
            graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")    
            gremioTipo = Relationship(nodeAsociacion, "PRESTAN", nodeTipoBeneficio) 
            graph.merge(gremioTipo)
        
        if col[105]!=" " and col[105]!= None and col[105]!="No sabe/no responde" :
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=col[105])
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeAsociacion, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)      
        
    if col[100]!=" " and col[100]!= None:
        nodeRed = Node("Organizacion",tipoOrganizacion="Red",nombre=col[100])
        graph.merge(nodeRed,"Organizacion","nombre")  
        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeRed)         
        graph.merge(industriaOrganizacion)

        if col[104]!=" " and col[104]!=None and col[104]!="No sabe/no responde" :
            nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[104]))
            graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")    
            gremioTipo = Relationship(nodeRed, "PRESTAN", nodeTipoBeneficio) 
            graph.merge(gremioTipo)
        
        if col[105]!=" " and col[105]!= None and col[105]!="No sabe/no responde":
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=tipoBeneficio(col[105]))
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeRed, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)     
            
            
#Para mecanismos de sostenabilidad  e Industria Creativa
    if str(col[107])!="No sabe/no responde" and col[107]!=None and col[107]!="No aplica":
        nodeMecanismo = Node("MecanismoSostenabilidad",nombre=tipoBeneficio(col[107]))
        graph.merge(nodeMecanismo,"MecanismoSostenabilidad","nombre")
        industriaMecanism = Relationship(industriaCreativa, "SE_MANTIENE", nodeMecanismo) 
        graph.merge(industriaMecanism)       
    #No sabe no se mapea 
    if str(col[108])!="No sabe/no responde" and col[108]!=None and col[108]!="No aplica":
        nodeMecanismo2 = Node("MecanismoSostenabilidad",nombre=col[108])
        graph.merge(nodeMecanismo2,"MecanismoSostenabilidad","nombre")
        industriaMecanism1 = Relationship(industriaCreativa, "SE_MANTIENE", nodeMecanismo2) 
        graph.merge(industriaMecanism1)                
            
    
#Para vinculos con la comunidad
    #125
    #"""
    
    if col[110]!= "Ninguna" and col[110]!=None and col[110]!="No aplica":    
        nodeVinculo = Node("VinculoComunidad",nombre=col[110])
        graph.merge(nodeVinculo,"VinculoComunidad","nombre")
        industriaVinculo = Relationship(industriaCreativa, "MANTIENEN", nodeVinculo) 
        graph.merge(industriaVinculo)    
    
    #126
    #"""
    if col[111]!= "Ninguna" and col[111]!=None and col[111]!="No aplica":
        nodeVinculo = Node("VinculoComunidad",nombre=col[111])
        graph.merge(nodeVinculo,"VinculoComunidad","nombre")
        industriaVinculo = Relationship(industriaCreativa, "MANTIENEN", nodeVinculo) 
        graph.merge(industriaVinculo)         
        
    
    
#Para Espacio Fisico, se relaciona como FUNCIONA_EN
    #133
    if col[113]!="No posee local" and col[113]!= None:
        espacioFisico = Node("EspacioFisico",nombre = col[113])
        graph.merge(espacioFisico,"EspacioFisico", "nombre")
        ind_espacioFis = Relationship(industriaCreativa, "FUNCIONA_EN", espacioFisico) 
        graph.merge(ind_espacioFis)
    #134
    if col[114]!= None and col[114]!=" ":
        espacioFisico = Node("EspacioFisico",nombre = col[114])
        graph.merge(espacioFisico,"EspacioFisico", "nombre")
        ind_espacioFis = Relationship(industriaCreativa, "FUNCIONA_EN", espacioFisico) 
        graph.merge(ind_espacioFis)
    
        
#Para Necesidades
        #152
        
    if col[124]!="No sabe/no responde" and col[124]!="Ninguna" and col[124]!=None and col[124]!="No aplica":        
        nodeNecesidad = Node("Necesidades",nombre=col[124])
        graph.merge(nodeNecesidad,"Necesidades", "nombre")
        ind_Necs = Relationship(industriaCreativa, "NECESITA", nodeNecesidad) 
        graph.merge(ind_Necs)  
    
    #153
    if col[125]!="No sabe/no responde" and  col[125]!="Ninguna" and col[125]!=None and col[125]!="No aplica":
        nodeNecesidad = Node("Necesidades",nombre=col[125])
        graph.merge(nodeNecesidad,"Necesidades", "nombre")
        ind_Necs = Relationship(industriaCreativa, "NECESITA", nodeNecesidad) 
        graph.merge(ind_Necs)        
        
        
    

#Para percepción Oportunidades PERCIBE
    

    percepcionNode = Node("Percepcion",nombre= col[129])
    graph.merge(percepcionNode,"Percepcion","nombre")
    if col[127]=="Otra, ¿cuál?":
        ind_Percep = Relationship(industriaCreativa, "PERCIBE", percepcionNode,razonPercepcion=col[128]) 
        graph.merge(ind_Percep)
    else:
        ind_Percep = Relationship(industriaCreativa, "PERCIBE", percepcionNode,razonPercepcion=col[127]) 
        graph.merge(ind_Percep)
        
    

#Industrias Que colaboran
    if col[132]!=None and col[132]!=" ":
        industriaCoope = Node("IndustriaCreativa",sector=sectorRename(col[132]),nombre= noneToNotDefine(col[131]))
        graph.merge(industriaCoope,"IndustriaCreativa", "nombre")
        indCoop= Relationship(industriaCreativa, "COOPERA", industriaCoope) 
        graph.merge(indCoop)
    
    if col[134]!=None and col[134]!=" ":
        industriaCoope = Node("IndustriaCreativa",sector=sectorRename(col[134]),nombre= noneToNotDefine(col[133]))
        graph.merge(industriaCoope,"IndustriaCreativa", "nombre")
        indCoop= Relationship(industriaCreativa, "COOPERA", industriaCoope) 
        graph.merge(indCoop)
    
    if col[136]!=None and col[136]!=" ":
        industriaCoope = Node("IndustriaCreativa",sector=sectorRename(col[136]),nombre= noneToNotDefine(col[135]))
        graph.merge(industriaCoope,"IndustriaCreativa", "nombre")
        indCoop= Relationship(industriaCreativa, "COOPERA", industriaCoope) 
        graph.merge(indCoop)      

            
