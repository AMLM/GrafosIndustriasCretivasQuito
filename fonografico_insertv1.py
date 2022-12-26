# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 14:48:33 2019

@author: Alexis Bautista
https://docs.google.com/spreadsheets/d/1-bCmRQDSpAOi7r5y1OuPTvNMnzHnqpBWmlKzl56WHgk/edit#gid=1110380932

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
nombreArchivo = "fonografico_datav2.csv"
fonograficoFile = pd.read_csv(nombreArchivo, sep=',', encoding='utf-8')
#Elimino 
del fonograficoFile['Unnamed: 21']
#Para reemplazar los valores nan 1
fonograficoFile = fonograficoFile.replace({np.nan:None})

#Motivos emprendimiento
motEmpr = ['Creación de fuentes de trabajo',
           'Desarrollo local y/o comunitario',
           'Negocio/empresa','Fomento cultural',
           'Formativo/Educativo','Alternativas de entretenimiento y/o uso de tiempo libre',
           'Difusión','Causa o mensaje social','Otra, ¿cuál?']
categoriasProducto = [
    'Posproducción',
    'Documentales y reportajes',
    'Servicios de grabación',
    'Corto y largo metrajes',
    'Publicidad',
    'Series',
    'Animación',
    'Formativos',
    'Agente manager',
    'Producción de espectáculos',
    'Promoción de nuevos talentos',
    'Otro']

espaciosFisicos=[
            'PUBLICO AL AIRE LIBRE',
            'PUBLICO NO CUBIERTO',
            'PRIVADO',
            'PROPIO'
            ]
    
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
    

def tipoBeneficio(tipoBeneficio):
    if tipoBeneficio=="No sabe/no responde":
        return "NO DEFINIDO"
    return tipoBeneficio    

def telefonos(campoTelefonos):
    if str(campoTelefonos)=="S/R" or str(campoTelefonos) == "S/N":
        return None
    else: 
        return campoTelefonos
    
def rucToNumber(ruc):
    if ruc!=None or ruc!=" ":
        ruc=ruc.replace(",","")
        return int(float(ruc))
    else:
        return ruc
    
def deleteCharacter(string):
    if string=="No sabe/no responde":
        return "NO_DEFINIDO"
    else:
        return str(string).replace("?","")

def sectorRename(sector):
    if sector=='ARTES ESCÉNICAS':
        sector='ARTES_ESCENICAS'
    if sector =='FONOGRÁFICO':
        sector='FONOGRAFICO'
    if sector == 'ARTES PLÁSICAS':
        sector='ARTES_PLASICAS'
    if sector ==' CINE':
        sector ='CINE'
    return  sector.upper()  

def renameRedesSociales(redes):
    if redes == "Redes Sociales":
        return "Redes sociales"
    else:
        return redes
    
def espacioDigitalDelete(string):
    if string == "S/N":
        return None
    else:
        return string

for index, col in fonograficoFile.iterrows():
    
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
    barrio = Node("Barrio",nombre= 'NO_DEFINIDO')
    graph.merge(barrio,"Barrio",'nombre')
    
    #Para crear el Barrio como no tenemos esto pues entonces se crea una con NO_DEFINIDO
    if col[172]!=None:
        barrio = Node("Barrio",nombre= col[172])
        graph.merge(barrio,"Barrio",'nombre')
    else:
        barrio = Node("Barrio",nombre= 'NO_DEFINIDO')
        graph.merge(barrio,"Barrio",'nombre')    
    
    #Relación entre bARRIO y Cantón
    barrioCanton = Relationship(barrio, "PERTENECE_CANTON",ubicacionG)
    graph.merge(barrioCanton)
    
    #Para la parte deIndustriaCreativa
    industriaCreativa = Node("IndustriaCreativa",sector="FONOGRAFICO", nombre = col[7], telConvencional = telefonos(col[8]), telCelular = telefonos(col[9]), ruc = rucToNumber(col[10]), correo = col[11],tiempoFuncionamientoAnios = col[19],tiempoFuncionamientoMeses = col[20], personal = col[21]  , callePrincipal = col[4], no = col[5], calleSecundaria = col[6],totalProductoAnio=col[122] )
    graph.merge(industriaCreativa,"IndustriaCreativa", "nombre")    
    
    #Relacion entre  IndustriaCreativa - Barrio = UBICADAS
    industriaUbica = Relationship(industriaCreativa, "UBICADA_EN",barrio) 
    graph.merge(industriaUbica)
    
    if col[12]!=None and col[12]!="NO TIENE" and col[12]!="N/T":
        
        #Espacio Digital
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
            
        elif str(col[12]).lower().startswith("youtube"):
            espacioDigital = Node("EspacioDigital", nombre= "YOUTUBE")
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
    #Circulacion
    if str(col[24]).lower() == "si":
        #Hay una relacion entre Circulacion y la industria Creativa
        actividadCircu = Node("ActividadesCadenaProductiva",nombre = 'CIRCULACION')
        graph.merge(actividadCircu,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "DESARROLLA", actividadCircu) 
        graph.merge(industriaProd)
  

    # Para motivo emprendimiento
    #Se toma de la lista que se ha generado en motivosEmprendimiento()
    #Para motivo 1 en la parte de motivo emprendimiento
    if str(col[26]) in motEmpr:
        motivo1 = Node("MotivoEmprendimiento",categoria = col[26])
        graph.merge(motivo1,"MotivoEmprendimiento", "categoria")
        motiv1_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo1) 
        graph.merge(motiv1_industria)
    #Para motivo 2 en la parte de motivo emprendimiento
    if str(col[27]) in motEmpr:
        motivo2 = Node("MotivoEmprendimiento",categoria = col[27])
        graph.merge(motivo2,"MotivoEmprendimiento", "categoria")
        motiv2_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo2) 
        graph.merge(motiv2_industria)
        
    #Para motivo 3 en la parte de motivo emprendimiento
    if str(col[28]) in motEmpr:
        if str(col[28]) =="Otra, ¿cuál?":
            motivo3 = Node("MotivoEmprendimiento",categoria = 'Otra')
            graph.merge(motivo3,"MotivoEmprendimiento", "categoria")
            motiv3_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo3,nombre=col[29])
            graph.merge(motiv3_industria)
        else:             
            motivo3 = Node("MotivoEmprendimiento",categoria = col[28])
            graph.merge(motivo3,"MotivoEmprendimiento", "categoria")
            motiv3_industria = Relationship(industriaCreativa, "MOTIVADO_POR", motivo3) 
            graph.merge(motiv3_industria)

    #Para relación 30
    motivo4 = Node("MotivoEmprendimiento",categoria=col[30])
    graph.merge(motivo4,"MotivoEmprendimiento","categoria")
    motiv4_industria = Relationship(industriaCreativa, "SE_RECONOCE", motivo4) 
    graph.merge(motiv4_industria)
    
    


    #Para Productos
    #Producen u Ofrecen
            #Para Productos y Servicios     
    
    if str(col[31]) in categoriasProducto:
        if str(col[31]) == "Otro":
            productoServicio = Node("ProductosYServicios",categoria = col[31])
            graph.merge(productoServicio,"ProductosYServicios", "categoria")
            prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio,nombre=col[33]) 
            graph.merge(prod_servicio)                   
        else:
            productoServicio = Node("ProductosYServicios",categoria = col[31])
            graph.merge(productoServicio,"ProductosYServicios", "categoria")
            prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio) 
            graph.merge(prod_servicio)                   
    if str(col[32]) in categoriasProducto:
        if str(col[32]) == "Otro":
            productoServicio = Node("ProductosYServicios",categoria = col[32])
            graph.merge(productoServicio,"ProductosYServicios", "categoria")
            prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio,nombre=col[33]) 
            graph.merge(prod_servicio)                   
        else:
            productoServicio = Node("ProductosYServicios",categoria = col[32])
            graph.merge(productoServicio,"ProductosYServicios", "categoria")
            prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio) 
            graph.merge(prod_servicio) 
            

    #Para taller
    if str(col[34]).lower() == "si":
        productoServicio = Node("ProductosYServicios",categoria = "Taller")
        #graph.merge(productoServicio,"ProductosYServicios", "categoria")
        graph.create(productoServicio)
        prod_servicio = Relationship(industriaCreativa, "PRODUCE_OFRECE", productoServicio) 
        graph.merge(prod_servicio)        

        #Para Espacios Físicos Publico al Aire Libre
        if str(col[35])=='SI':
            espacioFisico = Node("EspacioFisico",nombre = 'Publico al Aire Libre')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico,nombre= col[36]) 
            graph.merge(produc_espacioFis)
        #Para Espacios Cubierto No Pagados
        if str(col[37])=='SI':
            espacioFisico = Node("EspacioFisico",nombre = 'Publico No Cubierto')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico,nombre= col[38]) 
            graph.merge(produc_espacioFis)
        #Privado
        if str(col[39])=='SI':
            espacioFisico = Node("EspacioFisico",nombre = 'Privado')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico,nombre= col[40]) 
            graph.merge(produc_espacioFis)
        #Para Espacio Fisico Propio        
        if str(col[43])=='SI':
            espacioFisico = Node("EspacioFisico",nombre = 'Propio')
            graph.merge(espacioFisico,"EspacioFisico", "nombre")
            produc_espacioFis = Relationship(productoServicio, "SE_REALIZAN", espacioFisico) 
            graph.merge(produc_espacioFis)                 
            
        #Para temática 47-49
        if isinstance(col[44], str)and col[44]!='0' and col[44]!='19':
            listaTematicas = col[44].split(",")
            for i in listaTematicas:
                tematica = Node("Tematica",nombre = i)
                graph.merge(tematica,"Tematica","nombre")  
                produc_temati = Relationship(productoServicio, "CONTIENEN", tematica) 
                graph.merge(produc_temati)
        if (isinstance(col[45],str) and col[45]!='0'and col[45]!='19'):
            listaTematicas = col[45].split(",")
            for i in listaTematicas:
                tematica = Node("Tematica",nombre = i)
                graph.merge(tematica,"Tematica","nombre")  
                produc_temati = Relationship(productoServicio, "CONTIENEN", tematica) 
                graph.merge(produc_temati)      
                
        if (isinstance(col[46],str) and col[46]!='0'and col[46]!='19' and col[46]!= None):
            listaTematicas = col[46].split(",")
            for i in listaTematicas:
                tematica = Node("Tematica",nombre = i)
                graph.merge(tematica,"Tematica","nombre")  
                produc_temati = Relationship(productoServicio, "CONTIENEN", tematica) 
                graph.merge(produc_temati)
                
        #Consumidores
        #Solo si es taller- niños
        if str(col[47]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'niños')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)        
        
        #Para consumidores- JOVENES
        if str(col[48]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'jovenes')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)
        #Para consumidores-Adultos 
        if str(col[49]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'adultos')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)
        #Para consumidores- Solo si es taller Adultos Mayores
        if str(col[50]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Adultos Mayores')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)
        #Para consumidores- Poblacion vulnerable 
        if str(col[51]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Población Vulnerable')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)    
        #Para consumidores- Público en General
        if str(col[52]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Público en General')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)  
        #Para consumidores- Profesionales del Sector 
        if str(col[53]) == "SI":
            consumidor = Node("Consumidor",tipoConsumidor = 'Profesionales del Sector')
            graph.merge(consumidor,"Consumidor","tipoConsumidor")  
            produc_consum = Relationship(productoServicio, "SE_OFRECEN", consumidor) 
            graph.merge(produc_consum)    
            
            
            
        #Para tipo de Registro que ahora es Derechos de Autor---60-63-65
    if( col[56] != None and col[56]!="NO"):
        derechosAutor = Node("DerechosAutor",nombre = col[56])
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)            
        
    if( col[58] != None and col[58]!="NO"):
        derechosAutor = Node("DerechosAutor",nombre = col[58])
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)  

    if( col[60] != None and col[60]!="NO"):
        derechosAutor = Node("DerechosAutor",nombre = col[60])
        graph.merge(derechosAutor,"DerechosAutor","nombre") 
        industDerechosAutor = Relationship(industriaCreativa, "REGISTRA", derechosAutor)
        graph.merge(industDerechosAutor)        
        
        
        
    #Para PrensaEscrita 77
    if str(col[70]).lower()=='si':
        espacioFisico = Node("EspacioFisico",nombre = 'Prensa Escrita')
        graph.merge(espacioFisico,"EspacioFisico", "nombre")
        produc_espacioFis = Relationship(industriaCreativa, "PUBLICITAN", espacioFisico) 
        graph.merge(produc_espacioFis)
        
   #Para espacio Digital
       #Television Digital
    if str(col[71]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Televisión')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)      
       
       #Radio Digital
    if str(col[72]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Radio Digital')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)      
          
       #Internet
    if str(col[73]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Internet')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)       
       #Redes Sociales     
    if str(col[74]).lower()=='si':
        espacioDigital = Node("EspacioDigital",nombre = 'Redes sociales')
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        produc_espaDig = Relationship(industriaCreativa, "PUBLICITAN", espacioDigital) 
        graph.merge(produc_espaDig)  
        
        
#Para Alcance (85-90)
        
    #Para alcance Internacional
    if str(col[83]).lower()=="internacional" :
        alcance = Node("Alcance",nombre = 'Internacional')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)           
        #Para alcance Costa
    if str(col[78])=="SI":
        alcance = Node("Alcance",nombre = 'Costa')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)
    #Para alcance Sierra
    if str(col[79])=="SI":
        alcance = Node("Alcance",nombre = 'Sierra')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)                   
    #Para alcance Amazonía
    if str(col[80])=="SI":
        alcance = Node("Alcance",nombre = 'Amazonía')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)   
    #Para alcance Insular
    if str(col[81])=="SI":
        alcance = Node("Alcance",nombre = 'Insular')
        graph.merge(alcance,"Alcance","nombre")  
        industriaAlcance = Relationship(industriaCreativa, "ALCANCE", alcance) 
        graph.merge(industriaAlcance)   

        
        
    #Relacion SELIMITA Entre IndustriaCreativa Y factores de Dificultad

    #Factores Alcance Nacional
    if col[84]!= None and str(col[84])!="No aplica":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Nacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad ,nombreDificultad =deleteCharacter(col[84])) 
        graph.merge(alcanceFactores)   
    
    if col[85]!= None and str(col[85])!="No aplica":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Nacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad,nombreDificultad = deleteCharacter(col[85])) 
        graph.merge(alcanceFactores)  
    
    #Factores Alcance Internacional
    if col[86]!= None and str(col[86])!="No aplica":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Internacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad,nombreDificultad = deleteCharacter(col[86]))
        graph.merge(alcanceFactores)   
    
    if col[87]!= None and str(col[87])!="No aplica":
        factorDificultad = Node("FactorDificultad",tipoDificultad='Alcance Internacional')
        graph.merge(factorDificultad,"FactorDificultad","tipoDificultad")  
        alcanceFactores = Relationship(industriaCreativa, "SE_LIMITA", factorDificultad,nombreDificultad = deleteCharacter(col[87]))
        graph.merge(alcanceFactores)      
        

    #Relación INDUSTRIACREATIVA - SE DISTRIBUYEN - ESPACIO FISICO

#95            
	#Para digitales
    if str(col[88]) in espaciosDigitalDistribucion:
        espacioDigital = Node("EspacioDigital",nombre = renameRedesSociales(col[88]))
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        cadeProd_espaDig = Relationship(industriaCreativa, "DISTRIBUYEN", espacioDigital) 
        graph.merge(cadeProd_espaDig)                
	#Para fisicos
    if str(col[88]) in espaciosFisicoDistribucion :
        espacioFisico = Node("EspacioFisico",nombre = col[88])
        graph.merge(espacioFisico,"EspacioFisico","nombre")  
        cadeProd_espacioFis = Relationship(industriaCreativa, "DISTRIBUYEN", espacioFisico) 
        graph.merge(cadeProd_espacioFis)   
        
#89
	#Para digitales
    if str(col[89]) in espaciosDigitalDistribucion:
        espacioDigital = Node("EspacioDigital",nombre = renameRedesSociales(col[89]))
        graph.merge(espacioDigital,"EspacioDigital","nombre")  
        cadeProd_espaDig = Relationship(industriaCreativa, "DISTRIBUYEN", espacioDigital) 
        graph.merge(cadeProd_espaDig)                
	#Para fisicos
    if str(col[89]) in espaciosFisicoDistribucion :
        espacioFisico = Node("EspacioFisico",nombre = col[89])
        graph.merge(espacioFisico,"EspacioFisico","nombre")  
        cadeProd_espacioFis = Relationship(industriaCreativa, "DISTRIBUYEN", espacioFisico) 
        graph.merge(cadeProd_espacioFis)          
        
        
#97 Circulacion
	
    #Para fisicos
    if str(col[90]) in espaciosFisicosCirculacionExhibicion :
            
        espacioFisico = Node("EspacioFisico",nombre = col[90])
        graph.merge(espacioFisico,"EspacioFisico","nombre")  
        cadeProd_espacioFis = Relationship(industriaCreativa, "CIRCULAN_EXHIBEN", espacioFisico,nombre=espacioDigitalDelete(col[91])) 
        graph.merge(cadeProd_espacioFis)              
	#Para digitales
    if str(col[90]) in espaciosDigitalesCirculacionExhibicion:
        
        if col[90]=="Medios digitales (internet)":
            espacioDigital = Node("EspacioDigital",nombre = 'Internet')
            graph.merge(espacioDigital,"EspacioDigital","nombre")  
            cadeProd_espaDig = Relationship(industriaCreativa, "CIRCULAN_EXHIBEN", espacioDigital,nombre=espacioDigitalDelete(col[91]))
            graph.merge(cadeProd_espaDig)             
        else:
            espacioDigital = Node("EspacioDigital",nombre = col[90])
            graph.merge(espacioDigital,"EspacioDigital","nombre")  
            cadeProd_espaDig = Relationship(industriaCreativa, "CIRCULAN_EXHIBEN", espacioDigital,nombre=espacioDigitalDelete(col[91])) 
            graph.merge(cadeProd_espaDig)  	   
            

    #Para fisicos
    if str(col[92]) in espaciosFisicosCirculacionExhibicion :
            
        espacioFisico = Node("EspacioFisico",nombre = col[92])
        graph.merge(espacioFisico,"EspacioFisico","nombre")  
        cadeProd_espacioFis = Relationship(industriaCreativa, "CIRCULAN_EXHIBEN", espacioFisico,nombre=espacioDigitalDelete(col[93]))
        graph.merge(cadeProd_espacioFis)              
	#Para digitales
    if str(col[92]) in espaciosDigitalesCirculacionExhibicion:
        
        if col[92]=="Medios digitales (internet)":
            espacioDigital = Node("EspacioDigital",nombre = 'Internet')
            graph.merge(espacioDigital,"EspacioDigital","nombre")  
            cadeProd_espaDig = Relationship(industriaCreativa, "CIRCULAN_EXHIBEN", espacioDigital,nombre=espacioDigitalDelete(col[93])) 
            graph.merge(cadeProd_espaDig)             
        else:
            espacioDigital = Node("EspacioDigital",nombre = col[92])
            graph.merge(espacioDigital,"EspacioDigital","nombre")  
            cadeProd_espaDig = Relationship(industriaCreativa, "CIRCULAN_EXHIBEN", espacioDigital,nombre=espacioDigitalDelete(col[93])) 
            graph.merge(cadeProd_espaDig)  
            
            
#Tipo Consumidores- Relaciòn SIRVE entre INDUSTRIAC Y CONSUMIDOR
        
#Tipo de Consumidor - Relación CONSUMEN 
    #Con columna 101
    if str(col[94])!="No sabe/no responde" and col[94]!=None:
        tipoConsumidor = Node("Consumidor",tipoConsumidor = str(col[94]))
        graph.merge(tipoConsumidor,"Consumidor","tipoConsumidor")  
        consum_prod = Relationship(industriaCreativa, "SIRVE", tipoConsumidor) 
        graph.merge(consum_prod)
        
    #Con la columna 102
    if str(col[95])!="No sabe/no responde":        
        tipoConsumidor102 = Node("Consumidor",tipoConsumidor = str(col[95]))
        graph.merge(tipoConsumidor102,"Consumidor","tipoConsumidor")  
        consum_prod = Relationship(industriaCreativa, "SIRVE", tipoConsumidor102) 
        graph.merge(consum_prod)            
        
        
#INTERCAMBIONOMONETARIO Se forma de acuerdo a la relaciòn tal 
    #Produccion
    if str(col[97]).lower() == "si":
        #Hay una relacion entre Produccion y la industria Creativa
        actividadProd = Node("ActividadesCadenaProductiva",nombre = 'PRODUCCION')
        graph.merge(actividadProd,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "INTERCAMBIO", actividadProd)
        graph.merge(industriaProd)
    #Distribucion
    if str(col[98]).lower() == "si":
        #Hay una relacion entre Distribucion y la industria Creativa
        actividadDistr = Node("ActividadesCadenaProductiva",nombre = 'DISTRIBUCION')
        graph.merge(actividadDistr,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "INTERCAMBIO", actividadDistr)
        graph.merge(industriaProd)
    #Circulacion
    if str(col[99]).lower() == "si":
        #Hay una relacion entre Circulacion y la industria Creativa
        actividadCircu = Node("ActividadesCadenaProductiva",nombre = 'CIRCULACION')
        graph.merge(actividadCircu,"ActividadesCadenaProductiva", "nombre")
        industriaProd = Relationship(industriaCreativa, "INTERCAMBIO", actividadCircu) 
        graph.merge(industriaProd)



# ORGANIZACIONES
    #Primero debe estar un nodo tipo Beneficio
    #119
    
    #109
    if col[104]!=" " and col[104]!= None :
        nodeGremio = Node("Organizacion",tipoOrganizacion="Colectivo",nombre=col[104])
        nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[112]))
        graph.merge(nodeGremio,"Organizacion","nombre")
        graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")
        
        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeGremio) 
        graph.merge(industriaOrganizacion)
        gremioTipo = Relationship(nodeGremio, "PRESTAN", nodeTipoBeneficio) 
        graph.merge(gremioTipo)
        
        if col[113]!=" " and col[113]!= None and col[113]!="No sabe/no responde" :
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=col[113])
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeGremio, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)
        
    if col[106]!=" " and col[106]!= None:
        nodeAsociacion = Node("Organizacion",tipoOrganizacion="Asociacion",nombre=col[106])
        nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[112]))
        graph.merge(nodeAsociacion,"Organizacion","nombre")  
        graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")

        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeAsociacion) 
        graph.merge(industriaOrganizacion)        
        gremioTipo = Relationship(nodeAsociacion, "PRESTAN", nodeTipoBeneficio) 
        graph.merge(gremioTipo)
        
        if col[113]!=" " and col[113]!= None and col[113]!="No sabe/no responde":
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=tipoBeneficio(col[113]))
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeAsociacion, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)        
        
    if col[108]!=" " and col[108]!= None:
        nodeRed = Node("Organizacion",tipoOrganizacion="Red",nombre=col[108])
        nodeTipoBeneficio = Node("TipoBeneficio",nombre=tipoBeneficio(col[112]))
        graph.merge(nodeRed,"Organizacion","nombre")  
        graph.merge(nodeTipoBeneficio,"TipoBeneficio","nombre")

        industriaOrganizacion = Relationship(industriaCreativa, "FORMA_PARTE", nodeRed) 
        graph.merge(industriaOrganizacion)           
        gremioTipo = Relationship(nodeRed, "PRESTAN", nodeTipoBeneficio) 
        graph.merge(gremioTipo)
        
        if col[113]!=" " and col[113]!= None and col[113]!="No sabe/no responde":
            nodeTipoBeneficio2 = Node("TipoBeneficio",nombre=tipoBeneficio(col[113]))
            graph.merge(nodeTipoBeneficio2,"TipoBeneficio","nombre")
            gremioTipo = Relationship(nodeRed, "PRESTAN", nodeTipoBeneficio2) 
            graph.merge(gremioTipo)     
            
            
#Para mecanismos de sostenabilidad  e Industria Creativa
    nodeMecanismo = Node("MecanismoSostenabilidad",nombre=tipoBeneficio(col[115]))
    graph.merge(nodeMecanismo,"MecanismoSostenabilidad","nombre")
    industriaMecanism = Relationship(industriaCreativa, "SE_MANTIENE", nodeMecanismo) 
    graph.merge(industriaMecanism)       
    #No sabe no se mapea 
    if str(col[116])!="No sabe/no responde":
        nodeMecanismo2 = Node("MecanismoSostenabilidad",nombre=col[116])
        graph.merge(nodeMecanismo2,"MecanismoSostenabilidad","nombre")
        industriaMecanism1 = Relationship(industriaCreativa, "SE_MANTIENE", nodeMecanismo2) 
        graph.merge(industriaMecanism1)                
            
    
#Para vinculos con la comunidad
    #125
    #"""
    
    if col[118]!= "Ninguna":    
        nodeVinculo = Node("VinculoComunidad",nombre=col[118])
        graph.merge(nodeVinculo,"VinculoComunidad","nombre")
        industriaVinculo = Relationship(industriaCreativa, "MANTIENEN", nodeVinculo) 
        graph.merge(industriaVinculo)    
    
    #126
    #"""
    if col[119]!= "Ninguna" and col[119]!=None:
        nodeVinculo = Node("VinculoComunidad",nombre=col[119])
        graph.merge(nodeVinculo,"VinculoComunidad","nombre")
        industriaVinculo = Relationship(industriaCreativa, "MANTIENEN", nodeVinculo) 
        graph.merge(industriaVinculo)         
        
    
    
#Para Espacio Fisico, se relaciona como FUNCIONA_EN
    #133
    if col[126]!="No posee local":
        espacioFisico = Node("EspacioFisico",nombre = col[126])
        graph.merge(espacioFisico,"EspacioFisico", "nombre")
        ind_espacioFis = Relationship(industriaCreativa, "FUNCIONA_EN", espacioFisico) 
        graph.merge(ind_espacioFis)
    #134
    if col[127]!= None and col[127]!=" ":
        espacioFisico = Node("EspacioFisico",nombre = col[127])
        graph.merge(espacioFisico,"EspacioFisico", "nombre")
        ind_espacioFis = Relationship(industriaCreativa, "FUNCIONA_EN", espacioFisico) 
        graph.merge(ind_espacioFis)
    
#Para formatos: 142,144,146,148
    if col[129] == "SI":
        nodoFormato = Node("Formato",nombre='FISICO')
        graph.merge(nodoFormato,"Formato", "nombre")
        ind_formato = Relationship(industriaCreativa, "USA", nodoFormato) 
        graph.merge(ind_formato)

    if col[137] == "SI":
        nodoFormato = Node("Formato",nombre='DIGITAL')
        graph.merge(nodoFormato,"Formato", "nombre")
        ind_formato = Relationship(industriaCreativa, "USA", nodoFormato) 
        graph.merge(ind_formato)    
        
        
#Para Necesidades
        #152
    nodeNecesidad = Node("Necesidades",nombre=col[148])
    graph.merge(nodeNecesidad,"Necesidades", "nombre")
    ind_Necs = Relationship(industriaCreativa, "NECESITA", nodeNecesidad) 
    graph.merge(ind_Necs)  
    
    #153
    if col[153]!="No sabe/no responde":
        nodeNecesidad = Node("Necesidades",nombre=col[149])
        graph.merge(nodeNecesidad,"Necesidades", "nombre")
        ind_Necs = Relationship(industriaCreativa, "NECESITA", nodeNecesidad) 
        graph.merge(ind_Necs)        
        
        
    

#Para percepción Oportunidades PERCIBE
    

    percepcionNode = Node("Percepcion",nombre= col[161])
    graph.merge(percepcionNode,"Percepcion","nombre")
    if col[159]=="Otra, ¿cuál?":
        ind_Percep = Relationship(industriaCreativa, "PERCIBE", percepcionNode,razonPercepcion=col[160]) 
        graph.merge(ind_Percep)
    else:
        ind_Percep = Relationship(industriaCreativa, "PERCIBE", percepcionNode,razonPercepcion=col[159]) 
        graph.merge(ind_Percep)
        
    

#Industrias Que colaboran
    if col[164]!=None and col[164]!=" " and str(col[164])!='20':
        listaIndustrias = col[164].split(",")
        for i in listaIndustrias:
            industriaCoope = Node("IndustriaCreativa",sector=sectorRename(i),nombre= col[163])
            graph.merge(industriaCoope,"IndustriaCreativa", "nombre")
            indCoop= Relationship(industriaCreativa, "COOPERA", industriaCoope) 
            graph.merge(indCoop)
    
    if col[167]!=None and col[167]!=" ":
        listaIndustrias = col[167].split(",")
        for i in listaIndustrias:
            industriaCoope = Node("IndustriaCreativa",sector=sectorRename(i),nombre= col[166])
            graph.merge(industriaCoope,"IndustriaCreativa", "nombre")
            indCoop= Relationship(industriaCreativa, "COOPERA", industriaCoope) 
            graph.merge(indCoop)     
    
    if col[170]!=None and col[170]!=" ":
        listaIndustrias = col[170].split(",")
        for i in listaIndustrias:
            industriaCoope = Node("IndustriaCreativa",sector=sectorRename(i),nombre= col[169])
            graph.merge(industriaCoope,"IndustriaCreativa", "nombre")
            indCoop= Relationship(industriaCreativa, "COOPERA", industriaCoope) 
            graph.merge(indCoop)   
