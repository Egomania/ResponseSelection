'''
Created on Dec 12, 2013

'''

import random
import imp
import logging

from classes.Host import Host
from classes.Response import Response
from classes.Metric import Metric

def loadModules(modulesToUse, path):
    modules = {}

    for module in modulesToUse:

        selectedModulePath = path + '/' + module + '.py'
        selectedModuleName = module
        my_module = imp.load_source(selectedModuleName, selectedModulePath)
        modules[module] = my_module

    return modules

def printHostListe(host_liste):
    s = ""
    for e in host_liste:
        s = s + "    " + str(e.name) + ", " + str(e.htype)
    print (s)
    
def printResponseListe(resp_liste):
    s = ""
    for e in resp_liste:
        s = s + "    " + str(e.name) + ", " + str(e.source) + ", " + str(e.dest) + " , " + StringMetricListe(e.metrics) + ", " + str(e.conflicting_responses)
    print (s)
    
def printMetricListe(metric_liste):
    s = ""
    for e in metric_liste:
        s = s + "    " + str(e.name) + ", " + str(e.value)
    print (s)
    
def StringMetricListe(metric_liste):
    s = ""
    for e in metric_liste:
        s = s + "    " + str(e.name) + ", " + str(e.value)
    return (s)
    
def generateHosts(liste, tpe):
    newListe = []
    for e in liste:
        host = Host(str(e), tpe)
        newListe.append(host)
    return newListe

def tostring(liste):
    newListe = list()
    for e in liste:
        newListe.append(str(e))
    return newListe

#   name = ""     source = None    dest = None    metrics = None
def createResponse(list_entry, conflicts):
    i = 0
    name = ""
    source = ""
    dest = list()
    metrics = []
    m_name = ""
    m_value = 0.0
    for e in list_entry:
        if i == 0:
            dest = tostring(e)
            i = i + 1
            continue
        if i == 1:
            source = str(e)
            i = i + 1
            continue
        if i == 2:
            name = str(e)
            i = i + 1
            continue
        if i % 2 == 0:
            m_value = float(e)
            metr = Metric (m_name, m_value)
            metrics.append(metr)
            i = i + 1
        else:
            m_name = str(e)
            i = i + 1
    
    resp = Response(name, source, dest, metrics, conflicts)
    return resp

def combine(liste):
    tupel = liste.pop(0)
    newListe = []
    for element in tupel:
        newListe.append(element)
    for entry in liste:
        newListe.append(entry[3])
        newListe.append(entry[4])
    return newListe

def insertDistinct(liste, element):
    if liste.count(element) == 0:
        liste.append(element)
    return liste

def selectFrom(liste):
    numberSelectedResponses = random.randint(1,len(liste))
    newListe = []
    i = 0
    while i < numberSelectedResponses:
        newListe = insertDistinct(newListe, liste[random.randint(0, len(liste)) - 1])
        i = i + 1
    return newListe

def transformTupel(liste):
    for entr in liste:
        entr[0] = [entr[0]]  
    return liste

def contains(liste, elem, pos):
    
    for ent in liste:
        if ent[pos] == elem:
            return liste.index(ent)
    
    return len(liste) + 1

def transform(liste):
    liste = transformTupel(liste)
    newListe = []
    
    for ent in liste:
        position = contains(newListe, ent[2], 2)
        if position <= len(newListe):
            newListe[position][0].append(ent[0][0])
        else:
            newListe.append(ent)
        
     
    return newListe

def transformbug(liste):
    liste = transformTupel(liste)
    newListe = []
    i = 0
    first = True
    fuse = liste[0]

    if len(liste) == 1:
        return liste

    for e in liste:
 
        if first:
            first = False
            i = i + 1
            continue
        
        if fuse[2] != e[2]:
            newListe.append(fuse)
            fuse = e
            i = i + 1
            continue
        
        if fuse[2] == e[2]:
            x = fuse[0]
            x.append(e[0][0])
            fuse[0] = x
            i = 1 + 1
            continue
    
    newListe.append(fuse)
     
    return newListe
