'''
Created on Nov 27, 2013

'''

import logging
import psycopg2
import random

def randomDecimal():
    return random.randint(int(0), int(100 * 1000)) / 100000.0 

def insertDistinct(liste, element):
    if liste.count(element) == 0:
        liste.append(element)
    return liste

def startGen(DATA_SETTING, conn):

    NewRand = DATA_SETTING.newRandomEntity
    NewEffected = DATA_SETTING.newEffectedEntity
    NewUneffected = DATA_SETTING.newUneffectedEntity
    NewResponsesSelf = DATA_SETTING.newResponsesSelfHealing
    ResponseUneffected = DATA_SETTING.responseUneffected
    ResponseRandom = DATA_SETTING.responseRandom
    helpingFactor = DATA_SETTING.helpingFactor
    Conflicts = DATA_SETTING.conflicts
    Metrics = DATA_SETTING.metrics
    ResponseRand = (ResponseRandom - NewResponsesSelf*NewEffected)

    #Open Database Cursor for executing Queries
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()

    #Search for last host inserted
    cur.execute("SELECT max(id) FROM host;")
    maximumHost = cur.fetchone()[0]
    if maximumHost == None: 
        maximumHost = -1
    logging.info("Maximum Host ID: %s" , maximumHost)

    cur.execute("SELECT max(id) FROM response;")
    maximumResponse = cur.fetchone()[0]
    if maximumResponse == None:
        maximumResponse = -1
    logging.info("Maximum Response ID: %s" , maximumResponse)

    logging.info("Generate new Random Hosts: %s", NewRand)

    count = 0
    while (count != NewRand):

        effected = random.randint(0,1)

        maximumHost = maximumHost + 1

        if effected > 0: 
            cur.execute("INSERT INTO host VALUES(%s, %s)", (maximumHost, True))
            logging.debug("Insert Host : %s that is effected", maximumHost) 
        else: 
            cur.execute("INSERT INTO host VALUES(%s, %s)", (maximumHost, False))
            logging.debug("Insert Host : %s that is NOT effected" , maximumHost)

        count = count + 1

    logging.info("Generate Effected Hosts: %s", NewEffected)

    count = 0
    while (count != NewEffected):
        maximumHost = maximumHost + 1
        cur.execute("INSERT INTO host VALUES(%s, %s)", (maximumHost, True))
        logging.debug("Insert Host : %s that is effected", maximumHost)
        count = count + 1;

    logging.info("Generate Uneffected Hosts: %s", NewUneffected)
    count = 0
    while (count != NewUneffected):
        maximumHost = maximumHost + 1
        cur.execute("INSERT INTO host VALUES(%s, %s)", (maximumHost, False))
        logging.debug("Insert Host : %s that is NOT effected" , maximumHost)
        count = count + 1;

    logging.info("Generate: %s new self-healing Responses per Host", NewResponsesSelf)
    cur.execute("SELECT * FROM host where affected=true;")
    for record in cur:
        ActHost = record[0]
        cur2.execute("select count(*) from response, help, host where response.id = help.response and host.id = help.host and response.executedBy = host.id and host.id = %s;", (ActHost,))
        ActResponsesSelf = cur2.fetchone()[0]
        ActResponsesSelf = 0
        while (ActResponsesSelf < NewResponsesSelf):
            maximumResponse = maximumResponse + 1
            cur3.execute("INSERT INTO response VALUES(%s, %s)", (maximumResponse, ActHost))
            cur3.execute("INSERT INTO help VALUES(%s, %s)", (maximumResponse, ActHost))
            logging.debug("New Self-Healing Response for Host %s with ID %s" , ActHost, maximumResponse)
            ActResponsesSelf = ActResponsesSelf + 1

    logging.info("Generate: %s new Responses for not effected Host", ResponseUneffected)
    cur.execute("SELECT host.id FROM host where affected=true;")
    possibleInserts = cur.fetchall()
    entry = 0
    while entry < len(ResponseUneffected):
        NumberResponses = ResponseUneffected[entry]
        NumberEffected = entry + 1;
        logging.info("Generate: %s for every Host that effects %s Hosts", NumberResponses, NumberEffected)
        cur.execute("SELECT host.id FROM host where affected=false;")
        for record in cur:
            ins = 0
            while ins < NumberResponses:
                maximumResponse = maximumResponse + 1
                ActHost = record[0];
                logging.debug("New Response for Host %s with ID %s effecting %s hosts." , ActHost, maximumResponse, NumberEffected)
                cur2.execute("INSERT INTO response VALUES(%s, %s)", (maximumResponse, ActHost))
                insertionList = []
                counter = len(insertionList)
                while counter < NumberEffected:
                    insertionHost = possibleInserts[random.randint(0, len(possibleInserts)) - 1][0]
                    insertionList = insertDistinct(insertionList, insertionHost)
                    logging.debug("Response %s effects Host: %s", maximumResponse, insertionHost)
                    counter = len(insertionList)
                for elem in insertionList:
                    cur2.execute("INSERT INTO help VALUES(%s, %s)", (maximumResponse, elem))
                ins = ins + 1
        
        entry = entry + 1

    logging.info("Generate %s Responses executable from effected hosts with maximum helping factor: %s .", ResponseRand, helpingFactor)
        
    cur.execute("SELECT host.id FROM host where affected=true;")
    possibleInserts = cur.fetchall()

    cur.execute("SELECT host.id FROM host where affected=true;")
    responseCand = cur.fetchall()

    entry = 0
    while entry < ResponseRand:

        insertionHost = responseCand[random.randint(0, len(responseCand)) - 1][0]
        helpingFactorTMP = random.randint(1, helpingFactor)
        
        insertionList = []
        ins = len(insertionList)
        while ins < helpingFactorTMP:
            
            insHost = possibleInserts[random.randint(0, len(possibleInserts)) - 1][0]
            insertionList = insertDistinct(insertionList, insHost)
            
            ins = len(insertionList)
        
        logging.debug("Insert Response for %s using Response with helpingFactor: %s helping following hosts: %s", insertionHost, helpingFactorTMP, insertionList)
        
        maximumResponse = maximumResponse + 1
        cur.execute("INSERT INTO response VALUES(%s, %s)", (maximumResponse, insertionHost))
        for elem in insertionList:
            cur.execute("INSERT INTO help VALUES(%s, %s)", (maximumResponse, elem))
        
        entry = entry + 1
    

    logging.info("Generate Metrics: %s", Metrics)
    cur.execute("SELECT damage.name FROM damage;")

    metricsStoredTMP = cur.fetchall()
    metricsStored = []
    for elem in metricsStoredTMP:
        metricsStored.append(elem[0])

    for key in Metrics:
        value = Metrics[key][0]
        weight = Metrics[key][1]
        name = key

        if name not in metricsStored:
            cur.execute("Insert into damage Values(%s,%s,%s)", (name, value, weight, ))
        else:
            cur.execute("Update damage SET value = %s WHERE name LIKE (%s)", (value, name, ))
            cur.execute("Update damage SET weight = %s WHERE name LIKE (%s)", (weight, name, ))

    cur.execute("SELECT damage.name FROM damage;")
    for record in cur:
        metricName = record[0]
        logging.info("Add missing Metrics definitions for: %s", metricName)
        cur2.execute("SELECT response.id FROM response;")
        for record2 in cur2:
            response = record2[0]
            cur3.execute("SELECT metric.value FROM metric WHERE metric.name LIKE (%s) and metric.describes = %s;", (metricName, response, ));
            metricValue = cur3.fetchone()
            if metricValue == None: 
                metricValue = randomDecimal()
                cur3.execute("INSERT INTO metric VALUES(%s, %s, %s)", (metricName, response, metricValue))
                logging.debug("No Metric Definition - Insert Metric Value: %s for response: %s", metricValue, response)
            
    
    logging.info("Generate %s Conflicts", Conflicts)
    count = 0
    cur.execute("SELECT id FROM response;")
    buf = cur.fetchall()
    posConflicts = list()
    for resp in buf:
        posConflicts.append(resp[0])
        
    while (count != Conflicts):
        
        r1 = posConflicts[random.randint(int(0), int (len(posConflicts)-1))]
        r2 = posConflicts[random.randint(int(0), int (len(posConflicts)-1))]
        
        if r1 == r2:
            continue
        
        cur.execute("select count(*) from conflict where r1 = %s and r2 = %s", (r1, r2, ))
        conflictsact = cur.fetchone()[0]
        
        if conflictsact > 0:
            continue
            

        logging.debug("Add Conflict for Host %s and Host %s", r1, r2)
        cur.execute("INSERT INTO conflict VALUES(%s, %s)", (r1, r2))
        cur.execute("INSERT INTO conflict VALUES(%s, %s)", (r2, r1))
        
        count = count + 1

    conn.commit()

    cur.close()
    cur2.close()
    cur3.close()

