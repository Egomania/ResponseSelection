'''
Created on Dec 4, 2013

@author: ansii
'''
#### IMPORTS ####

import psycopg2
import sys
import datetime
import gc 
import random
import time
import logging
import os

from multiprocessing import Manager, Process

import helper.helper_functions as helper
import helper.db_helper as db

import modules.Solver as solverGeneric

from classes.Metric import Metric

def generateFromScript(expId, cur, database, solverName, solver, solverConfig, PATHS, SAFE, loop, DB_SETTING, dumpSolution, solPath):

    try:
        path = PATHS[solverName]
    except:
        path = PATHS['default']  
        path = "default/" + str(database) + "/" + solverName + "/"  + path

    if not os.path.exists(path):
        os.makedirs(path)         


    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    for elem in files:  
        insertMapper = createInsertMapperscript(expId, elem, cur, DB_SETTING)
        
        if insertMapper == None:
            logging.error("Could not create data to insert.")
            return
        runID = insertMapper['id']
        for i in range(loop):

            logging.info("Solver %s executes in Round %s", solverName, i)
            logging.info("Start new Experiment with run ID %s", insertMapper['id']) 

            try:
                solutionPath = solPath[solverName]
            except:
                solutionPath = solPath['default']  
                solutionPath = "default/" + str(database) + "/" + solverName + "/" + solutionPath

            if not os.path.exists(solutionPath):
                os.makedirs(solutionPath)         

            toInsert = solverGeneric.scriptSolver(solver, solverConfig, path + "/" + elem, dumpSolution, solutionPath)

            insertMapper['creation_time'] = toInsert[0]    
            insertMapper['start_creation_time'] = toInsert[1]
            insertMapper['end_creation_time'] = toInsert[2]
            insertMapper['calc_time'] = toInsert[3]
            insertMapper['start_calc_time'] = toInsert[4]
            insertMapper['end_calc_time'] = toInsert[5]

            insertMapper['costs'] = toInsert[7]
            insertMapper['selected_response'] = toInsert[6]

            if SAFE:
                insertValues(cur, insertMapper)

            runID = runID + 1
            insertMapper['id'] = runID

            logging.info("Solver %s evaluated %s to use with costs = %s ", solverName, toInsert[6], toInsert[7])

def createData(testCase, conflictsList, cur):

    data = {}
    
    list_hosts_attacked = list()
    list_hosts_executing = list()

    for elem in testCase:
        host_attacked = elem[0]
        list_hosts_attacked = helper.insertDistinct(list_hosts_attacked, host_attacked)
        host_executing = elem[1]
        list_hosts_executing = helper.insertDistinct(list_hosts_executing, host_executing)

    host_attacked = helper.generateHosts(list_hosts_attacked, "a")
    host_executing = helper.generateHosts(list_hosts_executing, "e")        

    data["attacked"] = host_attacked
    data["executing"] = host_executing

    responses_used = list()
    response_liste = helper.transform(testCase)

    for e in response_liste:
        response = e[2]
        confl = []
        for elem in conflictsList:
            if elem[0] == response:
                confl.append(elem[1])
            if elem[1] == response:
                confl.append(elem[0])
        resp = helper.createResponse(e, confl)
        responses_used.append(resp)

    data["response"] = responses_used

    metrics_used = list()
    damage_used = list()
    cur.execute("Select * from damage")
    buf = cur.fetchall()
    for ent in buf:
        if ent[1] == None:
            num = 1.0
        else:
            num = float(ent [1])
        metric = Metric (str(ent[0]), num)
        metrics_used.append(metric)
        dam = Metric (str(ent[0]), num)
        damage_used.append(dam)


    data["metric"] = metrics_used
    data["damage"] = damage_used
    data["conflict"] = conflictsList

    return data


def insertValues (cur, insertMapper):
    logging.info("Store Value with experiment ID %s", insertMapper['id'])
    insertStatement = "insert into experiment values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cur.execute(insertStatement, (insertMapper['id'], insertMapper['ref_id'], insertMapper['creation_time'], insertMapper['start_creation_time'], insertMapper['end_creation_time'], insertMapper['calc_time'], insertMapper['start_calc_time'], insertMapper['end_calc_time'], insertMapper['number_hosts'], insertMapper['number_responses'], insertMapper['number_conflicts'], insertMapper['number_metrics'] , insertMapper['number_self_healing'], insertMapper['number_multiple_help'], insertMapper['max_helps'], insertMapper['avg_helps'], insertMapper['costs'], insertMapper['selected_response'], ))

def createInsertMapper(expId, entity, response, conflict, metrics, cur):

    cur.execute("SELECT max(id) FROM experiment WHERE ref_id = %s;", (expId, ))
    runID = cur.fetchone()[0]
    if runID == None:
        runID = 0
    runID = runID + 1

    insertMapper = {}
    insertMapper['id'] = runID
    insertMapper['ref_id'] = expId
    insertMapper['creation_time'] = None    
    insertMapper['start_creation_time'] = None
    insertMapper['end_creation_time'] = None
    insertMapper['calc_time'] = None
    insertMapper['start_calc_time'] = None
    insertMapper['end_calc_time'] = None
    insertMapper['number_hosts'] = entity
    insertMapper['number_responses'] = response
    insertMapper['number_conflicts'] = conflict
    insertMapper['number_metrics'] = metrics
    insertMapper['number_self_healing'] = None
    insertMapper['number_multiple_help'] = None
    insertMapper['max_helps'] = None
    insertMapper['avg_helps'] = None
    insertMapper['costs'] = None
    insertMapper['selected_response'] = None

    return insertMapper

def createInsertMapperscript(expId, elem, cur, DB_SETTING):

    dataValues = elem.split("_")
    dataDB = dataValues[1]

    dataSetting = dataValues[3]
    dataRunId = dataValues[5].split(".")[0]

    conForData = db.connect(DB_SETTING, dataDB)
    curForData = conForData.cursor()

    getStatement = "select number_hosts, number_responses, number_conflicts, number_metrics, number_self_healing, number_multiple_help, max_helps, avg_helps from experiment WHERE ref_id = %s AND id = %s;"

    curForData.execute(getStatement, (dataSetting, dataRunId, ))

    dataToInsert = curForData.fetchone()

    if dataToInsert == []:
        logging.error("No data for file %s found.", elem)
        return None

    number_hosts = dataToInsert[0]
    number_responses = dataToInsert[1]
    number_conflicts = dataToInsert[2]
    number_metrics = dataToInsert[3]
    number_self_healing = dataToInsert[4]
    number_multiple_help = dataToInsert[5]
    max_helps = dataToInsert[6]
    avg_helps = dataToInsert[7]

    conForData.close()
    conForData.close

    insertMapper = createInsertMapper(expId, number_hosts, number_responses, number_conflicts, number_metrics, cur)

    insertMapper['number_self_healing'] = number_self_healing
    insertMapper['number_multiple_help'] = number_multiple_help
    insertMapper['max_helps'] = max_helps
    insertMapper['avg_helps'] = avg_helps

    return insertMapper

def createInsertMapperDatabase(expId, entity, response, conflict, metrics, cur, runNumResp, runNumHost):
    
    insertMapper = createInsertMapper(expId, entity, response, conflict, metrics, cur)

    selfHealingStatement = "select count(*) from help h, response r where h.response = r.id and r.executedby = h.host and r.id in %s;"
    cur.execute(selfHealingStatement, (tuple(runNumResp), ))
    selfHealing = cur.fetchone()[0]
    insertMapper['number_self_healing'] = selfHealing


    multipleHelpStatement = "select count(*) from help h, response r where h.response = r.id and r.executedby != h.host and r.id in %s;"
    cur.execute(multipleHelpStatement, (tuple(runNumResp), ))
    multipleHelp = cur.fetchone()[0]
    insertMapper['number_multiple_help'] = multipleHelp

    maxHelpStatement = "select max(tmp.counter) from (select response.id, count(help.host) as counter from response, help where response.id = help.response and response.id in %s and help.host in %s group by response.id order by counter asc) as tmp;"
    cur.execute(maxHelpStatement, (tuple(runNumResp), tuple(runNumHost), ))
    maxHelps = cur.fetchone()[0]
    insertMapper['max_helps'] = maxHelps

    avgHelpStatement = "select avg(tmp.counter) from (select response.id, count(help.host) as counter from response, help where response.id = help.response and response.id in %s and help.host in %s group by response.id order by counter asc) as tmp;"
    cur.execute(avgHelpStatement, (tuple(runNumResp), tuple(runNumHost), ))
    avgHelps = cur.fetchone()[0]

    insertMapper['avg_helps'] = avgHelps

    return insertMapper

def generateFromDatabase(expId, cur, entity, response, conflict, metrics, EXECUTION_SETTING, solverConfig, solver, solverName, bounds, SAFE, database):

    manager = Manager()
    return_list = manager.dict()

    runNumHost = []
    runNumResp = []

    outputFormat = []
    testCase = []
    maximumMetric = len(metrics)
    metricVar = 0

    statement = "SELECT x.host AS effected, response.executedby AS executer, x.response AS response, metric.name, metric.value FROM (SELECT ROW_NUMBER() OVER (PARTITION BY host ORDER BY response) AS r, t.* FROM help t) AS x, (SELECT id FROM host LIMIT %s) AS y, response, metric WHERE x.r <= %s AND y.id = host AND response.id = x.response AND metric.name in %s AND metric.describes = x.response ORDER BY x.host, x.response, response.executedby, metric.name;"

    responsePerHost = int(round((response/entity),0))

    cur.execute(statement, (entity, responsePerHost, tuple(metrics), ))
    dbBuffer = cur.fetchall()

    if dbBuffer == []:      
        logging.error("Got no data from database. Continue ...")
        return
    
    for dbEntry in dbBuffer:
        if metricVar == 0:
            metricVar = maximumMetric
            outputFormat = []        

        outputFormat.append(dbEntry)
        metricVar = metricVar - 1

        if len(outputFormat) == maximumMetric:
            singleEntry = helper.combine(outputFormat)
            testCase.append(singleEntry)

        runNumHost = helper.insertDistinct(runNumHost, dbEntry[0])
        runNumResp = helper.insertDistinct(runNumResp, dbEntry[2])


    if len(runNumResp) < response:
        while len(runNumResp) < response:
            logging.info("Not enough responses (%s) instead of %s", response, len(runNumResp))

            statementAdd = "SELECT help.host AS effected, response.executedby AS executer, help.response AS response, metric.name, metric.value FROM help, response, metric WHERE help.response = response.id AND response.id = metric.describes AND help.host in %s AND response.id not in %s order by help.response, help.host LIMIT %s;"
            cur.execute(statementAdd, (tuple(runNumHost), tuple(runNumResp), response - len(runNumResp)))
            dbBuffer = cur.fetchall()
            if len(dbBuffer) == 0:
                break
            metricVar = 0
            outputFormat = []
            for dbEntry in dbBuffer:
                if metricVar == 0:
                    metricVar = maximumMetric
                    outputFormat = []        

                outputFormat.append(dbEntry)
                metricVar = metricVar - 1

                if len(outputFormat) == maximumMetric:
                    singleEntry = helper.combine(outputFormat)
                    testCase.append(singleEntry)

                runNumHost = helper.insertDistinct(runNumHost, dbEntry[0])
                runNumResp = helper.insertDistinct(runNumResp, dbEntry[2])        


    elif len(runNumResp) == response:
        pass
    else:
        while len(runNumResp) > response:
            logging.info("to many responses (%s) instead of %s ", len(runNumResp), response)
            deleteNum = len(runNumResp)-response
            deleteList = []
            for i in range(0,deleteNum):          
                responseToDelete = runNumResp[(i*deleteNum) % len(runNumResp)]
                deleteList = helper.insertDistinct(deleteList, responseToDelete)

            for responseToDelete in deleteList:
                runNumResp.remove(responseToDelete)
                for elem in testCase:
                    if elem[2] == responseToDelete:
                        testCase.remove(elem)

    statementConflict = "select c1.r1, c1.r2 from (select distinct r1,r2 from conflict where r1 in %s and r2 in %s) c1 join (select distinct r1,r2 from conflict) c2 on c1.r2 = c2.r1 and c1.r1 = c2.r2 and c1.r1 < c2.r1 order by c1.r1 LIMIT %s;"
    cur.execute(statementConflict, (tuple(runNumResp), tuple(runNumResp), conflict))
    dbBuffer = cur.fetchall()
    conflictsList = []
    for dbEntry in dbBuffer:
        conflictsList.append((dbEntry[0], dbEntry[1]))

    if conflict > len(conflictsList):
        logging.warning("Not enough conflicts in dataset: %s instead of %s", len(conflictsList), conflict)
        conflict = len(conflictsList)

    data = createData(testCase, conflictsList, cur)

    if not bounds:
        data["damage"] = None

    insertMapper = createInsertMapperDatabase(expId, entity, response, conflict, len(metrics), cur, runNumResp, runNumHost)

    runID = insertMapper['id']

    if EXECUTION_SETTING.CreateOnly:
        loop = 1
    else:
        loop = EXECUTION_SETTING.Repetitions

    logging.info("Start Solver to execute %s with %s Repetitions", solverName, loop)

    try:
        path = EXECUTION_SETTING.DumpProblemPath[solverName]
    except:
        path = EXECUTION_SETTING.DumpProblemPath['default']
        path = "default/" + str(database) + "/" + solverName + "/"  + path

    if not os.path.exists(path):
        os.makedirs(path)

    try:
        solpath = EXECUTION_SETTING.DumpSolutionPath[solverName]
    except:
        solpath = EXECUTION_SETTING.DumpSolutionPath['default']
        solpath = "default/" + str(database) + "/" + solverName + "/"  + solpath

    if not os.path.exists(solpath):
        os.makedirs(solpath)

    for i in range (loop):

        logging.info("Solver %s executes in Round %s", solverName, i)
        logging.info("Start new Experiment with run ID %s", insertMapper['id'])

        p = Process(target=solverGeneric.startSolver, args=(data, EXECUTION_SETTING, solverConfig, solver, solverName, return_list, database, insertMapper['ref_id'], insertMapper['id'], path, solpath))       
        p.start()        
        p.join()        
        toInsert = return_list['erg']

        insertMapper['creation_time'] = toInsert[0]    
        insertMapper['start_creation_time'] = toInsert[1]
        insertMapper['end_creation_time'] = toInsert[2]
        insertMapper['calc_time'] = toInsert[3]
        insertMapper['start_calc_time'] = toInsert[4]
        insertMapper['end_calc_time'] = toInsert[5]

        insertMapper['costs'] = toInsert[7]
        insertMapper['selected_response'] = toInsert[6]

        if SAFE:
            insertValues(cur, insertMapper)

        runID = runID + 1
        insertMapper['id'] = runID

        logging.info("Solver %s evaluated %s to use with costs = %s ", solverName, toInsert[6], toInsert[7])

    del data    
    gc.collect() 
