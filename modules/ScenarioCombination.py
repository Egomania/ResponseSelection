'''
Created on Jun 11, 2015

@author: ansii
'''

import sys
import time
import datetime
import logging
import imp

import helper.db_helper as db
import helper.helper_functions as helper
import helper.init_helper as init

import modules.ScenarioGeneration as SG

def prepare_bounds(SCENARIO_SETTING, cur):

    ret = {}

    x_bound = SCENARIO_SETTING.HOST_UPPER_BOUND
    y_bound = SCENARIO_SETTING.RESPONSE_UPPER_BOUND
    z_bound = SCENARIO_SETTING.CONFLICT_UPPER_BOUND

    MAX = db.maxEntity(cur)
    if x_bound > MAX:
        logging.warning("Entities to use exceeded number Entities available. Using maximum Number available instead (%s).", MAX)
        x_bound = MAX
    MAX = db.maxResponse(cur)
    if y_bound > MAX:
        logging.warning("Responses to use exceeded number Responses available. Using maximum Number available instead (%s).", MAX)
        y_bound = MAX
    MAX = db.maxConflicts(cur)
    if z_bound > MAX:
        logging.warning("Conflicts to use exceeded number Conflicts available. Using maximum Number available instead (%s).", MAX)
        z_bound = MAX

    x = SCENARIO_SETTING.HOST_START
    y = SCENARIO_SETTING.RESPONSE_START
    z = SCENARIO_SETTING.CONFLICT_START

    if x > x_bound:
        x = x_bound
        logging.warning("Start value exceeded bound (Entity). Using Bound instead (%s).", x)
    if y > y_bound:
        y = y_bound
        logging.warning("Start value exceeded bound (Response). Using Bound instead (%s).", y)
    if z > z_bound:
        z = z_bound
        logging.warning("Start value exceeded bound (Conflict). Using Bound instead (%s).", z)

    x_step = SCENARIO_SETTING.HOST_STEPS
    y_step = SCENARIO_SETTING.RESPONSE_STEPS
    z_step = SCENARIO_SETTING.CONFLICT_STEPS
        
    logging.info("X (Entity) Settings: Start with %s up to %s with step size %s.", x, x_bound, x_step)
    logging.info("Y (Response) Settings: Start with %s up to %s with step size %s.", y, y_bound, y_step)
    logging.info("Z (Conflict) Settings: Start with %s up to %s with step size %s.", z, z_bound, z_step)

    ret["x"] = x
    ret["x_bound"] = x_bound
    ret["x_step"] = x_step

    ret["y"] = y
    ret["y_bound"] = y_bound
    ret["y_step"] = y_step

    ret["z"] = z
    ret["z_bound"] = z_bound
    ret["z_step"] = z_step
    
    return ret

def createScript(SCENARIO_SETTING, DB_SETTING, EXECUTION_SETTING, COMBI, solvers, CONFIGS):
    
    newSettings = {}

    for entry in COMBI:

        exp_time_start = time.time()
        database = entry[0]
        solver = entry[1]

        conn = db.connect(DB_SETTING, database)
        cur = conn.cursor()

        expId = db.experimentID(cur)
        ts = datetime.date.today()
        logging.info("Create new Experiment Setting with id %s in database %s for solver %s (%s)", expId, database, solver, ts)

        if SCENARIO_SETTING.SAFE:
            cur.execute("INSERT INTO experiment_setting VALUES (%s,%s,%s,%s,%s)", (expId, solver, ts, SCENARIO_SETTING.BOUNDS, "SCRIPT_"+str(CONFIGS[solver]), ))
            entry = str(database)+"_"+str(solver)
            newSettings[entry] = expId

        SG.generateFromScript(expId, cur, database, solver, solvers[solver], CONFIGS[solver], EXECUTION_SETTING.UseDumpFilesPath, SCENARIO_SETTING.SAFE, EXECUTION_SETTING.Repetitions, DB_SETTING, EXECUTION_SETTING.DumpSolution, EXECUTION_SETTING.DumpSolutionPath)

        conn.commit()

        cur.close()
        conn.close

        exp_time_stop = time.time()
        duration = exp_time_stop - exp_time_start
        logging.info("Duration for one setting: %s ", duration)

    return newSettings

def createDatabase(SCENARIO_SETTING, DB_SETTING, EXECUTION_SETTING, COMBI, solvers, CONFIGS):

    newSettings = {}

    for entry in COMBI:

        exp_time_start = time.time()
        database = entry[0]
        solver = entry[1]

        conn = db.connect(DB_SETTING, database)
        cur = conn.cursor()

        expId = db.experimentID(cur)
        ts = datetime.date.today()
        logging.info("Create new Experiment Setting with id %s in database %s for solver %s (%s)", expId, database, solver, ts)

        if SCENARIO_SETTING.SAFE:
            cur.execute("INSERT INTO experiment_setting VALUES (%s,%s,%s,%s,%s)", (expId, solver, ts, SCENARIO_SETTING.BOUNDS, "DATABASE_"+str(CONFIGS[solver]), ))
            entry = str(database)+"_"+str(solver)
            if not EXECUTION_SETTING.CreateOnly:
                newSettings[entry] = expId

        conn.commit()

        eb = prepare_bounds(SCENARIO_SETTING, cur)

        metrics = SCENARIO_SETTING.METRICS
        if len(metrics) == 0:
            metrics = db.allMetrics(cur)
            logging.warning("No Metrics to used are specified. All Metrics available are used: %s", metrics)
        else:
            logging.info("Use metrics: %s", metrics)

        for entity in range(eb["x"],eb["x_bound"]+1,eb["x_step"]):
            for response in range(eb["y"],eb["y_bound"]+1,eb["y_step"]):
                for conflict in range(eb["z"],eb["z_bound"]+1,eb["z_step"]):
                    logging.info("Start Experiment with %s entities, %s reponses and %s conflicts", entity, response, conflict)
                    SG.generateFromDatabase(expId, cur, entity, response, conflict, metrics, EXECUTION_SETTING, CONFIGS[solver], solvers[solver], solver, SCENARIO_SETTING.BOUNDS, SCENARIO_SETTING.SAFE, database)
                    conn.commit()

        cur.close()
        conn.close

        exp_time_stop = time.time()
        duration = exp_time_stop - exp_time_start
        logging.info("Duration for one setting: %s ", duration)

    return newSettings


def create(SCENARIO_SETTING, DB_SETTING, EXECUTION_SETTING):

    DB_NAMES = SCENARIO_SETTING.DB_NAMES
    SOLVER = SCENARIO_SETTING.SOLVER

    newSettings = {}
    newScriptSettings = {}
    newDBSettings = {}

    CONFIGS = {}

    for solvers in SOLVER:
        try:
            CONFIGS[solvers] = init.getSettings(SCENARIO_SETTING.SolverConfigs[solvers])
        except:
            CONFIGS[solvers] = {}

    COMBI = [ (database, solver) for database in DB_NAMES for solver in SOLVER]

    solvers = helper.loadModules(SOLVER, "solver/")

    time_start = time.time()

    if EXECUTION_SETTING.UseDatabase:
        logging.info("Start Creating Settings based on Database.")
        newDBSettings = createDatabase(SCENARIO_SETTING, DB_SETTING, EXECUTION_SETTING, COMBI, solvers, CONFIGS)

    if EXECUTION_SETTING.UseDumpFiles:
        logging.info("Start Creating Settings based on Dump Files.")
        newScriptSettings = createScript(SCENARIO_SETTING, DB_SETTING, EXECUTION_SETTING, COMBI, solvers, CONFIGS)

    for key, value in newDBSettings.iteritems():
        newSettings[key] = value
    for key, value in newScriptSettings.iteritems():
        newSettings[key] = value
    
    time_stop = time.time()
    duration = time_stop - time_start
    logging.info("Duration for the whole setting: %s ", duration)

    return newSettings
    
        


