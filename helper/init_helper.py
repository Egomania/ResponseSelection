import logging
import sys
import configparser

import classes.DatabaseSettings as DB
import classes.DataSettings as Data
import classes.ScenarioSettings as Scenario
import classes.ExecutionSettings as Execution
import classes.PrinterSettings as Printer

def getSettings(path):

    ret = {}

    Config = configparser.ConfigParser()
    config = Config.read(path)

    for section in Config:
        for option in Config[section]:
             ret[option] = Config[section][option]

    return ret

def checkValues(Config, section, attribute, defaultValue, string2Print, mult):
    try:
        if Config[section][attribute]:
            if mult:
                ret = [elem.strip() for elem in Config[section][attribute].split(',')]
            else:
                ret = Config[section][attribute]
            logging.info("%s is set to: %s", string2Print, ret)
        else:
            ret = defaultValue
            logging.warning("%s is set to: %s (default)", string2Print, ret)
    except:
        ret = defaultValue
        logging.warning("%s is set to: %s (default)", string2Print, ret) 

    return ret  

def checkValuesBool(Config, section, attribute, defaultValue, string2Print):

    try:
        if Config[section][attribute]:
            if Config[section][attribute] in ['true', 'True', 1, 'TRUE']:
                ret = True
                logging.info("%s is set to: %s", string2Print, ret)
            elif Config[section][attribute] in ['false', 'False', 0, 'FALSE']:
                ret = False
                logging.info("%s is set to: %s", string2Print, ret)
            else:
                ret = defaultValue
                logging.warning("%s is set to: %s (default)", string2Print, ret)
        else:
            ret = defaultValue
            logging.warning("%s is set to: %s (default)", string2Print, ret)
    except:
        ret = defaultValue
        logging.warning("%s is set to: %s (default)", string2Print, ret)

    return ret

def init_printer(Config):

    ret = Printer.PrinterSettings()

    ret.StorePrintInformation = checkValuesBool(Config, 'PrinterSettings', 'StorePrintInformation', True, "Store Print Information")
    ret.PrintInformationPath = str(checkValues(Config, 'PrinterSettings', 'PrintInformationPath', "default", "Print Information Path", False))
    ret.FilePath = str(checkValues(Config, 'PrinterSettings', 'FilePath', "pics", "File Path", False))
    ret.ResultsFile = str(checkValues(Config, 'PrinterSettings', 'ResultsFile', "results.csv", "Results File", False))

    ret.Label = {}
    TMP = checkValues(Config, 'PrinterSettings', 'Label', [], "Labels", True)
    for elem in TMP:
        solver = elem.split(':')[0]
        label  = elem.split(':')[1]
        ret.Label[solver] = label 
    
    ret.PrintFromFile = checkValuesBool(Config, 'PrinterSettings', 'PrintFromFile', False, "Print From File")
    ret.PrintFilePath = str(checkValues(Config, 'PrinterSettings', 'PrintFilePath', "default", "Print File Path", False))
    ret.Axis = str(checkValues(Config, 'PrinterSettings', 'Axis', "host", "Axis", False))    

    ret.Color = []
    TMP = checkValues(Config, 'PrinterSettings', 'Color', [], "Color", True)
    for elem in TMP:
        ret.Color.append(str(elem))

    ret.Marker = []
    TMP = checkValues(Config, 'PrinterSettings', 'Marker', [], "Marker", True)
    for elem in TMP:
        ret.Marker.append(str(elem))

    ret.Linestyle = []
    TMP = checkValues(Config, 'PrinterSettings', 'Linestyle', [], "Linestyle", True)
    for elem in TMP:
        ret.Linestyle.append(str(elem))

    return ret


def init_execution(Config):

    ret = Execution.ExecutionSettings()

    ret.DumpSolution = checkValuesBool(Config, 'ExecutionSettings', 'DumpSolution', True, "Dump Solution")
    ret.DumpProblem = checkValuesBool(Config, 'ExecutionSettings', 'DumpProblem', True, "Dump Problem")
    ret.UseDatabase = checkValuesBool(Config, 'ExecutionSettings', 'UseDatabase', True, "Use Database")
    ret.UseDumpFiles = checkValuesBool(Config, 'ExecutionSettings', 'UseDumpFiles', True, "Use DumpFiles")
    ret.CreateOnly = checkValuesBool(Config, 'ExecutionSettings', 'CreateOnly', True, "Create Only")

    ret.Repetitions = int(checkValues(Config, 'ExecutionSettings', 'Repetitions', 1, "Repetitions", False))

    TMP = checkValues(Config, 'ExecutionSettings', 'DumpSolutionPath', [], "Dump-Solution-Path", True)
    ret.DumpSolutionPath = {}
    ret.DumpSolutionPath['default'] = "solution_default"
    for elem in TMP:
        solver = elem.split(':')[0]
        path = elem.split(':')[1]
        ret.DumpSolutionPath[solver] = path 
    TMP = checkValues(Config, 'ExecutionSettings', 'DumpProblemPath', [], "Dump-Problem-Path", True)
    ret.DumpProblemPath = {}
    ret.DumpProblemPath['default'] = "problem_default"
    for elem in TMP:
        solver = elem.split(':')[0]
        path = elem.split(':')[1]
        ret.DumpProblemPath[solver] = path
    TMP = checkValues(Config, 'ExecutionSettings', 'UseDumpFilesPath', [], "Use Dump-FilesPath", True)
    ret.UseDumpFilesPath = {}
    ret.UseDumpFilesPath['default'] = "problem_default"
    for elem in TMP:
        solver = elem.split(':')[0]
        path = elem.split(':')[1]
        ret.UseDumpFilesPath[solver] = path

    if ret.DumpProblem and not ret.UseDatabase:
        logging.warning("Misleading Settings. DumpProblem set to %s and UseDatabase set to %s. Use default for UseDatabase = TRUE", ret.DumpProblem, ret.UseDatabase)
        ret.UseDatabase = True

    return ret

def init_scenario(Config, dbs):
    
    ret = Scenario.ScenarioSettings(dbs)

    ret.SAFE = checkValuesBool(Config, 'ScenarioSettings', 'SAFE', False, "SAFE to database")
    ret.FAST_SAFE = checkValuesBool(Config, 'ScenarioSettings', 'FAST_SAFE', True, "FAST_SAFE to database")
    ret.SOLVER = checkValues(Config, 'ScenarioSettings', 'SOLVER', [], "Solvers to use", True)
    ret.HOST_UPPER_BOUND = int(checkValues(Config, 'ScenarioSettings', 'HOST_UPPER_BOUND', 500, "HOST_UPPER_BOUND", False))
    ret.HOST_START = int(checkValues(Config, 'ScenarioSettings', 'HOST_START', 500, "HOST_START", False))
    ret.HOST_STEPS = int(checkValues(Config, 'ScenarioSettings', 'HOST_STEPS', 500, "HOST_STEPS", False))
    ret.RESPONSE_UPPER_BOUND = int(checkValues(Config, 'ScenarioSettings', 'RESPONSE_UPPER_BOUND', 100, "RESPONSE_UPPER_BOUND", False))
    ret.RESPONSE_START = int(checkValues(Config, 'ScenarioSettings', 'RESPONSE_START', 100, "RESPONSE_START", False))
    ret.RESPONSE_STEPS = int(checkValues(Config, 'ScenarioSettings', 'RESPONSE_STEPS', 100, "RESPONSE_STEPS", False))
    ret.CONFLICT_UPPER_BOUND = int(checkValues(Config, 'ScenarioSettings', 'CONFLICT_UPPER_BOUND', 100, "CONFLICT_UPPER_BOUND", False))
    ret.CONFLICT_START = int(checkValues(Config, 'ScenarioSettings', 'CONFLICT_START', 100, "CONFLICT_START", False))
    ret.CONFLICT_STEPS = int(checkValues(Config, 'ScenarioSettings', 'CONFLICT_STEPS', 100, "CONFLICT_STEPS", False))
    #ret.RESPONSE_FIX = int(checkValues(Config, 'ScenarioSettings', 'RESPONSE_FIX', 5000, "RESPONSE_FIX", False))	
    #ret.CONFLICT_LIMIT_PER_RESPONSE = int(checkValues(Config, 'ScenarioSettings', 'CONFLICT_LIMIT_PER_RESPONSE', 0, "CONFLICT_LIMIT_PER_RESPONSE", False))	
    #ret.CONFLICT_LIMIT_PART = float(checkValues(Config, 'ScenarioSettings', 'CONFLICT_LIMIT_PART', 0.0, "CONFLICT_LIMIT_PART", False))
    metric_TMP = checkValues(Config, 'ScenarioSettings', 'Metrics', [], "Metrics", True)
    ret.METRICS = []
    for elem in metric_TMP:
        ret.METRICS.append(str(elem))
    ret.BOUNDS = checkValuesBool(Config, 'ScenarioSettings', 'BOUNDS', True, "BOUNDS")

    solverConfig = checkValues(Config, 'ScenarioSettings', 'SolverConfigs', [], "SolverConfigs", True)
    ret.SolverConfigs = {}
    for elem in solverConfig:
        solver = elem.split(':')[0]
        configPath = elem.split(':')[1]
        ret.SolverConfigs[solver] = configPath

    return ret

def init_data(Config):
    
    ret = Data.DataSettings()

    ret.NEW = checkValuesBool(Config, 'DataSettings', 'DataNew', False, "New Values added to database")
    ret.newRandomEntity = int(checkValues(Config, 'DataSettings', 'NewRandomEntity', 0, 'New Ranom Entities', False))
    ret.newEffectedEntity = int(checkValues(Config, 'DataSettings', 'NewEffectedEntity', 0, 'New Effected Entities', False))
    ret.newUneffectedEntity = int(checkValues(Config, 'DataSettings', 'NewUneffectedEntity', 0, 'New Uneffected Entities', False))
    ret.newResponsesSelfHealing = int(checkValues(Config, 'DataSettings', 'NewResponsesSelfHealing', 0, 'New Responses for Self-Healing', False))
    listResponse = checkValues(Config, 'DataSettings', 'ResponseUneffected', [0], 'New Responses for uneffected Entities (List)', True)
    ret.responseUneffected = []
    for elem in listResponse:
        ret.responseUneffected.append(int(elem))
    ret.responseRandom = int(checkValues(Config, 'DataSettings', 'ResponseRandom', 0, 'New Random Responses', False))
    ret.helpingFactor = int(checkValues(Config, 'DataSettings', 'helpingFactor', 20, 'Maximum Helping/Coverage Factor for Responses', False))
    ret.conflicts = int(checkValues(Config, 'DataSettings', 'Conflicts', 0, 'Number of Conflicts between Responses', False))
    
    metrics = checkValues(Config, 'DataSettings', 'Metrics', ['cost:0'], 'Number of Metrics between Responses', True)   

    ret.metrics = {}

    for metric in metrics:
        name = metric.split(':')[0]
        value = metric.split(':')[1]
        weight = metric.split(':')[2]
        ret.metrics[str(name)] = (float(value), float(weight))

    return ret

def init_database(Config):

    ret = DB.DatabaseSettings()

    try:
        if Config['DatabaseSettings']['DBNames']:
            ret.DB_NAMES = [elem.strip() for elem in Config['DatabaseSettings']['DBNames'].split(',')]
            logging.info("The following databases are going to be used: '%s'.", ret.DB_NAMES)
        else:
            logging.critical("Provide a database name to connect to (Attribute is empty.). Use Section DatabaseSettings and Attribute DBNames. Provide more than one database using a , .")
            sys.exit(0)
    except:
        logging.critical("Provide a database name to connect to (Attribute does not exist). Use Section DatabaseSettings and Attribute DBNames. Provide more than one database using a , .")
        sys.exit(0)

    ret.user = checkValues(Config, 'DatabaseSettings', 'user', 'postgres', 'Database user', False)
    ret.pwd = checkValues(Config, 'DatabaseSettings', 'password', 'postgres', 'Database password', False)
    ret.host = checkValues(Config, 'DatabaseSettings', 'host', '127.0.0.1', 'Database location', False)
    ret.port = checkValues(Config, 'DatabaseSettings', 'port', '5432', 'Database port', False)

    return ret
