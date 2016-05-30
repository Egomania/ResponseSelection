'''
Created on Dec 4, 2013

@author: ansii
'''
import psycopg2
import sys
import datetime
import logging
import gc
import os

import time

import helper.helper_functions as helper

try:
    from csr_opt.Optimizer import *
    from csr_opt.Metric import Metric
    from csr_opt.Host import Host
    from csr_opt.Response import Response
except ImportError as e: 
    print "Could not import internal structure '" + str(e) + "'"


def scriptSolver(solver, solverConfig, elem, dumpSolution, solPath):

    currentSolver = solver.Solver()

    newList = list()

    newList = currentSolver.script(elem, solverConfig, dumpSolution, solPath)

    return newList

def startSolver(data, EXECUTION_SETTING, solverConfig, solver, solverName, return_list, database, settingID, experimentID, path, solpath):

    currentSolver = solver.Solver()

    newList = list()

    start = time.time()

    logging.info("Start Problem CREATION with %s.", solverName)

    problem = currentSolver.create_problem(data)

    end = time.time()
    
    exec_time = end - start
    
    logging.info("Created in: %s", exec_time)

    newList.append(exec_time)
    newList.append(start)
    newList.append(end)

    fileName = "DATABASE_" + str(database) + "_SETTING_" + str(settingID) + "_EXPERIMENT_" + str(experimentID)

    if EXECUTION_SETTING.CreateOnly:
        logging.info("Skipping SOLVING ... ")
        newList.append(None)
        newList.append(None)
        newList.append(None)
        newList.append(None)
        newList.append(None)

    else:
        start = time.time()
        logging.info("Start Problem SOLVING with %s.", solverName)
        erg = currentSolver.solve_problem(problem, solverConfig)
        problem = erg
        end = time.time()
        exec_time = end - start
    
        logging.info('Solved in: %s', exec_time)

        newList.append(exec_time)
        newList.append(start)
        newList.append(end)
    
        erg_liste = None
        erg_liste = currentSolver.evaluate_problem(data, erg)
        newList.append(erg_liste[0])
        newList.append(erg_liste[1])

        if EXECUTION_SETTING.DumpSolution:
            currentSolver.dump_solution(problem, str(solpath + "/" + fileName))

    if EXECUTION_SETTING.DumpProblem:
        currentSolver.dump_problem(problem, str(path + "/" + fileName))



    return_list['erg'] = newList


