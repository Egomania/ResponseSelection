import numpy as np
import sys
import gc
import logging
import subprocess
import time
import os
import xmltodict

import abc
from solver.SolverBase import SolverBase

from pycpx import CPlexModel

from classes.Host import *
from classes.Metric import *
from classes.Response import *

class Solver(SolverBase):

    def script(self, fileName, solverConfig, dumpSolution, solPath):

        timeListe = list()

        try:
            os.remove("tmp.cmd")
        except OSError:
            pass
        with open("tmp.cmd","w+") as f:
            f.write("read " + fileName + "\n")

        command = "cat tmp.cmd | cplex"
        logging.info("Start Problem CREATION.")

        start = time.time()

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        process.wait()

        end = time.time()

        exec_time = end - start
    
        logging.info("Created in: %s", exec_time)

        timeListe.append(exec_time)
        timeListe.append(start)
        timeListe.append(end)

        try:
            os.remove("tmp.cmd")
        except OSError:
            pass
        with open("tmp.cmd","w+") as f:
            f.write("read " + fileName + "\n") 
            f.write("opt" + "\n")

            if dumpSolution:
                parts = fileName.split("/")
                solName = parts[len(parts)-1]
                solCut = solName[9:]
                ext = solCut.split(".")[0]
                outFile = solPath + "/SCRIPT_" + ext + ".sol"
            else:
                outFile = "tmp.sol"
    
            f.write("write " + outFile + "\n")
            f.write("quit" + "\n")
        

        logging.info("Start Problem SOLVING.")
        command = "cat tmp.cmd | cplex"

        start = time.time()

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        process.wait()

        end = time.time()
    
        exec_time_second = end - start

        computingTime = exec_time_second - exec_time
        if computingTime < 0:
            computingTime = 0
    
        logging.info("Solved in: %s", computingTime)

        timeListe.append(computingTime)
        timeListe.append(start)
        timeListe.append(end)

        try:
            os.remove("tmp.cmd")
        except OSError:
            pass

        costs = None
        numberResponses = 0

        with open(outFile) as f:
            doc = xmltodict.parse(f.read())
            costs = float( doc['CPLEXSolution']['header']['@objectiveValue'])
            
            for var in doc['CPLEXSolution']['variables']['variable']:
                if var['@value'] == str(1):
                    numberResponses = numberResponses + 1

        if not dumpSolution:
            try:
                os.remove(outFile)
            except OSError:
                pass

        timeListe.append(numberResponses)
        timeListe.append(costs)
        
        return timeListe


    def dump_problem(self, problem, prefix):
        m = problem[0]
        text = m.asString()

        with open(prefix + ".lp", "w") as text_file:
            text_file.write("{0}".format(text))

    def dump_solution(self, problem, prefix):
        text = str(problem[1]) + "\n" + str(problem[2])

        with open(prefix + ".sol", "w") as text_file:
            text_file.write("{0}".format(text))        

    def delete_problem(self, problem):
        del problem[0]

    def create_problem(self, data): 

        host_attacked = data["attacked"]
        host_executing = data["executing"]
        metrics_used = data["metric"]
        damage_used = data["damage"]
        responses_used = data["response"]

        numResp = len(responses_used)
        damageMapper = {}
        costMapper = {}
        conflictMapper = []
        responseList = []

        for response in responses_used:
            responseList.append(response.name)

        logging.info('Responses Used: %s', numResp)

        if damage_used is not None:
            for elem in damage_used:
                damageMapper[elem.name] = elem.value

        for elem in metrics_used:
            costMapper[elem.name] = []
        
        costs = []
        for response in responses_used:
            cost = 0
            for r in response.metrics:
                cost = cost + r.value
                costMapper[r.name].append(cost)
            costs.append(cost)
            if response.conflicting_responses:
                for r in response.conflicting_responses:
                    tmp = [0] * numResp
                    tmp[responseList.index(response.name)] = 1
                    tmp[responseList.index(r)] = 1
                    conflictMapper.append(tmp)
        costs = np.array(costs)
        hostMatrix = []
        for host in host_attacked:
            hostRow = []
            host = host.name
            for response in responses_used:
                if host in response.dest:
                    hostRow.append(1)
                else:
                    hostRow.append(0)
            hostMatrix.append(hostRow)    

        m = CPlexModel(verbosity = 0)
        
        # Each Response can only be executed once
        x = m.new(numResp,vtype='bool', name='x')


        i = 0
        # all attacked hosts are freed
        for elem in hostMatrix:
            elemArr = np.array(elem)
            m.constrain(sum(x.mult(elemArr)) >= 1)
            i = i + 1

        logging.info('Freed Constraints: %s', i)

        i = 0
        # all single metrics used have to be below damage
        for key, elem in damageMapper.iteritems():
            elemArr = np.array(costMapper[key])
            m.constrain(sum(x.mult(elemArr)) <= elem)
            i = i + 1

        logging.info('Damage Constraints: %s', i)

        i = 0
        # no conflicting actions are executed
        for elem in conflictMapper:
            elemArr = np.array(elem)
            m.constrain(sum(x.mult(elemArr)) <= 1)
            i = i + 1
        
        logging.info('Conflicting Constraints: %s', i)

        return [m, x, costs]

    def solve_problem(self, problem, config):
        m = problem[0]
        x = problem[1]
        costs = problem[2]
        ret = []
        cost = 0
        try:
            cost = m.minimize(sum(x.mult(costs)))
            ret = m[x]
        except (CPlexNoSolution) as e:
            logging.warning("Problem integer unfeasible: %s", e)
        finally:
            return (m, ret, cost)

    def evaluate_problem(self, data, erg):
        cost = erg[2]
        counter = 0
        for elem in erg[1]:
            if elem > 1:
                counter = counter + 1

        return(counter, cost)
