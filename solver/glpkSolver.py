#  This file is part of CSR Optimizer.
# (C) 2013 Matthias Wachs (and other contributing authors)
#
# GNUnet is free software; you can redistribute it and/or modify3
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 3, or (at your
# option) any later version.
#
# GNUnet is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.3
#
# You should have received a copy of the GNU General Public License
# along with GNUnet; see the file COPYING.  If not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 02111-1307, USA.
#

#
# Author: Matthias Wachs (<lastname> [at] net.in.tum.de )
#

import abc
from solver.SolverBase import SolverBase

from classes.Host import Host
from classes.Metric import Metric
from classes.Response import Response

import logging
import glpk.glpkpi as glpki
import subprocess
import time

class Solver(SolverBase):

    def script(self, fileName, solverConfig, dumpSolution, solPath):

        timeListe = list()

        command = "glpsol --check --lp " + fileName
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

        if dumpSolution:
            parts = fileName.split("/")
            solName = parts[len(parts)-1]
            solCut = solName[9:]
            ext = solCut.split(".")[0]
            outFile = solPath + "/SCRIPT_" + ext + ".sol"
        else:
            outFile = "sol.tmp"

        command = "glpsol --lp " + fileName + " --output " + outFile

        logging.info("Start Problem SOLVING.")

        start = time.time()

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        process.wait()

        end = time.time()
    
        exec_time = end - start
    
        logging.info("Solved in: %s", exec_time)

        timeListe.append(exec_time)
        timeListe.append(start)
        timeListe.append(end)

        costs = None
        numberResponses = 0

        with open(outFile) as f:
            lines = f.readlines()
            for line in lines:
                if "Objective" in line:
                    costString = line.split("=")[1].strip().split(" ")[0]
                    costs = float(costString)
                if "C_n_" in line:
                    responseString = line.replace(" ", "")[-4]
                    if responseString == "1":
                        numberResponses = numberResponses + 1

        if not dumpSolution:
            command = "rm " + outFile
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
            process.wait()

        timeListe.append(numberResponses)
        timeListe.append(costs)
        
        return timeListe

    def dump_problem(self, problem, prefix):
        glpki.glp_write_lp (problem, None, prefix + ".lp")

    def dump_solution(self, problem, prefix):
        glpki.glp_print_mip (problem, prefix +".sol")

    def create_problem(self, data):

        hosts_attacked = data["attacked"]
        hosts_executing = data["executing"]
        metrics = data["metric"]
        damage = data["damage"]
        responses = data["response"]

        logging.info("CSR Optimizer creating problem...")
        glpki.glp_init_env ()
        problem = glpki.glp_create_prob()
        glpki.glp_set_prob_name (problem, "CSR")
        
        # Set optimization direction to minimize
        glpki.glp_set_obj_dir (problem, glpki.GLP_MIN);
        
        # Column C_m_i :Create a column for every property
        logging.debug("Adding %s columns for metrics", len (metrics))
        first = glpki.glp_add_cols (problem, len (metrics));
        for m in metrics:
            logging.debug("Adding column [%s] for metric '%s'", first, m.name)
            m.column = first        
            glpki.glp_set_col_name (problem, m.column, "C_m_"+ str(m.name));
            glpki.glp_set_col_bnds (problem, m.column, glpki.GLP_LO, 0, 0);
            glpki.glp_set_col_kind (problem, m.column, glpki.GLP_CV);
            glpki.glp_set_obj_coef (problem, m.column, 1.0);
            first += 1
            
        # Column C_m_i : Create a column for every response
        first = glpki.glp_add_cols (problem, len (responses));
        for r in responses:
            logging.debug("Adding column [%s] for response '%s'", first, r.name)
            r.column = first
            glpki.glp_set_col_name (problem, r.column, "C_n_"+ str(r.name));
            glpki.glp_set_col_bnds (problem, r.column, glpki.GLP_DB, 0, 1);
            glpki.glp_set_col_kind (problem, r.column, glpki.GLP_IV);
            glpki.glp_set_obj_coef (problem, r.column, 0);
            first += 1        
        # Row: C1) Every victim is freed: Create a constrain row for every attacked host
        logging.debug("Adding %s row for attacked hosts", len (hosts_attacked))
        first = glpki.glp_add_rows (problem, len (hosts_attacked));
        for a in hosts_attacked:        
            logging.debug("Adding constraint [%s] for attacked host '%s'", first, a.name)
            a.row_freed = first
            # set row name
            glpki.glp_set_row_name (problem, a.row_freed, a.name+"_freed");
            # set row bounds:
            glpki.glp_set_row_bnds (problem, a.row_freed, glpki.GLP_LO, 1, 0);        
            first += 1
        
        # Row: C2) Sum of costs is lower than damage: Create a constrain row for every property
        if (None != damage):
            logging.debug("Adding damage row %s row for metrics", len (metrics))
            first = glpki.glp_add_rows (problem, len (metrics));
            for m in metrics:
                logging.debug("Adding damage constraint [%s] for metric '%s'", first, m.name)
                m.row_damage = first
                # set row name
                glpki.glp_set_row_name (problem, m.row_damage, m.name+"_damage");
                # set row bounds:
                cur_damage = -1
                for d in damage:
                    if (d.name == m.name):
                        cur_damage = d.value
                assert (-1 != cur_damage)
                glpki.glp_set_row_bnds (problem, m.row_damage, glpki.GLP_UP, 0, cur_damage);        
                first += 1
        
        # Row C3) Cost of each metric: Create a row for every property
        logging.debug("Adding %s row for metrics", len (metrics))
        first = glpki.glp_add_rows (problem, len (metrics));
        for m in metrics:
            logging.debug("Adding constraint [%s] for metric '%s'", first, m.name)
            m.row_cost = first
            # set row name
            glpki.glp_set_row_name (problem, m.row_cost, m.name+"_cost");
            # set row bounds:
            glpki.glp_set_row_bnds (problem, m.row_cost, glpki.GLP_FX, 0, 0);        
            first += 1

        # Set problem matrix content
        c_conflicts = 0
        for r in responses:        
            for c in r.conflicting_responses:
                if (c == ""):
                    continue            
                c_conflicts += 1
        c_freed = 0          
        for a in hosts_attacked:
            for r in responses:            
                if (a.name == r.dest or r.dest.count(a.name) != 0):
                    c_freed += 1  
        array_size = 1 + c_freed +  2 * len (metrics) * len (responses) + len (metrics) + 2 * c_conflicts 
        ia = glpki.intArray(array_size)
        ja = glpki.intArray(array_size)
        ar = glpki.doubleArray(array_size)
        index = 1
        count = 0

        # Conflicting responses
        ConflictingContraints = 0
        for r in responses:        
            for c in r.conflicting_responses:
                if (c == ""):
                    continue            
                r2 = None
                for r_tmp in responses:                
                    if (c == r_tmp.name):
                        r2 = r_tmp
                
                if (None == r2) or (r.name == r2.name):
                    continue
                # Add a constraint for every conflict
                ConflictingContraints = ConflictingContraints + 1
                logging.debug("Adding constraint [%s] for conflict '%s' <-> '%s'", first, r.name, c)
                first = glpki.glp_add_rows (problem, 1);
                glpki.glp_set_row_name (problem, first, "conflict_"+r.name+"-"+r2.name);
                glpki.glp_set_row_bnds (problem, first, glpki.GLP_DB, 0, 1);
                
                ia[index] = first
                ja[index] = r.column           
                ar[index] = 1
                logging.debug("%s: [%s][%s] = %s", count, ia[index], ja[index], ar[index])
                index += 1
                count +=1
                ia[index] = first
                ja[index] = r2.column           
                ar[index] = 1
                logging.debug("%s: [%s][%s] = %s", count, ia[index], ja[index], ar[index])
                index += 1
                count +=1      
        
        # Constraint 1) Every attacked not has to be freed
        for a in hosts_attacked:
            logging.debug("1) Free %s" , a.name)
            for r in responses:			
                if (a.name == r.dest or r.dest.count(a.name) != 0):               
                    ia[index] = a.row_freed
                    ja[index] = r.column
                    ar[index] = 1.0
                    logging.debug("%s: [%s][%s] = %s", count, ia[index], ja[index], ar[index])
                    index += 1
                    count +=1           
        # Constraint 2) damage
        if (None != damage):
            for m in metrics:
                logging.debug("2) Damage for metric %s" , m.name)       
                for r in responses:                
                    ia[index] = m.row_damage
                    ja[index] = r.column         
                    ar[index] = r.get_cost (m.name)
                    logging.debug("%s: [%s][%s] = %s", count, ia[index], ja[index], ar[index])
                    index += 1
                    count +=1        
                       
        # Constraint 3) cost
        for m in metrics:
            logging.debug("3) Cost for metric %s" , m.name)
            ia[index] = m.row_cost
            ja[index] = m.column            
            ar[index] = 1
            logging.debug("H: %s [%s][%s] = %s", count, ia[index], ja[index], ar[index])
            index += 1
            count +=1 
            for r in responses:      
                ia[index] = m.row_cost
                ja[index] = r.column        
                ar[index] = -r.get_cost (m.name)
                logging.debug("%s: [%s][%s] = %s", count, ia[index], ja[index], ar[index])
                index += 1
                count +=1
                          
        glpki.glp_load_matrix (problem, count, ia, ja, ar);

        return problem
        
    def solve_problem (self, problem, solverConfig):
            
        if (solverConfig["simplex"] in ["True", "true", "1", "TRUE"]):
            lp_solver_str = "using simplex" 
            simplex = True
        else:
            lp_solver_str = "using interior point"
            simplex = False
        
        if (solverConfig["presolve"] in ["True", "true", "1", "TRUE"]):
            presolv_str = "with ILP presolver"
            lp_solver_str = ""
            presolve = True
        else:
            presolv_str = "no ILP presolver"
            presolve = False
            
        logging.info("CSR Optimizer solving problem using %s %s", presolv_str , lp_solver_str)

        if (False == presolve):
            if (True == simplex):    
                logging.info("CSR Optimizer solving problem using simplex...")
                glpk_lp_param = glpki.glp_smcp()
                glpki.glp_init_smcp(glpk_lp_param)
                glpk_lp_param.msg_lev = glpki.GLP_MSG_OFF
                glpki.glp_simplex (problem, glpk_lp_param)
                res = glpki.glp_get_status (problem)
            else:
                logging.info("CSR Optimizer solving problem using interior point...")
                glpk_lp_param = glpki.glp_iptcp()
                glpki.glp_init_iptcp(glpk_lp_param)
                glpk_lp_param.msg_lev = glpki.GLP_MSG_OFF     
                glpki.glp_interior(problem, glpk_lp_param)
                res = glpki.glp_ipt_status(problem) 

            if (res == glpki.GLP_OPT):
                logging.info("Linear optimal")
            else:
                logging.warning("Problem is linear unfeasible")
                return problem
       
        glpk_ilp_param = glpki.glp_iocp()    
        glpki.glp_init_iocp(glpk_ilp_param)

        if (True == presolve):
            glpk_ilp_param.presolve = glpki.GLP_ON
        else:
            glpk_ilp_param.presolve = glpki.GLP_OFF

        glpk_ilp_param.msg_lev = glpki.GLP_MSG_OFF 
        glpki.glp_intopt (problem, glpk_ilp_param)
        res = glpki.glp_mip_status(problem)

        if (res == glpki.GLP_OPT):
            logging.info("Integer optimal")
        else:
            logging.warning("Problem is integer unfeasible")
            return problem
        return problem

    def delete_problem (self, problem):
        glpki.glp_delete_prob(problem)


    def evaluate_problem(self, data, problem):

        responses = data["response"]
        metrics = data["metric"]

        counter = 0 
        obj_val = glpki.glp_mip_obj_val (problem);
        for r in responses:
            if (1.0 == (glpki.glp_mip_col_val (problem, r.column))):
                counter = counter + 1

        return (counter, obj_val)   
