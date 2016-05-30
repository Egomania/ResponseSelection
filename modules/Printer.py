'''
Created on Jun 11, 2015

@author: ansii
'''

import psycopg2
import numpy as np
import sys
import math
import ast
import os
import csv
import logging

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import helper.db_helper as db


color = ['orange', 'blue', 'green', 'red', 'magenta', 'yellow', '#75b6df', 'black']
m = ['s','v', 'D','o', '.', 'x','<','>']
s = ['-', '--', ':', '-.']

yAxis = {
0 : 'Time in s',
1 : 'Time in s',
3 : 'Time in s',
2 : 'Costs'
}

x_label = {
'host' : 'Number of Entities',
'response' : 'Number of Responses',
'conflict' : 'Number of Conflicts',
'coverage' : 'Average Coverage Factor',
}

criteria = {
0 : "creation",
1 : "calculation",
2 : "cost",
3 : "execution"
}

def getLabel(Labels, value):
    try:
        label = Labels['value']
    except:
        label = value
    return value

def getPath(pathWithFile):
    path = ""
    pos = pathWithFile.rfind("/")
    path = pathWithFile[:pos]
    return path


def printing(DB_SETTING,PRINTER_SETTING,newSettings):

    path = getPath(PRINTER_SETTING.PrintInformationPath)
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            logging.warning("Failed to create path/file %s.", PRINTER_SETTING.PrintInformationPath)
    path = getPath(PRINTER_SETTING.ResultsFile)
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            logging.warning("Failed to create path/file %s.", PRINTER_SETTING.ResultsFile)
    path = PRINTER_SETTING.FilePath
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            logging.warning("Failed to create path/file %s.", PRINTER_SETTING.FilePath)

    if PRINTER_SETTING.StorePrintInformation:
        logging.info("Store print Info to: %s", PRINTER_SETTING.PrintInformationPath)
        with open(PRINTER_SETTING.PrintInformationPath, "w+") as text_file:
            text_file.write("{0}".format(newSettings))

    if PRINTER_SETTING.PrintFromFile:
        try:
            logging.info("Use Values for printing Info from: %s", PRINTER_SETTING.PrintFilePath)
            with open(PRINTER_SETTING.PrintFilePath) as f:
                lines = f.readlines()
                StoredSettings = ast.literal_eval(''.join(lines))
            for key, value in StoredSettings.iteritems():
                newSettings[key] = value
        except:
            logging.warning("Given Printer file does not exist.")


    dataRowRaw = []

    for key, value in newSettings.iteritems():
        data = {}
        data['database'] = key.split("_")[0]
        data['solver'] = key.split("_")[1]
        data['ref_id'] = value
        conn = db.connect(DB_SETTING, data['database'])
        cur = conn.cursor()
        statement = "SELECT id, solver, ts, bounds, settings FROM experiment_setting WHERE id = %s"
        cur.execute(statement, (value, ))
        data['buff_setting'] = cur.fetchone()
        statement = "SELECT avg(creation_time), avg(calc_time), max(creation_time), max(calc_time), min(creation_time), min(calc_time), stddev(creation_time), stddev(calc_time), variance(calc_time), variance(creation_time), costs, number_hosts, number_responses, number_conflicts, avg_helps FROM experiment WHERE ref_id = %s group by number_hosts, number_responses, number_conflicts, costs, avg_helps"
        cur.execute(statement, (value, ))
        data['buff'] = cur.fetchall()
        cur.close()
        conn.close()
        dataRowRaw.append(data)

    with open(PRINTER_SETTING.ResultsFile, "w+") as f:
        logging.info("Store csv File with results to: %s", PRINTER_SETTING.ResultsFile)
        CVSwriter = csv.writer(f, delimiter=',',quotechar='|', quoting=csv.QUOTE_MINIMAL)
        CVSwriter.writerow(["database", "solver", "ref_id", "avg(creation_time)", "avg(calc_time)", "max(creation_time)", "max(calc_time)", "min(creation_time)", "min(calc_time)", "stddev(creation_time)", "stddev(calc_time)", "variance(calc_time)", "variance(creation_time)", "costs", "number_hosts", "number_responses", "number_conflicts", "avg_helps"])

        for entry in dataRowRaw:
            for elem in entry['buff']:
                inserts = []
                inserts.append(entry['database'])
                inserts.append(entry['solver'])
                inserts.append(entry['ref_id'])
                for field in elem:
                    inserts.append(field)
                CVSwriter.writerow(inserts)

    axisToUse = PRINTER_SETTING.Axis
    logging.info("Axis to use for printing: %s.", axisToUse)
    if axisToUse == 'host':
        dataRow = getPrintData(11, dataRowRaw)
    elif axisToUse == 'response':
        dataRow = getPrintData(12, dataRowRaw)
    elif axisToUse == 'conflict':
        dataRow = getPrintData(13, dataRowRaw)
    elif axisToUse == 'coverage':
        dataRow = getPrintDataCov(14, dataRowRaw)
    else:
        logging.error("Unkown axis to use %s. Stop Execution.", axisToUse)
        sys.exit()
    
    for key, value in dataRow.iteritems():
        for entry in range(0,4):
            oneLine(value, entry, x_label[axisToUse], PRINTER_SETTING, PRINTER_SETTING.FilePath + "/" + key + "_" + axisToUse + '_')

def getPrintDataCov(value, dataRowRaw):

    
    solver = {}

    for elem in dataRowRaw:
        try:
            for entry in elem['buff']:
                valueList = []
                valueList.append(entry[0])
                valueList.append(entry[1])
                valueList.append(entry[10])
                valueList.append(entry[value]) 
                solver[elem['solver']].append(valueList)
        except:
            solver[elem['solver']] = [] 
            for entry in elem['buff']:
                valueList = []
                valueList.append(entry[0])
                valueList.append(entry[1])
                valueList.append(entry[10])
                valueList.append(entry[value]) 
                solver[elem['solver']].append(valueList)


    data = {'coverage' : solver}

    return data

def getPrintData(value, dataRowRaw):

    data = {}

    for elem in dataRowRaw:
        data[elem['database']] = {}

    for elem in dataRowRaw:
        dataSet = []
        for entry in elem['buff']:
            valueList = []
            valueList.append(entry[0])
            valueList.append(entry[1])
            valueList.append(entry[10])
            valueList.append(entry[value]) 
            dataSet.append(valueList)
        data[elem['database']][elem['solver']] = dataSet       

    return data

def oneLine(data, yps, x_label, PRINTER_SETTING, prefix):

    i = 0

    fig = plt.figure()

    for key, value in data.iteritems():

        name = key
        elem = value
        data_row = []
        for entry in elem:
            if (yps == 3):
                data_y = float(entry[0]) + float(entry[1])
            else :
                data_y = float(entry[yps])

            if data_y != None:
                data_row.append((int(entry[3]), data_y))
        

        data_row.sort(key = lambda tup: tup[0])

        x = []
        y = []
            
        for tup in data_row:
            x.append(tup[0])
            y.append(tup[1])

        try:
            mark = PRINTER_SETTING.Marker[i]
        except:
            logging.warning("Could not use specified marker.")
            mark=m[i]
        try:
            col = PRINTER_SETTING.Color[i]
        except:
            logging.warning("Could not use specified colors.")
            col=color[i]
        try:
            ls = PRINTER_SETTING.Linestyle[i]
        except:
            logging.warning("Could not use specified linestyle.")
            ls=s[i]
        lab = getLabel(PRINTER_SETTING.Label, name)

        plt.plot(x,y, color=col, linewidth=2, marker=mark, label=lab, linestyle=ls)
            

        i = i + 1

    plt.ylabel(yAxis[yps], fontsize=18)
    plt.xlabel(x_label, fontsize=18)
    
    plt.legend(loc='best', numpoints=1, ncol=2, fontsize=16)
    pp = PdfPages(prefix + criteria[yps] + '.pdf')
    pp.savefig(fig)
    pp.close()
    

