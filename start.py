import configparser
import logging
import sys
import argparse
import psycopg2 

import classes.DatabaseSettings as dbs
import classes.DataSettings as ds

import modules.TestDataGenerator as dataGenerator
import modules.DatabaseCheck as dbCheck
import modules.ScenarioCombination as senCombi
import modules.Printer as printer

import helper.init_helper as init
import helper.db_helper as db

if __name__ == "__main__":  

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-l', '--log',
        help="Set log-level.",
        dest="loglevel", 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
    )
    parser.add_argument(
        '-c', '--config',
        help="Set path to config file.",
        type=str,
        dest="config", 
        nargs='+',
        default=['config/config.ini'],
    )

    args = parser.parse_args()
    
    numeric_level = getattr(logging, args.loglevel.upper(), None)

    logging.basicConfig(
            format='%(asctime)s %(module)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %T %Z',
            level=numeric_level)

    logging.info("Log-Level set to: '%s'.", args.loglevel)

    CONFIG_FILEPATHS = args.config
    logging.info("Config Files set to: '%s'.", CONFIG_FILEPATHS)
    Config = configparser.ConfigParser()
    config = Config.read(CONFIG_FILEPATHS)

    if len(config) != len(CONFIG_FILEPATHS):
        logging.critical("Not all specified configuration files can be found (%s is missing). Please check the following paths: %s", len(CONFIG_FILEPATHS)-len(config), CONFIG_FILEPATHS)
        sys.exit(0)

    DB_SETTING = init.init_database(Config)
    DATA_SETTING = init.init_data(Config)
    SCENARIO_SETTING = init.init_scenario(Config, DB_SETTING.DB_NAMES)
    EXECUTION_SETTING = init.init_execution(Config)
    PRINTER_SETTING = init.init_printer(Config)

    logging.info("Successfully read Configuration Files %s. - Starting with Stage 1: Checking Databases and prepare Data. ", CONFIG_FILEPATHS)

    for database in DB_SETTING.DB_NAMES:

        conn = dbCheck.check(DB_SETTING, database)

        if DATA_SETTING.NEW :
            dataGenerator.startGen(DATA_SETTING, conn)
            logging.info("Successfully created data for database %s.", database)
        else:
            logging.info("No Data has to be generated for database: %s", database)

        conn.close()

    logging.info("Successfully prepared databases %s. - Starting with Stage 2: Creating Szenarios to be tested. ", DB_SETTING.DB_NAMES)

    newSettings = senCombi.create(SCENARIO_SETTING, DB_SETTING, EXECUTION_SETTING)

    logging.info("Successfully created data %s. - Starting with Stage 3: Creating plots for generated data. ", newSettings)
   
    printer.printing(DB_SETTING,PRINTER_SETTING,newSettings)

