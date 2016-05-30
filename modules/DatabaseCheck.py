import logging
import psycopg2

import helper.db_helper as db

def check(DB_SETTING, database):

    try:
        conn = db.connect(DB_SETTING, database)
        logging.info("Successfully connected to database: %s", database)
    except:
        logging.warning("Database %s does not exist.", database)
        conn = db.connectDefault(DB_SETTING)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute('CREATE DATABASE ' + database)
        conn.commit()
        cur.close()
        conn.close()
        conn = db.connect(DB_SETTING, database)
        logging.info("Successfully created database: %s", database)

    cur = conn.cursor()
    for table in ['damage', 'host', 'response', 'metric', 'help', 'conflict', 'experiment_setting', 'experiment']:
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (table,))
        if bool(cur.fetchone()[0]):
            logging.info("Successfully checked in database %s for table: %s", database, table)
        else:
            db.createTable(conn, table, DB_SETTING.user)
            logging.info("Successfully created table %s in database %s.", table, database)
        
    cur.close()

    logging.info("Successfully checked and prepared database %s. - Start Data Generation.", database)

    return conn
