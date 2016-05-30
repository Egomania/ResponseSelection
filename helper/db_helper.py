
import psycopg2
import sys
import datetime
import logging

import classes.DatabaseSettings as DB

def connect(DB_SETTING, database):
    conn = psycopg2.connect(database=database, user=DB_SETTING.user, password=DB_SETTING.pwd, port=DB_SETTING.port, host=DB_SETTING.host)
    return conn

def connectDefault(DB_SETTING):
    conn = connect(DB_SETTING, 'postgres')
    return conn

def createTable(conn, table, owner):
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    if table == 'damage':
        cur.execute("CREATE TABLE damage(name text NOT NULL, value numeric, weight numeric, CONSTRAINT damage_pkey PRIMARY KEY (name )) WITH (OIDS=FALSE);")

    elif table == 'host':
        cur.execute("CREATE TABLE host(id integer NOT NULL, affected boolean, CONSTRAINT host_pkey PRIMARY KEY (id )) WITH (  OIDS=FALSE);")        

    elif table == 'response':
        cur.execute("CREATE TABLE response(id integer NOT NULL, executedby integer, CONSTRAINT response_pkey PRIMARY KEY (id ), CONSTRAINT response_executedby_fkey FOREIGN KEY (executedby) REFERENCES host (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION) WITH (OIDS=FALSE);")

    elif table == 'metric':
        cur.execute("CREATE TABLE metric( name text NOT NULL, describes integer NOT NULL, value numeric, CONSTRAINT metric_pkey PRIMARY KEY (name , describes ), CONSTRAINT metric_describes_fkey FOREIGN KEY (describes) REFERENCES response (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION, CONSTRAINT metric_name_fkey FOREIGN KEY (name) REFERENCES damage (name) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION) WITH ( OIDS=FALSE);")

    elif table == 'help':
        cur.execute("CREATE TABLE help( response integer NOT NULL, host integer NOT NULL, CONSTRAINT help_pkey PRIMARY KEY (response , host ), CONSTRAINT help_host_fkey FOREIGN KEY (host) REFERENCES host (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION, CONSTRAINT help_response_fkey FOREIGN KEY (response) REFERENCES response (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION) WITH ( OIDS=FALSE );")

    elif table == 'conflict':
        cur.execute("CREATE TABLE conflict( r1 integer NOT NULL, r2 integer NOT NULL, CONSTRAINT conflicts_pkey PRIMARY KEY (r1 , r2 ), CONSTRAINT conflicts_r1_fkey FOREIGN KEY (r1)  REFERENCES response (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION, CONSTRAINT conflicts_r2_fkey FOREIGN KEY (r2) REFERENCES response (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION) WITH ( OIDS=FALSE );")

    elif table == 'experiment_setting':
        cur.execute("CREATE TABLE experiment_setting( id integer NOT NULL, solver text NOT NULL, ts timestamp without time zone, bounds boolean, settings text, CONSTRAINT experiment_settings_pkey PRIMARY KEY (id )) WITH ( OIDS=FALSE );")

    elif table == 'experiment':
        cur.execute("CREATE TABLE experiment( id integer NOT NULL, ref_id integer NOT NULL,  creation_time numeric, start_creation_time numeric, end_creation_time numeric, calc_time numeric,  start_calc_time numeric, end_calc_time numeric, number_hosts integer NOT NULL, number_responses integer NOT NULL, number_conflicts integer NOT NULL, number_metrics integer NOT NULL,  number_self_healing integer NOT NULL, number_multiple_help integer NOT NULL, max_helps integer NOT NULL, avg_helps numeric NOT NULL, costs numeric, selected_response integer, CONSTRAINT experiment_pkey PRIMARY KEY (ref_id, id),  CONSTRAINT experiment_id_fkey FOREIGN KEY (ref_id) REFERENCES experiment_setting (id) MATCH SIMPLE  ON UPDATE CASCADE ON DELETE CASCADE) WITH ( OIDS=FALSE );")

    else:
        logging.warning("Unkown table: %s", table)
        cur.close()
        return

    cur.execute("ALTER TABLE "+table+" OWNER TO "+owner+";")

    cur.close()

def experimentID(cur):
    
    cur.execute("SELECT max(id) FROM experiment_setting;")
    experimentID = cur.fetchone()[0]
    if experimentID == None:
        experimentID = 0
    experimentID = experimentID + 1
    
    return experimentID

def maxEntity(cur):

    cur.execute("SELECT count(id) FROM host where affected = true;")
    maxEffHost = cur.fetchone()[0]
    if maxEffHost == None: 
        logging.warning("Warning - Database (Host) is empty!")
        return 0
    
    return maxEffHost

def maxResponse(cur):
    
    cur.execute("SELECT count(id) FROM response;")
    maximumResponse = cur.fetchone()[0]
    if maximumResponse == None:
        logging.warning("Warning - Database (Response) is empty!")
        return 0
        
    return maximumResponse

def maxConflicts(cur):
    
    cur.execute("Select count(*) from conflict")
    number_conflicts = cur.fetchone()[0]
    if number_conflicts == None:
        logging.warning("Warning - Database (Conflict) is empty!")
        return 0   
        
    return (number_conflicts / 2)

def allMetrics(cur):
    
    usedMetrics = []

    cur.execute("Select distinct damage.name from damage")
    buf = cur.fetchall()
    for ent in buf:
        usedMetrics.append(str(ent[0]))
    
    return usedMetrics

