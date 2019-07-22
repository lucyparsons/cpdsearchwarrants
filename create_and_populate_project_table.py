#!/usr/bin/python3

import docopt
import io
import json
import pandas as pd
import psycopg2cffi
import re

from jinjasql import JinjaSql

def pg_conn(db_name, db_host, db_user, db_pass):
    conn_vars = (db_name, db_host, db_user, db_pass)
    conn_str = "dbname={} host={} user={} password={}".format(*conn_vars)

    conn = psycopg2cffi.connect(conn_str)

    return conn

def get_column_type(column_name, rows):
    """Tests to find data type"""

    conn = pg_conn(**project_conf['db_info'])
    curs = conn.cursor()


    j = JinjaSql()

    ##testing column name
    templ = """create temp table test_table ({{column_name | sqlsafe}} TEXT)"""
    vals = dict(column_name=column_name)

    try:
        query, bind_params = j.prepare_query(templ, vals)
    except:
        print("Unable to prepare query from jinja for: {}".format(key))
        return None
    try:
        curs.execute(query, list(bind_params))
    except:
        print("Query failed for: {}".format(key))
        return None

    rows_io = io.StringIO('\n'.join(map(str, rows)))
    curs.copy_from(rows_io, 'test_table')
    conn.commit()

    curs.execute("SELECT count(*) from test_table")

    templ = """SELECT {{column_name|sqlsafe}}::{{test_type|sqlsafe}}  
               FROM test_table TABLESAMPLE SYSTEM ({{percent|sqlsafe}})"""

    possible_types = ['BOOLEAN', 'TIMESTAMP', 'TIME', 'DATE', 'INT', 'FLOAT', 'TEXT']

    working_types = []
    for test_type in possible_types:
        vals = dict(column_name=column_name, test_type=test_type, percent=10)
        try:
            query, bind_params = j.prepare_query(templ, vals)
        except:
            print("Jinja unable to parse {} for type: {}".format(templ, vals))
    
        try:
            curs.execute(query, list(bind_params))
            sample_good = True
        except:
            sample_good = False
            conn.rollback()

        if sample_good:
            vals['percent'] = 100
            query, bind_params = j.prepare_query(templ, vals)
            try:
                curs.execute(query, list(bind_params))
                working_types.append(test_type)
            except:
                conn.rollback()
                pass

    if working_types:
        return working_types[0]

    else:
        return None

    return working_types[0]
    
with open('project.conf', 'r') as fh:
    project_conf = json.load(fh)

df = pd.read_csv(project_conf['data_path'])
keys = df.keys()

columns_and_types = []
for key in keys:
    orig_key = key
    key = key.lower().replace(' ', '_')

    replaces = [('#', 'num'), ('[ \t.]', '_')]
    for from_str, to_str in replaces:
        key = re.sub(from_str, to_str, key)

    df = df.rename(index=str, columns={orig_key: key})

    sample_size = int(len(df[key]) * .1)
    test_rows = df[key].sample(n=sample_size, random_state=1)
    column_type = get_column_type(key, test_rows)

    columns_and_types.append((key, column_type))

create_str = "CREATE TABLE {} (\n".format(project_conf['project_name'])

for column_name, column_type in columns_and_types:
    create_str += '  {} {},\n'.format(column_name, column_type)

create_str = '{}\n)'.format(create_str[:-2]) #remove last comma and ends query

conn = pg_conn(**project_conf['db_info'])
curs = conn.cursor()

curs.execute(create_str)
conn.commit()

curs.execute("""copy {} from '{}' with (FORMAT CSV, DELIMITER ',', QUOTE'"', HEADER);""".format(project_conf['project_name'], project_conf['data_path']))
conn.commit()
