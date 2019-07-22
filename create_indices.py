#!/usr/bin/python3

import psycopg2
from psycopg2 import extensions as ext
from psycopg2 import sql
import csv
import time
from datetime import date
from datetime import datetime

from project_conf import conf as proj_conf
from data_conf import data_conf

def pg_conn():
     conn = psycopg2.connect(proj_conf.conn_str)
     return conn

conn = pg_conn()
curs = conn.cursor()

column_names = [c['column_name'] for c in data_conf.multi_selectors]

def column_type(col):
    template = """
select data_type from information_schema.columns 
where table_name = '{}' and column_name = '{}'
    """
    sqlstr = template.format(proj_conf.project_name, col)
    curs.execute(sqlstr)

    results = curs.fetchone()
    if results:
        results = results[0]

    else:
        results = None

    return results

def create_foreign_key_table(col):
    col_type = column_type(col)

    if not col_type:
        print("No col found for {}".format(col))
        return None

    print("CREATING TABLE, %s_tbl" % col)

    template = "CREATE TABLE IF NOT EXISTS {}_tbl (id serial primary key, {} {} UNIQUE)"
    sqlstr = template.format(col, col, col_type)
    curs.execute(sqlstr)

    conn.commit()

    return True

def uniq_col_vals(col):
    template = "SELECT DISTINCT({}) from {}"
    sqlstr = template.format(col, proj_conf.project_name)
    curs.execute(sqlstr)

    uniq_vals = set([i[0] for i in curs.fetchall() ])
    return list(uniq_vals)


def populate_table(col):
    uniq_vals = uniq_col_vals(col)

    table_name = "{}_tbl".format(col)

    #special snowflake
    #if table_name.startswith('reason_'):
    #    table_fmt = ext.quote_ident('{}_tbl'.format(col), curs)
    #    sqlstr = "INSERT INTO %s (SELECT * FROM tow_reason_tbl)" % table_fmt
    #    curs.execute(sqlstr)

    for val in uniq_vals:
        if not val and val not in (False, '', 0) : ##ignore empty, but allow False
            continue  
    
        table_fmt = ext.quote_ident(table_name, curs)
        col_fmt = ext.quote_ident(col, curs)
        sqlstr = "INSERT INTO %s (%s) values (%%s)" % (table_fmt, col_fmt)
        curs.execute(sqlstr, [val])
   
    conn.commit()
    return

def swap_col_to_fkeys(col):
    #adds new column based on `col`, 
    #populate it with the foreign key of "old" column
    #delete "old" column
    #rename new column to old column's name

    table_name = proj_conf.project_name
    fkey_table = "{}_tbl".format(col)

    template = "alter table {} add column IF NOT EXISTS {}_lolnew INTEGER ;"
    sqlstr = template.format(proj_conf.project_name, col)
    curs.execute(sqlstr)

    template = "alter table {} add foreign key ({}_lolnew) references {} (id) ;"
    sqlstr = template.format(proj_conf.project_name, col, fkey_table)
    curs.execute(sqlstr)

    template = "ALTER TABLE {} RENAME COLUMN {} TO {}_old ;"
    sqlstr = template.format(proj_conf.project_name, col, col)
    curs.execute(sqlstr)

    template = "ALTER TABLE {} RENAME COLUMN {}_lolnew TO {} ;"
    sqlstr = template.format(proj_conf.project_name, col, col)
    curs.execute(sqlstr)
    conn.commit()

    sqlstr = """
        UPDATE {}
        SET {} = fk_tbl.id
        FROM {} fk_tbl
        WHERE fk_tbl.{} = {}.{}_old ;
    """.format(proj_conf.project_name, col, fkey_table, col, proj_conf.project_name, col)
        
    curs.execute(sqlstr)

    curs.execute("ALTER TABLE {} DROP COLUMN IF EXISTS {}_old".format(proj_conf.project_name, col))

    conn.commit()

def process_col(col_name):
    print("Creating foreign key table: ", col_name)
    success = create_foreign_key_table(col_name)
    if not success: 
        print(":(")
        return None

    print("Populating foreign key table: ", col_name)
    populate_table(col_name)

    print("Swapping out columns: ", col_name, "{}_old".format(col_name))
    swap_col_to_fkeys(col_name)

for col_name in column_names:
    process_col(col_name)
