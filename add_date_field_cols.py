#!/usr/bin/python3

import json
import psycopg2cffi
import re

from datetime import datetime
from jinjasql import JinjaSql

from project_conf import conf as proj_conf
from data_conf import data_conf

conn = psycopg2cffi.connect(proj_conf.conn_str)
curs = conn.cursor()

j = JinjaSql()

for date_field in data_conf.date_fields:
    time_segments = ['hour', 'dow', 'day', 'month', 'year']
    for time_segm in time_segments:
        if time_segm == 'day':
            col_type = 'TEXT'
        else:
            col_type = 'INTEGER'


        col_name = '{}_{}'.format(date_field, time_segm)

        sql_templ = """
          ALTER TABLE {{project_name | sqlsafe}} 
          ADD COLUMN {{col_name | sqlsafe}} {{col_type | sqlsafe}}
        """

        sql_vals = dict(
            project_name=proj_conf.project_name, 
            date_field=date_field,
            col_name=col_name,
            col_type=col_type
        )
        query, bind_params = j.prepare_query(sql_templ, sql_vals)

        print(query)

        curs.execute(query, list(bind_params))

        if time_segm == 'day':
            sql_templ = """
              UPDATE {{project_name | sqlsafe}} 
              SET {{col_name | sqlsafe}} = initcap(trim(both from to_char({{date_field | sqlsafe}}, 'DAY')))
        """

        else:
            sql_templ = """
              UPDATE {{project_name | sqlsafe}} 
              SET {{col_name | sqlsafe}} = EXTRACT('{{time_segm | sqlsafe}}' 
                                                     FROM {{date_field | sqlsafe}})
"""

        sql_vals = dict(
            project_name=proj_conf.project_name, 
            date_field=date_field,
            time_segm=time_segm,
            col_name=col_name,
        )
        query, bind_params = j.prepare_query(sql_templ, sql_vals)

        curs.execute(query, list(bind_params))


    conn.commit()
