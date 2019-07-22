#!/usr/bin/python3

import json
import psycopg2cffi
import re

from datetime import datetime
from jinjasql import JinjaSql

from project_conf import conf as proj_conf

def get_columns():
    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    sqlstr = """
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = %s"""
    
    curs.execute(sqlstr, [proj_conf.project_name])
    columns = list(curs.fetchall())

    return columns

columns = get_columns()

date_columns = [i[0] for i in columns if re.match('(timestamp.*|date|time)', i[1])]
column_names = [i[0] for i in columns]

def get_timeframe():
    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    start_date = datetime(9999, 1, 1, 1, 1)
    end_date = datetime(1, 1, 1, 1, 1)
    
    for date_col in date_columns:
        sqlstr = """
          SELECT MIN({}) date_col 
          FROM {}""".format(date_col, proj_conf.project_name)
    
        curs.execute(sqlstr)
        col_start_date = curs.fetchall()[0][0]

        if col_start_date < start_date:
            start_date = col_start_date
    
        sqlstr = """
          SELECT MAX({}) date_col 
          FROM {}""".format(date_col, proj_conf.project_name)
    
        curs.execute(sqlstr)
        col_end_date = curs.fetchall()[0][0]

        if col_end_date > end_date:
            end_date = col_end_date
    
    return start_date, end_date

def get_primary_date(date_columns):
    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    highest_count = 0
    primary_col = None

    for date_col in date_columns:
        sqlstr = """
          SELECT COUNT(*) 
          FROM {}
          WHERE {} IS NOT NULL
        """.format(proj_conf.project_name, date_col)
        curs.execute(sqlstr)
        col_count = curs.fetchall()[0][0]
        
        if col_count > highest_count:
            primary_col = date_col

    return primary_col

def doublecheck_useful_col(col_name):
    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    sqlstr = """
        SELECT count({}),{} 
        FROM {}
        WHERE {} IS NOT NULL
        GROUP BY {}
        ORDER BY count DESC
      """.format(col_name, col_name, proj_conf.project_name, col_name, col_name)

    curs.execute(sqlstr)
    col_counts = curs.fetchall()

    ##check if only N/E/S/W
    dirs = ['N','E','S','W']
    no_nesw_len = len([[count,col_val] for count,col_val in col_counts if col_val not in dirs])

    if no_nesw_len <= 1:
        return False

    return True # default

def get_useful_and_useless_cols(column_names, cutoff_count=100):
    """Gets columns whose distinct count is greater than a cutoff"""
    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    useful_cols = []
    useless_cols = []
    for col_name in column_names:
        sqlstr = """
          SELECT COUNT(DISTINCT({})) FROM {}
          WHERE {} IS NOT NULL
          """.format(col_name, proj_conf.project_name, proj_conf.project_name, col_name)

        curs.execute(sqlstr)
        col_count = curs.fetchall()[0][0]
        if col_count <= cutoff_count and col_count > 1:
            if doublecheck_useful_col(col_name):
                useful_cols.append(col_name)

            else:
                useless_cols.append(col_name)
        else:
            useless_cols.append(col_name)

    return useful_cols, useless_cols

def get_x_y_cols(x_min=-125.0011, x_max=-66.9326, y_min=24.9493, y_max=49.5904, min_ratio=.9):
    j = JinjaSql()

    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    sql_templ = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = '{}'
      AND data_type in ('double precision')""".format(proj_conf.project_name)

    curs.execute(sql_templ)

    float_cols = [i[0] for i in curs.fetchall()]

    def get_ratios(min_val, max_val):
        sql_templ = """
          SELECT COUNT(proj.*) / t.total
          FROM {{project_name | sqlsafe}} proj, 
               (SELECT COUNT(*) total FROM {{project_name|sqlsafe}}) t
          WHERE {{col_name | sqlsafe}} IS NOT NULL
          AND {{col_name | sqlsafe}} >= {{min_val}}
          AND {{col_name | sqlsafe}} <= {{max_val}}
          GROUP BY t.total
          """

        sql_vals = { 
            'min_val': min_val,
            'min_val': min_val,
            'max_val': max_val,
            'project_name': proj_conf.project_name, 
            'col_name': col_name,
        }

        query, bind_params = j.prepare_query(sql_templ, sql_vals)
        curs.execute(query, list(bind_params))

        results = curs.fetchall()
        if results:
            ratio = results[0][0]
        else:
            return None

        return ratio

    x_cols = []
    y_cols = []
    for col_name in float_cols:
       x_ratio = get_ratios(x_min, x_max)
       y_ratio = get_ratios(y_min, y_max)

       if x_ratio:
           if x_ratio > min_ratio:
               x_cols.append(col_name)
           
       if y_ratio: 
           if y_ratio > min_ratio:
               y_cols.append(col_name)

    return x_cols, y_cols

def is_business_dist_relevant(min_cutoff=.2):
    conn = psycopg2cffi.connect(proj_conf.conn_str)
    curs = conn.cursor()

    sqlstr = """
      SELECT is_business_district, count(is_business_district)
      FROM  warrants group by is_business_district
    """

    curs.execute(sqlstr)
    results = dict(curs.fetchall())

    if results[True] / results[False] < min_cutoff:
        return False
    else:
        return True


start_date, end_date = get_timeframe()
useful_cols, useless_cols = get_useful_and_useless_cols(column_names)

lng_cols, lat_cols = get_x_y_cols()

primary_date = get_primary_date(date_columns)

multi_selectors = []
for col_name in useful_cols:
    if col_name in lng_cols or col_name in lat_cols:
        continue

    title = col_name.replace('_', ' ').title()
    multi_selectors.append(dict(column_name=col_name, title=title))
    multi_selectors.append({'column_name': 'ward2015', 'title': 'Ward'}) #TODO: generalize
    #multi_selectors.append({'column_name': 'ward2003', 'title': 'Wards (2003-2015)'})
    multi_selectors.append({'column_name': '{}_day'.format(primary_date), 'title': 'Weekday'})
    multi_selectors.append({'column_name': 'address_merged', 'title': 'Block'})

    multi_selectors.append({'column_name': 'census_tract', 'title': 'Census Tract'})

if len(lng_cols) > 0:
    x_col = lng_cols[0]
else:
    x_col = None

if len(lat_cols) > 0:
    y_col = lat_cols[0]
else:
    y_col = None

geo_files = [
    dict(path="data/Boundaries - Census Tracts - 2010.geojson", bounds_key='name10', proj_key='census_tract', name='Census Tract', table='census_tracts_geo'),
    dict(path="data/Boundaries - Wards (2015-).geojson", bounds_key='ward', proj_key='ward2015', name='Ward', table='wards2015_geo'),
    dict(path="data/grid_canvas_cropped.geojson", bounds_key='id', proj_key='grid_id', name='Grid ID', table='grid_geo'),
]

data_conf = dict(
    start_date=datetime.strftime(start_date, '%Y-%m-%d'),
    end_date=datetime.strftime(end_date, '%Y-%m-%d'),
    date_fields=date_columns,
    primary_date=primary_date,
    unneeded_fields=useless_cols,
    x_col=x_col,
    y_col=y_col,
    multi_selectors=multi_selectors,
    coordinate_crs='epsg:4326', #TODO: generalize
    include_business_district=False, #TODO: implement is_business_dist_relevant()
    geo_files=geo_files,
)

with open('data.conf', 'w') as fh:
    json.dump(data_conf, fh, indent=4)
