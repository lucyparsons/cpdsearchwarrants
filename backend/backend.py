#!/usr/bin/python3

import csv
import json
import pandas as pd

from hashlib import md5

from datetime import datetime, timedelta
from jinjasql import JinjaSql

from os import stat
from flask import Flask, request 
from flask_restful  import Resource, Api

#import viz_config as conf
from project_conf import conf as proj_conf
from data_conf import data_conf

import psycopg2cffi

def pg_conn():
     conn = psycopg2cffi.connect(proj_conf.conn_str)
     return conn

conn = pg_conn()
curs = conn.cursor()

#proj_conf.empty_grid_geojson_path
with open(proj_conf.empty_grid_geojson_path, 'r') as fh:
    empty_grid_json = json.load(fh)

def get_map_geojson(request_data):
    j = JinjaSql()

    sqlstr_template ="""SELECT json_build_object(
    'type',     'FeatureCollection',
    'features', jsonb_agg(feature)) 
FROM (
  SELECT json_build_object(
      'type',       'Feature',
      'id',         row.id,
      'geometry',   ST_AsGeoJSON(geom)::jsonb,
      'properties', to_jsonb(row) - 'geom'
  ) AS feature
  FROM (
    SELECT 
      {{geo_table|sqlsafe}}.{{bounds_key|sqlsafe}} id,
      wkb_geometry_mercator AS geom, 
      gt.data_val 
    FROM 
      {{geo_table | sqlsafe}}, 
      ({{inner_sql | sqlsafe}}) gt
    WHERE {{geo_table|sqlsafe}}.{{bounds_key|sqlsafe}}::text = gt.{{proj_key|sqlsafe}}::text ) row
  ) top_level"""


    #sqlstr_template_old = """
#SELECT json_build_object(
    #'type',     'FeatureCollection',
    ##'features', jsonb_agg(feature)) 
#FROM (
  #SELECT json_build_object(
      #'type',       'Feature',
      #'id',         id,
      #'geometry',   ST_AsGeoJSON(geom)::jsonb,
      #'properties', to_jsonb(row) - 'id' - 'geom'
  #) AS feature
  #FROM (
    #SELECT 
      #grid_geo.id, 
      #wkb_geometry_mercator AS geom, 
      #gt.data_val 
    #FROM 
      #grid_geo, 
      #(%s) gt
    #WHERE grid_geo.id = gt.grid_id ) row
  #) top_level"""

    geo_conf = data_conf.geo_files[request_data['map_by']]

    sql_vals = dict(
        geo_table=geo_conf['table'], 
        bounds_key=geo_conf['bounds_key'], 
        inner_sql='%s',
        proj_key=geo_conf['proj_key']
    )

    sqlstr_template, bind_params = j.prepare_query(sqlstr_template, sql_vals)

    resolution = proj_conf.pg_resolutions[request_data['resolution_idx']]
    ignore_business_district = request_data['is_business_district']

    project_fields = [i['column_name'] for i in data_conf.multi_selectors]
    chart_by = "grid_id"

    date_by = data_conf.date_fields[request_data['date_by']]
    table = '{}_mv'.format(proj_conf.project_name)
    #table = '{}'.format(proj_conf.project_name)

    inner_template = """
        SELECT count({{proj_key|sqlsafe}}) data_val, {{proj_key|sqlsafe}} FROM {{ table | sqlsafe }} 
        """

    template_vals = dict(
        resolution=resolution,
        date_by=date_by,
        chart_by=chart_by,
        table=table, 
        bounds_key=geo_conf['bounds_key'],
        proj_key=geo_conf['proj_key']
      )

    inner_where_template = ""
    for key, vals in request_data['project_selectors'].items():
        if key not in [i['column_name'] for i in data_conf.multi_selectors]:
            continue

        inner_where_template += """
            AND {{ %s | sqlsafe }} in {{ %s_vals | inclause }}""" % (key, key)

        template_vals[key] = key
        template_vals["{}_vals".format(key)] = vals

    if request_data['date_sliders']:
        inner_where_template += """
            AND {{ date_by | sqlsafe }} >= {{ start_date }} 
            AND {{ date_by | sqlsafe }} <= {{ end_date }}"""
        template_vals['start_date'] = request_data['date_sliders'][data_conf.primary_date][0]
        template_vals['end_date'] = request_data['date_sliders'][data_conf.primary_date][1]

    if ignore_business_district:
        inner_where_template += """ AND is_business_district IS NOT TRUE"""

    template_grouporder = """
        GROUP BY {{ chart_by | sqlsafe }}, {{proj_key | sqlsafe}}
        ORDER BY {{ chart_by | sqlsafe }}
    """

    print("inner_where_template", inner_where_template)
    
    inner_where_template = inner_where_template.replace('AND', 'WHERE', 1)
    template = sqlstr_template % str(inner_template + inner_where_template + template_grouporder)

    print(template_vals)
    template_vals['chart_by'] = bool(template_vals['chart_by'])

    query, bind_params = j.prepare_query(template, template_vals)
    print(query)

    ##QUICK FIX

    curs.execute(query, list(bind_params))
    geojson = curs.fetchone()[0]

    if not geojson['features']:
        geojson['features'] = empty_grid_json['features']

    return geojson

def prepare_sql(request_data):
    j = JinjaSql()

    template_vals = {}

    resolution = proj_conf.pg_resolutions[request_data['resolution_idx']]
    ignore_business_district = request_data['is_business_district']

    project_fields = [i['column_name'] for i in data_conf.multi_selectors]
    chart_by = project_fields[int(request_data['chart_by_idx'])]

    date_by = data_conf.date_fields[request_data['date_by']]
    table = '{}_mv'.format(proj_conf.project_name)
    #table = '{}'.format(proj_conf.project_name)

    inner_template = """
        SELECT date_trunc({{ resolution }}, {{ date_by | sqlsafe }})::text truncd,
               count({{ chart_by }}),
               {{ chart_by | sqlsafe }}
        FROM "{{ table | sqlsafe}}"
        """

    template_vals = dict(resolution=resolution, date_by=date_by, 
                         chart_by=chart_by, table=table)

    print(request_data)
    inner_where_template = "WHERE {{ date_by | sqlsafe}} IS NOT NULL "
    for key, vals in request_data['project_selectors'].items():
        if key not in [i['column_name'] for i in data_conf.multi_selectors]:
            continue

        inner_where_template += """
            AND {{ %s | sqlsafe }} in {{ %s_vals | inclause }}""" % (key, key)

        template_vals[key] = key
        template_vals["{}_vals".format(key)] = vals

    if request_data['date_sliders']:
        inner_where_template += """
            AND {{ date_by | sqlsafe }} >= {{ start_date }} 
            AND {{ date_by | sqlsafe }} <= {{ end_date }}"""
        template_vals['start_date'] = request_data['date_sliders'][data_conf.primary_date][0]
        template_vals['end_date'] = request_data['date_sliders'][data_conf.primary_date][1]

    template_grouporder = """
        GROUP BY {{ chart_by | sqlsafe }}, truncd
        ORDER BY truncd
    """

    if ignore_business_district:
        inner_where_template += """ AND is_business_district IS NOT TRUE"""

    print("inner_where_template", inner_where_template)

    template = inner_template + inner_where_template + template_grouporder

    query, bind_params = j.prepare_query(template, template_vals)
    print(query)
    print(bind_params)

    return query, list(bind_params)

def clean_request_data(request_data, is_date=False):
    #remove args which include "All"
    new_data = {}
    for key, vals in request_data.items():
        if key == 'date_sliders':
            new_data[key] = clean_request_data(vals, is_date=True)
            
        elif key == 'project_selectors':
            new_data[key] = clean_request_data(vals)

        else:
            if is_date:
                if vals[0] == data_conf.start_date and vals[1] == data_conf.end_date:
                    continue

                new_data[key] = vals
                
            elif type(vals) == list:
                if 'All' in vals:
                    continue

                new_data[key] = vals

            else:
                if vals == 'All':
                    continue

                new_data[key] = vals

    return new_data

def get_chart_data(request_data):
    sqlstr, vals = prepare_sql(request_data)
    curs.execute(sqlstr, vals)
    datas = curs.fetchall()

    chart_type = request_data['chart_type']

    grouped_results_x = {}
    grouped_results_y = {}

    grouped_results = {}

    if not datas:
        return {'xs': [], 'ys': [], 'keys':[]}

    line_len = len(datas[0][0])

    prev_ys = [0 for i in range(line_len)]
    print(len(prev_ys))
    for x,y,key in datas:
        y = int(y)
        if key not in grouped_results:
            grouped_results[key] = {'xs':[], 'ys':[], 'prev_y':0}

        position = len(grouped_results[key]['xs'])

        grouped_results[key]['xs'].append(x) 

        if chart_type == 1: #cumulative
            prev_y = grouped_results[key]['prev_y']
            y += prev_y

        elif chart_type == 2: #total
            print('prevys, position', prev_ys, position)
            if position > 0:
                prev_ys[position] += y

        grouped_results[key]['ys'].append(y) 
        grouped_results[key]['prev_y'] = y

    ret_data = {'keys':[], 'xs':[], 'ys':[]}

    for key, vals in grouped_results.items():
        ret_data['keys'].append(key)
        ret_data['xs'].append(vals['xs'])
        ret_data['ys'].append(vals['ys'])

    if chart_type == 2: #total
        ret_data['xs'] = ret_data['xs'][0]
        ret_data['ys'] = prev_ys

    return ret_data

def cache_results(geojson_data, filename, path=proj_conf.cache_dir):
    try:
        with open('{}/{}'.format(path, filename), 'w') as fh:
            json.dump(geojson_data, fh)
    except:
        print(path, filename)
        print("oh noooo")

def get_cached_geojson(filename):
    try:
        with open('{}/{}'.format(proj_conf.cache_dir, filename)) as fh:
            return json.load(fh)
    except:
        print("Failed to get cache!")
        return None

def get_table_cols():
    sqlstr = """
        select column_name
        from information_schema.columns 
        where table_name = {}
    """

    curs.execute(sqlstr, (proj_conf.project_name))

def initial_data():
    print('Loading ', proj_conf.indexed_csv)

    datas = pd.read_csv(proj_conf.indexed_csv)
    return datas
