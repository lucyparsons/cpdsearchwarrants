#!/usr/bin/python3

import csv
import json
import pandas as pd

from hashlib import md5

from datetime import datetime, timedelta

from os import stat

from flask import Flask, request
from flask_restful  import Resource, Api

#import viz_config as conf
from project_conf import conf as proj_conf
from data_conf import data_conf

from backend import backend as be


#print('Loading ', conf.indexed_csv)
#data_df = be.initial_data()
#
#max_week_idx = max(data_df[conf.primary_date])
#max_day_idx = max(datas.day_idx)
#
#date_format = '%Y-%m-%d'
#data_beginning = datetime.strptime('1995-12-31', date_format)
#
#cached_week_strs = {}
#cached_day_strs = {}
#
#cached_index_map = dict([ (s['column_name'],s['index_map']) 
#                           for s in conf.selectors if 'index_map' in s ])

#print(cached_index_map)
#for week_idx in range(0, max_week_idx+1):
#    time_dt = data_beginning + timedelta(days=week_idx*7)
#    time_str =  time_dt.strftime(date_format)
#    cached_week_strs[week_idx] = time_str
#
#for day_idx in range(0, max_day_idx+1):
#    time_dt = data_beginning + timedelta(days=day_idx)
#    time_str = time_dt.strftime(date_format)
#    cached_day_strs[day_idx] = time_str
#
#cached_opts = {}
#for selector in conf.selectors:
#    name = selector['column_name']
#
#    with open('{}/{}.{}.txt'.format(conf.dropdown_dir, name, conf.environment), 'r') as fh:
#        cached_opts[name] = [i.rstrip() for i in fh.readlines()]

#with open(conf.empty_grid_geojson, 'r') as fh:
#    empty_grid_json = json.load(fh)

app = Flask(__name__)
api = Api(app)

class ProjectEndpoint(Resource):
    def get(self):
        json_input = json.loads(request.get_json())
        cleaned_input = be.clean_request_data(json_input)
        print(type(cleaned_input))
        #print(request.form)
        #req_json = json.loads(request.form['data'])
        #req_json['end_time'] = datetime.strptime(req_json['end_time'], '%Y-%m-%d')
        #req_json['start_time'] = datetime.strptime(req_json['start_time'], '%Y-%m-%d')
        #req_json['start_hour'] = req_json['start_hour']
        #req_json['end_hour'] = req_json['end_hour']

        print('request data: ', cleaned_input)

        ret_geojson = be.get_map_geojson(cleaned_input)
        ret_chart_data = be.get_chart_data(cleaned_input)

        ret = dict(geojson=ret_geojson, chart_xys=ret_chart_data)

        return json.dumps(ret)

api.add_resource(ProjectEndpoint, '/')

if __name__ == '__main__':
    if proj_conf.environment in ['dev']: 
        app.run(debug=True)
    elif proj_conf.environment in ['prod', 'superprod']: 
        app.run(debug=False)
