#!/usr/bin/python3

import docopt
import json
import pandas as pd

project_name = None #input("Name of project [warrants]: ")
if not project_name:
    project_name = 'warrants'

root_dir = '/opt/{}_viz'.format(project_name)
data_dir = '{}/data'.format(root_dir)

original_data_path = None #input("Path of data? [./P494685_Martinez_Freddy_Chicago_SWs.geocoded.csv]: ".format(data_dir, project_name))
if not original_data_path:
    original_data_path = 'P494685_Martinez_Freddy_Chicago_SWs.geocoded.csv'

db_host = None #input("Host of database [localhost]: ")
if not db_host:
    db_host = 'localhost'

environment = None #input("Environment (prod or dev) [dev]: ")
if not environment:
    environment = 'dev'

base_config = dict(
    project_name=project_name,
    base_conf_path='{}/project.conf'.format(root_dir),
    project_title='{} Visualization'.format(project_name.replace('_', ' ')).title(),
    original_data_path=original_data_path,
    data_path='{}/{}.csv'.format(data_dir, project_name),
    db_info={"db_name": project_name, "db_user": project_name,
             "db_pass": project_name, "db_host": db_host},
    conn_str="dbname={} host={} user={} password={}".format(project_name, db_host, project_name, project_name),
    environment=environment,
    cache_dir='{}/cache'.format(root_dir),
    data_dir=data_dir,
    dropdown_dir='{}/dropdowns'.format(data_dir),
    empty_grid_geojson_path='{}/blank_grid_mercator.geojson'.format(data_dir),
    indexed_csv='{}/{}.csv'.format(data_dir, project_name, environment),
    pg_resolutions = ['year', 'month', 'week', 'day']
)

#def check_keys(

#def describe_data(fp):
#    df = pd.read_csv(data_fp)
#
#    keys = df.keys()

#last step
with open('project.conf', 'w') as fh:
    json.dump(base_config, fh, indent=4)
