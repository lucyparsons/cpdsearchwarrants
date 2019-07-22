#!/usr/bin/python3

import json
import psycopg2cffi

from project_conf import conf as proj_conf
from data_conf import data_conf

conn = psycopg2cffi.connect(proj_conf.conn_str)
curs = conn.cursor()

def csv_to_geojson(rows):
    features = {'type': 'FeatureCollection', 'features': []}
    for row in rows:
        lng = float(row.pop('lng'))
        lat = float(row.pop('lat'))

        properties = row
        geometry = {'type': 'Point', 'coordinates': [lng, lat]}

        feature_obj = {'type': 'Feature', 'properties': properties, 'geometry': geometry}
        features['features'].append(feature_obj)

    return features

sqlstr = "SELECT search_warrant_num, type_of_warrant, date::text, address_merged, address, street_dir, street_name, city, state, country, lng, lat, was_arrested, date_hour, date_dow, date_month, date_year, grid_id, geom_point, is_business_district, ward2003, ward2015, census_tract FROM warrants"

keys = ['search_warrant_num', 'type_of_warrant', 'date', 'address_merged', 'address', 'street_dir', 'street_name', 'city', 'state', 'country', 'lng', 'lat', 'was_arrested', 'date_hour', 'date_dow', 'date_month', 'date_year', 'grid_id', 'geom_point', 'is_business_district', 'ward2003', 'ward2015', 'census_tract']

curs.execute(sqlstr)
rows = curs.fetchall()
rows = [ dict(zip(keys, r)) for r in rows ]

geojson_out = csv_to_geojson(rows)
with open('/tmp/chicago_warrants.geojson','w') as fh:
    json.dump(geojson_out, fh)
