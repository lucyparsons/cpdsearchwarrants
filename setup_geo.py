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

###TODO: TURN THIS INTO A SQL FILE TODO###

crs_number = data_conf.coordinate_crs.split(':')[1]

curs.execute("ALTER TABLE %s ADD COLUMN grid_id INTEGER" % proj_conf.project_name)
curs.execute("ALTER TABLE %s ADD COLUMN geom_point GEOMETRY ;" % proj_conf.project_name)
curs.execute("ALTER TABLE %s ADD COLUMN is_business_district BOOLEAN ;" % proj_conf.project_name)
curs.execute("ALTER TABLE %s ADD COLUMN ward2015 INTEGER ;" % proj_conf.project_name)
curs.execute("ALTER TABLE %s ADD COLUMN ward2003 TEXT ;" % proj_conf.project_name)
curs.execute("ALTER TABLE %s ADD COLUMN census_tract TEXT ;" % proj_conf.project_name)

sql_templ = """
    UPDATE {{project_name|sqlsafe}}
    SET geom_point = ST_SetSRID(ST_TRANSFORM(GEOMETRY(POINT({{x_col|sqlsafe}}, {{y_col|sqlsafe}})),
                                 '+proj=longlat +datum=WGS84 +no_defs', {{crs_number|sqlsafe}}), {{crs_number|sqlsafe}})
    WHERE geom_point IS NULL
  """

#sql_templ = """
#    UPDATE {{project_name|sqlsafe}}
#    SET geom_point = ST_SetSRID(GEOMETRY(POINT({{x_col|sqlsafe}}, {{y_col|sqlsafe}})), {{crs_number|sqlsafe}})
#    WHERE geom_point IS NULL
#  """

sql_vals = dict(
             project_name=proj_conf.project_name,
             x_col=data_conf.x_col,
             y_col=data_conf.y_col,
             crs_number=crs_number
           )
 
query, bind_params = j.prepare_query(sql_templ, sql_vals)
curs.execute(query, list(bind_params))

sql_templ = """
    UPDATE {{project_name|sqlsafe}} 
    SET {{x_col|sqlsafe}} = st_x(geom_point), {{y_col|sqlsafe}} = st_y(geom_point)
    WHERE {{y_col|sqlsafe}} IS NOT NULL
    AND {{y_col|sqlsafe}} IS NOT NULL
  """

sql_vals = dict(
             project_name=proj_conf.project_name,
             x_col=data_conf.x_col,
             y_col=data_conf.y_col,
           )
 
query, bind_params = j.prepare_query(sql_templ, sql_vals)
curs.execute(query, list(bind_params))

curs.execute("DELETE FROM grid_geo WHERE ST_IsValid(wkb_geometry) IS NOT TRUE")

curs.execute("ALTER TABLE grid_geo ADD COLUMN wkb_geometry_mercator GEOMETRY")
curs.execute("ALTER TABLE census_tracts_geo ADD COLUMN wkb_geometry_mercator GEOMETRY")
curs.execute("ALTER TABLE wards2015_geo ADD COLUMN wkb_geometry_mercator GEOMETRY")

sqlstr = """UPDATE %s SET census_tract = name10
FROM census_tracts_geo g
WHERE ST_intersects(geom_point, g.wkb_geometry::geometry) ;"""
curs.execute(sqlstr % proj_conf.project_name)

sqlstr = """UPDATE %s SET ward2015 = g.ward::int
FROM wards2015_geo g
WHERE ST_intersects(geom_point, g.wkb_geometry::geometry) ;"""
curs.execute(sqlstr % proj_conf.project_name)

sqlstr = """UPDATE %s SET ward2003 = g.ward
FROM wards2003_geo g
WHERE ST_intersects(geom_point, g.wkb_geometry::geometry) ;"""
curs.execute(sqlstr % proj_conf.project_name)

sqlstr = """
    UPDATE grid_geo
    SET wkb_geometry_mercator = ST_TRANSFORM(wkb_geometry::geometry, 3857)
  """
curs.execute(sqlstr)

sqlstr = """
    UPDATE census_tracts_geo
    SET wkb_geometry_mercator = ST_TRANSFORM(wkb_geometry::geometry, 3857)
  """
curs.execute(sqlstr)

sqlstr = """
    UPDATE wards2015_geo
    SET wkb_geometry_mercator = ST_TRANSFORM(wkb_geometry::geometry, 3857)
  """
curs.execute(sqlstr)

sqlstr = """UPDATE %s SET grid_id = g.id 
FROM grid_geo g
WHERE ST_intersects(geom_point, g.wkb_geometry::geometry) ;"""
curs.execute(sqlstr % proj_conf.project_name)

sqlstr = """UPDATE %s SET is_business_district = True
FROM cbd_geo g
WHERE ST_intersects(geom_point, g.wkb_geometry::geometry) ;"""
curs.execute(sqlstr % proj_conf.project_name)

sqlstr = """UPDATE %s SET is_business_district = False
WHERE is_business_district is Null ;"""
curs.execute(sqlstr % proj_conf.project_name)

conn.commit()
