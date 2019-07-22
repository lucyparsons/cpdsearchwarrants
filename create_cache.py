#!/usr/bin/python3

import psycopg2cffi
import csv 
import time
from datetime import date
from datetime import datetime 

#import viz_config as conf
from project_conf import conf as proj_conf
from data_conf import data_conf

def pg_conn():  
     conn = psycopg2cffi.connect(proj_conf.conn_str)  
     return conn

conn = pg_conn() 
curs = conn.cursor() 

def get_data():
    sqlstr = """
    SELECT *
    FROM {}
    WHERE {} >= %(start_date)s
    AND {} <= %(end_date)s
    ORDER BY {}
    """.format(proj_conf.project_name, data_conf.primary_date, 
               data_conf.primary_date, data_conf.primary_date)
    print_vars = (proj_conf.environment, data_conf.start_date, data_conf.end_date)
    print("Creating {} cache file. {} - {}".format(*print_vars))

    sql_vals = { 
        'project_name': proj_conf.project_name, 'primary_date': data_conf.primary_date,
        'start_date': data_conf.start_date, 'end_date': data_conf.end_date,
    }
    curs.execute(sqlstr, sql_vals)
    
    results = curs.fetchall()
    fieldnames = [desc[0] for desc in curs.description]

    return results, fieldnames

def selector_opts(field_name, sorted_by='count', order='desc'):
    """sorted_by can be 'count', 'field'
       order must be 'asc' or 'desc'"""

#   if sorted_by == 'count':

    if field_name.startswith('reason_'):
        sqlstr = """
SELECT tr.id, tr.tow_reason, SUM(count) 
from (
  SELECT count(reason_1), reason_1, created_date 
    FROM towing GROUP BY reason_1, created_date
  UNION 

  SELECT count(reason_2), reason_2, created_date
    FROM towing GROUP BY reason_2, created_date
  UNION 

  SELECT count(reason_3), reason_3, created_date
    FROM towing group by reason_3, created_date
) s1, tow_reason_tbl tr 
WHERE tr.id = reason_1 
AND created_date > '{}'
AND created_date < '{}'
GROUP BY tr.tow_reason, tr.id
ORDER BY SUM DESC;""".format(data_conf.start_date, data_conf.end_date)

    else:
        sqlstr = """
            SELECT a.{}, fk_tbl.{}, count(a.{})
            FROM {} a, {}_tbl fk_tbl
            WHERE a.{} >= '{}'
            AND a.{} <= '{}'
            AND fk_tbl.id = a.{}
            GROUP BY a.{}, fk_tbl.{}
            ORDER BY count(a.{}) {}
        """

        fields = [field_name, field_name, field_name, proj_conf.project_name, 
                  field_name, data_conf.primary_date, data_conf.start_date, 
                  data_conf.primary_date, data_conf.end_date, field_name, 
                  field_name, field_name, field_name, order]
    
        sqlstr = sqlstr.format(*fields)
 
    sql_vals = {'start_date': data_conf.start_date, 'end_date': data_conf.end_date}

    curs.execute(sqlstr)
    results = curs.fetchall()

    total = sum([i[-1] for i in results])

    dropdown_vals = [('', 'All', total)]
    dropdown_vals += results

    return dropdown_vals

data, fieldnames = tuple(get_data())

opts_txt = {}

for selector in data_conf.multi_selectors:
    column_name = selector['column_name']
    opts = selector_opts(column_name)

    fp = '{}/{}.{}.txt'.format(proj_conf.dropdown_dir, column_name, proj_conf.environment)

    with open(fp, 'w') as fh:
        w = csv.writer(fh)
        w.writerow("id,description,count".split(','))
        w.writerows(opts)

    opts_txt[column_name] = []
    for opt in opts:
        opts_txt[column_name].append(opt[0])

#TODO: change weeks_start
weeks_start = datetime.strptime('1995-12-31', '%Y-%m-%d')

header = fieldnames.copy()

#create header based on selectors and date fields
indexable_cols = [(s['column_name'], fieldnames.index(s['column_name'])) 
                     for s in data_conf.multi_selectors]

unneeded_indices = []
for unneeded in list(data_conf.unneeded_fields) + list(data_conf.date_fields):
    unneeded_idx = fieldnames.index(unneeded)
    unneeded_indices.append(unneeded_idx)

unneeded_indices.sort()
unneeded_indices.reverse()

csv_header = header.copy()
for unneeded in unneeded_indices:
    csv_header.pop(unneeded)

w = csv.DictWriter(open(proj_conf.indexed_csv, 'w'), fieldnames=csv_header)
w.writeheader()

final_rows = []

for row in data:
    row = list(row)
    row = dict(zip(header, row))

    for date_field in data_conf.date_fields:
        field_date = row[date_field]

        if not field_date:
            continue

        delta = field_date - weeks_start 

        day_idx = delta.days
        week_idx = int(day_idx / 7)

        if field_date.month < 10:
            month_idx = '{}0{}'.format(field_date.year, field_date.month)
        else:
            month_idx = '{}{}'.format(field_date.year, field_date.month)

        row['{}_month'.format(date_field)] = month_idx
        row['{}_week'.format(date_field)] = week_idx
        row['{}_dow'.format(date_field)] = week_idx

    for column_name, col_idx in indexable_cols:
        col_val = row[column_name]
        if not col_val:
            continue

        row[column_name] = opts_txt[column_name].index(col_val)

    csv_line = dict([(field, row[field]) for field in csv_header]) #removes unnecessary fields
    w.writerow(csv_line)

print("=== Creating cached data for {} ===".format(proj_conf.environment))
