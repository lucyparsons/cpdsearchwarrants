#!/usr/bin/python3

import csv
import json
from geocodio import GeocodioClient

def get_client():
    auth_id = ":"

    credentials = StaticCredentials(auth_id, auth_token)
    client = ClientBuilder(credentials).build_us_street_api_client()

    return client

def get_addresses():
    addresses = []

    ##   12SW4005,SEARCH,3-Jan-12,6100,S,OAK PARK AVE,CHICAGO,Y
    fp = '/home/matt/lpl/SearchWarrantLogs_CPD.csv'
    with open(fp, 'r') as fh:
        reader = csv.reader(fh)
        reader.__next__()
        for row in reader:
            row[3] = str(int(row[3]) + 1)
            addresses.append(' '.join(row[3:6]))
    
    addresses = ['{}, Chicago IL'.format(a).upper() for a in set(addresses)]

    return addresses

def geocode_files(geocoded_addresses, fp):
    fp = '/home/matt/lpl/SearchWarrantLogs_CPD.csv'
    with open(fp, 'r') as fh:
        reader = csv.reader(fh)
        rows = [l for l in reader]
    
    new_rows = []
    for row in rows:
        if not row[-1]:
            continue

        new_row = row 
        try:
            new_row += geocoded_addresses[' '.join(row[3:6])]
        except:
            continue

        new_rows.append(new_row)

    with open(fp, 'w') as fh:
        w = csv.writer(fh)
        w.writerow(rows[0] + ['lng', 'lat'])
        w.writerows(new_rows[1:])

    return rows

addresses = get_addresses()

#geocode_results = client.geocode(addresses)

with open('/home/matt/lpl/geocoded_addresses.json', 'r') as fh:
    geocoded_results = json.load(fh)

cutoff = .9

geocoded_addresses = {}
for geocoded in geocoded_results:
    highest_result = None

    for result in geocoded['results']:
        accuracy = result['accuracy']
        if accuracy < cutoff:
            continue

        if not highest_result:
            highest_result = result

        elif accuracy > highest_result['accuracy']:
            highest_result = result 

        elif accuracy == highest_result['accuracy']:
            pass

    if not highest_result:
        #print(geocoded['input']['formatted_address'])
        continue

    latlng = highest_result['location']

    original_addr = geocoded['input']['formatted_address']
    original_addr = original_addr.replace(', Chicago, IL', '')
    orig_num = str(int(original_addr.split()[0]) - 1)
    original_addr = orig_num + ' ' + ' '.join(original_addr.split()[1:])
    original_addr = original_addr.upper()

    geocoded_addresses[original_addr] = [latlng['lng'], latlng['lat']]

rows = geocode_files(geocoded_addresses, fp='/home/matt/lpl/SearchWarrantLogs_CPD.geocoded.csv')

def csv_to_geojson(rows, fp='/home/matt/lpl/SearchWarrantLogs_CPD.geocoded.csv'):
    with open(fp, 'r') as fh:
        reader = csv.DictReader(fh)
        rows = [i for i in reader]


    features = {'type': 'FeatureCollection', 'features': []}
    for row in rows:
        lng = float(row.pop('lng'))
        lat = float(row.pop('lat'))

        properties = row
        geometry = {'type': 'Point', 'coordinates': [lng, lat]} 

        feature_obj = {'type': 'Feature', 'properties': properties, 'geometry': geometry}
        features['features'].append(feature_obj)

    return features

geojson_out = csv_to_geojson(rows)
with open('/tmp/chicago_warrants.geojson','w') as fh:
    json.dump(geojson_out, fh)
