project_name = $(`jq -r '.project_name' project.conf`)

setup_view: create_cache
	psql -dwarrants -Uwarrants < sql/setup_view.sql

create_cache: create_indices
	./create_cache.py

create_indices: setup_geo
	./create_indices.py

setup_geo: add_date_cols
	psql -dwarrants -Uwarrants < sql/setup_extensions.sql
	ogr2ogr -f "PostgreSQL" PG:"dbname=warrants host=localhost user=warrants password=warrants" "data/grid_canvas_cropped.geojson" -nln grid_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=warrants host=localhost user=warrants password=warrants" "data/Boundaries - Central Business District.geojson" -nln cbd_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=warrants host=localhost user=warrants password=warrants" "data/Boundaries - Wards (2015-).geojson" -nln wards2015_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=warrants host=localhost user=warrants password=warrants" "data/Boundaries - Wards (2003-2015).geojson" -nln wards2003_geo -overwrite
	ogr2ogr -f "PostgreSQL" PG:"dbname=warrants host=localhost user=warrants password=warrants" "data/Boundaries - Census Tracts - 2010.geojson" -nln census_tracts_geo -overwrite
	./setup_geo.py

add_date_cols: data.conf
	./add_date_field_cols.py

data.conf: project_table
	./create_data_conf.py

project_table: data_file
	./create_and_populate_project_table.py

data_file: data_dirs
	ORIG_DATA_PATH=`jq -r '.original_data_path' project.conf`; \
	DATA_PATH=`jq -r '.data_path' project.conf`; \
	cp -p $$ORIG_DATA_PATH $$DATA_PATH

data_dirs: project.conf
        :
	#jq . project.conf | egrep '^  ".*_path":' | awk -F\" '{print $4}' | xargs -I {} dirname {} | xargs mkdir -p

project.conf: setup_database
	./generate_base_config.py

setup_database: 
	echo $(project_name)
	cat sql/clean.sql | sudo -u postgres psql -v project_name=warrants
