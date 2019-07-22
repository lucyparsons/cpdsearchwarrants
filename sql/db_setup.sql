--DELETE FROM towing WHERE created_date < '2010-01-01' OR created_date IS NULL or tow_reason is null ;
UPDATE towing SET tow_reason = trim(both ', -' from tow_reason) ;

--UPDATE towing SET tow_reason = replace(tow_reason, '- Vehicle Impoundment', '') ;
--UPDATE towing SET tow_reason = trim(both ' -,' from replace(tow_reason, '- Immediate', '')) ;
--UPDATE towing SET tow_reason = trim(both ' -,' from regexp_replace(tow_reason, '[ \-]{1,},', ',')) ;
--UPDATE towing SET tow_reason = trim(both ' -,' from regexp_replace(tow_reason, '[ \-]{1,},', ',')) ;
--UPDATE towing SET tow_reason = trim(both ' -,' from regexp_replace(tow_reason, '[ \-]{1,},', ',')) ;
--

--WITH unique_rows AS (
  --select split_part(tow_reason, ',', 1) as sp FROM towing 
  --union 
  --select split_part(tow_reason, ',', 2) as sp from towing
  --union 
  --select split_part(tow_reason, ',', 3) as sp from towing
  --union 
  --select split_part(tow_reason, ',', 4) as sp from towing
--) 
--INSERT INTO tow_reason_tbl (tow_reason) 
  --SELECT * from unique_rows where sp != '';

--ALTER TABLE towing ADD COLUMN reason_1 TEXT ;
--ALTER TABLE towing ADD COLUMN reason_2 TEXT ;
--ALTER TABLE towing ADD COLUMN reason_3 TEXT ;
--ALTER TABLE towing ADD COLUMN reason_4 TEXT ;

--UPDATE towing SET reason_1 = split_part(towing.tow_reason, ',', 1) ;
--UPDATE towing SET reason_2 = split_part(towing.tow_reason, ',', 2) ;
--UPDATE towing SET reason_3 = split_part(towing.tow_reason, ',', 3) ;
----UPDATE towing SET reason_4 = split_part(towing.tow_reason, ',', 4) ;
--
--update towing set reason_1 = reason_2, reason_2 = reason_1 where reason_2 > reason_1 ;
--update towing set reason_2 = reason_3, reason_3 = reason_2 where reason_3 < reason_2 ;
--update towing set reason_3 = reason_4, reason_4 = reason_3 where reason_4 < reason_3 ;

ALTER TABLE towing ADD COLUMN is_business_district BOOLEAN ;

ALTER TABLE towing ADD COLUMN arrival_date_dow INTEGER ;
ALTER TABLE towing ADD COLUMN sale_created_date_dow INTEGER ;
ALTER TABLE towing ADD COLUMN sale_complete_date_dow INTEGER ;
ALTER TABLE towing ADD COLUMN created_date_dow INTEGER ;

UPDATE towing SET arrival_date_dow = EXTRACT('dow' FROM arrival_date) ;
UPDATE towing SET sale_created_date_dow = EXTRACT('dow' FROM sale_created_date) ;
UPDATE towing SET sale_complete_date_dow = EXTRACT('dow' FROM sale_complete_date) ;
UPDATE towing SET created_date_dow = EXTRACT('dow' FROM created_date) ;

ALTER TABLE towing ADD COLUMN arrival_date_month INTEGER ;
ALTER TABLE towing ADD COLUMN sale_date_created_month INTEGER ;
ALTER TABLE towing ADD COLUMN sale_date_complete_month INTEGER ;
ALTER TABLE towing ADD COLUMN created_date_month INTEGER ;

UPDATE towing SET arrival_date_month = EXTRACT('month' FROM arrival_date) ;
UPDATE towing SET sale_date_created_month = EXTRACT('month' FROM sale_created_date) ;
UPDATE towing SET sale_date_complete_month = EXTRACT('month' FROM sale_complete_date) ;
UPDATE towing SET created_date_month = EXTRACT('month' FROM created_date) ;

ALTER TABLE towing ADD COLUMN arrival_date_hour INTEGER ;
ALTER TABLE towing ADD COLUMN sale_created_date_hour INTEGER ;
ALTER TABLE towing ADD COLUMN sale_complete_date_hour INTEGER ;
ALTER TABLE towing ADD COLUMN created_date_hour INTEGER ;

UPDATE towing SET arrival_date_hour = EXTRACT('hour' FROM arrival_date) ;
UPDATE towing SET sale_created_date_hour = EXTRACT('hour' FROM sale_created_date) ;
UPDATE towing SET sale_complete_date_hour = EXTRACT('hour' FROM sale_complete_date) ;
UPDATE towing SET created_date_hour = EXTRACT('hour' FROM created_date) ;

ALTER TABLE towing ADD COLUMN arrival_date_year INTEGER ;
ALTER TABLE towing ADD COLUMN sale_created_date_year INTEGER ;
ALTER TABLE towing ADD COLUMN sale_complete_date_year INTEGER ;
ALTER TABLE towing ADD COLUMN created_date_year INTEGER ;

UPDATE towing SET arrival_date_year = EXTRACT('year' FROM arrival_date) ;
UPDATE towing SET sale_created_date_year = EXTRACT('year' FROM sale_created_date) ;
UPDATE towing SET sale_complete_date_year = EXTRACT('year' FROM sale_complete_date) ;
UPDATE towing SET created_date_year = EXTRACT('year' FROM created_date) ;

ALTER TABLE towing ADD COLUMN grid_id INTEGER ;

--Swap projection and normalize SRID
ALTER TABLE towing ADD COLUMN geom_point GEOMETRY ;

--for each missing address, check if there's a copy from another address, then update based on that
UPDATE towing 
SET x_coord = t3.x_coord,
    y_coord = t3.y_coord
FROM (
    SELECT DISTINCT(t.tow_location),t2.x_coord, t2.y_coord
    FROM towing t, (SELECT * FROM towing WHERE tow_location IS NOT NULL) t2 
    WHERE t2.tow_location = t.tow_location
    AND t.tow_location IS NOT NULL 
    AND t.x_coord IS NULL 
    AND t.y_coord IS NULL 
    AND t2.x_coord IS NOT NULL
    AND t2.y_coord IS NOT NULL
) t3 
WHERE t3.tow_location = towing.tow_location ;
