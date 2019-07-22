--TODO: GENERALIZE THIS

DROP MATERIALIZED VIEW IF EXISTS warrants_mv ;

CREATE MATERIALIZED VIEW warrants_mv AS
SELECT 
  was_arrested_tbl.was_arrested,
  warrants.date,
  warrants.grid_id,
  warrants.is_business_district,
  ward2015_tbl.ward2015,
--  ward2003_tbl.ward2003,
  date_day_tbl.date_day,
  address_merged_tbl.address_merged,
  census_tract_tbl.census_tract
FROM warrants
LEFT OUTER JOIN was_arrested_tbl on was_arrested_tbl.id = warrants.was_arrested
LEFT OUTER JOIN date_day_tbl on date_day_tbl.id = warrants.date_day
LEFT OUTER JOIN ward2015_tbl on ward2015_tbl.id = warrants.ward2015
--LEFT OUTER JOIN ward2003_tbl on ward2003_tbl.id = warrants.ward2003
LEFT OUTER JOIN address_merged_tbl on address_merged_tbl.id = warrants.address_merged
LEFT OUTER JOIN census_tract_tbl on census_tract_tbl.id = warrants.census_tract
ORDER BY date ;
