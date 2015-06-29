/* This example is designed to work on the ons_pd table created by using
   https://github.com/sociam/OLAF-import-ons_pd */

DROP TABLE IF EXISTS ons_pd_short;
CREATE TABLE ons_pd_short AS (SELECT * FROM ons_pd WHERE doterm IS NOT NULL LIMIT 10000);
CREATE INDEX ons_pd_short_idx_geom ON ons_pd_short USING GIST(geom);

DROP TABLE IF EXISTS temp_active_postcodes;
CREATE TABLE temp_active_postcodes AS (SELECT pcd AS new_pcd, geom FROM ons_pd WHERE doterm IS NULL);
CREATE INDEX temp_active_postcodes_idx_geom ON temp_active_postcodes USING GIST(geom);
