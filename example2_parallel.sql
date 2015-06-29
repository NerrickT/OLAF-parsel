--- setup the table to collect the results
DROP TABLE IF EXISTS pcd_fix;
CREATE TABLE pcd_fix (new_pcd CHAR(7), old_pcd CHAR(7), distance_meters REAL, distance_degrees REAL);

-- run the thing
SELECT parsel(
	'olaf',
	'ons_pd_short',
	'
	SELECT old_pcd, new_pcd, distance_meters, distance_degrees
		FROM
			(SELECT pcd AS old_pcd, geom
			FROM ons_pd_short) AS a,

			LATERAL (SELECT new_pcd, ST_Distance_Sphere(a.geom, geom) AS distance_meters, ST_Distance(a.geom, geom) AS distance_degrees
			FROM temp_active_postcodes
			WHERE ST_DWithin(a.geom, geom, 0.2)
			ORDER BY distance_meters ASC
			LIMIT 1) AS b;
	',
	'pcd_fix',
	'',
	3);

-- This, chunking into 3 parts a 10,000 records _ons_pd_short_, takes 1:15 on a MacBook Pro 15" (Mid-2015) 2.8 GHz
-- Intel Core i7, 16Gb RAM.
