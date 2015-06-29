DROP TABLE IF EXISTS pcd_fix;
CREATE TABLE pcd_fix AS (
	SELECT old_pcd, new_pcd, distance_meters, distance_degrees
		FROM
			(SELECT pcd AS old_pcd, geom
			FROM ons_pd_short) AS a,

			LATERAL (SELECT new_pcd, ST_Distance_Sphere(a.geom, geom) AS distance_meters, ST_Distance(a.geom, geom) AS distance_degrees
			FROM temp_active_postcodes
			WHERE ST_DWithin(a.geom, geom, 0.2)
			ORDER BY distance_meters ASC
			LIMIT 1) AS b
);

-- This takes 2:30 on a MacBook Pro 15" (Mid-2015) 2.8 GHz Intel Core i7, 16Gb RAM
