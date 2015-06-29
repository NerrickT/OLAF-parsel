/* This trivial example works on the ons_pd table created by using https://github.com/sociam/OLAF-import-ons_pd and
   adding a convenience _INTEGER_ column _id_ as _PRIMARY KEY_. */
DROP TABLE IF EXISTS temp_output;
CREATE TABLE temp_output (no_of_records INTEGER);
SELECT parsel(
	'olaf',
	'ons_pd',
	'id',
	'select count(a.id) as no_of_records from ons_pd;',
	'temp_output',
	'a',
	2);
SELECT sum(no_of_records) FROM temp_output;
