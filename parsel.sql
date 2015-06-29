DROP FUNCTION IF EXISTS parsel(db TEXT, table_to_chunk TEXT, pkey TEXT, query TEXT, output_table TEXT, table_to_chunk_alias TEXT, num_chunks INTEGER);

CREATE OR REPLACE FUNCTION parsel (
		db                   TEXT,
		table_to_chunk       TEXT,
		pkey                 TEXT,
		query                TEXT,
		output_table         TEXT,
		table_to_chunk_alias TEXT DEFAULT '',
		num_chunks           INTEGER default 2
	)
	RETURNS text AS

$BODY$
DECLARE
	sql             TEXT;
	min_id          INTEGER;
	max_id          INTEGER;
	step_size       INTEGER;
	lbnd            INTEGER;
	ubnd            INTEGER;
	subquery        TEXT;
	insert_query    TEXT;
	i               INTEGER;
	conn            TEXT;
	n               INTEGER;
	num_done        INTEGER;
	status          INTEGER;
	dispatch_result INTEGER;
	dispatch_error  TEXT;
	part            TEXT;
	rand            TEXT;

BEGIN

	--find minimum pkey id
	EXECUTE 'SELECT MIN(' || pkey || ') FROM ' || table_to_chunk || ';' INTO min_id;
	--find maximum pkey id
	EXECUTE 'SELECT MAX(' || pkey || ') FROM ' || table_to_chunk || ';' INTO max_id;
	-- determine size of chunks based on min id, max id and number of chunks
	-- note that the _+1_ below is necessary as all variables are INTEGER and the division rounds to the floor
	EXECUTE 'SELECT (' || max_id || ' - ' || min_id || ' + 1) / ' || num_chunks || ' + 1;' INTO step_size;

	i := 0;
	FOR lbnd IN SELECT generate_series(min_id,  max_id, step_size) AS lbnd
	LOOP
		i := i + 1;
		ubnd := lbnd + step_size;
		conn := 'conn_' || i;
		RAISE NOTICE 'Chunk %: % >= % and % < %', i, pkey, lbnd, pkey, ubnd;
		--create a new db connection
		EXECUTE 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ', ' || QUOTE_LITERAL('dbname=' || db) ||');';
		-- create a subquery string that will replace the table name in the original query
		IF table_to_chunk_alias = '' THEN
			EXECUTE 'SELECT ''squery'' || ((10000*random())::integer::text);' INTO table_to_chunk_alias;
		END IF;
		part := '(SELECT * FROM ' || table_to_chunk || ' WHERE ' || pkey || ' >= ' || lbnd || ' AND ' || pkey || ' < ' || ubnd || ') AS ' || table_to_chunk_alias;
		--edit the input query using the subsquery string
		EXECUTE
			'SELECT REPLACE(' || QUOTE_LITERAL(query) || ',' || QUOTE_LITERAL(table_to_chunk) || ',' || QUOTE_LITERAL(part) || ')'
			INTO subquery;
		insert_query := 'INSERT INTO ' || output_table || ' ' || subquery || ';';
		RAISE NOTICE '%', insert_query;
		--send the query asynchronously using the dblink connection
		EXECUTE
			'SELECT dblink_send_query(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(insert_query) || ');'
			INTO dispatch_result;
		-- check for errors dispatching the query
		IF dispatch_result = 0 THEN
			EXECUTE
				'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');'
				INTO dispatch_error;
			RAISE '%', dispatch_error;
		END IF;
	END LOOP;
	-- wait until all queries are finished
	LOOP
		num_done := 0;
		FOR i IN 1..num_chunks LOOP
			conn := 'conn_' || i;
			EXECUTE
				'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');'
				INTO status;
			IF status = 0 THEN
				-- check for error messages
				EXECUTE
					'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');'
					INTO dispatch_error;
				IF dispatch_error <> 'OK' THEN
					RAISE '%', dispatch_error;
				END IF;
				num_done := num_done + 1;
			END IF;
		END LOOP;
		IF num_done >= num_chunks THEN
			EXIT;
		END IF;
	END LOOP;
	-- disconnect the dblinks
	FOR i IN 1..num_chunks LOOP
		EXECUTE 'SELECT dblink_disconnect(' || QUOTE_LITERAL('conn_' || i) || ');';
	END LOOP;
	RETURN 'Success';

	-- error catching to disconnect dblink connections, if error occurs
	EXCEPTION WHEN OTHERS THEN
		BEGIN
			RAISE NOTICE '% %', SQLERRM, SQLSTATE;
			FOR n IN SELECT generate_series(1, i) AS n LOOP
				EXECUTE 'SELECT dblink_disconnect(' || QUOTE_LITERAL('conn_' || n) || ');';
			END LOOP;
			EXCEPTION WHEN OTHERS THEN
				RAISE NOTICE '% %', SQLERRM, SQLSTATE;
	  	END;

END
$BODY$

LANGUAGE plpgsql STABLE
COST 100;
