DROP FUNCTION IF EXISTS parsel(db TEXT, table_to_chunk TEXT, query TEXT, output_table TEXT, table_to_chunk_alias TEXT, num_chunks INTEGER);

CREATE OR REPLACE FUNCTION parsel (
		db                   TEXT,
		table_to_chunk       TEXT,
		query                TEXT,
		output_table         TEXT,
		table_to_chunk_alias TEXT DEFAULT '',
		num_chunks           INTEGER default 2
	)
	RETURNS text AS

$BODY$
DECLARE
	sql             TEXT;
	no_of_records   INTEGER;
	step_size       INTEGER;
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

	EXECUTE 'SELECT COUNT(*) FROM ' || table_to_chunk || ';' INTO no_of_records;
	-- note that the _+1_ below is necessary as the variables are INTEGER and the division rounds to the floor
	EXECUTE 'SELECT ' || no_of_records || ' / ' || num_chunks || ' + 1;' INTO step_size;

	FOR i IN 1..num_chunks LOOP
		conn := 'conn_' || i;
		RAISE NOTICE 'Chunk %', i;
		--create a new db connection
		EXECUTE 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ', ' || QUOTE_LITERAL('dbname=' || db) ||');';
		-- create a subquery string that will replace the table name in the original query
		IF table_to_chunk_alias = '' THEN
			EXECUTE 'SELECT ''squery'' || ((10000*random())::integer::text);' INTO table_to_chunk_alias;
		END IF;
		part := '(SELECT * FROM ' || table_to_chunk || ' LIMIT ' || step_size || ' OFFSET ' || ((i - 1) * step_size) || ') AS ' || table_to_chunk_alias;
		--edit the input query using the subsquery string
		-- TODO: check the REPLACE statement below: what if the _table_to_chunk_ string is part of some other name
		--       used in the query, too? It is likely I need to check for whole words instead.
		EXECUTE
			'SELECT REPLACE(' || QUOTE_LITERAL(query) || ',' || QUOTE_LITERAL(table_to_chunk) || ',' || QUOTE_LITERAL(part) || ')'
			INTO subquery;
		insert_query := 'INSERT INTO ' || output_table || ' ' || subquery || ';';
		-- RAISE NOTICE '%', insert_query;
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
