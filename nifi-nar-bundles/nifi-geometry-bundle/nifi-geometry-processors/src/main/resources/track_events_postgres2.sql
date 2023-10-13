-- FUNCTION: public.f_for_nifi_event_track2()
-- DROP FUNCTION public.f_for_nifi_event_track2();

-- For case of Postgres table with composite key

CREATE FUNCTION public.f_for_nifi_event_track2()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    table_name_main varchar;
    table_name_events varchar;
    allkey TEXT[];
	values_event varchar;
	
	key_values varchar;
	key_columns varchar;
	where_clause varchar;
	key_to_in text;
	k text;
    v text;
	b BOOLEAN;
	fpos BOOLEAN;
	
	
BEGIN
    table_name_main = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
    table_name_events = TG_TABLE_SCHEMA || '.nifi_' || TG_TABLE_NAME;
	allkey = (SELECT ARRAY(SELECT a.attname
	FROM   pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE  i.indrelid = table_name_main::regclass
	AND    i.indisprimary));
	
	fpos = false;
	FOR k, v IN SELECT * FROM json_each_text(row_to_json(OLD))
	LOOP
		b = (SELECT k = ANY (allkey));
		IF (b = 't') THEN
			IF (fpos = 'f') THEN
				key_columns  = k;
				key_values   = '''' || v || '''';
				where_clause = k || ' = ' || '''' || v || '''';
				fpos = 't';				
			ELSE
				key_columns  = key_columns  || ',' || k;
				key_values   = key_values   || ',' || '''' || v || '''';
				where_clause = where_clause || ' AND ' || k || ' = ' || '''' || v || '''';
			END IF;
		END IF;	
	END LOOP;

	EXECUTE 'SELECT ' || key_columns || ' FROM ' || table_name_events || ' WHERE ' || where_clause INTO key_to_in USING OLD;
    IF (TG_OP = 'DELETE') THEN
        values_event := 'd';
		IF (key_to_in is NULL) THEN
			EXECUTE 'INSERT INTO ' || table_name_events || '(' || key_columns || ',event) VALUES (' || key_values || ',''' || values_event || ''');' USING OLD;
		ELSE
			EXECUTE 'UPDATE ' || table_name_events || ' SET event = ''' || values_event || ''', changed = ''' || CURRENT_TIMESTAMP || ''' WHERE ' || where_clause || ''';' USING OLD;
		END IF;
    END IF;
	
	IF (TG_OP = 'UPDATE') THEN
        values_event := 'u';
		IF (key_to_in is NULL) THEN
			EXECUTE 'INSERT INTO ' || table_name_events || '(' || key_columns || ',event) VALUES (' || key_values || ',''' || values_event || ''');' USING OLD;
		ELSE
			EXECUTE 'UPDATE ' || table_name_events || ' SET changed = ''' || CURRENT_TIMESTAMP || ''' WHERE ' || where_clause || ''';' USING OLD;
		END IF;
    END IF;
	
    IF (TG_OP = 'DELETE') THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$BODY$;

ALTER FUNCTION public.f_for_nifi_event_track2()
    OWNER TO postgres;


CREATE TRIGGER _nifi_track_events
    AFTER DELETE OR UPDATE 
    ON public.compositekey
    FOR EACH ROW
    EXECUTE FUNCTION public.f_for_nifi_event_track2();