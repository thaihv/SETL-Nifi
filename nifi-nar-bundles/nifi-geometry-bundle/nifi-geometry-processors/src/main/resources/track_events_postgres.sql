-- FUNCTION: public.f_for_nifi_event_track()

-- DROP FUNCTION public.f_for_nifi_event_track();

CREATE FUNCTION public.f_for_nifi_event_track()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    table_name_main varchar;
    table_name_events varchar;
    column_key varchar;
	values_event varchar;
	key_to_in int;
	key_value int;
BEGIN
    table_name_main = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
    table_name_events = TG_TABLE_SCHEMA || '.nifi_' || TG_TABLE_NAME;
	column_key = (SELECT a.attname
	FROM   pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE  i.indrelid = table_name_main::regclass
	AND    i.indisprimary);

	EXECUTE 'SELECT ($1).' || column_key INTO key_value USING OLD;
	EXECUTE 'SELECT ' || column_key || ' FROM ' || table_name_events || ' WHERE ' || column_key || ' = ' || key_value INTO key_to_in USING OLD;

    IF (TG_OP = 'DELETE') THEN
        values_event := 'd';
		IF (key_to_in is NULL) THEN
			EXECUTE 'INSERT INTO ' || table_name_events || '(' || column_key || ',event) VALUES (' || key_value || ',''' || values_event || ''');' USING OLD;
		ELSE
			EXECUTE 'UPDATE ' || table_name_events || ' SET event = ''' || values_event || ''', changed = ''' || CURRENT_TIMESTAMP || ''' WHERE ' || column_key || ' = ''' || key_to_in || ''';' USING OLD;
		END IF;
    END IF;
	IF (TG_OP = 'UPDATE') THEN
        values_event := 'u';
		IF (key_to_in is NULL) THEN
			EXECUTE 'INSERT INTO ' || table_name_events || '(' || column_key || ',event) VALUES (' || key_value || ',''' || values_event || ''');' USING OLD;
		ELSE
			EXECUTE 'UPDATE ' || table_name_events || ' SET changed = ''' || CURRENT_TIMESTAMP || ''' WHERE ' || column_key || ' = ''' || key_to_in || ''';' USING OLD;
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

ALTER FUNCTION public.f_for_nifi_event_track()
    OWNER TO postgres;

COMMENT ON FUNCTION public.f_for_nifi_event_track()
    IS 'This function is called after a change is happening in a table to push the former values to the historic keeping table.';
