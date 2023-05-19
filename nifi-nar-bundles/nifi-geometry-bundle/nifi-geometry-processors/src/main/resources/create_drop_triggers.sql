/* Create event tracker trigger from source table , ex public.src_geo */
CREATE TRIGGER _nifi_track_events
    AFTER DELETE OR UPDATE 
    ON public.src_geo
    FOR EACH ROW
    EXECUTE FUNCTION public.f_for_nifi_event_track();

/* Create event tracker table with prefix nifi_ */    
CREATE TABLE public.nifi_src_geo
(
    id integer NOT NULL DEFAULT nextval('county_id_seq'::regclass),
    event character(1) COLLATE pg_catalog."default",
    changed timestamp without time zone NOT NULL DEFAULT now()
)
TABLESPACE pg_default;
ALTER TABLE public.nifi_src_geo
    OWNER to postgres;
GRANT ALL ON TABLE public.nifi_src_geo TO postgres;
GRANT SELECT ON TABLE public.nifi_src_geo TO replicator;    

/* Change Primary Key to nifiuid */
select concat('alter table public.target_geo drop constraint ', constraint_name) as dropstatement
from information_schema.table_constraints where table_schema = 'public' and table_name = 'target_geo' and constraint_type = 'PRIMARY KEY';
alter table public.target_geo drop constraint target_geo_pkey
ALTER TABLE public.target_geo ADD PRIMARY KEY (nifiuid)