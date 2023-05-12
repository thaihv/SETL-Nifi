CREATE TRIGGER _nifi_track_events
    AFTER DELETE OR UPDATE 
    ON public.src_geo
    FOR EACH ROW
    EXECUTE FUNCTION public.f_for_nifi_event_track();
    
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