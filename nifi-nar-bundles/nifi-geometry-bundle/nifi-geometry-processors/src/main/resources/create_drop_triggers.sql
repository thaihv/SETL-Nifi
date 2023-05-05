CREATE TRIGGER _nifi_track_events
    AFTER DELETE OR UPDATE 
    ON public.src_geo
    FOR EACH ROW
    EXECUTE FUNCTION public.f_for_nifi_event_track();