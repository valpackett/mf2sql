-------------------------------------------------------------------------------------------- Notifications
CREATE OR REPLACE FUNCTION mf2.objects_notify() RETURNS trigger AS $$
BEGIN
	CASE TG_OP
		WHEN 'INSERT' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'insert', 'url', ARRAY[substring(NEW.properties->'url'->>0 for 3000)])::text);
		WHEN 'UPDATE' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'update', 'url', ARRAY[substring(NEW.properties->'url'->>0 for 3000)])::text);
		WHEN 'DELETE' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'delete', 'url', ARRAY[substring(OLD.properties->'url'->>0 for 3000)])::text);
	END CASE;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS objects_notify_trigger ON mf2.objects;

CREATE TRIGGER objects_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON mf2.objects
FOR EACH ROW EXECUTE PROCEDURE mf2.objects_notify();
