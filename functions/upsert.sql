-------------------------------------------------------------------------------------------- Normalization

CREATE OR REPLACE FUNCTION mf2._objects_normalize_inner(data jsonb) RETURNS jsonb AS $$
BEGIN
	CASE jsonb_typeof(data)
	WHEN 'object' THEN
		IF data->'type' IS NOT NULL AND data->'properties'->'url'->>0 IS NOT NULL THEN
			INSERT INTO _objects_normalize_temp
			SELECT jsonb_build_object(
				'type', data->'type',
				'properties', mf2._objects_normalize_inner(data->'properties'),
				'children', mf2._objects_normalize_inner(data->'children'),
				'acl', data->'acl',
				'deleted', data->'deleted'
			);
			RETURN data->'properties'->'url'->0;
		END IF;
		RETURN (SELECT jsonb_object_agg(key, CASE key WHEN 'url' THEN value WHEN 'uid' THEN value ELSE mf2._objects_normalize_inner(value) END) FROM jsonb_each(data));
	WHEN 'array' THEN RETURN (SELECT jsonb_agg(mf2._objects_normalize_inner(value)) FROM jsonb_array_elements(data));
	ELSE RETURN data;
	END CASE;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mf2.objects_normalize(data jsonb) RETURNS SETOF jsonb AS $$
DECLARE
	result jsonb;
BEGIN
	IF (SELECT to_regclass('_objects_normalize_temp')) IS NULL THEN
		CREATE TEMPORARY TABLE _objects_normalize_temp (data jsonb);
	ELSE
		TRUNCATE TABLE _objects_normalize_temp;
	END IF;
	SELECT mf2._objects_normalize_inner(data) INTO result;
	RETURN QUERY SELECT _objects_normalize_temp.data FROM _objects_normalize_temp UNION VALUES (result);
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION mf2.objects_normalize(data jsonb) IS $$
	Recursively un-embeds embedded objects, flattening the hierarchy.
$$;

CREATE OR REPLACE FUNCTION mf2.jsonb_array_to_pg_array(data jsonb) RETURNS jsonb[] AS $$
	SELECT CASE data
	WHEN NULL THEN NULL
	WHEN 'null'::jsonb THEN NULL
	ELSE (SELECT array_agg(x)::jsonb[] FROM jsonb_array_elements(data) AS x)
	END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION mf2.jsonb_array_to_pg_array_of_text(data jsonb) RETURNS text[] AS $$
	SELECT CASE data
	WHEN NULL THEN NULL
	WHEN 'null'::jsonb THEN NULL
	ELSE (SELECT array_agg(x) FROM jsonb_array_elements_text(data) AS x)
	END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION mf2.objects_normalized_upsert(data jsonb) RETURNS void AS $$
	INSERT INTO mf2.objects (type, properties, children, acl)
	SELECT
		-- distinct because upsert (ON CONFLICT UPDATE) doesn't support affecting one row twice
		DISTINCT ON ((objects_normalize->'properties'->'url'->>0))
		(SELECT mf2.jsonb_array_to_pg_array_of_text(objects_normalize->'type')),
		objects_normalize->'properties',
		(SELECT mf2.jsonb_array_to_pg_array(objects_normalize->'children')),
		(CASE WHEN jsonb_typeof(objects_normalize->'acl') = 'array'
			THEN (SELECT array_agg(x) FROM (SELECT jsonb_array_elements_text(objects_normalize->'acl') x) x)
			ELSE '{*}' END)
	FROM mf2.objects_normalize(data)
	WHERE (objects_normalize->'properties'->'url'->>0) IS NOT NULL
	ON CONFLICT ((properties->'url'->>0))
	DO UPDATE SET type = EXCLUDED.type, properties = EXCLUDED.properties, children = EXCLUDED.children, acl = EXCLUDED.acl;
$$ LANGUAGE sql;

COMMENT ON FUNCTION mf2.objects_normalized_upsert(data jsonb) IS $$
	Upserts an object and its embedded objects after flattening the hierarchy.
$$;
