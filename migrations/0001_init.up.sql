-- PostgreSQL: the best Microformats2 JSON object store

CREATE TABLE objects (
	type text[] NOT NULL,
	properties jsonb NOT NULL DEFAULT '{}',
	children jsonb[],
	deleted boolean NOT NULL DEFAULT False,
	tsv tsvector
);

CREATE FUNCTION cast_timestamp(data text) RETURNS timestamptz AS $$
BEGIN
	RETURN data::timestamp;
EXCEPTION WHEN others THEN RETURN NULL;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE UNIQUE INDEX url_idx ON objects ((properties->'url'->>0));
CREATE INDEX pub_time_idx ON objects (cast_timestamp(properties->'published'->>0));
CREATE INDEX properties_idx ON objects USING GIN(properties jsonb_path_ops);


-------------------------------------------------------------------------------------------- Full Text Search
CREATE INDEX tsv_idx ON objects USING GIST(tsv);

CREATE FUNCTION flatten_jsonb(data jsonb) RETURNS SETOF jsonb AS $$
	-- based on https://stackoverflow.com/a/27742278/239140
	-- modified to use jsonb_typeof
	WITH RECURSIVE deconstruct (jsonlevel) AS (
		VALUES (data)
		UNION ALL
		SELECT
			CASE jsonb_typeof(jsonlevel)
			WHEN 'object' THEN (jsonb_each(jsonlevel)).value
			WHEN 'array' THEN jsonb_array_elements(jsonlevel)
			END AS jsonlevel
		FROM deconstruct
		WHERE jsonb_typeof(jsonlevel) IN ('object', 'array')
	) SELECT * FROM deconstruct;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION tsv_from_jsonb(data jsonb) RETURNS tsvector AS $$
	SELECT to_tsvector(coalesce(string_agg(flat::text, ' '), ''))
	FROM flatten_jsonb(data) flat
	WHERE jsonb_typeof(flat) = 'string';
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION objects_set_tsv() RETURNS trigger AS $$
BEGIN
	NEW.tsv :=
	   setweight(tsv_from_jsonb(NEW.properties->'name'), 'A')
	|| setweight(tsv_from_jsonb(NEW.properties->'summary'), 'B')
	|| setweight(tsv_from_jsonb(NEW.properties->'content'), 'B')
	;
	RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER objects_set_tsv_trigger
BEFORE INSERT OR UPDATE ON objects
FOR EACH ROW EXECUTE PROCEDURE objects_set_tsv();

CREATE FUNCTION objects_search(query text) RETURNS TABLE(type text[], properties jsonb, rank float4) AS $$
	SELECT type, properties, ts_rank_cd(tsv, querytsv) AS rank
	FROM objects, to_tsquery(query) querytsv
	WHERE querytsv @@ tsv
	ORDER BY rank DESC;
$$ LANGUAGE sql;


-------------------------------------------------------------------------------------------- Notifications
CREATE FUNCTION objects_notify() RETURNS trigger AS $$
BEGIN
	CASE TG_OP
		WHEN 'INSERT' THEN PERFORM pg_notify('objects',
			json_build_object('op', 'insert', 'url', NEW.properties->'url')::text);
		WHEN 'UPDATE' THEN PERFORM pg_notify('objects',
			json_build_object('op', 'update', 'url', NEW.properties->'url')::text);
		WHEN 'DELETE' THEN PERFORM pg_notify('objects',
			json_build_object('op', 'delete', 'url', OLD.properties->'url')::text);
	END CASE;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER objects_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON objects
FOR EACH ROW EXECUTE PROCEDURE objects_notify();


-------------------------------------------------------------------------------------------- Denormalization
CREATE FUNCTION objects_denormalize(data jsonb) RETURNS jsonb AS $$
DECLARE
	result jsonb;
BEGIN
	CASE jsonb_typeof(data)
	WHEN 'object' THEN RETURN (SELECT jsonb_object_agg(key, CASE key WHEN 'url' THEN value ELSE objects_denormalize(value) END) FROM jsonb_each(data));
	WHEN 'array'  THEN RETURN (SELECT jsonb_agg(objects_denormalize(value)) FROM jsonb_array_elements(data));
	WHEN 'string' THEN
		SELECT jsonb_build_object('type', type, 'properties', objects_denormalize(properties), 'children', objects_denormalize(children))
		INTO result
		FROM objects
		WHERE properties->'url'->0 = data;
		IF FOUND THEN
			RETURN result;
		ELSE
			RETURN data;
		END IF;
	ELSE RETURN data;
	END CASE;
END
$$ LANGUAGE plpgsql;


-------------------------------------------------------------------------------------------- Normalization
CREATE FUNCTION _objects_normalize_inner(data jsonb) RETURNS jsonb AS $$
BEGIN
	CASE jsonb_typeof(data)
	WHEN 'object' THEN
		IF data->'type' IS NOT NULL AND data->'properties'->'url'->>0 IS NOT NULL THEN
			INSERT INTO _objects_normalize_temp
			SELECT jsonb_build_object(
				'type', data->'type',
				'properties', _objects_normalize_inner(data->'properties'),
				'children', _objects_normalize_inner(data->'children')
			);
			RETURN data->'properties'->'url'->0;
		ELSE
			RETURN (SELECT jsonb_object_agg(key, CASE key WHEN 'url' THEN value ELSE _objects_normalize_inner(value) END) FROM jsonb_each(data));
		END IF;
	WHEN 'array' THEN RETURN (SELECT jsonb_agg(_objects_normalize_inner(value)) FROM jsonb_array_elements(data));
	ELSE RETURN data;
	END CASE;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION objects_normalize(data jsonb) RETURNS SETOF jsonb AS $$
DECLARE
	result jsonb;
BEGIN
	DROP TABLE IF EXISTS _objects_normalize_temp;
	CREATE TEMPORARY TABLE _objects_normalize_temp (data jsonb NOT NULL) ON COMMIT DROP;
	SELECT _objects_normalize_inner(data) INTO result;
	RETURN QUERY SELECT _objects_normalize_temp.data FROM _objects_normalize_temp UNION VALUES (result);
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION jsonb_array_to_pg_array(data jsonb) RETURNS jsonb[] AS $$
	SELECT CASE data
	WHEN NULL THEN NULL
	WHEN 'null'::jsonb THEN NULL
	ELSE (SELECT array_agg(x)::jsonb[] FROM jsonb_array_elements(data) AS x)
	END;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION jsonb_array_to_pg_array_of_text(data jsonb) RETURNS text[] AS $$
	SELECT CASE data
	WHEN NULL THEN NULL
	WHEN 'null'::jsonb THEN NULL
	ELSE (SELECT array_agg(x) FROM jsonb_array_elements_text(data) AS x)
	END;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION objects_normalized_upsert(data jsonb) RETURNS void AS $$
	INSERT INTO objects (type, properties, children)
	SELECT
		-- distinct because upsert (ON CONFLICT UPDATE) doesn't support affecting one row twice
		DISTINCT ON ((objects_normalize->'properties'->'url'->>0))
		(SELECT jsonb_array_to_pg_array_of_text(objects_normalize->'type')),
		objects_normalize->'properties',
		(SELECT jsonb_array_to_pg_array(objects_normalize->'children'))
	FROM objects_normalize(data)
	WHERE (objects_normalize->'properties'->'url'->>0) IS NOT NULL
	ON CONFLICT ((properties->'url'->>0))
	DO UPDATE SET type = EXCLUDED.type, properties = EXCLUDED.properties, children = EXCLUDED.children;
$$ LANGUAGE sql;
