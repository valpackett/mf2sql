-- PostgreSQL: the best Microformats2 JSON object store

CREATE SCHEMA mf2;

CREATE TABLE mf2.objects (
	type text[] NOT NULL,
	properties jsonb NOT NULL DEFAULT '{}',
	children jsonb[],
	acl text[] NOT NULL DEFAULT '{*}',
	deleted boolean NOT NULL DEFAULT False,
	tsv tsvector
);

-- The function exists because index functions need to be IMMUTABLE.
-- Technically, this hack is bad because casting to timestamps actually depends on current settings, but you're not gonna change them, are you?
CREATE FUNCTION mf2.cast_timestamp(data text) RETURNS timestamptz AS $$
BEGIN
	RETURN data::timestamp;
EXCEPTION WHEN others THEN RETURN NULL;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE UNIQUE INDEX url_idx ON mf2.objects ((properties->'url'->>0));
CREATE INDEX pub_time_idx ON mf2.objects (mf2.cast_timestamp(properties->'published'->>0));
CREATE INDEX properties_idx ON mf2.objects USING GIN(properties jsonb_path_ops);


-------------------------------------------------------------------------------------------- Full Text Search
CREATE INDEX tsv_idx ON mf2.objects USING GIST(tsv);

CREATE FUNCTION mf2.flatten_jsonb(data jsonb) RETURNS SETOF jsonb AS $$
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

CREATE FUNCTION mf2.tsv_from_jsonb(data jsonb) RETURNS tsvector AS $$
	SELECT to_tsvector(coalesce(string_agg(flat::text, ' '), ''))
	FROM mf2.flatten_jsonb(data) flat
	WHERE jsonb_typeof(flat) = 'string';
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION mf2.objects_set_tsv() RETURNS trigger AS $$
BEGIN
	NEW.tsv :=
	   setweight(mf2.tsv_from_jsonb(NEW.properties->'name'), 'A')
	|| setweight(mf2.tsv_from_jsonb(NEW.properties->'item'), 'A')
	|| setweight(mf2.tsv_from_jsonb(NEW.properties->'summary'), 'B')
	|| setweight(mf2.tsv_from_jsonb(NEW.properties->'content'), 'B')
	;
	RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER objects_set_tsv_trigger
BEFORE INSERT OR UPDATE ON mf2.objects
FOR EACH ROW EXECUTE PROCEDURE mf2.objects_set_tsv();


-------------------------------------------------------------------------------------------- Notifications
CREATE FUNCTION mf2.objects_notify() RETURNS trigger AS $$
BEGIN
	CASE TG_OP
		WHEN 'INSERT' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'insert', 'url', NEW.properties->'url')::text);
		WHEN 'UPDATE' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'update', 'url', NEW.properties->'url')::text);
		WHEN 'DELETE' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'delete', 'url', OLD.properties->'url')::text);
	END CASE;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER objects_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON mf2.objects
FOR EACH ROW EXECUTE PROCEDURE mf2.objects_notify();


-------------------------------------------------------------------------------------------- Denormalization
-- NOTE: don't bother trying to optimize the performance here, this function cannot be super instant.
-- The time is taken by literally just looking up the URLs. There's just, like, many of them.
-- Recursion in PL/pgSQL and the temporary table are *not slow*!
-- I've tried making a PLV8 implementation (with the tree walk and visited set inside of JS), it was actually a bit slower.
-- It's a bit faster without storing visited URLs, but we need it to prevent infinite recursion.
CREATE FUNCTION mf2._objects_denormalize_inner(data jsonb, lvl int) RETURNS jsonb AS $$
DECLARE
	result jsonb;
BEGIN
	IF lvl <= 0 THEN
		RETURN data;
	END IF;
	CASE jsonb_typeof(data)
	WHEN 'string' THEN
		IF EXISTS (SELECT 1 FROM _objects_denormalize_temp WHERE url = trim(both from data::text, '"')) THEN
			RETURN data;
		END IF;
		INSERT INTO _objects_denormalize_temp VALUES (data::text);
		SELECT jsonb_build_object('type', type, 'properties', mf2._objects_denormalize_inner(properties, lvl + 1), 'children', mf2._objects_denormalize_inner(to_jsonb(children), lvl + 1), 'acl', acl, 'deleted', deleted)
		INTO result
		FROM mf2.objects
		WHERE properties->'url'->>0 = trim(both from data::text, '"');
		IF FOUND THEN
			RETURN result;
		END IF;
		RETURN data;
	WHEN 'object' THEN
		INSERT INTO _objects_denormalize_temp
		SELECT jsonb_array_elements_text(data->'url')
		UNION ALL SELECT jsonb_array_elements_text(data->'uid');
		RETURN (
			SELECT
				jsonb_object_agg(key, CASE
					WHEN key = 'url' OR key = 'uid' THEN value
					WHEN (jsonb_typeof(value) = 'string' AND NOT value::text LIKE '"http%') THEN value
					ELSE mf2._objects_denormalize_inner(value, lvl + 1)
				END)
			FROM jsonb_each(data)
		);
	WHEN 'array' THEN
		RETURN (
			SELECT
				jsonb_agg(CASE
					WHEN jsonb_typeof(value) = 'string' AND NOT value::text LIKE '"http%' THEN value
					ELSE mf2._objects_denormalize_inner(value, lvl + 1)
				END)
			FROM jsonb_array_elements(data)
		);
	ELSE
		RETURN data;
	END CASE;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION mf2.objects_denormalize(data jsonb, lvl int) RETURNS jsonb AS $$
BEGIN
	IF (SELECT to_regclass('_objects_denormalize_temp')) IS NULL THEN
		CREATE TEMPORARY TABLE _objects_denormalize_temp (url text);
		CREATE INDEX ON _objects_denormalize_temp (url);
	ELSE
		TRUNCATE TABLE _objects_denormalize_temp;
	END IF;
	RETURN (SELECT mf2._objects_denormalize_inner(data, lvl));
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION mf2.objects_denormalize(data jsonb) RETURNS jsonb AS $$
	SELECT mf2.objects_denormalize(data, coalesce(current_setting('mf2sql.denormalize_default_depth_limit', true)::int, 64));
$$ LANGUAGE sql;
COMMENT ON FUNCTION mf2.objects_denormalize(data jsonb) IS $$
	Recursively embeds other objects referenced by URL, embedding each object only once and stopping recursion after a depth limit, determined by the mf2sql.denormalize_default_depth_limit setting.
$$;

CREATE FUNCTION mf2.objects_denormalize_unlimited(data jsonb) RETURNS jsonb AS $$
DECLARE
	result jsonb;
BEGIN
	CASE jsonb_typeof(data)
	WHEN 'string' THEN
		SELECT jsonb_build_object('type', type, 'properties', mf2.objects_denormalize_unlimited(properties), 'children', mf2.objects_denormalize_unlimited(to_jsonb(children)), 'acl', acl, 'deleted', deleted)
		INTO result
		FROM mf2.objects
		WHERE properties->'url'->>0 = trim(both from data::text, '"');
		IF FOUND THEN
			RETURN result;
		END IF;
		RETURN data;
	WHEN 'object' THEN RETURN (
		SELECT
			jsonb_object_agg(key, CASE
				WHEN key = 'url' OR key = 'uid' THEN value
				ELSE mf2.objects_denormalize_unlimited(value)
			END)
		FROM jsonb_each(data)
	);
	WHEN 'array' THEN RETURN (SELECT jsonb_agg(mf2.objects_denormalize_unlimited(value)) FROM jsonb_array_elements(data));
	ELSE RETURN data;
	END CASE;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION mf2.objects_denormalize_unlimited(data jsonb) IS $$
	Recursively embeds other objects referenced by URL.
	Will loop infinitely on circular references, so not use.
	Use the not-_unlimited variant instead.
$$;

-------------------------------------------------------------------------------------------- Normalization
CREATE FUNCTION mf2._objects_normalize_inner(data jsonb) RETURNS jsonb AS $$
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

CREATE FUNCTION mf2.objects_normalize(data jsonb) RETURNS SETOF jsonb AS $$
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

CREATE FUNCTION mf2.jsonb_array_to_pg_array(data jsonb) RETURNS jsonb[] AS $$
	SELECT CASE data
	WHEN NULL THEN NULL
	WHEN 'null'::jsonb THEN NULL
	ELSE (SELECT array_agg(x)::jsonb[] FROM jsonb_array_elements(data) AS x)
	END;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION mf2.jsonb_array_to_pg_array_of_text(data jsonb) RETURNS text[] AS $$
	SELECT CASE data
	WHEN NULL THEN NULL
	WHEN 'null'::jsonb THEN NULL
	ELSE (SELECT array_agg(x) FROM jsonb_array_elements_text(data) AS x)
	END;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION mf2.objects_normalized_upsert(data jsonb) RETURNS void AS $$
	INSERT INTO mf2.objects (type, properties, children)
	SELECT
		-- distinct because upsert (ON CONFLICT UPDATE) doesn't support affecting one row twice
		DISTINCT ON ((objects_normalize->'properties'->'url'->>0))
		(SELECT mf2.jsonb_array_to_pg_array_of_text(objects_normalize->'type')),
		objects_normalize->'properties',
		(SELECT mf2.jsonb_array_to_pg_array(objects_normalize->'children'))
	FROM mf2.objects_normalize(data)
	WHERE (objects_normalize->'properties'->'url'->>0) IS NOT NULL
	ON CONFLICT ((properties->'url'->>0))
	DO UPDATE SET type = EXCLUDED.type, properties = EXCLUDED.properties, children = EXCLUDED.children;
$$ LANGUAGE sql;
COMMENT ON FUNCTION mf2.objects_normalized_upsert(data jsonb) IS $$
	Upserts and object and its embedded objects after flattening the hierarchy.
$$;


-------------------------------------------------------------------------------------------- Fetching
CREATE FUNCTION mf2.substitute_params(data jsonb, params jsonb) RETURNS jsonb AS $$
DECLARE
	temp text;
BEGIN
	CASE jsonb_typeof(data)
	WHEN 'string' THEN
		SELECT params->trim(both from trim(both from data::text, '"'), '{}') INTO temp;
		IF temp IS NULL THEN
			RETURN data;
		ELSE
			RETURN temp;
		END IF;
	WHEN 'object' THEN RETURN (SELECT jsonb_object_agg(key, mf2.substitute_params(value, params)) FROM jsonb_each(data));
	WHEN 'array'  THEN RETURN (SELECT jsonb_agg(mf2.substitute_params(value, params)) FROM jsonb_array_elements(data));
	ELSE RETURN data;
	END CASE;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION mf2.objects_smart_fetch(uri text, uri_prefix text, lim int, before timestamptz, after timestamptz, params jsonb) RETURNS jsonb AS $$
DECLARE
	filter jsonb[];
	unfilter jsonb[];
	result jsonb;
	items jsonb;
BEGIN
	SELECT jsonb_build_object('type', type, 'properties', properties, 'children', children, 'acl', acl, 'deleted', deleted)
	FROM mf2.objects INTO result
	WHERE trim(both from properties->'url'->>0, '"') = uri;
	CASE
	WHEN result->'type' @> '"h-x-dynamic-feed"' THEN
		-- NOTE: putting the filter code inside of the ANY() causes the planner to use seq scan, but only if it has mf2.substitute_params
		SELECT mf2.jsonb_array_to_pg_array(mf2.substitute_params(result->'properties'->'filter', params)) INTO filter;
		SELECT mf2.jsonb_array_to_pg_array(mf2.substitute_params(result->'properties'->'unfilter', params)) INTO unfilter;
		IF before IS NULL AND after IS NOT NULL THEN
			SELECT json_agg(obj) FROM (
				SELECT obj FROM (
					WITH stuff AS (
						SELECT *
						FROM mf2.objects
						WHERE coalesce(properties @> ANY(filter), True)
						AND NOT coalesce(properties @> ANY(unfilter), False)
						AND ('*' = ANY(acl) OR current_setting('mf2sql.current_user_url', true) = ANY(acl) OR current_setting('mf2sql.current_user_url', true) || '/' = ANY(acl))
						AND (properties->'url'->>0)::text LIKE uri_prefix || '%'
						AND coalesce(mf2.cast_timestamp(properties->'published'->>0) > after, True)
						-- Instead of using LIMIT here, we use dates to limit only the number of non-deleted objects
						-- This could've been much easier if we didn't want tombstones
					)
					SELECT jsonb_build_object('type', type, 'properties', mf2.objects_denormalize(properties, 4), 'children', children, 'acl', acl, 'deleted', deleted) AS obj
					FROM stuff
					WHERE coalesce(mf2.cast_timestamp(properties->'published'->>0) < (
							SELECT mf2.cast_timestamp(properties->'published'->>0)
							FROM stuff
							WHERE deleted IS NOT True
							ORDER BY mf2.cast_timestamp(properties->'published'->>0) ASC
							OFFSET lim LIMIT 1 -- the way to get the *last* row lol
					), True)
				) subsubq ORDER BY mf2.cast_timestamp(obj->'properties'->'published'->>0) DESC
			) subq INTO items;
		ELSE
			SELECT json_agg(obj) FROM (
				WITH stuff AS (
					SELECT *
					FROM mf2.objects
					WHERE coalesce(properties @> ANY(filter), True)
					AND NOT coalesce(properties @> ANY(unfilter), False)
					AND ('*' = ANY(acl) OR current_setting('mf2sql.current_user_url', true) = ANY(acl) OR current_setting('mf2sql.current_user_url', true) || '/' = ANY(acl))
					AND (properties->'url'->>0)::text LIKE uri_prefix || '%'
					AND coalesce(mf2.cast_timestamp(properties->'published'->>0) < before, True)
				)
				SELECT jsonb_build_object('type', type, 'properties', mf2.objects_denormalize(properties, 4), 'children', children, 'acl', acl, 'deleted', deleted) AS obj
				FROM stuff
				WHERE coalesce(mf2.cast_timestamp(properties->'published'->>0) > coalesce(after, (
						SELECT mf2.cast_timestamp(properties->'published'->>0)
						FROM stuff
						WHERE deleted IS NOT True
						ORDER BY mf2.cast_timestamp(properties->'published'->>0) DESC
						OFFSET lim LIMIT 1 -- the way to get the *last* row lol
				)), True)
				ORDER BY mf2.cast_timestamp(properties->'published'->>0) DESC
			) subq INTO items;
		END IF;
		SELECT jsonb_set(result, '{children}', coalesce(items, '[]')) INTO result;
		RETURN result;
	ELSE
		SELECT jsonb_set(
			jsonb_set(result, '{properties}', mf2.objects_denormalize(result->'properties')),
			'{children}', mf2.objects_denormalize(result->'children')) INTO result;
		RETURN result;
	END CASE;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION mf2.objects_smart_fetch(uri text, uri_prefix text, lim int, before timestamptz, after timestamptz, params jsonb) IS $$
	Denormalizes an object if it's a normal entry or something, but:
	If it's a 'feed configuration' object (type 'h-x-dynamic-feed'), turns it into an 'h-feed'
	with matching objects from the same domain correctly paginated inside, without deleted / unauthorized posts.
$$;

CREATE FUNCTION mf2.objects_fetch_feeds(uri_prefix text) RETURNS jsonb AS $$
	SELECT jsonb_agg(obj) FROM (
		SELECT jsonb_build_object('type', type, 'properties', properties, 'children', children, 'acl', acl, 'deleted', deleted) AS obj
		FROM mf2.objects
		WHERE (properties->'url'->>0)::text LIKE uri_prefix || '%'
		AND ('*' = ANY(acl) OR current_setting('mf2sql.current_user_url', true) = ANY(acl) OR current_setting('mf2sql.current_user_url', true) || '/' = ANY(acl))
		AND type @> '{h-x-dynamic-feed}'
		AND deleted IS NOT True
	) subq
$$ LANGUAGE sql;
COMMENT ON FUNCTION mf2.objects_fetch_feeds(uri_prefix text) IS $$
	Fetches all 'h-x-dynamic-feed' objects on a given URI prefix (host), except deleted/unauthorized.
$$;

CREATE FUNCTION mf2.objects_fetch_categories(uri_prefix text) RETURNS jsonb AS $$
	SELECT jsonb_agg(tag_rows) FROM (
		SELECT DISTINCT jsonb_array_elements_text(properties->'category') AS name, count(*) AS obj_count
		FROM mf2.objects
		WHERE (properties->'url'->>0)::text LIKE uri_prefix || '%'
		AND ('*' = ANY(acl) OR current_setting('mf2sql.current_user_url', true) = ANY(acl) OR current_setting('mf2sql.current_user_url', true) || '/' = ANY(acl))
		GROUP BY name
		ORDER BY obj_count DESC
	) tag_rows
$$ LANGUAGE sql;

-------------------------------------------------------------------------------------------- Maintenance
CREATE FUNCTION mf2.objects_rename_domain(old_uri_prefix text, new_uri_prefix text) RETURNS void AS $$
	UPDATE mf2.objects
	SET properties = jsonb_set(properties, '{url,0}'::text[], to_jsonb(replace(properties->'url'->>0, old_uri_prefix, new_uri_prefix)))
	WHERE (properties->'url'->>0)::text LIKE old_uri_prefix || '%';
	UPDATE mf2.objects
	SET acl = array_replace(array_replace(acl, old_uri_prefix, new_uri_prefix), old_uri_prefix || '/', new_uri_prefix || '/');
$$ LANGUAGE sql;
COMMENT ON FUNCTION mf2.objects_rename_domain(old_uri_prefix text, new_uri_prefix text) IS $$
	Change the URI prefix (scheme://domain) to move a website from one domain to another (or change its protocol to https, or something).
$$;
