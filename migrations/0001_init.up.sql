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

CREATE UNIQUE INDEX url_idx ON mf2.objects ((properties->'url'->>0) text_pattern_ops);
CREATE INDEX pub_time_idx ON mf2.objects (mf2.cast_timestamp(properties->'published'->>0) DESC NULLS LAST);
CREATE INDEX properties_idx ON mf2.objects USING GIN(properties jsonb_path_ops);


-------------------------------------------------------------------------------------------- Full Text Search
CREATE INDEX tsv_idx ON mf2.objects USING GIST(tsv);

CREATE FUNCTION mf2.jsonb_values(data jsonb) RETURNS SETOF jsonb AS $$
BEGIN
	IF jsonb_typeof(jsonlevel) = 'object' THEN
		RETURN QUERY SELECT (jsonb_each(jsonlevel)).value;
	ELSIF jsonb_typeof(jsonlevel) = 'array' THEN
		RETURN QUERY SELECT jsonb_array_elements(jsonlevel);
	ELSE
		RETURN QUERY SELECT true WHERE false;
	END IF;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION mf2.flatten_jsonb(data jsonb) RETURNS SETOF jsonb AS $$
	-- based on https://stackoverflow.com/a/27742278/239140
	-- modified to use jsonb_typeof
	WITH RECURSIVE deconstruct (jsonlevel) AS (
		VALUES (data)
		UNION ALL
		SELECT mf2.jsonb_values(jsonlevel) AS jsonlevel
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
			json_build_object('op', 'insert', 'url', ARRAY[substring(NEW.properties->'url'->>0 for 3000)])::text);
		WHEN 'UPDATE' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'update', 'url', ARRAY[substring(NEW.properties->'url'->>0 for 3000)])::text);
		WHEN 'DELETE' THEN PERFORM pg_notify('mf2_objects',
			json_build_object('op', 'delete', 'url', ARRAY[substring(OLD.properties->'url'->>0 for 3000)])::text);
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
		SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(data->'url') = 'array' THEN data->'url' ELSE '[]'::jsonb END)
		UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(data->'uid') = 'array' THEN data->'uid' ELSE '[]'::jsonb END);
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

CREATE FUNCTION mf2.objects_fetch(uri text, uri_prefix text, lim int, before timestamptz, after timestamptz, params jsonb) RETURNS jsonb AS $$
DECLARE
	depth int := 5;
	filter jsonb[];
	unfilter jsonb[];
	urls_children text[];
	urls_to_preload hstore; -- set
	urls_excl_tmp text[];
	preloaded jsonb := '{}';
	result jsonb;
BEGIN
	SELECT jsonb_build_object('type', type, 'properties', properties, 'children', children, 'acl', acl, 'deleted', deleted)
	FROM mf2.objects INTO result
	WHERE properties->'url'->>0 = uri
	LIMIT 1;

	SELECT mf2.jsonb_array_to_pg_array(mf2.substitute_params(result->'properties'->'filter', params)) INTO filter;
	SELECT mf2.jsonb_array_to_pg_array(mf2.substitute_params(result->'properties'->'unfilter', params)) INTO unfilter;

	IF filter IS NULL THEN
		filter := ARRAY[]::jsonb[];
	END IF;
	IF unfilter IS NULL THEN
		unfilter := ARRAY[]::jsonb[];
	END IF;

	-- get list of children, configure max preload depth
	IF result->'type' @> '"h-x-reader-channel"' THEN
		SELECT array_agg(url)
		FROM (
			SELECT DISTINCT jsonb_array_elements_text(coalesce(sub->'entries', '[]'::jsonb)) url
			FROM jsonb_array_elements(result->'properties'->'subscriptions') sub
		) AS urls
		INTO urls_children;
		depth := coalesce(current_setting('mf2.preload_depth_feed', true)::int, 4);
	ELSIF result->'type' @> '"h-x-dynamic-feed"' THEN
		SELECT array_agg(properties->'url'->>0)
		FROM mf2.objects
		WHERE (properties->'url'->>0)::text LIKE uri_prefix || '%'
		INTO urls_children;
		depth := coalesce(current_setting('mf2.preload_depth_feed', true)::int, 4);
	ELSE
		depth := coalesce(current_setting('mf2.preload_depth_entry', true)::int, 16);
	END IF;

	IF urls_children IS NOT NULL THEN
		-- filter children
		SELECT array_agg(properties->'url'->>0)
		FROM mf2.objects
		WHERE (properties->'url'->>0)::text = ANY(urls_children)
		AND (acl && ARRAY['*', rtrim(current_setting('mf2sql.current_user_url', true), '/'), rtrim(current_setting('mf2sql.current_user_url', true), '/') || '/'])
		-- NOTE: coalesce(..@>..) blocks jsonb_path_ops index usage
		AND properties @> ANY(filter)
		AND NOT properties @> ANY(unfilter)
		INTO urls_children;

		-- limit and sort children
		WITH stuff AS (
			SELECT properties->'url'->>0 AS url
			FROM mf2.objects
			WHERE (properties->'url'->>0)::text = ANY(urls_children)
			AND (
				CASE WHEN before IS NOT NULL
				THEN mf2.cast_timestamp(properties->'published'->>0) < before
				ELSE True
				END
			)
			AND (
				CASE WHEN after IS NOT NULL
				THEN mf2.cast_timestamp(properties->'published'->>0) > after
				ELSE True
				END
			)
			AND (
				CASE WHEN before IS NULL AND after IS NOT NULL THEN
					coalesce(mf2.cast_timestamp(properties->'published'->>0) < (
							SELECT mf2.cast_timestamp(properties->'published'->>0)
							FROM mf2.objects
							WHERE (properties->'url'->>0)::text = ANY(urls_children)
							AND deleted IS NOT True
							ORDER BY mf2.cast_timestamp(properties->'published'->>0) ASC
							OFFSET lim LIMIT 1 -- the row after lim
					), True)
				ELSE
					coalesce(mf2.cast_timestamp(properties->'published'->>0) > coalesce(after, (
							SELECT mf2.cast_timestamp(properties->'published'->>0)
							FROM mf2.objects
							WHERE (properties->'url'->>0)::text = ANY(urls_children)
							AND deleted IS NOT True
							ORDER BY mf2.cast_timestamp(properties->'published'->>0) DESC
							OFFSET lim LIMIT 1 -- the row after lim
					)), True)
				END
			)
			ORDER BY mf2.cast_timestamp(properties->'published'->>0) DESC
		)
		SELECT array_agg(url)
		FROM stuff
		INTO urls_children;
	END IF;

	-- preload all referenced entries
	WITH stuff AS (
		SELECT value->>0 AS url
		FROM jsonb_each(result->'properties')
		WHERE value->>0 LIKE 'https://%' OR value->>0 LIKE 'http://%'
		UNION ALL
		SELECT *
		FROM unnest(urls_children)
	)
	SELECT hstore(array_agg(url), NULL)
	FROM stuff
	INTO urls_to_preload;
	urls_to_preload := delete(urls_to_preload, result->'properties'->'url'->>0);

	FOR d IN 1..depth LOOP
		SELECT jsonb_object_agg(key, value)
		FROM (
			SELECT properties->'url'->>0 AS key,
				jsonb_build_object('type', type, 'properties', properties, 'children', children, 'acl', acl, 'deleted', deleted) AS value
			FROM mf2.objects
			WHERE properties->'url'->>0 = ANY(akeys(urls_to_preload))
			AND (acl && ARRAY['*', rtrim(current_setting('mf2sql.current_user_url', true), '/'), rtrim(current_setting('mf2sql.current_user_url', true), '/') || '/'])
			UNION ALL
			SELECT key, value
			FROM jsonb_each(preloaded)
		) subq
		INTO preloaded;

		SELECT array_agg(k)
		FROM jsonb_object_keys(preloaded) AS k
		INTO urls_excl_tmp;

		WITH stuff AS (
			SELECT props.value->>0 AS url
			FROM jsonb_each(preloaded) AS entries,
				jsonb_each(entries.value->'properties') AS props
			WHERE props.value->>0 LIKE 'https://%' OR props.value->>0 LIKE 'http://%'
			AND NOT props.value->>0 = ANY(urls_excl_tmp)
		)
		SELECT hstore(array_agg(url), NULL)
		FROM stuff
		INTO urls_to_preload;
		urls_to_preload := delete(urls_to_preload, result->'properties'->'url'->>0);
	END LOOP;

	RETURN jsonb_set(
		jsonb_set(result, '{preloaded}', coalesce(preloaded, 'null'::jsonb)),
		'{children}', coalesce(to_jsonb(urls_children), 'null'::jsonb));
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION mf2.objects_fetch(uri text, uri_prefix text, lim int, before timestamptz, after timestamptz, params jsonb) IS $$
	Loads an entry and preloads all referenced objects up to a certain depth.
	Handles reader channels and dynamic feeds.
	Checks ACL on referenced objects, but not the top level one.
$$;

CREATE FUNCTION mf2.objects_fetch_feeds(uri_prefix text) RETURNS jsonb AS $$
	SELECT jsonb_agg(obj) FROM (
		SELECT jsonb_build_object('type', type, 'properties', properties, 'children', children, 'acl', acl, 'deleted', deleted) AS obj
		FROM mf2.objects
		WHERE (properties->'url'->>0)::text LIKE uri_prefix || '%'
		AND (acl && ARRAY['*', rtrim(current_setting('mf2sql.current_user_url', true), '/'), rtrim(current_setting('mf2sql.current_user_url', true), '/') || '/'])
		AND (type @> '{h-x-dynamic-feed}' OR type @> '{h-x-reader-channel}')
		AND deleted IS NOT True
	) subq
$$ LANGUAGE sql;
COMMENT ON FUNCTION mf2.objects_fetch_feeds(uri_prefix text) IS $$
	Fetches all 'h-x-dynamic-feed' and 'h-x-reader-channel' objects on a given URI prefix (host), except deleted/unauthorized.
$$;

CREATE FUNCTION mf2.objects_fetch_categories(uri_prefix text) RETURNS jsonb AS $$
	SELECT jsonb_agg(tag_rows) FROM (
		SELECT DISTINCT
			jsonb_array_elements_text(CASE WHEN jsonb_typeof(properties->'category') = 'array'
				THEN properties->'category' ELSE '[]'::jsonb END) AS name,
			count(*) AS obj_count
		FROM mf2.objects
		WHERE (properties->'url'->>0)::text LIKE uri_prefix || '%'
		AND (acl && ARRAY['*', rtrim(current_setting('mf2sql.current_user_url', true), '/'), rtrim(current_setting('mf2sql.current_user_url', true), '/') || '/'])
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
