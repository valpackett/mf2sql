-------------------------------------------------------------------------------------------- Fetching

CREATE OR REPLACE FUNCTION mf2.substitute_params(data jsonb, params jsonb) RETURNS jsonb AS $$
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

CREATE OR REPLACE FUNCTION mf2.objects_fetch(uri text, uri_prefix text, lim int, before timestamptz, after timestamptz, params jsonb) RETURNS jsonb AS $$
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
