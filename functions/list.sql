-------------------------------------------------------------------------------------------- Listing

CREATE OR REPLACE FUNCTION mf2.objects_fetch_feeds(uri_prefix text) RETURNS jsonb AS $$
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

CREATE OR REPLACE FUNCTION mf2.objects_fetch_categories(uri_prefix text) RETURNS jsonb AS $$
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
