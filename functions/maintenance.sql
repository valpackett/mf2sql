-------------------------------------------------------------------------------------------- Maintenance

CREATE OR REPLACE FUNCTION mf2.objects_rename_domain(old_uri_prefix text, new_uri_prefix text) RETURNS void AS $$
	UPDATE mf2.objects
	SET properties = jsonb_set(properties, '{url,0}'::text[], to_jsonb(replace(properties->'url'->>0, old_uri_prefix, new_uri_prefix)))
	WHERE (properties->'url'->>0)::text LIKE old_uri_prefix || '%';
	UPDATE mf2.objects
	SET acl = array_replace(array_replace(acl, old_uri_prefix, new_uri_prefix), old_uri_prefix || '/', new_uri_prefix || '/');
$$ LANGUAGE sql;

COMMENT ON FUNCTION mf2.objects_rename_domain(old_uri_prefix text, new_uri_prefix text) IS $$
	Change the URI prefix (scheme://domain) to move a website from one domain to another (or change its protocol to https, or something).
$$;
