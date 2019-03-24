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
CREATE INDEX tsv_idx ON mf2.objects USING GIST(tsv);
