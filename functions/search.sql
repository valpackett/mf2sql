-------------------------------------------------------------------------------------------- Full Text Search

CREATE OR REPLACE FUNCTION mf2.jsonb_values(data jsonb) RETURNS SETOF jsonb AS $$
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

CREATE OR REPLACE FUNCTION mf2.flatten_jsonb(data jsonb) RETURNS SETOF jsonb AS $$
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

CREATE OR REPLACE FUNCTION mf2.tsv_from_jsonb(data jsonb) RETURNS tsvector AS $$
	SELECT to_tsvector(coalesce(string_agg(flat::text, ' '), ''))
	FROM mf2.flatten_jsonb(data) flat
	WHERE jsonb_typeof(flat) = 'string';
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION mf2.objects_set_tsv() RETURNS trigger AS $$
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

DROP TRIGGER IF EXISTS objects_set_tsv_trigger ON mf2.objects;

CREATE TRIGGER objects_set_tsv_trigger
BEFORE INSERT OR UPDATE ON mf2.objects
FOR EACH ROW EXECUTE PROCEDURE mf2.objects_set_tsv();
