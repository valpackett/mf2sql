-- pgTAP test, run with pg_prove
BEGIN; SELECT plan(14);

SELECT has_table('objects');
SELECT col_type_is('objects', 'type', 'text[]');
SELECT col_type_is('objects', 'properties', 'jsonb');
SELECT col_type_is('objects', 'children', 'jsonb[]');


-------------------------------------------------------------------------------------------- URL Uniqueness
SELECT lives_ok($$
	INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://uniq/1"]}');
$$);

SELECT throws_like($$
	INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://uniq/1"]}');
$$, '%unique constraint%');

SELECT lives_ok($$
	INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://uniq/fine-as-secondary", "http://uniq/1"]}');
$$);


-------------------------------------------------------------------------------------------- Full Text Search
INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://search/1"], "name": ["Hello world"], "content": "First Post"}');
INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://search/2"], "name": ["Hello"], "content": "Second Post"}');

SELECT results_eq($$
	SELECT properties->'url'->>0 FROM objects_search('firsts');
$$, $$ VALUES ('http://search/1'); $$);

SELECT results_eq($$
	SELECT properties->'url'->>0 FROM objects_search('first|second');
$$, $$ VALUES ('http://search/1'), ('http://search/2'); $$);

SELECT results_eq($$
	SELECT properties->'url'->>0 FROM objects_search('post');
$$, $$ VALUES ('http://search/1'), ('http://search/2'); $$);

SELECT is_empty($$
	SELECT properties->'url'->>0 FROM objects_search('search');
$$);

-------------------------------------------------------------------------------------------- Denormalization
INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://1"], "name": ["Test 1"]}');
INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://2"], "name": ["Test 2"], "comments": ["http://1", {"comment": "http://1"}]}');
INSERT INTO objects VALUES ('{"h-entry"}', '{"url": ["http://3"], "name": ["Test 3"], "comments": ["http://2"]}');

SELECT results_eq($$
	SELECT objects_denormalize_unlimited(properties) FROM objects WHERE properties->'url'->>0 = 'http://3';
$$, $$
	SELECT '{
	"url": ["http://3"],
	"name": ["Test 3"],
	"comments": [
		{
			"type": ["h-entry"],
			"children": null,
			"properties": {
				"url": ["http://2"],
				"name": ["Test 2"],
				"comments": [
					{
						"type": ["h-entry"],
						"children": null,
						"properties": {"url": ["http://1"], "name": ["Test 1"]}
					},
					{"comment": {
						"type": ["h-entry"],
						"children": null,
						"properties": {"url": ["http://1"], "name": ["Test 1"]}
					}}
				]
			}
		}
	]
}'::jsonb;
$$);


SELECT results_eq($$
	SELECT objects_denormalize(properties) FROM objects WHERE properties->'url'->>0 = 'http://3';
$$, $$
	SELECT '{
	"url": ["http://3"],
	"name": ["Test 3"],
	"comments": [
		{
			"type": ["h-entry"],
			"children": null,
			"properties": {
				"url": ["http://2"],
				"name": ["Test 2"],
				"comments": [
					{
						"type": ["h-entry"],
						"children": null,
						"properties": {"url": ["http://1"], "name": ["Test 1"]}
					},
					{"comment": "http://1"}
				]
			}
		}
	]
}'::jsonb;
$$);

-------------------------------------------------------------------------------------------- Normalization
SELECT results_eq($$
	SELECT objects_normalize('{
	"url": ["http://3"],
	"name": ["Test 3"],
	"comments": [
		{
			"type": ["h-entry"],
			"properties": {
				"url": ["http://2"],
				"name": ["Test 2"],
				"comments": [
					{
						"type": ["h-entry"],
						"properties": {"url": ["http://1"], "name": ["Test 1"]}
					},
					{"comment": {
						"type": ["h-entry"],
						"properties": {"url": ["http://1"], "name": ["Test 1"]}
					}}
				]
			}
		}
	]
}'::jsonb);
$$, $$
	VALUES
		('{"type": ["h-entry"], "children": null, "properties": {"url": ["http://2"], "name": ["Test 2"], "comments": ["http://1", {"comment": "http://1"}]}}'::jsonb),
		('{"type": ["h-entry"], "children": null, "properties": {"url": ["http://1"], "name": ["Test 1"]}}'::jsonb),
		('{"url": ["http://3"], "name": ["Test 3"], "comments": ["http://2"]}'::jsonb);
$$);

SELECT * FROM finish();
ROLLBACK;
