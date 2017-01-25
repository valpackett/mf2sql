#!/usr/bin/env ruby
require 'pg'
require 'uri'
require 'yaml'
require 'optparse'

uri = ENV['DATABASE_URL']

op = OptionParser.new do |opts|
  opts.banner = 'Usage: import.rb [options] <files>'
  opts.on('-h', '--help', 'Prints this help') { puts opts; exit }
  opts.on('-dURI', '--db=URI', 'Database connection URI') { |u| uri = u }
end
op.parse!

uri = URI.parse(uri)
db = PG.connect(uri.hostname, uri.port, nil, nil, uri.path[1..-1], uri.user, uri.password)
db.set_error_verbosity(PG::PQERRORS_VERBOSE)

def parse(data)
  JSON.load(data)
rescue JSON::ParserError
  YAML.safe_load(data)
end

json_enc = PG::TextEncoder::JSON.new

db.transaction do |tx|
  stmt = tx.prepare('import', <<SQL)
INSERT INTO objects (type, properties, children)
SELECT
  DISTINCT ON ((objects_normalize->'properties'->'url'->>0))
  (SELECT array_agg(x) FROM jsonb_array_elements_text(objects_normalize->'type') AS x) AS typee,
  objects_normalize->'properties',
  CASE objects_normalize->'children'
  WHEN NULL THEN NULL
  WHEN 'null'::jsonb THEN NULL
  ELSE (SELECT array_agg(x)::jsonb[] FROM jsonb_array_elements(objects_normalize->'children') AS x)
  END
FROM objects_normalize($1::jsonb)
WHERE (objects_normalize->'properties'->'url'->>0) IS NOT NULL
ON CONFLICT ((properties->'url'->>0))
DO UPDATE SET type = EXCLUDED.type, properties = EXCLUDED.properties, children = EXCLUDED.children;
SQL
  ARGV.each do |arg|
    data = parse(File.read(arg))
    tx.exec_prepared('import', [json_enc.encode(data)])
  end
end
