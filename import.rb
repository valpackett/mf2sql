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
  stmt = tx.prepare('import', 'SELECT mf2.objects_normalized_upsert($1);')
  ARGV.each do |arg|
    data = parse(File.read(arg))
    tx.exec_prepared('import', [json_enc.encode(data)])
  end
end
