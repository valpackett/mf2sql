# mf2sql [![unlicense](https://img.shields.io/badge/un-license-green.svg?style=flat)](http://unlicense.org)

A PostgreSQL (10) schema for storing [microformats2](http://microformats.org/wiki/microformats2) objects (as the canonical JSON representation), featuring:

- Fast storage and querying ([jsonb](https://www.postgresql.org/docs/9.6/static/datatype-json.html) + [GIN](https://www.postgresql.org/docs/9.6/static/indexes-types.html))
- Uniqueness constraint on the first `url` value
- [Full text search](https://www.postgresql.org/docs/current/static/textsearch-intro.html)
- Automatic insert/update/delete [notifications](https://www.postgresql.org/docs/current/static/sql-notify.html)
- Denormalization (embed linked objects like replies for exporting into JSON files or rendering to HTML templates)
- Normalization (un-embed linked objects when importing from JSON files)
- And more

## Setup

- Create a database e.g. `sudo -u postgres createdb mywebsite`
- Run `SELECT current_setting('default_text_search_config');` to check if your default language for full text search matches your website's (if not, change that setting)
- Run the migrations using [migrate](https://github.com/golang-migrate/migrate) e.g. `migrate -path=migrations -url=postgres://localhost/mywebsite\?sslmode=disable up` (or just run the `migrations/*.up.sql` files with `psql` if you don't want to bother with `migrate`)
- Import JSON files using `import.rb` if you want

## Development

Grab [pgTAP](http://pgtap.org) (including `pg_prove`) to run the tests!

## Contributing

Please feel free to submit pull requests!

By participating in this project you agree to follow the [Contributor Code of Conduct](http://contributor-covenant.org/version/1/4/).

[The list of contributors is available on GitHub](https://github.com/myfreeweb/mf2sql/graphs/contributors).

## License

This is free and unencumbered software released into the public domain.  
For more information, please refer to the `UNLICENSE` file or [unlicense.org](http://unlicense.org).
