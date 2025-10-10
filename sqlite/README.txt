	The query manager will properly initialize and upgrade the database schema
based on files in this folder. The initial schema should be in `schema.sql` and
upgrades should be in `upgrade-N.sql` where N is a number starting from 1. The
`upgrade-N.sql` file should take the database from version N to N + 1.
	The only restriction to these files is that they can't have transaction
statements ("BEGIN", "ROLLBACK", "COMMIT") because the query manager will
already bundle them into a transaction that also sets `user_version`, and
and nested transactions aren't allowed.
	The current database version can be retrieved with the "PRAGMA user_version"
query in the SQLite shell.