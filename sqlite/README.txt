  The query manager will initialize and patch the database schema, at startup,
based on the files in this folder. The initial schema is inside `schema.sql` and
shouldn't be modified, to make sure the database can be initialized if everything
else fails. Patches and modifications should be placed in `patches/` with no
particular name restrictions except for having an `.sql` extension. These patch
files will be executed exactly ONCE. If multiple patches are pending at startup,
they're executed in alphabetical order.
  As for statement restrictions, the only thing prohibited is the presence of
transaction statements "BEGIN", "ROLLBACK", and "COMMIT". This is because all
patches will be bundled into the same transaction, to ensure atomicity.

