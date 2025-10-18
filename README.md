# Tibia 7.7 Query Manager
This is a query manager designed to support [Tibia Game Server](https://github.com/fusion32/tibia-game), [Tibia Login Server](https://github.com/fusion32/tibia-login), and [Tibia Web Server](https://github.com/fusion32/tibia-web). It should be able to support any database system, but only SQLite and PostgreSQL are currently implemented. For SQLite, it uses the SQLite3 3.50.2 amalgamation, which avoids external dependencies and effectivelly turns the query manager into the database itself. For PostgreSQL, you'll need to link against *libpq*, but the query manager turns into a relay to the actual database. The communication protocol is NOT encrypted at all and it's configured to listen to local connections only. If you need a distributed infrastructure, use PostgreSQL!

## Compiling
Currently only Linux is supported. It shouldn't be too difficult to support Windows but I don't think it would add any value, considering the game server is somewhat bound to Linux, and that both need to run on the same machine. The only dependency is *libpq* when using PostgreSQL. The makefile is very simple but there are a few parameters that can be modified to customize compilation:
- *DEBUG* can be set to a non-zero value to build in debug mode. Defaults to zero.
- *DATABASE* can be set to either *sqlite* or *postgres* to modify the database system. Defaults to *sqlite*.

Compiling different translation units with different compilation parameters may cause things to blow up so it is recommended to always do a full rebuild. This can be achieved by running `make clean` before compiling or specifying the `-B` option to *make*. Here is a list of recommended commands:
```
make -B DEBUG=0 DATABASE=sqlite        # build in release mode with SQLite
make -B DEBUG=1 DATABASE=sqlite        # build in debug mode with SQLite
make -B DEBUG=0 DATABASE=postgres      # build in release mode with PostgreSQL
make -B DEBUG=1 DATABASE=postgres      # build in debug mode with PostgreSQL
make clean                             # remove `build` directory
```

## Running (SQLite)
The query manager becomes the database, automatically initializing and maintaining the schema, based on the files in `sqlite/` (see `sqlite/README.txt`). The default schema file won't automatically insert any initial data but that can be changed by using a patch (again, see `sqlite/README.txt`). There are a few configuration options, and in particular `SQLite.*` options that can be adjusted in `config.cfg` but the defaults should work for most use cases.

## Running (PostgreSQL)
The query manager becomes a relay to the actual database. And with PostgreSQL being a distributed database system, it makes no sense to have individual clients managing the schema, since there could be multiple, each with their own assumptions. For that reason there is a `SchemaInfo` table with a `VERSION` row that will be queried at startup and compared against `POSTGRESQL_SCHEMA_VERSION`, defined in `src/database_postgres.cc`, to make sure there is an agreement on the schema version. It is hardcoded because schema changes will usually result in query changes.

Aside from having to properly setup and startup a PostgreSQL server (see `postgres/README.txt`), there are quite a few specific extra options (`PostgreSQL.*`) that should be properly configured to be able to reach the database.

## Running
For testing purposes you could simply compile and launch the application from the shell, but if you plan to run the game server on a dedicated machine, it is recommended that it is setup as a service. There is a *systemd* configuration file (`tibia-querymanager.service`) in the repository that may be used for that purpose. The process is very similar to the one described in the [Game Server](https://github.com/fusion32/tibia-game) so I won't repeat myself here.

## Text Encoding
The original game client and server uses LATIN1 text encoding, which can be a problem if we're assuming strings are UTF8 encoded. For that reason, `TReadBuffer::ReadString` will automatically convert from LATIN1 to UTF8, and `TWriteBuffer::WriteString` will automatically convert from UTF8 to LATIN1, to ensure that LATIN1 is still used as the text encoding of the underlying protocol. This behaviour can be disabled with the `-DCLIENT_ENCODING_UTF8=1` compilation flag but unless you move this encoding bridge to the server-client boundary, you'll have problems. And it's not such a simple task either. Upgrading the game server to UTF8 would require updating game files, changing the script parser, and would ultimately make it incompatible with the leaked game files.

