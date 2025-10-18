WARNING: This is not meant to be a complete guide on PostgreSQL, but rather a
"first steps" kind of guide. It'll only cover things on the surface level. For
a deep dive into how the database operates, how to properly configure it, and
ultimately properly administrate it, you MUST refer to the PostgreSQL manual
for your version. The most current version of the manual will describe the most
recent features, but not all features are present in all versions.
  If anything, you should absolutely consult the section that regards server
administration "III. Server Administration".

   MANUAL   https://www.postgresql.org/docs/current/index.html

Installation
------------
  PostgreSQL is available in most Linux distributions as a package which is
the preferred way to get it installed. Some will automatically setup a service,
create service users, initialize the database cluster, etc... If not, you might
need to do one or more steps manually. If you're having trouble, most systems
will have specific instructions on how to set everything up. Just as an example
here are a few links for common systems:
   DEBIAN   https://www.postgresql.org/download/linux/debian/
   REDHAT   https://www.postgresql.org/download/linux/redhat/
   SUSE     https://www.postgresql.org/download/linux/suse/
   UBUNTU   https://www.postgresql.org/download/linux/ubuntu/
   ARCH     https://wiki.archlinux.org/title/PostgreSQL

Configuration
-------------
  By default, configuration files will be in the `data` directory which can
change locations depending on how the server was installed but is usually in
`/var/lib/postgres/data`.
  All files in the `data` directory are OWNED by the *postgres* SYSTEM user,
meaning you'll only be able to modify them if you're logged in as *postgres*,
by using *sudo* privileges, or both with `sudo su postgres`.
  The bulk of the configuration is inside `postgresql.conf` which has multiple
options, but of particular interest are the "CONNECTIONS AND AUTHENTICATION"
options. I won't go over specifics here but if you're planning on accepting
remote connections, you MUST properly configure SSL communication.
  Access to the database is controlled with `pg_hba.conf`. This is different
from MySQL where you'd specify users as 'user'@'host' with SQL to restrict
them to certain hosts. Instead you need to specify how certain users/roles
may connect to the database in this file. Properly configuring it is probably
the most important step in securing the database, aside from configuring SSL
communication.
  The last file is `pg_ident.conf` which declares mappings from system users
to database users. These mappings alone don't do anything. They must be
explicitly referenced as `map=MAPNAME` in `pg_hba.conf` for supported
authentication methods.

  Here is an example of a `pg_hba.conf` + `pg_ident.conf` local access config.
It'll allow *systemuser* to connect as *postgres* to any database using the
*peer* method which checks the system user name. It'll also allow the *tibia*
user to connect to the *tibia* database using the *scram-sha-256* password
authentication scheme. Local connections will use UNIX-domain sockets and for
that matter you'd leave `PostgreSQL.Host` empty.

```
# pg_hba.conf
# TYPE   DATABASE        USER           ADDRESS                 METHOD
local    all             postgres                               peer map=super
local    tibia           tibia                                  scram-sha-256

# pg_ident.conf
# MAPNAME       SYSTEM-USERNAME         PG-USERNAME
super           systemuser              postgres
```

   MANUAL   https://www.postgresql.org/docs/current/runtime-config.html
   MANUAL   https://www.postgresql.org/docs/current/client-authentication.html

Database Setup
--------------
  It is highly advised to not use a SUPERUSER when connecting to the database
from the query manager, or any other service for that matter. This warrants the
creation of a secondary user that has access, but not administrative privileges.
  I figured it would be simpler to have a sequence of *PSQL* commands with their
descriptions. Having a database minimaly ready for the query manager should be
a matter of following this sequence.

  Unless a database is specified, *PSQL* will connect to one with the same name
as the specified user. If the user is not explicitly specified, the system user
name will be used. Running `psql -U postgres` will connect to *postgres* as the
user *postgres*. Note that you can't connect without a database, so you'd connect
to *postgres* in order to create new databases.

1 - Create and connect to a new database. Note that the `OWNER = postgres` clause
is redundant here but it's just to show that having the database owned by the
super user is intended.
```
psql -U postgres -c "CREATE DATABASE tibia OWNER = postgres;"
psql -U postgres tibia
```

2 - Set default privileges. Newly created databases may have some default PUBLIC
privileges that we'll want to revoke to make sure the set of users that are able
to connect is tighly controlled. Then, for users that are able to connect, we
want to give default access privileges to tables.
```
REVOKE ALL ON DATABASE tibia FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLES TO PUBLIC;
```

3 - Initialize schema. This is done by executing commands from `postgres/schema.sql`,
and optionally `postgres/initial-data.sql`. Note that since we set default privileges
before creating any tables, they should already have the approppriate privileges.
If done the other way around, we'd need to manually update table privileges.
```
\i postgres/schema.sql
\i postgres/initial-data.sql
```

4 - Create secondary user. This is straighforward. Create a user with *LOGIN*
privileges and a *PASSWORD*. Then grant *CONNECT* privileges to the database.
```
CREATE ROLE tibia WITH LOGIN PASSWORD '********';
GRANT CONNECT ON DATABASE tibia TO tibia;
```

  This is just one way. There are probably other, more optimal setups, but
for a small testing bench, it will do. And don't take my word on anything.
You should always check the manual for a complete description on how things
work.

  To wrap, here is a list of helpful commands available in *PSQL*. They'll
show up along with a lot of other commands when running `\?`.
```
\q      # quit
\l      # list databases (will show database privileges)
\du     # list users     (will show user privileges)
\dO     # list collations
\dt     # list tables
\dv     # list views
\ds     # list sequences
\di     # list indexes
\d NAME # describe table/view/sequence/index
\dp     # list privileges
\ddp    # list default privileges
```

   MANUAL   https://www.postgresql.org/docs/current/sql-commands.html

