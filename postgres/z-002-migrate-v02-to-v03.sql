-- NOTE(fusion): This file contains the migration script from v0.2 to v0.3. It's
-- put inside a transaction to avoid errors from leaving the database in some
-- partial state.
--  These changes are already present in the latest `schema.sql`, so trying to
-- apply them on a newly created database will result in errors.
--==============================================================================

BEGIN;

DROP INDEX CharactersGuildIndex;
ALTER TABLE Characters DROP COLUMN Guild;
ALTER TABLE Characters DROP COLUMN Rank;
ALTER TABLE Characters DROP COLUMN Title;

CREATE TABLE Guilds (
    WorldID INTEGER NOT NULL,
    GuildID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY,
    Name TEXT NOT NULL COLLATE NOCASE,
    LeaderID INTEGER NOT NULL,
    Created TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (GuildID),
    UNIQUE (Name),
    UNIQUE (LeaderID)
);

CREATE TABLE GuildRanks (
    GuildID INTEGER NOT NULL,
    Rank SMALLINT NOT NULL,
    Name TEXT NOT NULL,
    PRIMARY KEY (GuildID, Rank)
);

CREATE TABLE GuildMembers (
    GuildID INTEGER NOT NULL,
    CharacterID INTEGER NOT NULL,
    Rank SMALLINT NOT NULL,
    Title TEXT NOT NULL,
    Joined TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (CharacterID)
);
CREATE INDEX GuildMembersGuildIndex ON GuildMembers(GuildID, Rank);

CREATE TABLE GuildInvites (
    GuildID INTEGER NOT NULL,
    CharacterID INTEGER NOT NULL,
    RecruiterID INTEGER NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (GuildID, CharacterID)
);
CREATE INDEX GuildInvitesCharacterIndex ON GuildInvites(CharacterID);
CREATE INDEX GuildInvitesRecruiterIndex ON GuildInvites(RecruiterID);

COMMIT;

