-- NOTE(fusion): Everything is inside a transaction to avoid errors from leaving
-- the database in some partial state. Also, notice we don't use `IF NOT EXISTS`
-- because it could also leave the database in a bad state if for example, some
-- old version of a table already existed.
BEGIN;

-- IMPORTANT(fusion): PosgreSQL doesn't have a defined `NOCASE` collation like
-- SQLite and doesn't use a default case-insensitive collation like MySQL. The
-- simplest alternative is to create a custom non-deterministic ICU collation.
--  ICU as a collation provider is supported with PostgreSQL 10+, while
-- non-deterministic collations are supported with PostgreSQL 12+. Either way,
-- it should be safe to assume that it's widely supported, given that version
-- 12 is roughly 6 years old and well past its end-of-life.
--  There are also drawbacks from using these types of collations but I won't
-- go into details. For more information see:
--   https://www.postgresql.org/docs/current/collation.html
CREATE COLLATION NOCASE (
    provider = icu,
    deterministic = false,
    locale = 'und-u-ks-level2'
);

-- IMPORTANT(fusion): Since we're already assuming PostgreSQL 12+ for collations,
-- it's probably a good idea to use IDENTITY columns instead of SERIAL. They're
-- supported with PostgreSQL 10+ and are supposed so fix some shortcommings of
-- SERIAL.

-- TODO(fusion): SQLite tables didn't use foreign key constraints so I'm also not
-- using them here. It might be a good idea for consistency, but it's not a hard
-- requirement.

-- Primary Tables
--==============================================================================
CREATE TABLE Worlds (
    WorldID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY,
    Name TEXT NOT NULL COLLATE NOCASE,
    Type SMALLINT NOT NULL,
    RebootTime SMALLINT NOT NULL,
    Host TEXT NOT NULL,
    Port INTEGER NOT NULL,
    MaxPlayers SMALLINT NOT NULL,
    PremiumPlayerBuffer SMALLINT NOT NULL,
    MaxNewbies SMALLINT NOT NULL,
    PremiumNewbieBuffer SMALLINT NOT NULL,
    OnlinePeak SMALLINT NOT NULL DEFAULT 0,
    OnlinePeakTimestamp TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    LastStartup TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    LastShutdown TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    PRIMARY KEY (WorldID),
    UNIQUE (Name)
);

CREATE TABLE Accounts (
    AccountID INTEGER NOT NULL,
    Email TEXT NOT NULL COLLATE NOCASE,
    Auth BYTEA NOT NULL,
    PremiumEnd TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    PendingPremiumDays SMALLINT NOT NULL DEFAULT 0,
    Deleted BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (AccountID),
    UNIQUE (Email)
);

CREATE TABLE Characters (
    WorldID INTEGER NOT NULL,
    CharacterID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY,
    AccountID INTEGER NOT NULL,
    Name TEXT NOT NULL COLLATE NOCASE,
    Sex SMALLINT NOT NULL,
    Guild TEXT NOT NULL COLLATE NOCASE DEFAULT '',
    Rank TEXT NOT NULL COLLATE NOCASE DEFAULT '',
    Title TEXT NOT NULL DEFAULT '',
    Level SMALLINT NOT NULL DEFAULT 0,
    Profession TEXT NOT NULL DEFAULT '',
    Residence TEXT NOT NULL DEFAULT '',
    LastLoginTime TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    TutorActivities INTEGER NOT NULL DEFAULT 0,
    IsOnline SMALLINT NOT NULL DEFAULT 0,
    Deleted BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (CharacterID),
    UNIQUE (Name)
);
CREATE INDEX CharactersWorldIndex   ON Characters(WorldID, IsOnline);
CREATE INDEX CharactersAccountIndex ON Characters(AccountID, IsOnline);
CREATE INDEX CharactersGuildIndex   ON Characters(Guild, Rank);

-- NOTE(fusion): It seems `RIGHT` is a reserved keyword and trying to use
-- it as a column name will generate an error.
CREATE TABLE CharacterRights (
    CharacterID INTEGER NOT NULL,
    Name TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY(CharacterID, Name)
);

CREATE TABLE CharacterDeaths (
    CharacterID INTEGER NOT NULL,
    Level SMALLINT NOT NULL,
    OffenderID INTEGER NOT NULL,
    Remark TEXT NOT NULL,
    Unjustified BOOLEAN NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL
);
CREATE INDEX CharacterDeathsCharacterIndex ON CharacterDeaths(CharacterID, Level);
CREATE INDEX CharacterDeathsOffenderIndex  ON CharacterDeaths(OffenderID, Unjustified);

CREATE TABLE Buddies (
    WorldID INTEGER NOT NULL,
    AccountID INTEGER NOT NULL,
    BuddyID INTEGER NOT NULL,
    PRIMARY KEY (WorldID, AccountID, BuddyID)
);

CREATE TABLE WorldInvitations (
    WorldID INTEGER NOT NULL,
    CharacterID INTEGER NOT NULL,
    PRIMARY KEY (WorldID, CharacterID)
);

CREATE TABLE LoginAttempts (
    AccountID INTEGER NOT NULL,
    IPAddress INET NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL,
    Failed BOOLEAN NOT NULL
);
CREATE INDEX LoginAttemptsAccountIndex ON LoginAttempts(AccountID, Timestamp);
CREATE INDEX LoginAttemptsAddressIndex ON LoginAttempts(IPAddress, Timestamp);

-- House Tables
--==============================================================================
CREATE TABLE Houses (
    WorldID INTEGER NOT NULL,
    HouseID INTEGER NOT NULL,
    Name TEXT NOT NULL,
    Rent INTEGER NOT NULL,
    Description TEXT NOT NULL,
    Size INTEGER NOT NULL,
    PositionX INTEGER NOT NULL,
    PositionY INTEGER NOT NULL,
    PositionZ INTEGER NOT NULL,
    Town TEXT NOT NULL,
    GuildHouse BOOLEAN NOT NULL,
    PRIMARY KEY (WorldID, HouseID)
);

CREATE TABLE HouseOwners (
    WorldID INTEGER NOT NULL,
    HouseID INTEGER NOT NULL,
    OwnerID INTEGER NOT NULL,
    PaidUntil TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (WorldID, HouseID)
);

-- NOTE(fusion): Auctions with a NULL `FinishTime` aren't active, to avoid running
-- multiple times with no actual bidder. It should be set after the first bid along
-- with `BidderID` and `BidAmount`.
CREATE TABLE HouseAuctions (
    WorldID INTEGER NOT NULL,
    HouseID INTEGER NOT NULL,
    BidderID INTEGER DEFAULT NULL,
    BidAmount INTEGER DEFAULT NULL,
    FinishTime TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY (WorldID, HouseID)
);

CREATE TABLE HouseTransfers (
    WorldID INTEGER NOT NULL,
    HouseID INTEGER NOT NULL,
    NewOwnerID INTEGER NOT NULL,
    Price INTEGER NOT NULL,
    PRIMARY KEY (WorldID, HouseID)
);

CREATE TABLE HouseAuctionExclusions (
    CharacterID INTEGER NOT NULL,
    Issued TIMESTAMPTZ NOT NULL,
    Until TIMESTAMPTZ NOT NULL,
    BanishmentID INTEGER NOT NULL
);
CREATE INDEX HouseAuctionExclusionsIndex ON HouseAuctionExclusions(CharacterID, Until);

CREATE TABLE HouseAssignments (
    WorldID INTEGER NOT NULL,
    HouseID INTEGER NOT NULL,
    OwnerID INTEGER NOT NULL,
    Price INTEGER NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL
);
CREATE INDEX HouseAssignmentsHouseIndex ON HouseAssignments(WorldID, HouseID);
CREATE INDEX HouseAssignmentsTimeIndex  ON HouseAssignments(WorldID, Timestamp);
CREATE INDEX HouseAssignmentsOwnerIndex ON HouseAssignments(OwnerID);

-- Banishment Tables
--==============================================================================
CREATE TABLE Banishments (
    BanishmentID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY,
    AccountID INTEGER NOT NULL,
    IPAddress INET NOT NULL,
    GamemasterID INTEGER NOT NULL,
    Reason TEXT NOT NULL,
    Comment TEXT NOT NULL,
    FinalWarning BOOLEAN NOT NULL,
    Issued TIMESTAMPTZ NOT NULL,
    Until TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (BanishmentID)
);
CREATE INDEX BanishmentsAccountIndex ON Banishments(AccountID, Until, FinalWarning);

CREATE TABLE IPBanishments (
    CharacterID INTEGER NOT NULL,
    IPAddress INET NOT NULL,
    GamemasterID INTEGER NOT NULL,
    Reason TEXT NOT NULL,
    Comment TEXT NOT NULL,
    Issued TIMESTAMPTZ NOT NULL,
    Until TIMESTAMPTZ NOT NULL
);
CREATE INDEX IPBanishmentsAddressIndex   ON IPBanishments(IPAddress);
CREATE INDEX IPBanishmentsCharacterIndex ON IPBanishments(CharacterID);

CREATE TABLE Namelocks (
    CharacterID INTEGER NOT NULL,
    IPAddress INET NOT NULL,
    GamemasterID INTEGER NOT NULL,
    Reason TEXT NOT NULL,
    Comment TEXT NOT NULL,
    Attempts INTEGER NOT NULL DEFAULT 0,
    Approved BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (CharacterID)
);

CREATE TABLE Notations (
    CharacterID INTEGER NOT NULL,
    IPAddress INET NOT NULL,
    GamemasterID INTEGER NOT NULL,
    Reason TEXT NOT NULL,
    Comment TEXT NOT NULL
);
CREATE INDEX NotationsCharacterIndex ON Notations(CharacterID);

CREATE TABLE Statements (
    WorldID INTEGER NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL,
    StatementID INTEGER NOT NULL,
    CharacterID INTEGER NOT NULL,
    Channel TEXT NOT NULL,
    Text TEXT NOT NULL,
    PRIMARY KEY (WorldID, Timestamp, StatementID)
);
CREATE INDEX StatementsCharacterIndex ON Statements(CharacterID, Timestamp);

CREATE TABLE ReportedStatements (
    WorldID INTEGER NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL,
    StatementID INTEGER NOT NULL,
    CharacterID INTEGER NOT NULL,
    BanishmentID INTEGER NOT NULL,
    ReporterID INTEGER NOT NULL,
    Reason TEXT NOT NULL,
    Comment TEXT NOT NULL,
    PRIMARY KEY (WorldID, Timestamp, StatementID)
);
CREATE INDEX ReportedStatementsCharacterIndex  ON ReportedStatements(CharacterID, Timestamp);
CREATE INDEX ReportedStatementsBanishmentIndex ON ReportedStatements(BanishmentID);

-- Info Tables
--==============================================================================
CREATE TABLE KillStatistics (
    WorldID INTEGER NOT NULL,
    RaceName TEXT NOT NULL COLLATE NOCASE,
    TimesKilled INTEGER NOT NULL,
    PlayersKilled INTEGER NOT NULL,
    PRIMARY KEY (WorldID, RaceName)
);

CREATE TABLE OnlineCharacters (
    WorldID INTEGER NOT NULL,
    Name TEXT NOT NULL COLLATE NOCASE,
    Level SMALLINT NOT NULL,
    Profession TEXT NOT NULL,
    PRIMARY KEY (WorldID, Name)
);

-- Schema Info
--==============================================================================
-- NOTE(fusion): The `SchemaInfo` table should hold information about the schema
-- and be used for consistency checks at startup. It currently only contains the
-- schema version, which I feel is the only value needed.
CREATE TABLE SchemaInfo (
    Key TEXT NOT NULL COLLATE NOCASE,
    Value TEXT NOT NULL,
    PRIMARY KEY (Key)
);

INSERT INTO SchemaInfo (Key, Value) VALUES ('VERSION', '1');

COMMIT;
