-- Database ApplicationID and UserVersion
--==============================================================================
-- NOTE(fusion): SQLite's application id, used to identify an existing database.
-- The query manager will only access an existing database whose application id
-- is exactly 0x54694442 which is the ASCII for "TiDB" or "Tibia Database".
PRAGMA application_id = 0x54694442;

-- NOTE(fusion): SQLite's user version, used to track the current schema version.
-- The query manager will only access an existing database whose user version is
-- exactly `SQLITE_USER_VERSION`, defined in `database_sqlite.cc`.
PRAGMA user_version = 1;

-- NOTE(fusion): A table with the history of applied patches. It will be inspected
-- at startup to decide which patches still need to be applied. See `sqlite/README.txt`
-- for more details.
CREATE TABLE Patches (
	FileName TEXT NOT NULL COLLATE NOCASE,
	Timestamp INTEGER NOT NULL,
	UNIQUE (FileName)
);

-- Primary Tables
--==============================================================================
CREATE TABLE Worlds (
	WorldID INTEGER NOT NULL,
	Name TEXT NOT NULL COLLATE NOCASE,
	Type INTEGER NOT NULL,
	RebootTime INTEGER NOT NULL,
	Host TEXT NOT NULL,
	Port INTEGER NOT NULL,
	MaxPlayers INTEGER NOT NULL,
	PremiumPlayerBuffer INTEGER NOT NULL,
	MaxNewbies INTEGER NOT NULL,
	PremiumNewbieBuffer INTEGER NOT NULL,
	OnlinePeak INTEGER NOT NULL DEFAULT 0,
	OnlinePeakTimestamp INTEGER NOT NULL DEFAULT 0,
	LastStartup INTEGER NOT NULL DEFAULT 0,
	LastShutdown INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (WorldID),
	UNIQUE (Name)
);

CREATE TABLE Accounts (
	AccountID INTEGER NOT NULL,
	Email TEXT NOT NULL COLLATE NOCASE,
	Auth BLOB NOT NULL,
	PremiumEnd INTEGER NOT NULL DEFAULT 0,
	PendingPremiumDays INTEGER NOT NULL DEFAULT 0,
	Deleted INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (AccountID),
	UNIQUE (Email)
);

CREATE TABLE Characters (
	WorldID INTEGER NOT NULL,
	CharacterID INTEGER NOT NULL,
	AccountID INTEGER NOT NULL,
	Name TEXT NOT NULL COLLATE NOCASE,
	Sex INTEGER NOT NULL,
	Guild TEXT NOT NULL COLLATE NOCASE DEFAULT '',
	Rank TEXT NOT NULL COLLATE NOCASE DEFAULT '',
	Title TEXT NOT NULL DEFAULT '',
	Level INTEGER NOT NULL DEFAULT 0,
	Profession TEXT NOT NULL DEFAULT '',
	Residence TEXT NOT NULL DEFAULT '',
	LastLoginTime INTEGER NOT NULL DEFAULT 0,
	TutorActivities INTEGER NOT NULL DEFAULT 0,
	IsOnline INTEGER NOT NULL DEFAULT 0,
	Deleted INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (CharacterID),
	UNIQUE (Name)
);
CREATE INDEX CharactersWorldIndex   ON Characters(WorldID, IsOnline);
CREATE INDEX CharactersAccountIndex ON Characters(AccountID, IsOnline);
CREATE INDEX CharactersGuildIndex   ON Characters(Guild, Rank);

/*
-- TODO(fusion): Have group rights instead of adding individual rights to characters?
ALTER TABLE Characters ADD GroupID INTEGER NOT NULL;
CREATE TABLE CharacterRights (
	GroupID INTEGER NOT NULL,
	Name TEXT NOT NULL COLLATE NOCASE,
	PRIMARY KEY(GroupID, Name)
);
*/

CREATE TABLE CharacterRights (
	CharacterID INTEGER NOT NULL,
	Name TEXT NOT NULL COLLATE NOCASE,
	PRIMARY KEY(CharacterID, Name)
);

CREATE TABLE CharacterDeaths (
	CharacterID INTEGER NOT NULL,
	Level INTEGER NOT NULL,
	OffenderID INTEGER NOT NULL,
	Remark TEXT NOT NULL,
	Unjustified INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL
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
	IPAddress INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL,
	Failed INTEGER NOT NULL
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
	GuildHouse INTEGER NOT NULL,
	PRIMARY KEY (WorldID, HouseID)
);

CREATE TABLE HouseOwners (
	WorldID INTEGER NOT NULL,
	HouseID INTEGER NOT NULL,
	OwnerID INTEGER NOT NULL,
	PaidUntil INTEGER NOT NULL,
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
	FinishTime INTEGER DEFAULT NULL,
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
	Issued INTEGER NOT NULL,
	Until INTEGER NOT NULL,
	BanishmentID INTEGER NOT NULL
);
CREATE INDEX HouseAuctionExclusionsIndex ON HouseAuctionExclusions(CharacterID, Until);

CREATE TABLE HouseAssignments (
	WorldID INTEGER NOT NULL,
	HouseID INTEGER NOT NULL,
	OwnerID INTEGER NOT NULL,
	Price INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL
);
CREATE INDEX HouseAssignmentsHouseIndex ON HouseAssignments(WorldID, HouseID);
CREATE INDEX HouseAssignmentsTimeIndex  ON HouseAssignments(WorldID, Timestamp);
CREATE INDEX HouseAssignmentsOwnerIndex ON HouseAssignments(OwnerID);

-- Banishment Tables
--==============================================================================
CREATE TABLE Banishments (
	BanishmentID INTEGER NOT NULL,
	AccountID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL,
	FinalWarning INTEGER NOT NULL,
	Issued INTEGER NOT NULL,
	Until INTEGER NOT NULL,
	PRIMARY KEY (BanishmentID)
);
CREATE INDEX BanishmentsAccountIndex ON Banishments(AccountID, Until, FinalWarning);

CREATE TABLE IPBanishments (
	CharacterID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL,
	Issued INTEGER NOT NULL,
	Until INTEGER NOT NULL
);
CREATE INDEX IPBanishmentsAddressIndex   ON IPBanishments(IPAddress);
CREATE INDEX IPBanishmentsCharacterIndex ON IPBanishments(CharacterID);

CREATE TABLE Namelocks (
	CharacterID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL,
	Attempts INTEGER NOT NULL DEFAULT 0,
	Approved INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (CharacterID)
);

CREATE TABLE Notations (
	CharacterID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL
);
CREATE INDEX NotationsCharacterIndex ON Notations(CharacterID);

CREATE TABLE Statements (
	WorldID INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL,
	StatementID INTEGER NOT NULL,
	CharacterID INTEGER NOT NULL,
	Channel TEXT NOT NULL,
	Text TEXT NOT NULL,
	PRIMARY KEY (WorldID, Timestamp, StatementID)
);
CREATE INDEX StatementsCharacterIndex ON Statements(CharacterID, Timestamp);

CREATE TABLE ReportedStatements (
	WorldID INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL,
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
	Level INTEGER NOT NULL,
	Profession TEXT NOT NULL,
	PRIMARY KEY (WorldID, Name)
);

