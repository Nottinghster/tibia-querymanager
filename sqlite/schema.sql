-- Primary Tables
--==============================================================================
CREATE TABLE IF NOT EXISTS Worlds (
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
	OnlineRecord INTEGER NOT NULL DEFAULT 0,
	OnlineRecordTimestamp INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (WorldID),
	UNIQUE (Name)
);

CREATE TABLE IF NOT EXISTS Accounts (
	AccountID INTEGER NOT NULL,
	Email TEXT NOT NULL COLLATE NOCASE,
	Auth BLOB NOT NULL,
	PremiumEnd INTEGER NOT NULL DEFAULT 0,
	PendingPremiumDays INTEGER NOT NULL DEFAULT 0,
	Deleted INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (AccountID),
	UNIQUE (Email)
);

CREATE TABLE IF NOT EXISTS Characters (
	WorldID INTEGER NOT NULL,
	CharacterID INTEGER NOT NULL,
	AccountID INTEGER NOT NULL,
	Name TEXT NOT NULL COLLATE NOCASE,
	Sex INTEGER NOT NULL,
	Guild TEXT NOT NULL COLLATE NOCASE DEFAULT '',
	Rank TEXT NOT NULL COLLATE NOCASE DEFAULT '',
	Title TEXT NOT NULL COLLATE NOCASE DEFAULT '',
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
CREATE INDEX IF NOT EXISTS CharactersWorldIndex
		ON Characters(WorldID, IsOnline);
CREATE INDEX IF NOT EXISTS CharactersAccountIndex
		ON Characters(AccountID, IsOnline);
CREATE INDEX IF NOT EXISTS CharactersGuildIndex
		ON Characters(Guild, Rank);

/*
-- TODO(fusion): Have group rights instead of adding individual rights to characters?
ALTER TABLE Characters ADD GroupID INTEGER NOT NULL;
CREATE TABLE IF NOT EXISTS CharacterRights (
	GroupID INTEGER NOT NULL,
	Right TEXT NOT NULL COLLATE NOCASE,
	PRIMARY KEY(GroupID, Right)
);
*/

CREATE TABLE IF NOT EXISTS CharacterRights (
	CharacterID INTEGER NOT NULL,
	Right TEXT NOT NULL COLLATE NOCASE,
	PRIMARY KEY(CharacterID, Right)
);

CREATE TABLE IF NOT EXISTS CharacterDeaths (
	CharacterID INTEGER NOT NULL,
	Level INTEGER NOT NULL,
	OffenderID INTEGER NOT NULL,
	Remark TEXT NOT NULL,
	Unjustified INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS CharacterDeathsCharacterIndex
		ON CharacterDeaths(CharacterID, Level);
CREATE INDEX IF NOT EXISTS CharacterDeathsOffenderIndex
		ON CharacterDeaths(OffenderID, Unjustified);

CREATE TABLE IF NOT EXISTS Buddies (
	WorldID INTEGER NOT NULL,
	AccountID INTEGER NOT NULL,
	BuddyID INTEGER NOT NULL,
	PRIMARY KEY (WorldID, AccountID, BuddyID)
);

CREATE TABLE IF NOT EXISTS WorldInvitations (
	WorldID INTEGER NOT NULL,
	CharacterID INTEGER NOT NULL,
	PRIMARY KEY (WorldID, CharacterID)
);

CREATE TABLE IF NOT EXISTS LoginAttempts (
	AccountID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL,
	Failed INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS LoginAttemptsAccountIndex
		ON LoginAttempts(AccountID, Timestamp);
CREATE INDEX IF NOT EXISTS LoginAttemptsAddressIndex
		ON LoginAttempts(IPAddress, Timestamp);

-- House Tables
--==============================================================================
CREATE TABLE IF NOT EXISTS Houses (
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

CREATE TABLE IF NOT EXISTS HouseOwners (
	WorldID INTEGER NOT NULL,
	HouseID INTEGER NOT NULL,
	OwnerID INTEGER NOT NULL,
	PaidUntil INTEGER NOT NULL,
	PRIMARY KEY (WorldID, HouseID)
);

-- NOTE(fusion): An auction would have a non null `FinishTime` but it doesn't make
-- sense to finish an auction just to restart it afterwards so it should only be
-- set after the first bid, along with `BidderID` and `BidAmount`.
CREATE TABLE IF NOT EXISTS HouseAuctions (
	WorldID INTEGER NOT NULL,
	HouseID INTEGER NOT NULL,
	BidderID INTEGER DEFAULT NULL,
	BidAmount INTEGER DEFAULT NULL,
	FinishTime INTEGER DEFAULT NULL,
	PRIMARY KEY (WorldID, HouseID)
);

CREATE TABLE IF NOT EXISTS HouseTransfers (
	WorldID INTEGER NOT NULL,
	HouseID INTEGER NOT NULL,
	NewOwnerID INTEGER NOT NULL,
	Price INTEGER NOT NULL,
	PRIMARY KEY (WorldID, HouseID)
);

CREATE TABLE IF NOT EXISTS HouseAuctionExclusions (
	CharacterID INTEGER NOT NULL,
	Issued INTEGER NOT NULL,
	Until INTEGER NOT NULL,
	BanishmentID INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS HouseAuctionExclusionsIndex
		ON HouseAuctionExclusions(CharacterID, Until);

CREATE TABLE IF NOT EXISTS HouseAssignments (
	WorldID INTEGER NOT NULL,
	HouseID INTEGER NOT NULL,
	OwnerID INTEGER NOT NULL,
	Price INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS HouseAssignmentsHouseIndex
		ON HouseAssignments(WorldID, HouseID);
CREATE INDEX IF NOT EXISTS HouseAssignmentsTimeIndex
		ON HouseAssignments(WorldID, Timestamp);
CREATE INDEX IF NOT EXISTS HouseAssignmentsOwnerIndex
		ON HouseAssignments(OwnerID);

-- Banishment Tables
--==============================================================================
CREATE TABLE IF NOT EXISTS Banishments (
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
CREATE INDEX IF NOT EXISTS BanishmentsAccountIndex
		ON Banishments(AccountID, Until, FinalWarning);

CREATE TABLE IF NOT EXISTS IPBanishments (
	CharacterID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL,
	Issued INTEGER NOT NULL,
	Until INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS IPBanishmentsAddressIndex
		ON IPBanishments(IPAddress);
CREATE INDEX IF NOT EXISTS IPBanishmentsCharacterIndex
		ON IPBanishments(CharacterID);

CREATE TABLE IF NOT EXISTS Namelocks (
	CharacterID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL,
	Attempts INTEGER NOT NULL DEFAULT 0,
	Approved INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (CharacterID)
);

CREATE TABLE IF NOT EXISTS Notations (
	CharacterID INTEGER NOT NULL,
	IPAddress INTEGER NOT NULL,
	GamemasterID INTEGER NOT NULL,
	Reason TEXT NOT NULL,
	Comment TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS NotationsCharacterIndex
		ON Notations(CharacterID);

CREATE TABLE IF NOT EXISTS Statements (
	WorldID INTEGER NOT NULL,
	Timestamp INTEGER NOT NULL,
	StatementID INTEGER NOT NULL,
	CharacterID INTEGER NOT NULL,
	Channel TEXT NOT NULL,
	Text TEXT NOT NULL,
	PRIMARY KEY (WorldID, Timestamp, StatementID)
);
CREATE INDEX IF NOT EXISTS StatementsCharacterIndex
		ON Statements(CharacterID, Timestamp);

CREATE TABLE IF NOT EXISTS ReportedStatements (
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
CREATE INDEX IF NOT EXISTS ReportedStatementsCharacterIndex
		ON ReportedStatements(CharacterID, Timestamp);
CREATE INDEX IF NOT EXISTS ReportedStatementsBanishmentIndex
		ON ReportedStatements(BanishmentID);

-- Info Tables
--==============================================================================
CREATE TABLE IF NOT EXISTS KillStatistics (
	WorldID INTEGER NOT NULL,
	RaceName TEXT NOT NULL COLLATE NOCASE,
	TimesKilled INTEGER NOT NULL,
	PlayersKilled INTEGER NOT NULL,
	PRIMARY KEY (WorldID, RaceName)
);

CREATE TABLE IF NOT EXISTS OnlineCharacters (
	WorldID INTEGER NOT NULL,
	Name TEXT NOT NULL COLLATE NOCASE,
	Level INTEGER NOT NULL,
	Profession TEXT NOT NULL,
	PRIMARY KEY (WorldID, Name)
);
