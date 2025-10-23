-- NOTE(fusion): This file contains the migration script from v0.1 to v0.2. It
-- must be manually executed as `sqlite3 -bail -echo tibia.db < migration.sql`
-- because the original schema didn't have a `Patches` table which is necessary
-- with the new automatic patching system. Future migration scripts can be placed
-- in `patches/` for automatic execution but not this one, unfortunately. For
-- more details see `sqlite/README.txt`.
--  These changes are already present in the latest `schema.sql`, so trying to
-- apply them to a newly created database will result in errors.
--==============================================================================

BEGIN;

PRAGMA application_id = 0x54694442;

PRAGMA user_version = 1;

CREATE TABLE Patches (
	FileName TEXT NOT NULL COLLATE NOCASE,
	Timestamp INTEGER NOT NULL,
	UNIQUE (FileName)
);

ALTER TABLE Worlds          RENAME COLUMN OnlineRecord TO OnlinePeak;
ALTER TABLE Worlds          RENAME COLUMN OnlineRecordTimestamp TO OnlinePeakTimestamp;
ALTER TABLE Worlds          ADD COLUMN LastStartup INTEGER NOT NULL DEFAULT 0;
ALTER TABLE Worlds          ADD COLUMN LastShutdown INTEGER NOT NULL DEFAULT 0;
ALTER TABLE CharacterRights RENAME COLUMN Right TO Name;

COMMIT;

