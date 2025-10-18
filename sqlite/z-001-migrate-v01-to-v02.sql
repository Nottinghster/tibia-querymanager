-- NOTE(fusion): This file contains the migration script from v0.1 to v0.2. It
-- can be placed into `patches/` to upgrade an existing database. Note that these
-- changes are already present in the latest `schema.sql`, so trying to patch a
-- newly created database will probably result in errors. See `sqlite/README.txt`
-- for more details.
--==============================================================================

ALTER TABLE Worlds          RENAME COLUMN OnlineRecord          TO OnlinePeak;
ALTER TABLE Worlds          RENAME COLUMN OnlineRecordTimestamp TO OnlinePeakTimestamp;
ALTER TABLE CharacterRights RENAME COLUMN Right                 TO Name;

ALTER TABLE Worlds          ADD COLUMN LastStartup INTEGER NOT NULL DEFAULT 0;
ALTER TABLE Worlds          ADD COLUMN LastShutdown INTEGER NOT NULL DEFAULT 0;

