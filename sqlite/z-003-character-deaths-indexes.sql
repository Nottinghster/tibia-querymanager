-- NOTE(fusion): This file contains index adjustments for the `CharacterDeaths`
-- table. It is not strictly necessary as there are no modified tables but should
-- help with queries such as the most recent deaths/kills from a specific character
-- or overall.
--  It can be executed automatically as a patch if placed at `sqlite/patches`. For
-- more details see `sqlite/README.txt`. The changes are already present in the
-- latest `schema.sql`, so trying to apply them on a newly created database will
-- result in errors.
--==============================================================================

DROP INDEX CharacterDeathsCharacterIndex;
DROP INDEX CharacterDeathsOffenderIndex;

CREATE INDEX CharacterDeathsCharacterIndex ON CharacterDeaths(CharacterID, Timestamp);
CREATE INDEX CharacterDeathsOffenderIndex  ON CharacterDeaths(OffenderID, Timestamp);
CREATE INDEX CharacterDeathsTimeIndex      ON CharacterDeaths(Timestamp);

