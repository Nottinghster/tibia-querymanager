-- NOTE(fusion): This file contains sample initial data. It's wrapped into a
-- transaction to avoid errors from leaving the database in some partial state.
BEGIN;

-- 111111/tibia
INSERT INTO Accounts (AccountID, Email, Auth)
    VALUES (111111, '@tibia', '\x206699cbc2fae1683118c873d746aa376049cb5923ef0980298bb7acbba527ec9e765668f7a338dffea34acf61a20efb654c1e9c62d35148dba2aeeef8dc7788');

-- NOTE(fusion): This would generally be a sequence of insertions but the way
-- PostgreSQL handles unique auto increment columns makes so manually inserted
-- values end up colliding with generated values at some point.
--  On a real scenario, we'd never have manually assigned ids unless we're
-- restoring some database dump, in which case it would already have saved
-- sequences.
WITH
    WorldIns AS (
        INSERT INTO Worlds (Name, Type, RebootTime, Host, Port, MaxPlayers,
                        PremiumPlayerBuffer, MaxNewbies, PremiumNewbieBuffer)
            VALUES ('Zanera', 0, 5, 'localhost', 7172, 1000, 100, 300, 100)
            RETURNING WorldID, Name
    ),
    CharacterIns AS (
        INSERT INTO Characters (WorldID, AccountID, Name, Sex)
            SELECT W.WorldID, C.AccountID, C.Name, C.Sex
            FROM WorldIns AS W,
                LATERAL (VALUES
                    (111111, 'Gamemaster on ' || W.Name, 1),
                    (111111, 'Player on ' || W.Name, 1)
                ) AS C (AccountID, Name, Sex)
            RETURNING CharacterID, Name
    )
INSERT INTO CharacterRights(CharacterID, Name)
    SELECT C.CharacterID, R.RightName
    FROM CharacterIns AS C,
        (VALUES
            ('NOTATION'),
            ('NAMELOCK'),
            ('STATEMENT_REPORT'),
            ('BANISHMENT'),
            ('FINAL_WARNING'),
            ('IP_BANISHMENT'),
            ('KICK'),
            ('HOME_TELEPORT'),
            ('GAMEMASTER_BROADCAST'),
            ('ANONYMOUS_BROADCAST'),
            ('NO_BANISHMENT'),
            ('ALLOW_MULTICLIENT'),
            ('LOG_COMMUNICATION'),
            ('READ_GAMEMASTER_CHANNEL'),
            ('READ_TUTOR_CHANNEL'),
            ('HIGHLIGHT_HELP_CHANNEL'),
            ('SEND_BUGREPORTS'),
            ('NAME_INSULTING'),
            ('NAME_SENTENCE'),
            ('NAME_NONSENSICAL_LETTERS'),
            ('NAME_BADLY_FORMATTED'),
            ('NAME_NO_PERSON'),
            ('NAME_CELEBRITY'),
            ('NAME_COUNTRY'),
            ('NAME_FAKE_IDENTITY'),
            ('NAME_FAKE_POSITION'),
            ('STATEMENT_INSULTING'),
            ('STATEMENT_SPAMMING'),
            ('STATEMENT_ADVERT_OFFTOPIC'),
            ('STATEMENT_ADVERT_MONEY'),
            ('STATEMENT_NON_ENGLISH'),
            ('STATEMENT_CHANNEL_OFFTOPIC'),
            ('STATEMENT_VIOLATION_INCITING'),
            ('CHEATING_BUG_ABUSE'),
            ('CHEATING_GAME_WEAKNESS'),
            ('CHEATING_MACRO_USE'),
            ('CHEATING_MODIFIED_CLIENT'),
            ('CHEATING_HACKING'),
            ('CHEATING_MULTI_CLIENT'),
            ('CHEATING_ACCOUNT_TRADING'),
            ('CHEATING_ACCOUNT_SHARING'),
            ('GAMEMASTER_THREATENING'),
            ('GAMEMASTER_PRETENDING'),
            ('GAMEMASTER_INFLUENCE'),
            ('GAMEMASTER_FALSE_REPORTS'),
            ('KILLING_EXCESSIVE_UNJUSTIFIED'),
            ('DESTRUCTIVE_BEHAVIOUR'),
            ('SPOILING_AUCTION'),
            ('INVALID_PAYMENT'),
            ('TELEPORT_TO_CHARACTER'),
            ('TELEPORT_TO_MARK'),
            ('TELEPORT_VERTICAL'),
            ('TELEPORT_TO_COORDINATE'),
            ('LEVITATE'),
            ('SPECIAL_MOVEUSE'),
            ('MODIFY_GOSTRENGTH'),
            ('SHOW_COORDINATE'),
            ('RETRIEVE'),
            ('ENTER_HOUSES'),
            ('OPEN_NAMEDOORS'),
            ('INVULNERABLE'),
            ('UNLIMITED_MANA'),
            ('KEEP_INVENTORY'),
            ('ALL_SPELLS'),
            ('UNLIMITED_CAPACITY'),
            ('ATTACK_EVERYWHERE'),
            ('NO_LOGOUT_BLOCK'),
            ('GAMEMASTER_OUTFIT'),
            ('ILLUMINATE'),
            ('CHANGE_PROFESSION'),
            ('IGNORED_BY_MONSTERS'),
            ('SHOW_KEYHOLE_NUMBERS'),
            ('CREATE_OBJECTS'),
            ('CREATE_MONEY'),
            ('CREATE_MONSTERS'),
            ('CHANGE_SKILLS'),
            ('CLEANUP_FIELDS'),
            ('NO_STATISTICS')
        ) AS R (RightName)
    WHERE C.Name ILIKE 'Gamemaster%' COLLATE "default";

COMMIT;
