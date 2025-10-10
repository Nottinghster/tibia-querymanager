-- NOTE(fusion): The query manager WON'T automatically run this but the game
-- server still requires at least the world config to be able to boot up. It
-- is probably a good idea to keep this separated from `schema.sql` and then
-- running it with `sqlite3 -echo tibia.db < sqlite/init.sql`, although it is
-- not mandatory.
--	Because this isn't automatically managed, all queries are wrapped in a
-- transaction to avoid partial writes in case of errors.
--==============================================================================
BEGIN;

INSERT INTO Worlds (WorldID, Name, Type, RebootTime, Host, Port, MaxPlayers,
					PremiumPlayerBuffer, MaxNewbies, PremiumNewbieBuffer)
	VALUES (1, 'Zanera', 0, 5, 'localhost', 7172, 1000, 100, 300, 100);

-- 111111/tibia
INSERT INTO Accounts (AccountID, Email, Auth)
	VALUES (111111, '@tibia', X'206699cbc2fae1683118c873d746aa376049cb5923ef0980298bb7acbba527ec9e765668f7a338dffea34acf61a20efb654c1e9c62d35148dba2aeeef8dc7788');

INSERT INTO Characters (WorldID, CharacterID, AccountID, Name, Sex)
	VALUES (1, 1, 111111, 'Gamemaster', 1), (1, 2, 111111, 'Player', 1);

INSERT INTO CharacterRights (CharacterID, Right)
	VALUES
		(1, 'NOTATION'),
		(1, 'NAMELOCK'),
		(1, 'STATEMENT_REPORT'),
		(1, 'BANISHMENT'),
		(1, 'FINAL_WARNING'),
		(1, 'IP_BANISHMENT'),
		(1, 'KICK'),
		(1, 'HOME_TELEPORT'),
		(1, 'GAMEMASTER_BROADCAST'),
		(1, 'ANONYMOUS_BROADCAST'),
		(1, 'NO_BANISHMENT'),
		(1, 'ALLOW_MULTICLIENT'),
		(1, 'LOG_COMMUNICATION'),
		(1, 'READ_GAMEMASTER_CHANNEL'),
		(1, 'READ_TUTOR_CHANNEL'),
		(1, 'HIGHLIGHT_HELP_CHANNEL'),
		(1, 'SEND_BUGREPORTS'),
		(1, 'NAME_INSULTING'),
		(1, 'NAME_SENTENCE'),
		(1, 'NAME_NONSENSICAL_LETTERS'),
		(1, 'NAME_BADLY_FORMATTED'),
		(1, 'NAME_NO_PERSON'),
		(1, 'NAME_CELEBRITY'),
		(1, 'NAME_COUNTRY'),
		(1, 'NAME_FAKE_IDENTITY'),
		(1, 'NAME_FAKE_POSITION'),
		(1, 'STATEMENT_INSULTING'),
		(1, 'STATEMENT_SPAMMING'),
		(1, 'STATEMENT_ADVERT_OFFTOPIC'),
		(1, 'STATEMENT_ADVERT_MONEY'),
		(1, 'STATEMENT_NON_ENGLISH'),
		(1, 'STATEMENT_CHANNEL_OFFTOPIC'),
		(1, 'STATEMENT_VIOLATION_INCITING'),
		(1, 'CHEATING_BUG_ABUSE'),
		(1, 'CHEATING_GAME_WEAKNESS'),
		(1, 'CHEATING_MACRO_USE'),
		(1, 'CHEATING_MODIFIED_CLIENT'),
		(1, 'CHEATING_HACKING'),
		(1, 'CHEATING_MULTI_CLIENT'),
		(1, 'CHEATING_ACCOUNT_TRADING'),
		(1, 'CHEATING_ACCOUNT_SHARING'),
		(1, 'GAMEMASTER_THREATENING'),
		(1, 'GAMEMASTER_PRETENDING'),
		(1, 'GAMEMASTER_INFLUENCE'),
		(1, 'GAMEMASTER_FALSE_REPORTS'),
		(1, 'KILLING_EXCESSIVE_UNJUSTIFIED'),
		(1, 'DESTRUCTIVE_BEHAVIOUR'),
		(1, 'SPOILING_AUCTION'),
		(1, 'INVALID_PAYMENT'),
		(1, 'TELEPORT_TO_CHARACTER'),
		(1, 'TELEPORT_TO_MARK'),
		(1, 'TELEPORT_VERTICAL'),
		(1, 'TELEPORT_TO_COORDINATE'),
		(1, 'LEVITATE'),
		(1, 'SPECIAL_MOVEUSE'),
		(1, 'MODIFY_GOSTRENGTH'),
		(1, 'SHOW_COORDINATE'),
		(1, 'RETRIEVE'),
		(1, 'ENTER_HOUSES'),
		(1, 'OPEN_NAMEDOORS'),
		(1, 'INVULNERABLE'),
		(1, 'UNLIMITED_MANA'),
		(1, 'KEEP_INVENTORY'),
		(1, 'ALL_SPELLS'),
		(1, 'UNLIMITED_CAPACITY'),
		(1, 'ATTACK_EVERYWHERE'),
		(1, 'NO_LOGOUT_BLOCK'),
		(1, 'GAMEMASTER_OUTFIT'),
		(1, 'ILLUMINATE'),
		(1, 'CHANGE_PROFESSION'),
		(1, 'IGNORED_BY_MONSTERS'),
		(1, 'SHOW_KEYHOLE_NUMBERS'),
		(1, 'CREATE_OBJECTS'),
		(1, 'CREATE_MONEY'),
		(1, 'CREATE_MONSTERS'),
		(1, 'CHANGE_SKILLS'),
		(1, 'CLEANUP_FIELDS'),
		(1, 'NO_STATISTICS');

COMMIT;
