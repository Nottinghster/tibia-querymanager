#include "querymanager.hh"
#include "sqlite3.h"

struct TCachedStatement{
	sqlite3_stmt *Stmt;
	int64 LastUsed;
	uint32 Hash;
};

static sqlite3 *g_Database = NULL;
static TCachedStatement *g_CachedStatements = NULL;

// NOTE(fusion): SQLite's application id. We're currently setting it to ASCII
// "TiDB" for "Tibia Database".
constexpr int g_ApplicationID = 0x54694442;

// Statement Cache
//==============================================================================
// IMPORTANT(fusion): Prepared statements that are not reset after use may keep
// transactions open in which case an older view to the database is held, making
// changes from other processes, including the sqlite shell, not visible. I have
// not found anything on the SQLite docs but here's a stack overflow question:
//	`https://stackoverflow.com/questions/43949228`
struct AutoStmtReset{
private:
	sqlite3_stmt *m_Stmt;

public:
	AutoStmtReset(sqlite3_stmt *Stmt){
		m_Stmt = Stmt;
	}

	~AutoStmtReset(void){
		if(m_Stmt){
			sqlite3_reset(m_Stmt);
			m_Stmt = NULL;
		}
	}
};

uint32 HashText(const char *Text){
	// FNV1a 32-bits
	uint32 Hash = 0x811C9DC5U;
	for(int i = 0; Text[i] != 0; i += 1){
		Hash ^= (uint32)Text[i];
		Hash *= 0x01000193U;
	}
	return Hash;
}

sqlite3_stmt *PrepareQuery(const char *Text){
	sqlite3_stmt *Stmt = NULL;
	int LeastRecentlyUsed = 0;
	int64 LeastRecentlyUsedTime = g_CachedStatements[0].LastUsed;
	uint32 Hash = HashText(Text);
	for(int i = 0; i < g_MaxCachedStatements; i += 1){
		TCachedStatement *Entry = &g_CachedStatements[i];

		if(Entry->LastUsed < LeastRecentlyUsedTime){
			LeastRecentlyUsed = i;
			LeastRecentlyUsedTime = Entry->LastUsed;
		}

		if(Entry->Stmt != NULL && Entry->Hash == Hash){
			const char *EntryText = sqlite3_sql(Entry->Stmt);
			ASSERT(EntryText != NULL);
			if(strcmp(EntryText, Text) == 0){
				Stmt = Entry->Stmt;
				Entry->LastUsed = g_MonotonicTimeMS;
				break;
			}
		}
	}

	if(Stmt == NULL){
		if(sqlite3_prepare_v3(g_Database, Text, -1,
				SQLITE_PREPARE_PERSISTENT, &Stmt, NULL) != SQLITE_OK){
			LOG_ERR("Failed to prepare query: %s", sqlite3_errmsg(g_Database));
			return NULL;
		}

		TCachedStatement *Entry = &g_CachedStatements[LeastRecentlyUsed];
		if(Entry->Stmt != NULL){
			sqlite3_finalize(Entry->Stmt);
		}

		Entry->Stmt = Stmt;
		Entry->LastUsed = g_MonotonicTimeMS;
		Entry->Hash = Hash;
	}else{
		if(sqlite3_stmt_busy(Stmt) != 0){
			LOG_WARN("Statement \"%.30s%s\" wasn't properly reset. Use the"
					" `AutoStmtReset` wrapper or manually reset it after usage"
					" to avoid it holding onto an older view of the database,"
					" making changes from other processes not visible.",
					Text, (strlen(Text) > 30 ? "..." : ""));
			sqlite3_reset(Stmt);
		}

		sqlite3_clear_bindings(Stmt);
	}

	return Stmt;
}

bool InitStatementCache(void){
	ASSERT(g_CachedStatements == NULL);
	g_CachedStatements = (TCachedStatement*)calloc(
			g_MaxCachedStatements, sizeof(TCachedStatement));
	return true;
}

void ExitStatementCache(void){
	if(g_CachedStatements != NULL){
		for(int i = 0; i < g_MaxCachedStatements; i += 1){
			TCachedStatement *Entry = &g_CachedStatements[i];
			if(Entry->Stmt != NULL){
				sqlite3_finalize(Entry->Stmt);
				Entry->Stmt = NULL;
			}

			Entry->LastUsed = 0;
			Entry->Hash = 0;
		}

		free(g_CachedStatements);
		g_CachedStatements = NULL;
	}
}

// TransactionScope
//==============================================================================
TransactionScope::TransactionScope(const char *Context){
	m_Context = (Context != NULL ? Context : "NOCONTEXT");
	m_Running = false;
}

TransactionScope::~TransactionScope(void){
	if(m_Running && !ExecInternal("ROLLBACK")){
		LOG_ERR("Failed to rollback transaction (%s)", m_Context);
	}
}

bool TransactionScope::Begin(void){
	if(m_Running){
		LOG_ERR("Transaction (%s) already running", m_Context);
		return false;
	}

	if(!ExecInternal("BEGIN")){
		LOG_ERR("Failed to begin transaction (%s)", m_Context);
		return false;
	}

	m_Running = true;
	return true;
}

bool TransactionScope::Commit(void){
	if(!m_Running){
		LOG_ERR("Transaction (%s) not running", m_Context);
		return false;
	}

	if(!ExecInternal("COMMIT")){
		LOG_ERR("Failed to commit transaction (%s)", m_Context);
		return false;
	}

	m_Running = false;
	return true;
}

// Primary tables
//==============================================================================
int GetWorldID(const char *WorldName){
	ASSERT(WorldName != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT WorldID FROM Worlds WHERE Name = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_text(Stmt, 1, WorldName, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldName: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	return (ErrorCode == SQLITE_ROW ? sqlite3_column_int(Stmt, 0) : 0);
}

bool GetWorlds(DynamicArray<TWorld> *Worlds){
	ASSERT(Worlds != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"WITH N (WorldID, NumPlayers) AS ("
				"SELECT WorldID, COUNT(*) FROM OnlineCharacters GROUP BY WorldID"
			")"
			" SELECT W.Name, W.Type, COALESCE(N.NumPlayers, 0), W.MaxPlayers,"
				" W.OnlineRecord, W.OnlineRecordTimestamp"
			" FROM Worlds AS W"
			" LEFT JOIN N ON W.WorldID = N.WorldID");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	while(sqlite3_step(Stmt) == SQLITE_ROW){
		TWorld World = {};
		StringCopy(World.Name, sizeof(World.Name),
				(const char*)sqlite3_column_text(Stmt, 0));
		World.Type = sqlite3_column_int(Stmt, 1);
		World.NumPlayers = sqlite3_column_int(Stmt, 2);
		World.MaxPlayers = sqlite3_column_int(Stmt, 3);
		World.OnlineRecord = sqlite3_column_int(Stmt, 4);
		World.OnlineRecordTimestamp = sqlite3_column_int(Stmt, 5);
		Worlds->Push(World);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetWorldConfig(int WorldID, TWorldConfig *WorldConfig){
	ASSERT(WorldConfig != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT Type, RebootTime, Host, Port, MaxPlayers,"
				" PremiumPlayerBuffer, MaxNewbies, PremiumNewbieBuffer"
			" FROM Worlds WHERE WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_ROW){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int IPAddress;
	const char *HostName = (const char*)sqlite3_column_text(Stmt, 2);
	if(HostName == NULL || !ResolveHostName(HostName, &IPAddress)){
		LOG_ERR("Failed to resolve world %d host name \"%s\"", WorldID, HostName);
		return false;
	}

	WorldConfig->Type					= sqlite3_column_int(Stmt, 0);
	WorldConfig->RebootTime				= sqlite3_column_int(Stmt, 1);
	WorldConfig->IPAddress				= IPAddress;
	WorldConfig->Port					= sqlite3_column_int(Stmt, 3);
	WorldConfig->MaxPlayers				= sqlite3_column_int(Stmt, 4);
	WorldConfig->PremiumPlayerBuffer	= sqlite3_column_int(Stmt, 5);
	WorldConfig->MaxNewbies				= sqlite3_column_int(Stmt, 6);
	WorldConfig->PremiumNewbieBuffer	= sqlite3_column_int(Stmt, 7);
	return true;
}

bool AccountExists(int AccountID, const char *Email){
	ASSERT(Email != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM Accounts WHERE AccountID = ?1 OR Email = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID)        != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 2, Email, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrCode = sqlite3_step(Stmt);
	if(ErrCode != SQLITE_ROW && ErrCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrCode == SQLITE_ROW);
}

bool AccountNumberExists(int AccountID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM Accounts WHERE AccountID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID)!= SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrCode = sqlite3_step(Stmt);
	if(ErrCode != SQLITE_ROW && ErrCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrCode == SQLITE_ROW);
}

bool AccountEmailExists(const char *Email){
	ASSERT(Email != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM Accounts WHERE Email = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_text(Stmt, 1, Email, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind Email: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrCode = sqlite3_step(Stmt);
	if(ErrCode != SQLITE_ROW && ErrCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrCode == SQLITE_ROW);
}

bool CreateAccount(int AccountID, const char *Email, const uint8 *Auth, int AuthSize){
	ASSERT(Email != NULL && Auth != NULL && AuthSize > 0);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO Accounts (AccountID, Email, Auth)"
			" VALUES (?1, ?2, ?3)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID)             != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 2, Email, -1, NULL)      != SQLITE_OK
	|| sqlite3_bind_blob(Stmt, 3, Auth, AuthSize, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrCode = sqlite3_step(Stmt);
	if(ErrCode != SQLITE_DONE && ErrCode != SQLITE_CONSTRAINT){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrCode == SQLITE_DONE);
}

bool GetAccountData(int AccountID, TAccount *Account){
	ASSERT(Account != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT AccountID, Email, Auth,"
			" MAX(PremiumEnd - UNIXEPOCH(), 0),"
			" PendingPremiumDays, Deleted"
		" FROM Accounts WHERE AccountID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	memset(Account, 0, sizeof(TAccount));
	if(ErrorCode == SQLITE_ROW){
		Account->AccountID = sqlite3_column_int(Stmt, 0);
		StringCopy(Account->Email, sizeof(Account->Email),
				(const char*)sqlite3_column_text(Stmt, 1));
		if(sqlite3_column_bytes(Stmt, 2) == sizeof(Account->Auth)){
			memcpy(Account->Auth, sqlite3_column_blob(Stmt, 2), sizeof(Account->Auth));
		}
		Account->PremiumDays = RoundSecondsToDays(sqlite3_column_int(Stmt, 3));
		Account->PendingPremiumDays = sqlite3_column_int(Stmt, 4);
		Account->Deleted = (sqlite3_column_int(Stmt, 5) != 0);
	}

	return true;
}

int GetAccountOnlineCharacters(int AccountID){
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT COUNT(*) FROM Characters"
		" WHERE AccountID = ?1 AND IsOnline != 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	if(sqlite3_step(Stmt) != SQLITE_ROW){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	return sqlite3_column_int(Stmt, 0);
}

bool IsCharacterOnline(int CharacterID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT IsOnline FROM Characters WHERE CharacterID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind CharacterID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	bool Result = false;
	if(ErrorCode == SQLITE_ROW){
		Result = (sqlite3_column_int(Stmt, 0) != 0);
	}

	return Result;
}

bool ActivatePendingPremiumDays(int AccountID){
	sqlite3_stmt *Stmt = PrepareQuery(
		"UPDATE Accounts"
		" SET PremiumEnd = MAX(PremiumEnd, UNIXEPOCH()) + PendingPremiumDays * 86400,"
			" PendingPremiumDays = 0"
		" WHERE AccountID = ?1 AND PendingPremiumDays > 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetCharacterEndpoints(int AccountID, DynamicArray<TCharacterEndpoint> *Characters){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT C.Name, W.Name, W.Host, W.Port"
			" FROM Characters AS C"
			" INNER JOIN Worlds AS W ON W.WorldID = C.WorldID"
			" WHERE C.AccountID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		int WorldAddress;
		const char *CharacterName = (const char*)sqlite3_column_text(Stmt, 0);
		const char *WorldName = (const char*)sqlite3_column_text(Stmt, 1);
		const char *HostName = (const char*)sqlite3_column_text(Stmt, 2);
		if(HostName == NULL || !ResolveHostName(HostName, &WorldAddress)){
			LOG_ERR("Failed to resolve world \"%s\" host name \"%s\" for character \"%s\"",
					WorldName, HostName, CharacterName);
			continue;
		}

		TCharacterEndpoint Character = {};
		StringCopy(Character.Name, sizeof(Character.Name), CharacterName);
		StringCopy(Character.WorldName, sizeof(Character.WorldName), WorldName);
		Character.WorldAddress = WorldAddress;
		Character.WorldPort = sqlite3_column_int(Stmt, 3);
		Characters->Push(Character);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetCharacterSummaries(int AccountID, DynamicArray<TCharacterSummary> *Characters){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT C.Name, W.Name, C.Level, C.Profession, C.IsOnline, C.Deleted"
			" FROM Characters AS C"
			" LEFT JOIN Worlds AS W ON W.WorldID = C.WorldID"
			" WHERE C.AccountID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		TCharacterSummary Character = {};
		StringCopy(Character.Name, sizeof(Character.Name),
				(const char*)sqlite3_column_text(Stmt, 0));
		StringCopy(Character.World, sizeof(Character.World),
				(const char*)sqlite3_column_text(Stmt, 1));
		Character.Level = sqlite3_column_int(Stmt, 2);
		StringCopy(Character.Profession, sizeof(Character.Profession),
				(const char*)sqlite3_column_text(Stmt, 3));
		Character.Online = (sqlite3_column_int(Stmt, 4) != 0);
		Character.Deleted = (sqlite3_column_int(Stmt, 5) != 0);
		Characters->Push(Character);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool CharacterNameExists(const char *Name){
	ASSERT(Name != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM Characters WHERE Name = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_text(Stmt, 1, Name, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind Email: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrCode = sqlite3_step(Stmt);
	if(ErrCode != SQLITE_ROW && ErrCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrCode == SQLITE_ROW);
}

bool CreateCharacter(int WorldID, int AccountID, const char *Name, int Sex){
	ASSERT(Name != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO Characters (WorldID, AccountID, Name, Sex)"
			" VALUES (?1, ?2, ?3, ?4)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)         != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, AccountID)       != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 3, Name, -1, NULL) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 4, Sex)             != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrCode = sqlite3_step(Stmt);
	if(ErrCode != SQLITE_DONE && ErrCode != SQLITE_CONSTRAINT){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrCode == SQLITE_DONE);
}

int GetCharacterID(int WorldID, const char *CharacterName){
	ASSERT(CharacterName != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT CharacterID FROM Characters"
			" WHERE WorldID = ?1 AND Name = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)                  != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 2, CharacterName, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	return (ErrorCode == SQLITE_ROW ? sqlite3_column_int(Stmt, 0) : 0);
}

bool GetCharacterLoginData(const char *CharacterName, TCharacterLoginData *Character){
	ASSERT(CharacterName != NULL && Character != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT WorldID, CharacterID, AccountID, Name,"
				" Sex, Guild, Rank, Title, Deleted"
			" FROM Characters WHERE Name = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_text(Stmt, 1, CharacterName, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind CharacterName: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	memset(Character, 0, sizeof(TCharacterLoginData));
	if(ErrorCode == SQLITE_ROW){
		Character->WorldID = sqlite3_column_int(Stmt, 0);
		Character->CharacterID = sqlite3_column_int(Stmt, 1);
		Character->AccountID = sqlite3_column_int(Stmt, 2);
		StringCopy(Character->Name, sizeof(Character->Name),
				(const char*)sqlite3_column_text(Stmt, 3));
		Character->Sex = sqlite3_column_int(Stmt, 4);
		StringCopy(Character->Guild, sizeof(Character->Guild),
				(const char*)sqlite3_column_text(Stmt, 5));
		StringCopy(Character->Rank, sizeof(Character->Rank),
				(const char*)sqlite3_column_text(Stmt, 6));
		StringCopy(Character->Title, sizeof(Character->Title),
				(const char*)sqlite3_column_text(Stmt, 7));
		Character->Deleted = (sqlite3_column_int(Stmt, 8) != 0);
	}

	return true;
}

bool GetCharacterProfile(const char *CharacterName, TCharacterProfile *Character){
	ASSERT(CharacterName != NULL && Character != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT C.Name, W.Name, C.Sex, C.Guild, C.Rank, C.Title, C.Level,"
				" C.Profession, C.Residence, C.LastLoginTime, C.IsOnline,"
				" C.Deleted, MAX(A.PremiumEnd - UNIXEPOCH(), 0)"
			" FROM Characters AS C"
			" LEFT JOIN Worlds AS W ON W.WorldID = C.WorldID"
			" LEFT JOIN Accounts AS A ON A.AccountID = C.AccountID"
			" LEFT JOIN CharacterRights AS R"
				" ON R.CharacterID = C.CharacterID"
				" AND R.Right = 'NO_STATISTICS'"
			" WHERE C.Name = ?1 AND R.Right IS NULL");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_text(Stmt, 1, CharacterName, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind CharacterName: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	memset(Character, 0, sizeof(TCharacterProfile));
	if(ErrorCode == SQLITE_ROW){
		StringCopy(Character->Name, sizeof(Character->Name),
				(const char*)sqlite3_column_text(Stmt, 0));
		StringCopy(Character->World, sizeof(Character->World),
				(const char*)sqlite3_column_text(Stmt, 1));
		Character->Sex = sqlite3_column_int(Stmt, 2);
		StringCopy(Character->Guild, sizeof(Character->Guild),
				(const char*)sqlite3_column_text(Stmt, 3));
		StringCopy(Character->Rank, sizeof(Character->Rank),
				(const char*)sqlite3_column_text(Stmt, 4));
		StringCopy(Character->Title, sizeof(Character->Title),
				(const char*)sqlite3_column_text(Stmt, 5));
		Character->Level = sqlite3_column_int(Stmt, 6);
		StringCopy(Character->Profession, sizeof(Character->Profession),
				(const char*)sqlite3_column_text(Stmt, 7));
		StringCopy(Character->Residence, sizeof(Character->Residence),
				(const char*)sqlite3_column_text(Stmt, 8));
		Character->LastLogin = sqlite3_column_int(Stmt, 9);
		Character->Online = (sqlite3_column_int(Stmt, 10) != 0);
		Character->Deleted = (sqlite3_column_int(Stmt, 11) != 0);
		Character->PremiumDays = RoundSecondsToDays(sqlite3_column_int(Stmt, 12));
	}

	return true;
}

bool GetCharacterRight(int CharacterID, const char *Right){
	ASSERT(Right != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM CharacterRights"
			" WHERE CharacterID = ?1 AND Right = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID)      != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 2, Right, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrorCode == SQLITE_ROW);
}

bool GetCharacterRights(int CharacterID, DynamicArray<TCharacterRight> *Rights){
	ASSERT(Rights != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT Right FROM CharacterRights WHERE CharacterID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind CharacterID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		TCharacterRight Right = {};
		StringCopy(Right.Name, sizeof(Right.Name),
				(const char*)sqlite3_column_text(Stmt, 0));
		Rights->Push(Right);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetGuildLeaderStatus(int WorldID, int CharacterID){
	// NOTE(fusion): Same as `DecrementIsOnline`.
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT Guild, Rank FROM Characters"
			" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	bool Result = false;
	if(ErrorCode == SQLITE_ROW){
		const char *Guild = (const char*)sqlite3_column_text(Stmt, 0);
		const char *Rank = (const char*)sqlite3_column_text(Stmt, 1);
		if(Guild != NULL && !StringEmpty(Guild) && Rank != NULL && StringEqCI(Rank, "Leader")){
			Result = true;
		}
	}
	return Result;
}

bool IncrementIsOnline(int WorldID, int CharacterID){
	// NOTE(fusion): Same as `DecrementIsOnline`.
	sqlite3_stmt *Stmt = PrepareQuery(
			"UPDATE Characters SET IsOnline = IsOnline + 1"
			" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)     != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

bool DecrementIsOnline(int WorldID, int CharacterID){
	// NOTE(fusion): A character is uniquely identified by its id. The world id
	// check is purely to avoid a world from modifying a character from another
	// world.
	sqlite3_stmt *Stmt = PrepareQuery(
			"UPDATE Characters SET IsOnline = IsOnline - 1"
			" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)     != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

bool ClearIsOnline(int WorldID, int *NumAffectedCharacters){
	ASSERT(NumAffectedCharacters != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"UPDATE Characters SET IsOnline = 0"
			" WHERE WorldID = ?1 AND IsOnline != 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	*NumAffectedCharacters = sqlite3_changes(g_Database);
	return true;
}

bool LogoutCharacter(int WorldID, int CharacterID, int Level,
		const char *Profession, const char *Residence, int LastLoginTime,
		int TutorActivities){
	ASSERT(Profession != NULL && Residence != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"UPDATE Characters"
			" SET Level = ?3,"
				" Profession = ?4,"
				" Residence = ?5,"
				" LastLoginTime = ?6,"
				" TutorActivities = ?7,"
				" IsOnline = IsOnline - 1"
			" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)               != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID)           != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, Level)                 != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 4, Profession, -1, NULL) != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 5, Residence, -1, NULL)  != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 6, LastLoginTime)         != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 7, TutorActivities)       != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

bool GetCharacterIndexEntries(int WorldID, int MinimumCharacterID,
		int MaxEntries, int *NumEntries, TCharacterIndexEntry *Entries){
	ASSERT(MaxEntries > 0 && NumEntries != NULL && Entries != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT CharacterID, Name FROM Characters"
			" WHERE WorldID = ?1 AND CharacterID >= ?2"
			" ORDER BY CharacterID ASC LIMIT ?3");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)            != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, MinimumCharacterID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, MaxEntries)         != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	// NOTE(fusion): We shouldn't get more than `MaxEntries` rows but it's
	// always better to be safe.
	int EntryIndex = 0;
	while(sqlite3_step(Stmt) == SQLITE_ROW && EntryIndex < MaxEntries){
		Entries[EntryIndex].CharacterID = sqlite3_column_int(Stmt, 0);
		StringCopy(Entries[EntryIndex].Name,
				sizeof(Entries[EntryIndex].Name),
				(const char*)sqlite3_column_text(Stmt, 1));
		EntryIndex += 1;
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	*NumEntries = EntryIndex;
	return true;
}

bool InsertCharacterDeath(int WorldID, int CharacterID, int Level,
		int OffenderID, const char *Remark, bool Unjustified, int Timestamp){
	ASSERT(Remark != NULL);
	// NOTE(fusion): Same as `DecrementIsOnline`.
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO CharacterDeaths (CharacterID, Level,"
				" OffenderID, Remark, Unjustified, Timestamp)"
			" SELECT ?2, ?3, ?4, ?5, ?6, ?7 FROM Characters"
				" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)               != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID)           != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, Level)                 != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 4, OffenderID)            != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 5, Remark, -1, NULL)     != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 6, (Unjustified ? 1 : 0)) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 7, Timestamp)             != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

bool InsertBuddy(int WorldID, int AccountID, int BuddyID){
	// NOTE(fusion): Same as `DecrementIsOnline`.
	// NOTE(fusion): Use the `IGNORE` conflict resolution to make duplicate row
	// errors appear as successful insertions.
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT OR IGNORE INTO Buddies (WorldID, AccountID, BuddyID)"
			" SELECT ?1, ?2, ?3 FROM Characters"
				" WHERE WorldID = ?1 AND CharacterID = ?3");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, AccountID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, BuddyID)   != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool DeleteBuddy(int WorldID, int AccountID, int BuddyID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"DELETE FROM Buddies"
			" WHERE WorldID = ?1 AND AccountID = ?2 AND BuddyID = ?3");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, AccountID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, BuddyID)   != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	// NOTE(fusion): Always return true here even if there were no deleted rows
	// to make them appear as successful deletions.
	return true;
}

bool GetBuddies(int WorldID, int AccountID, DynamicArray<TAccountBuddy> *Buddies){
	ASSERT(Buddies != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT B.BuddyID, C.Name"
		" FROM Buddies AS B"
		" INNER JOIN Characters AS C"
			" ON C.WorldID = B.WorldID AND C.CharacterID = B.BuddyID"
		" WHERE B.WorldID = ?1 AND B.AccountID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		TAccountBuddy Buddy = {};
		Buddy.CharacterID = sqlite3_column_int(Stmt, 0);
		StringCopy(Buddy.Name, sizeof(Buddy.Name),
				(const char*)sqlite3_column_text(Stmt, 1));
		Buddies->Push(Buddy);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetWorldInvitation(int WorldID, int CharacterID){
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT 1 FROM WorldInvitations"
		" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)     != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrorCode == SQLITE_ROW);
}

bool InsertLoginAttempt(int AccountID, int IPAddress, bool Failed){
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO LoginAttempts (AccountID, IPAddress, Timestamp, Failed)"
			" VALUES (?1, ?2, UNIXEPOCH(), ?3)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID)        != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, IPAddress)        != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, (Failed ? 1 : 0)) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

int GetAccountFailedLoginAttempts(int AccountID, int TimeWindow){
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT COUNT(*) FROM LoginAttempts"
		" WHERE AccountID = ?1 AND Timestamp >= (UNIXEPOCH() - ?2) AND Failed != 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID)  != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, TimeWindow) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	if(sqlite3_step(Stmt) != SQLITE_ROW){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	return sqlite3_column_int(Stmt, 0);
}

int GetIPAddressFailedLoginAttempts(int IPAddress, int TimeWindow){
	sqlite3_stmt *Stmt = PrepareQuery(
		"SELECT COUNT(*) FROM LoginAttempts"
		" WHERE IPAddress = ?1 AND Timestamp >= (UNIXEPOCH() - ?2) AND Failed != 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, IPAddress)  != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, TimeWindow) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	if(sqlite3_step(Stmt) != SQLITE_ROW){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	return sqlite3_column_int(Stmt, 0);
}

// House tables
//==============================================================================
bool FinishHouseAuctions(int WorldID, DynamicArray<THouseAuction> *Auctions){
	ASSERT(Auctions != NULL);
	// TODO(fusion): If the application crashes while processing finished auctions,
	// non processed auctions will be lost but with no other side-effects. It could
	// be an inconvenience but it's not a big problem.
	sqlite3_stmt *Stmt = PrepareQuery(
			"DELETE FROM HouseAuctions"
			" WHERE WorldID = ?1 AND FinishTime IS NOT NULL AND FinishTime <= UNIXEPOCH()"
			" RETURNING HouseID, BidderID, BidAmount, FinishTime,"
				" (SELECT Name FROM Characters WHERE CharacterID = BidderID)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		THouseAuction Auction = {};
		Auction.HouseID = sqlite3_column_int(Stmt, 0);
		Auction.BidderID = sqlite3_column_int(Stmt, 1);
		Auction.BidAmount = sqlite3_column_int(Stmt, 2);
		Auction.FinishTime = sqlite3_column_int(Stmt, 3);
		StringCopy(Auction.BidderName, sizeof(Auction.BidderName),
				(const char*)sqlite3_column_text(Stmt, 4));
		Auctions->Push(Auction);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool FinishHouseTransfers(int WorldID, DynamicArray<THouseTransfer> *Transfers){
	ASSERT(Transfers != NULL);
	// TODO(fusion): Same as `FinishHouseAuctions` but with house transfers.
	sqlite3_stmt *Stmt = PrepareQuery(
			"DELETE FROM HouseTransfers"
			" WHERE WorldID = ?1"
			" RETURNING HouseID, NewOwnerID, Price,"
				" (SELECT Name FROM Characters WHERE CharacterID = NewOwnerID)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		THouseTransfer Transfer = {};
		Transfer.HouseID = sqlite3_column_int(Stmt, 0);
		Transfer.NewOwnerID = sqlite3_column_int(Stmt, 1);
		Transfer.Price = sqlite3_column_int(Stmt, 2);
		StringCopy(Transfer.NewOwnerName, sizeof(Transfer.NewOwnerName),
				(const char*)sqlite3_column_text(Stmt, 4));
		Transfers->Push(Transfer);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetFreeAccountEvictions(int WorldID, DynamicArray<THouseEviction> *Evictions){
	ASSERT(Evictions != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT O.HouseID, O.OwnerID"
			" FROM HouseOwners AS O"
			" LEFT JOIN Characters AS C ON C.CharacterID = O.OwnerID"
			" LEFT JOIN Accounts AS A ON A.AccountID = C.AccountID"
			" WHERE O.WorldID = ?1"
				" AND (A.PremiumEnd IS NULL OR A.PremiumEnd < UNIXEPOCH())");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		THouseEviction Eviction = {};
		Eviction.HouseID = sqlite3_column_int(Stmt, 0);
		Eviction.OwnerID = sqlite3_column_int(Stmt, 1);
		Evictions->Push(Eviction);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetDeletedCharacterEvictions(int WorldID, DynamicArray<THouseEviction> *Evictions){
	ASSERT(Evictions != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT O.HouseID, O.OwnerID"
			" FROM HouseOwners AS O"
			" LEFT JOIN Characters AS C ON C.CharacterID = O.OwnerID"
			" WHERE O.WorldID = ?1"
				" AND (C.CharacterID IS NULL OR C.Deleted != 0)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		THouseEviction Eviction = {};
		Eviction.HouseID = sqlite3_column_int(Stmt, 0);
		Eviction.OwnerID = sqlite3_column_int(Stmt, 1);
		Evictions->Push(Eviction);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool InsertHouseOwner(int WorldID, int HouseID, int OwnerID, int PaidUntil){
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO HouseOwners (WorldID, HouseID, OwnerID, PaidUntil)"
			" VALUES (?1, ?2, ?3, ?4)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, HouseID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, OwnerID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 4, PaidUntil) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool UpdateHouseOwner(int WorldID, int HouseID, int OwnerID, int PaidUntil){
	sqlite3_stmt *Stmt = PrepareQuery(
			"UPDATE HouseOwners SET OwnerID = ?3, PaidUntil = ?4"
			" WHERE WorldID = ?1 AND HouseID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, HouseID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, OwnerID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 4, PaidUntil) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

bool DeleteHouseOwner(int WorldID, int HouseID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"DELETE FROM HouseOwners"
			" WHERE WorldID = ?1 AND HouseID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, HouseID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

bool GetHouseOwners(int WorldID, DynamicArray<THouseOwner> *Owners){
	ASSERT(Owners != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT O.HouseID, O.OwnerID, C.Name, O.PaidUntil"
			" FROM HouseOwners AS O"
			" LEFT JOIN Characters AS C ON C.CharacterID = O.OwnerID"
			" WHERE O.WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		THouseOwner Owner = {};
		Owner.HouseID = sqlite3_column_int(Stmt, 0);
		Owner.OwnerID = sqlite3_column_int(Stmt, 1);
		StringCopy(Owner.OwnerName, sizeof(Owner.OwnerName),
				(const char*)sqlite3_column_text(Stmt, 2));
		Owner.PaidUntil = sqlite3_column_int(Stmt, 3);
		Owners->Push(Owner);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetHouseAuctions(int WorldID, DynamicArray<int> *Auctions){
	ASSERT(Auctions != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT HouseID FROM HouseAuctions WHERE WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		Auctions->Push(sqlite3_column_int(Stmt, 0));
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool StartHouseAuction(int WorldID, int HouseID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO HouseAuctions (WorldID, HouseID) VALUES (?1, ?2)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, HouseID)   != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool DeleteHouses(int WorldID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"DELETE FROM Houses WHERE WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool InsertHouses(int WorldID, int NumHouses, THouse *Houses){
	ASSERT(NumHouses > 0 && Houses != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO Houses (WorldID, HouseID, Name, Rent, Description,"
				" Size, PositionX, PositionY, PositionZ, Town, GuildHouse)"
			" VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	for(int i = 0; i < NumHouses; i += 1){
		if(sqlite3_bind_int(Stmt, 2, Houses[i].HouseID)                != SQLITE_OK
		|| sqlite3_bind_text(Stmt, 3, Houses[i].Name, -1, NULL)        != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 4, Houses[i].Rent)                   != SQLITE_OK
		|| sqlite3_bind_text(Stmt, 5, Houses[i].Description, -1, NULL) != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 6, Houses[i].Size)                   != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 7, Houses[i].PositionX)              != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 8, Houses[i].PositionY)              != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 9, Houses[i].PositionZ)              != SQLITE_OK
		|| sqlite3_bind_text(Stmt, 10, Houses[i].Town, -1, NULL)       != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 11, Houses[i].GuildHouse ? 1: 0)     != SQLITE_OK){
			LOG_ERR("Failed to bind parameters for house %d: %s",
					Houses[i].HouseID, sqlite3_errmsg(g_Database));
			return false;
		}

		if(sqlite3_step(Stmt) != SQLITE_DONE){
			LOG_ERR("Failed to insert house %d: %s",
					Houses[i].HouseID, sqlite3_errmsg(g_Database));
			return false;
		}

		sqlite3_reset(Stmt);
	}

	return true;
}

bool ExcludeFromAuctions(int WorldID, int CharacterID, int Duration, int BanishmentID){
	// NOTE(fusion): Same as `DecrementIsOnline`.
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO HouseAuctionExclusions (CharacterID, Issued, Until, BanishmentID)"
			" SELECT ?2, UNIXEPOCH(), (UNIXEPOCH() + ?3), ?4 FROM Characters"
				" WHERE WorldID = ?1 AND CharacterID = ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)      != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, CharacterID)  != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, Duration)     != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 4, BanishmentID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return sqlite3_changes(g_Database) > 0;
}

// Banishment tables
//==============================================================================
bool IsCharacterNamelocked(int CharacterID){
	TNamelockStatus Status = GetNamelockStatus(CharacterID);
	return Status.Namelocked && !Status.Approved;
}

TNamelockStatus GetNamelockStatus(int CharacterID){
	TNamelockStatus Status = {};
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT Approved FROM Namelocks WHERE CharacterID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return Status;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return Status;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return Status;
	}

	Status.Namelocked = (ErrorCode == SQLITE_ROW);
	if(Status.Namelocked){
		Status.Approved = (sqlite3_column_int(Stmt, 0) != 0);
	}
	return Status;
}

bool InsertNamelock(int CharacterID, int IPAddress, int GamemasterID,
		const char *Reason, const char *Comment){
	ASSERT(Reason != NULL && Comment != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO Namelocks (CharacterID, IPAddress, GamemasterID, Reason, Comment)"
			" VALUES (?1, ?2, ?3, ?4, ?5)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID)        != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, IPAddress)          != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, GamemasterID)       != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 4, Reason, -1, NULL)  != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 5, Comment, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool IsAccountBanished(int AccountID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM Banishments"
			" WHERE AccountID = ?1"
				" AND (Until = Issued OR Until > UNIXEPOCH())");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, AccountID) != SQLITE_OK){
		LOG_ERR("Failed to bind AccountID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrorCode == SQLITE_ROW);
}

TBanishmentStatus GetBanishmentStatus(int CharacterID){
	TBanishmentStatus Status = {};
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT B.FinalWarning, (B.Until = B.Issued OR B.Until > UNIXEPOCH())"
			" FROM Banishments AS B"
			" LEFT JOIN Characters AS C ON C.AccountID = B.AccountID"
			" WHERE C.CharacterID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return Status;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return Status;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		Status.TimesBanished += 1;

		if(sqlite3_column_int(Stmt, 0) != 0){
			Status.FinalWarning = true;
		}

		if(sqlite3_column_int(Stmt, 1) != 0){
			Status.Banished = true;
		}
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return Status;
	}

	return Status;
}

bool InsertBanishment(int CharacterID, int IPAddress, int GamemasterID,
		const char *Reason, const char *Comment, bool FinalWarning,
		int Duration, int *BanishmentID){
	ASSERT(Reason != NULL && Comment != NULL && BanishmentID != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO Banishments (AccountID, IPAddress, GamemasterID,"
				" Reason, Comment, FinalWarning, Issued, Until)"
			" SELECT AccountID, ?2, ?3, ?4, ?5, ?6, UNIXEPOCH(), UNIXEPOCH() + ?7"
				" FROM Characters WHERE CharacterID = ?1"
			" RETURNING BanishmentID");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID)        != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, IPAddress)          != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, GamemasterID)       != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 4, Reason, -1, NULL)  != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 5, Comment, -1, NULL) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 6, FinalWarning)       != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 7, Duration)           != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_ROW){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	*BanishmentID = sqlite3_column_int(Stmt, 0);
	return true;
}

int GetNotationCount(int CharacterID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT COUNT(*) FROM Notations WHERE CharacterID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	if(sqlite3_step(Stmt) != SQLITE_ROW){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	return sqlite3_column_int(Stmt, 0);
}

bool InsertNotation(int CharacterID, int IPAddress, int GamemasterID,
		const char *Reason, const char *Comment){
	ASSERT(Reason != NULL && Comment != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO Notations (CharacterID, IPAddress,"
				" GamemasterID, Reason, Comment)"
			" VALUES (?1, ?2, ?3, ?4, ?5)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID)        != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, IPAddress)          != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, GamemasterID)       != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 4, Reason, -1, NULL)  != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 5, Comment, -1, NULL) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool IsIPBanished(int IPAddress){
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM IPBanishments"
			" WHERE IPAddress = ?1"
				" AND (Until = Issued OR Until > UNIXEPOCH())");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, IPAddress) != SQLITE_OK){
		LOG_ERR("Failed to bind IPAddress: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrorCode == SQLITE_ROW);
}

bool InsertIPBanishment(int CharacterID, int IPAddress, int GamemasterID,
		const char *Reason, const char *Comment, int Duration){
	ASSERT(Reason != NULL && Comment != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO IPBanishments (CharacterID, IPAddress,"
				" GamemasterID, Reason, Comment, Issued, Until)"
			" VALUES (?1, ?2, ?3, ?4, ?5, UNIXEPOCH(), UNIXEPOCH() + ?6)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, CharacterID)        != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, IPAddress)          != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, GamemasterID)       != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 4, Reason, -1, NULL)  != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 5, Comment, -1, NULL) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 6, Duration)           != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool IsStatementReported(int WorldID, TStatement *Statement){
	ASSERT(Statement != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT 1 FROM Statements"
			" WHERE WorldID = ?1 AND Timestamp = ?2 AND StatementID = ?3");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return 0;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)                != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, Statement->Timestamp)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, Statement->StatementID) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return 0;
	}

	int ErrorCode = sqlite3_step(Stmt);
	if(ErrorCode != SQLITE_ROW && ErrorCode != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return (ErrorCode == SQLITE_ROW);
}

bool InsertStatements(int WorldID, int NumStatements, TStatement *Statements){
	// NOTE(fusion): Use the `IGNORE` conflict resolution because different
	// reports may include the same statements for context and I assume it's
	// not uncommon to see overlaps.
	ASSERT(NumStatements > 0 && Statements != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT OR IGNORE INTO Statements (WorldID, Timestamp,"
				" StatementID, CharacterID, Channel, Text)"
			" VALUES (?1, ?2, ?3, ?4, ?5, ?6)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	for(int i = 0; i < NumStatements; i += 1){
		if(Statements[i].StatementID == 0){
			LOG_WARN("Skipping statement without id");
			continue;
		}

		if(sqlite3_bind_int(Stmt, 2, Statements[i].Timestamp)          != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 3, Statements[i].StatementID)        != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 4, Statements[i].CharacterID)        != SQLITE_OK
		|| sqlite3_bind_text(Stmt, 5, Statements[i].Channel, -1, NULL) != SQLITE_OK
		|| sqlite3_bind_text(Stmt, 6, Statements[i].Text, -1, NULL)    != SQLITE_OK){
			LOG_ERR("Failed to bind parameters for statement %d: %s",
					Statements[i].StatementID, sqlite3_errmsg(g_Database));
			return false;
		}

		if(sqlite3_step(Stmt) != SQLITE_DONE){
			LOG_ERR("Failed to insert statement %d: %s",
					Statements[i].StatementID, sqlite3_errmsg(g_Database));
			return false;
		}

		sqlite3_reset(Stmt);
	}

	return true;
}

bool InsertReportedStatement(int WorldID, TStatement *Statement, int BanishmentID,
		int ReporterID, const char *Reason, const char *Comment){
	ASSERT(Statement != NULL && Reason != NULL && Comment != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO ReportedStatements (WorldID, Timestamp,"
				" StatementID, CharacterID, BanishmentID, ReporterID,"
				" Reason, Comment)"
			" VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)                != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, Statement->Timestamp)   != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 3, Statement->StatementID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 4, Statement->CharacterID) != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 5, BanishmentID)           != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 6, ReporterID)             != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 7, Reason, -1, NULL)      != SQLITE_OK
	|| sqlite3_bind_text(Stmt, 8, Comment, -1, NULL)     != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

// Info tables
//==============================================================================
bool GetKillStatistics(int WorldID, DynamicArray<TKillStatistics> *Stats){
	ASSERT(Stats != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT RaceName, TimesKilled, PlayersKilled"
			" FROM KillStatistics WHERE WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		TKillStatistics Entry = {};
		StringCopy(Entry.RaceName, sizeof(Entry.RaceName),
				(const char*)sqlite3_column_text(Stmt, 0));
		Entry.TimesKilled = sqlite3_column_int(Stmt, 1);
		Entry.PlayersKilled = sqlite3_column_int(Stmt, 2);
		Stats->Push(Entry);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool MergeKillStatistics(int WorldID, int NumStats, TKillStatistics *Stats){
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO KillStatistics (WorldID, RaceName, TimesKilled, PlayersKilled)"
			" VALUES (?1, ?2, ?3, ?4)"
			" ON CONFLICT DO UPDATE SET TimesKilled = TimesKilled + Excluded.TimesKilled,"
									" PlayersKilled = PlayersKilled + Excluded.PlayersKilled");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	for(int i = 0; i < NumStats; i += 1){
		if(sqlite3_bind_text(Stmt, 2, Stats[i].RaceName, -1, NULL) != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 3, Stats[i].TimesKilled)         != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 4, Stats[i].PlayersKilled)       != SQLITE_OK){
			LOG_ERR("Failed to bind parameters for \"%s\" stats: %s",
					Stats[i].RaceName, sqlite3_errmsg(g_Database));
			return false;
		}

		if(sqlite3_step(Stmt) != SQLITE_DONE){
			LOG_ERR("Failed to insert \"%s\" stats: %s",
					Stats[i].RaceName, sqlite3_errmsg(g_Database));
			return false;
		}

		sqlite3_reset(Stmt);
	}

	return true;
}

bool GetOnlineCharacters(int WorldID, DynamicArray<TOnlineCharacter> *Characters){
	ASSERT(Characters != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"SELECT Name, Level, Profession"
			" FROM OnlineCharacters WHERE WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	while(sqlite3_step(Stmt) == SQLITE_ROW){
		TOnlineCharacter Character = {};
		StringCopy(Character.Name, sizeof(Character.Name),
				(const char*)sqlite3_column_text(Stmt, 0));
		Character.Level = sqlite3_column_int(Stmt, 1);
		StringCopy(Character.Profession, sizeof(Character.Profession),
				(const char*)sqlite3_column_text(Stmt, 2));
		Characters->Push(Character);
	}

	if(sqlite3_errcode(g_Database) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool DeleteOnlineCharacters(int WorldID){
	sqlite3_stmt *Stmt = PrepareQuery(
			"DELETE FROM OnlineCharacters WHERE WorldID = ?1");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool InsertOnlineCharacters(int WorldID, int NumCharacters, TOnlineCharacter *Characters){
	sqlite3_stmt *Stmt = PrepareQuery(
			"INSERT INTO OnlineCharacters (WorldID, Name, Level, Profession)"
			" VALUES (?1, ?2, ?3, ?4)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID) != SQLITE_OK){
		LOG_ERR("Failed to bind WorldID: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	for(int i = 0; i < NumCharacters; i += 1){
		if(sqlite3_bind_text(Stmt, 2, Characters[i].Name, -1, NULL)       != SQLITE_OK
		|| sqlite3_bind_int(Stmt, 3, Characters[i].Level)                 != SQLITE_OK
		|| sqlite3_bind_text(Stmt, 4, Characters[i].Profession, -1, NULL) != SQLITE_OK){
			LOG_ERR("Failed to bind parameters for character \"%s\": %s",
					Characters[i].Name, sqlite3_errmsg(g_Database));
			return false;
		}

		if(sqlite3_step(Stmt) != SQLITE_DONE){
			LOG_ERR("Failed to insert character \"%s\": %s",
					Characters[i].Name, sqlite3_errmsg(g_Database));
			return false;
		}

		sqlite3_reset(Stmt);
	}

	return true;
}

bool CheckOnlineRecord(int WorldID, int NumCharacters, bool *NewRecord){
	ASSERT(NewRecord != NULL);
	sqlite3_stmt *Stmt = PrepareQuery(
			"UPDATE Worlds SET OnlineRecord = ?2,"
				" OnlineRecordTimestamp = UNIXEPOCH()"
			" WHERE WorldID = ?1 AND OnlineRecord < ?2");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	AutoStmtReset StmtReset(Stmt);
	if(sqlite3_bind_int(Stmt, 1, WorldID)       != SQLITE_OK
	|| sqlite3_bind_int(Stmt, 2, NumCharacters) != SQLITE_OK){
		LOG_ERR("Failed to bind parameters: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_step(Stmt) != SQLITE_DONE){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	*NewRecord = sqlite3_changes(g_Database) > 0;
	return true;
}

// Database Initialization
//==============================================================================
// NOTE(fusion): From `https://www.sqlite.org/pragma.html`:
//	"Some pragmas take effect during the SQL compilation stage, not the execution
// stage. This means if using the C-language sqlite3_prepare(), sqlite3_step(),
// sqlite3_finalize() API (or similar in a wrapper interface), the pragma may run
// during the sqlite3_prepare() call, not during the sqlite3_step() call as normal
// SQL statements do. Or the pragma might run during sqlite3_step() just like normal
// SQL statements. Whether or not the pragma runs during sqlite3_prepare() or
// sqlite3_step() depends on the pragma and on the specific release of SQLite."
//
//	Depending on the pragma, queries will fail in the sqlite3_prepare() stage when
// using bound parameters. This means we need to assemble the entire query before
// hand with snprintf or other similar formatting functions. In particular, this
// rule apply for `application_id` and `user_version` which we modify.

bool FileExists(const char *FileName){
	FILE *File = fopen(FileName, "rb");
	bool Result = (File != NULL);
	if(Result){
		fclose(File);
	}
	return Result;
}

bool ExecFile(const char *FileName){
	FILE *File = fopen(FileName, "rb");
	if(File == NULL){
		LOG_ERR("Failed to open file \"%s\"", FileName);
		return false;
	}

	fseek(File, 0, SEEK_END);
	usize FileSize = (usize)ftell(File);
	fseek(File, 0, SEEK_SET);

	bool Result = true;
	if(FileSize > 0){
		char *Text = (char*)malloc(FileSize + 1);
		Text[FileSize] = 0;
		if(Result && fread(Text, 1, FileSize, File) != FileSize){
			LOG_ERR("Failed to read \"%s\" (ferror: %d, feof: %d)",
					FileName, ferror(File), feof(File));
			Result = false;
		}

		if(Result && sqlite3_exec(g_Database, Text, NULL, NULL, NULL) != SQLITE_OK){
			LOG_ERR("Failed to execute \"%s\": %s",
					FileName, sqlite3_errmsg(g_Database));
			Result = false;
		}

		free(Text);
	}

	fclose(File);
	return Result;
}

bool ExecInternal(const char *Format, ...){
	va_list ap;
	va_start(ap, Format);
	char Text[1024];
	int Written = vsnprintf(Text, sizeof(Text), Format, ap);
	va_end(ap);

	if(Written >= (int)sizeof(Text)){
		LOG_ERR("Query is too long");
		return false;
	}

	if(sqlite3_exec(g_Database, Text, NULL, NULL, NULL) != SQLITE_OK){
		LOG_ERR("Failed to execute query: %s", sqlite3_errmsg(g_Database));
		return false;
	}

	return true;
}

bool GetPragmaInt(const char *Name, int *OutValue){
	char Text[1024];
	snprintf(Text, sizeof(Text), "PRAGMA %s", Name);

	sqlite3_stmt *Stmt;
	if(sqlite3_prepare_v2(g_Database, Text, -1, &Stmt, NULL) != SQLITE_OK){
		LOG_ERR("Failed to retrieve %s (PREP): %s", Name, sqlite3_errmsg(g_Database));
		return false;
	}

	bool Result = (sqlite3_step(Stmt) == SQLITE_ROW);
	if(!Result){
		LOG_ERR("Failed to retrieve %s (PREP): %s", Name, sqlite3_errmsg(g_Database));
	}else if(OutValue){
		*OutValue = sqlite3_column_int(Stmt, 0);
	}

	sqlite3_finalize(Stmt);
	return Result;
}

bool InitDatabaseSchema(void){
	TransactionScope Tx("SchemaInit");
	if(!Tx.Begin()){
		return false;
	}

	if(!ExecFile("sql/schema.sql")){
		LOG_ERR("Failed to execute \"sql/schema.sql\"");
		return false;
	}

	if(!ExecInternal("PRAGMA application_id = %d", g_ApplicationID)){
		LOG_ERR("Failed to set application id");
		return false;
	}

	if(!ExecInternal("PRAGMA user_version = 1")){
		LOG_ERR("Failed to set user version");
		return false;
	}

	if(!Tx.Commit()){
		return false;
	}

	return true;
}

bool UpgradeDatabaseSchema(int UserVersion){
	char FileName[256];
	int NewVersion = UserVersion;
	while(true){
		snprintf(FileName, sizeof(FileName), "sql/upgrade-%d.sql", NewVersion);
		if(FileExists(FileName)){
			NewVersion += 1;
		}else{
			break;
		}
	}

	if(UserVersion != NewVersion){
		LOG("Upgrading database schema to version %d", NewVersion);

		TransactionScope Tx("SchemaUpgrade");
		if(!Tx.Begin()){
			return false;
		}

		while(UserVersion < NewVersion){
			snprintf(FileName, sizeof(FileName), "upgrade-%d.sql", UserVersion);
			if(!ExecFile(FileName)){
				LOG_ERR("Failed to execute \"%s\"", FileName);
				return false;
			}
			UserVersion += 1;
		}

		if(!ExecInternal("PRAGMA user_version = %d", UserVersion)){
			LOG_ERR("Failed to set user version");
			return false;
		}

		if(!Tx.Commit()){
			return false;
		}
	}

	return true;
}

bool CheckDatabaseSchema(void){
	int ApplicationID, UserVersion;
	if(!GetPragmaInt("application_id", &ApplicationID)
	|| !GetPragmaInt("user_version", &UserVersion)){
		return false;
	}

	if(ApplicationID != g_ApplicationID){
		if(ApplicationID != 0){
			LOG_ERR("Database has unknown application id %08X (expected %08X)",
					ApplicationID, g_ApplicationID);
			return false;
		}else if(UserVersion != 0){
			LOG_ERR("Database has non zero user version %d", UserVersion);
			return false;
		}else if(!InitDatabaseSchema()){
			LOG_ERR("Failed to initialize database schema");
			return false;
		}

		UserVersion = 1;
	}

	if(!UpgradeDatabaseSchema(UserVersion)){
		LOG_ERR("Failed to upgrade database schema");
		return false;
	}

	LOG("Database version: %d", UserVersion);
	return true;
}

bool InitDatabase(void){
	LOG("Database file: \"%s\"", g_DatabaseFile);
	LOG("Max cached statements: %d", g_MaxCachedStatements);

	int Flags = SQLITE_OPEN_READWRITE
			| SQLITE_OPEN_CREATE
			| SQLITE_OPEN_NOMUTEX;
	if(sqlite3_open_v2(g_DatabaseFile, &g_Database, Flags, NULL) != SQLITE_OK){
		LOG_ERR("Failed to open database at \"%s\": %s\n",
				g_DatabaseFile, sqlite3_errmsg(g_Database));
		return false;
	}

	if(sqlite3_db_readonly(g_Database, NULL)){
		LOG_ERR("Failed to open database file \"%s\" with WRITE PERMISSIONS."
				" Make sure the file has the appropriate permissions and is"
				" owned by the same user running the query manager.",
				g_DatabaseFile);
		return false;
	}

	if(!InitStatementCache()){
		LOG_ERR("Failed to initialize statement cache");
		return false;
	}

	if(!CheckDatabaseSchema()){
		LOG_ERR("Failed to check database schema");
		return false;
	}

	return true;
}

void ExitDatabase(void){
	ExitStatementCache();

	if(g_Database != NULL){
		// NOTE(fusion): `sqlite3_close` can only fail if there are associated
		// prepared statements, blob handles, or backup objects that were not
		// finalized.
		if(sqlite3_close(g_Database) == SQLITE_OK){
			g_Database = NULL;
		}else{
			LOG_ERR("Failed to close database: %s", sqlite3_errmsg(g_Database));
		}
	}
}

TDatabase *OpenDatabase(void){
	//TODO
	return NULL;
}

void CloseDatabase(TDatabase *Database){
	//TODO
}

