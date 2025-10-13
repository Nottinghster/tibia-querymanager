#if DATABASE_POSTGRESQL
#include "querymanager.hh"
#include "libpq-fe.h"

// IMPORTANT(fusion): These are the OIDs for a few of built-in data types in
// PostgreSQL. They're taken from `catalog/pg_type_d.h` which is not included
// with libpq but should be STABLE across different versions and are needed
// for properly handling binary data from the server.
#define BOOLOID 16
#define BYTEAOID 17
#define CHAROID 18
#define INT8OID 20
#define INT2OID 21
#define INT4OID 23
#define TEXTOID 25
#define FLOAT4OID 700
#define FLOAT8OID 701
#define INETOID 869
#define VARCHAROID 1043
#define DATEOID 1082
#define TIMEOID 1083
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184
#define INTERVALOID 1186
#define TIMETZOID 1266

struct TCachedStatement{
	char             Name[16];
	int              LastUsed;
	uint32           Hash;
	char             *Text;
};

struct TDatabase{
	PGconn           *Handle;
	int              MaxCachedStatements;
	TCachedStatement *CachedStatements;
};

// Statement Cache
//==============================================================================
// NOTE(fusion): Prepared statements are stored server-side and only referenced
// by name. They're not shared between sessions and are automatically cleaned up
// when the connection is CLOSED or RESET.

void EnsureStatementCache(TDatabase *Database){
	ASSERT(Database != NULL);
	if(Database->CachedStatements == NULL){
		ASSERT(g_Config.PostgreSQL.MaxCachedStatements > 0);
		Database->MaxCachedStatements = g_Config.PostgreSQL.MaxCachedStatements;
		if(Database->MaxCachedStatements > 9999){
			LOG_WARN("There is currently a hard limit of 9999 max cached statements"
					" for PostgreSQL but it should be way more than needed because"
					" there are ABSOLUTELY NOT 9999 different queries.");
			Database->MaxCachedStatements = 9999;
		}

		Database->CachedStatements = (TCachedStatement*)calloc(
				Database->MaxCachedStatements, sizeof(TCachedStatement));
		for(int i = 0; i < Database->MaxCachedStatements; i += 1){
			if(!StringBufFormat(Database->CachedStatements[i].Name, "STMT%d", i)){
				PANIC("Failed to format statement cache entry name for STMT%d", i);
			}
		}
	}
}

void DeleteStatementCache(TDatabase *Database){
	ASSERT(Database != NULL);
	if(Database->CachedStatements != NULL){
		ASSERT(Database->MaxCachedStatements > 0);
		for(int i = 0; i < Database->MaxCachedStatements; i += 1){
			TCachedStatement *Entry = &Database->CachedStatements[i];
			// NOTE(fusion): There is little reason to use `PQclosePrepared` here
			// because this function would usually be called with `PQreset` or
			// `PQfinish` which should already clear any prepared statements
			// created for the session.
			if(Entry->Text != NULL){
				free(Entry->Text);
				Entry->LastUsed = 0;
				Entry->Hash = 0;
				Entry->Text = NULL;
			}
		}

		// TODO(fusion): We might not need to use `PQclosePrepared` but it could
		// be a good idea to do some ExecInternal("DEALLOCATE ALL"), which would
		// clear all prepared statements from the current session.

		free(Database->CachedStatements);
		Database->MaxCachedStatements = 0;
		Database->CachedStatements = NULL;
	}
}

// IMPORTANT(fusion): Even though it is possible to declare parameter types with
// OIDs, it is simpler to use explicit casts such as `$1::INTEGER` to enforce types.
// It also makes so all relevant information about the query is packed into `Text`
// so we don't need to track anything else to ensure statements with different
// types are kept separate.
const char *PrepareQuery(TDatabase *Database, const char *Text){
	ASSERT(Database != NULL);
	EnsureStatementCache(Database);

	TCachedStatement *Stmt = NULL;
	int LeastRecentlyUsed = 0;
	int LeastRecentlyUsedTime = Database->CachedStatements[0].LastUsed;
	uint32 Hash = HashString(Text);
	for(int i = 0; i < Database->MaxCachedStatements; i += 1){
		TCachedStatement *Entry = &Database->CachedStatements[i];

		if(Entry->LastUsed < LeastRecentlyUsedTime){
			LeastRecentlyUsed = i;
			LeastRecentlyUsedTime = Entry->LastUsed;
		}

		if(Entry->Text != NULL && Entry->Hash == Hash){
			if(StringEq(Entry->Text, Text)){
				Stmt = Entry;
				Entry->LastUsed = GetMonotonicUptimeMS();
				break;
			}
		}
	}

	if(Stmt == NULL){
		Stmt = &Database->CachedStatements[LeastRecentlyUsed];

		if(Stmt->Text != NULL){
			PGresult *Result = PQclosePrepared(Database->Handle, Stmt->Name);
			if(PQresultStatus(Result) != PGRES_COMMAND_OK){
				char OldPreview[30];
				StringBufCopyEllipsis(OldPreview, Stmt->Text);
				LOG_ERR("Failed to close prepared query \"%s\": %s",
						OldPreview, PQerrorMessage(Database->Handle));
			}
			PQclear(Result);
			free(Stmt->Text);
		}

		{
			PGresult *Result = PQprepare(Database->Handle, Stmt->Name, Text, 0, NULL);
			if(PQresultStatus(Result) != PGRES_COMMAND_OK){
				char NewPreview[30];
				StringBufCopyEllipsis(NewPreview, Text);
				LOG_ERR("Failed to prepare query \"%s\": %s",
						NewPreview, PQerrorMessage(Database->Handle));
				PQclear(Result);
				return NULL;
			}
			PQclear(Result);
		}


		Stmt->LastUsed = GetMonotonicUptimeMS();
		Stmt->Hash = Hash;
		Stmt->Text = strdup(Text);
		ASSERT(Stmt->Text != NULL);

#if 1 // DEBUG_STATEMENT_CACHE
		{
			char Preview[30];
			StringBufCopyEllipsis(Preview, Text);
			LOG("New statement cached: \"%s\"", Preview);

			PGresult *Result = PQdescribePrepared(Database->Handle, Stmt->Name);
			if(PQresultStatus(Result) == PGRES_COMMAND_OK){
				LOG("  PARAM OIDs:");
				for(int i = 0; i < PQnparams(Result); i += 1){
					LOG("    $%d: %d", i, PQparamtype(Result, i));
				}

				LOG("  RESULT OIDs:");
				for(int i = 0; i < PQnfields(Result); i += 1){
					LOG("    (%d) %s: %d", i, PQfname(Result, i), PQftype(Result, i));
				}
			}
			PQclear(Result);
		}
#endif
	}

	return Stmt->Name;
}

// Database Management
//==============================================================================
void DatabaseClose(TDatabase *Database){
	if(Database != NULL){
		if(Database->Handle != NULL){
			PQfinish(Database->Handle);
			Database->Handle = NULL;
		}

		free(Database);
	}
}

TDatabase *DatabaseOpen(void){
	const char *Keys[] = {
		"host",
		"port",
		"dbname",
		"user",
		"password",
		"connect_timeout",
		"client_encoding",
		"application_name",
		"sslmode",
		"sslrootcert",
		NULL, // sentinel
	};

	const char *Values[] = {
		g_Config.PostgreSQL.Host,
		g_Config.PostgreSQL.Port,
		g_Config.PostgreSQL.DBName,
		g_Config.PostgreSQL.User,
		g_Config.PostgreSQL.Password,
		g_Config.PostgreSQL.ConnectTimeout,
		g_Config.PostgreSQL.ClientEncoding,
		g_Config.PostgreSQL.ApplicationName,
		g_Config.PostgreSQL.SSLMode,
		g_Config.PostgreSQL.SSLRootCert,
		NULL, // sentinel
	};

	TDatabase *Database = (TDatabase*)calloc(1, sizeof(TDatabase));
	Database->Handle = PQconnectdbParams(Keys, Values, 0);
	if(Database->Handle == NULL){
		LOG_ERR("Failed to allocate database connection");
		DatabaseClose(Database);
		return NULL;
	}

	if(PQstatus(Database->Handle) != CONNECTION_OK){
		LOG_ERR("Failed to establish connection: %s", PQerrorMessage(Database->Handle));
		DatabaseClose(Database);
		return NULL;
	}

//=====
	// TODO(fusion): REMOVE.
	{
		const char *Stmt = PrepareQuery(Database,
				"SELECT Value::INTEGER FROM SchemaInfo WHERE Key = $1::TEXT");
		if(Stmt == NULL){
			LOG_ERR("Failed to prepare query");
			DatabaseClose(Database);
			return NULL;
		}

		const char *ParamValues[] = { "VERSION" };
		PGresult *Result = PQexecPrepared(Database->Handle, Stmt, 1, ParamValues, NULL, NULL, 1);
		if(PQresultStatus(Result) != PGRES_TUPLES_OK){
			LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
			PQclear(Result);
			DatabaseClose(Database);
			return NULL;
		}

		LOG("VERSION: %d", BufferRead32BE((const uint8*)PQgetvalue(Result, 0, 0)));
		PQclear(Result);
	}

	const char *Stmt = PrepareQuery(Database, "SELECT Value FROM SchemaInfo WHERE Key = 'VERSION'");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		DatabaseClose(Database);
		return NULL;
	}

	int i = 0;
	do{
		PGresult *Result = PQexecPrepared(Database->Handle, Stmt, 0, NULL, NULL, NULL, 1);
		if(PQresultStatus(Result) != PGRES_TUPLES_OK){
			LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
			PQclear(Result);
			DatabaseClose(Database);
			return NULL;
		}
		LOG("VERSION: OID = %u, LEN = %d", PQftype(Result, 0), PQgetlength(Result, 0, 0));
		PQclear(Result);
		//PQreset(Database->Handle);
	}while(i++ < 2);
//=====

	return Database;
}

int DatabaseChanges(TDatabase *Database){
	ASSERT(Database != NULL);
	// TODO?
	return 0;
}

bool DatabaseCheckpoint(TDatabase *Database){
	ASSERT(Database != NULL);
	bool Result = true;
	if(PQstatus(Database->Handle) != CONNECTION_OK){
		DeleteStatementCache(Database);
		PQreset(Database->Handle);
		Result = (PQstatus(Database->Handle) == CONNECTION_OK);
	}
	return Result;
}

int DatabaseMaxConcurrency(void){
	return INT_MAX;
}

// TransactionScope
//==============================================================================
TransactionScope::TransactionScope(const char *Context){
	m_Context = (Context != NULL ? Context : "NOCONTEXT");
	m_Database = NULL;
}

TransactionScope::~TransactionScope(void){
	// TODO
}

bool TransactionScope::Begin(TDatabase *Database){
	// TODO
	return false;
}

bool TransactionScope::Commit(void){
	// TODO
	return false;
}

// Primary Tables
//==============================================================================
bool GetWorldID(TDatabase *Database, const char *World, int *WorldID){
//	const Oid ParamTypes[] = { TEXTOID };
	const char *Stmt = PrepareQuery(Database,
			"SELECT WorldID FROM Worlds WHERE Name = $1::TEXT");
//
	return false;
}

bool GetWorlds(TDatabase *Database, DynamicArray<TWorld> *Worlds){
	return false;
}

bool GetWorldConfig(TDatabase *Database, int WorldID, TWorldConfig *WorldConfig){
	return false;
}

bool AccountExists(TDatabase *Database, int AccountID, const char *Email, bool *Result){
	return false;
}

bool AccountNumberExists(TDatabase *Database, int AccountID, bool *Result){
	return false;
}

bool AccountEmailExists(TDatabase *Database, const char *Email, bool *Result){
	return false;
}

bool CreateAccount(TDatabase *Database, int AccountID, const char *Email, const uint8 *Auth, int AuthSize){
	return false;
}

bool GetAccountData(TDatabase *Database, int AccountID, TAccount *Account){
	return false;
}

bool GetAccountOnlineCharacters(TDatabase *Database, int AccountID, int *OnlineCharacters){
	return false;
}

bool IsCharacterOnline(TDatabase *Database, int CharacterID, bool *Result){
	return false;
}

bool ActivatePendingPremiumDays(TDatabase *Database, int AccountID){
	return false;
}

bool GetCharacterEndpoints(TDatabase *Database, int AccountID, DynamicArray<TCharacterEndpoint> *Characters){
	return false;
}

bool GetCharacterSummaries(TDatabase *Database, int AccountID, DynamicArray<TCharacterSummary> *Characters){
	return false;
}

bool CharacterNameExists(TDatabase *Database, const char *Name, bool *Result){
	return false;
}

bool CreateCharacter(TDatabase *Database, int WorldID, int AccountID, const char *Name, int Sex){
	return false;
}

bool GetCharacterID(TDatabase *Database, int WorldID, const char *CharacterName, int *CharacterID){
	return false;
}

bool GetCharacterLoginData(TDatabase *Database, const char *CharacterName, TCharacterLoginData *Character){
	return false;
}

bool GetCharacterProfile(TDatabase *Database, const char *CharacterName, TCharacterProfile *Character){
	return false;
}

bool GetCharacterRight(TDatabase *Database, int CharacterID, const char *Right, bool *Result){
	return false;
}

bool GetCharacterRights(TDatabase *Database, int CharacterID, DynamicArray<TCharacterRight> *Rights){
	return false;
}

bool GetGuildLeaderStatus(TDatabase *Database, int WorldID, int CharacterID, bool *Result){
	return false;
}

bool IncrementIsOnline(TDatabase *Database, int WorldID, int CharacterID){
	return false;
}

bool DecrementIsOnline(TDatabase *Database, int WorldID, int CharacterID){
	return false;
}

bool ClearIsOnline(TDatabase *Database, int WorldID, int *NumAffectedCharacters){
	return false;
}

bool LogoutCharacter(TDatabase *Database, int WorldID, int CharacterID, int Level,
		const char *Profession, const char *Residence, int LastLoginTime, int TutorActivities){
	return false;
}

bool GetCharacterIndexEntries(TDatabase *Database, int WorldID, int MinimumCharacterID,
		int MaxEntries, int *NumEntries, TCharacterIndexEntry *Entries){
	return false;
}

bool InsertCharacterDeath(TDatabase *Database, int WorldID, int CharacterID, int Level,
		int OffenderID, const char *Remark, bool Unjustified, int Timestamp){
	return false;
}

bool InsertBuddy(TDatabase *Database, int WorldID, int AccountID, int BuddyID){
	return false;
}

bool DeleteBuddy(TDatabase *Database, int WorldID, int AccountID, int BuddyID){
	return false;
}

bool GetBuddies(TDatabase *Database, int WorldID, int AccountID, DynamicArray<TAccountBuddy> *Buddies){
	return false;
}

bool GetWorldInvitation(TDatabase *Database, int WorldID, int CharacterID, bool *Result){
	return false;
}

bool InsertLoginAttempt(TDatabase *Database, int AccountID, int IPAddress, bool Failed){
	return false;
}

bool GetAccountFailedLoginAttempts(TDatabase *Database, int AccountID, int TimeWindow, int *Result){
	return false;
}

bool GetIPAddressFailedLoginAttempts(TDatabase *Database, int IPAddress, int TimeWindow, int *Result){
	return false;
}


// House Tables
//==============================================================================
bool FinishHouseAuctions(TDatabase *Database, int WorldID, DynamicArray<THouseAuction> *Auctions){
	return false;
}

bool FinishHouseTransfers(TDatabase *Database, int WorldID, DynamicArray<THouseTransfer> *Transfers){
	return false;
}

bool GetFreeAccountEvictions(TDatabase *Database, int WorldID, DynamicArray<THouseEviction> *Evictions){
	return false;
}

bool GetDeletedCharacterEvictions(TDatabase *Database, int WorldID, DynamicArray<THouseEviction> *Evictions){
	return false;
}

bool InsertHouseOwner(TDatabase *Database, int WorldID, int HouseID, int OwnerID, int PaidUntil){
	return false;
}

bool UpdateHouseOwner(TDatabase *Database, int WorldID, int HouseID, int OwnerID, int PaidUntil){
	return false;
}

bool DeleteHouseOwner(TDatabase *Database, int WorldID, int HouseID){
	return false;
}

bool GetHouseOwners(TDatabase *Database, int WorldID, DynamicArray<THouseOwner> *Owners){
	return false;
}

bool GetHouseAuctions(TDatabase *Database, int WorldID, DynamicArray<int> *Auctions){
	return false;
}

bool StartHouseAuction(TDatabase *Database, int WorldID, int HouseID){
	return false;
}

bool DeleteHouses(TDatabase *Database, int WorldID){
	return false;
}

bool InsertHouses(TDatabase *Database, int WorldID, int NumHouses, THouse *Houses){
	return false;
}

bool ExcludeFromAuctions(TDatabase *Database, int WorldID, int CharacterID, int Duration, int BanishmentID){
	return false;
}


// Banishment Tables
//==============================================================================
bool IsCharacterNamelocked(TDatabase *Database, int CharacterID, bool *Result){
	return false;
}

bool GetNamelockStatus(TDatabase *Database, int CharacterID, TNamelockStatus *Status){
	return false;
}

bool InsertNamelock(TDatabase *Database, int CharacterID, int IPAddress,
		int GamemasterID, const char *Reason, const char *Comment){
	return false;
}

bool IsAccountBanished(TDatabase *Database, int AccountID, bool *Result){
	return false;
}

bool GetBanishmentStatus(TDatabase *Database, int CharacterID, TBanishmentStatus *Status){
	return false;
}

bool InsertBanishment(TDatabase *Database, int CharacterID, int IPAddress, int GamemasterID,
		const char *Reason, const char *Comment, bool FinalWarning, int Duration, int *BanishmentID){
	return false;
}

bool GetNotationCount(TDatabase *Database, int CharacterID, int *Result){
	return false;
}

bool InsertNotation(TDatabase *Database, int CharacterID, int IPAddress,
		int GamemasterID, const char *Reason, const char *Comment){
	return false;
}

bool IsIPBanished(TDatabase *Database, int IPAddress, bool *Result){
	return false;
}

bool InsertIPBanishment(TDatabase *Database, int CharacterID, int IPAddress,
		int GamemasterID, const char *Reason, const char *Comment, int Duration){
	return false;
}

bool IsStatementReported(TDatabase *Database, int WorldID, TStatement *Statement, bool *Result){
	return false;
}

bool InsertStatements(TDatabase *Database, int WorldID, int NumStatements, TStatement *Statements){
	return false;
}

bool InsertReportedStatement(TDatabase *Database, int WorldID, TStatement *Statement,
		int BanishmentID, int ReporterID, const char *Reason, const char *Comment){
	return false;
}


// Info Tables
//==============================================================================
bool GetKillStatistics(TDatabase *Database, int WorldID, DynamicArray<TKillStatistics> *Stats){
	return false;
}

bool MergeKillStatistics(TDatabase *Database, int WorldID, int NumStats, TKillStatistics *Stats){
	return false;
}

bool GetOnlineCharacters(TDatabase *Database, int WorldID, DynamicArray<TOnlineCharacter> *Characters){
	return false;
}

bool DeleteOnlineCharacters(TDatabase *Database, int WorldID){
	return false;
}

bool InsertOnlineCharacters(TDatabase *Database, int WorldID,
		int NumCharacters, TOnlineCharacter *Characters){
	return false;
}

bool CheckOnlineRecord(TDatabase *Database, int WorldID, int NumCharacters, bool *NewRecord){
	return false;
}

#endif //DATABASE_POSTGRESQL
