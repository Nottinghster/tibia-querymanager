#include "querymanager.hh"

#include <pthread.h>

enum : int {
	WORKER_STATUS_SPAWNING = 0,
	WORKER_STATUS_ACTIVE,
	WORKER_STATUS_DONE,
};

struct TQueryQueue{
	pthread_mutex_t Mutex;
	pthread_cond_t WorkAvailable;
	pthread_cond_t RoomAvailable;
	uint32 ReadPos;
	uint32 WritePos;
	uint32 MaxQueries;
	TQuery **Queries;
};

struct TWorker{
	int WorkerID;
	AtomicInt Status;
	AtomicInt Stop;
	pthread_t Thread;
};

static int g_NumWorkers;
static TWorker *g_Workers;
static TQueryQueue *g_QueryQueue;

// Query Name
//==============================================================================
const char *QueryName(int QueryType){
	const char *Name = "";
	switch(QueryType){
		case QUERY_LOGIN:                    Name = "LOGIN"; break;
		case QUERY_INTERNAL_RESOLVE_WORLD:   Name = "INTERNAL_RESOLVE_WORLD"; break;
		case QUERY_CHECK_ACCOUNT_PASSWORD:   Name = "CHECK_ACCOUNT_PASSWORD"; break;
		case QUERY_LOGIN_ACCOUNT:            Name = "LOGIN_ACCOUNT"; break;
		case QUERY_LOGIN_ADMIN:              Name = "LOGIN_ADMIN"; break;
		case QUERY_LOGIN_GAME:               Name = "LOGIN_GAME"; break;
		case QUERY_LOGOUT_GAME:              Name = "LOGOUT_GAME"; break;
		case QUERY_SET_NAMELOCK:             Name = "SET_NAMELOCK"; break;
		case QUERY_BANISH_ACCOUNT:           Name = "BANISH_ACCOUNT"; break;
		case QUERY_SET_NOTATION:             Name = "SET_NOTATION"; break;
		case QUERY_REPORT_STATEMENT:         Name = "REPORT_STATEMENT"; break;
		case QUERY_BANISH_IP_ADDRESS:        Name = "BANISH_IP_ADDRESS"; break;
		case QUERY_LOG_CHARACTER_DEATH:      Name = "LOG_CHARACTER_DEATH"; break;
		case QUERY_ADD_BUDDY:                Name = "ADD_BUDDY"; break;
		case QUERY_REMOVE_BUDDY:             Name = "REMOVE_BUDDY"; break;
		case QUERY_DECREMENT_IS_ONLINE:      Name = "DECREMENT_IS_ONLINE"; break;
		case QUERY_FINISH_AUCTIONS:          Name = "FINISH_AUCTIONS"; break;
		case QUERY_TRANSFER_HOUSES:          Name = "TRANSFER_HOUSES"; break;
		case QUERY_EVICT_FREE_ACCOUNTS:      Name = "EVICT_FREE_ACCOUNTS"; break;
		case QUERY_EVICT_DELETED_CHARACTERS: Name = "EVICT_DELETED_CHARACTERS"; break;
		case QUERY_EVICT_EX_GUILDLEADERS:    Name = "EVICT_EX_GUILDLEADERS"; break;
		case QUERY_INSERT_HOUSE_OWNER:       Name = "INSERT_HOUSE_OWNER"; break;
		case QUERY_UPDATE_HOUSE_OWNER:       Name = "UPDATE_HOUSE_OWNER"; break;
		case QUERY_DELETE_HOUSE_OWNER:       Name = "DELETE_HOUSE_OWNER"; break;
		case QUERY_GET_HOUSE_OWNERS:         Name = "GET_HOUSE_OWNERS"; break;
		case QUERY_GET_AUCTIONS:             Name = "GET_AUCTIONS"; break;
		case QUERY_START_AUCTION:            Name = "START_AUCTION"; break;
		case QUERY_INSERT_HOUSES:            Name = "INSERT_HOUSES"; break;
		case QUERY_CLEAR_IS_ONLINE:          Name = "CLEAR_IS_ONLINE"; break;
		case QUERY_CREATE_PLAYERLIST:        Name = "CREATE_PLAYERLIST"; break;
		case QUERY_LOG_KILLED_CREATURES:     Name = "LOG_KILLED_CREATURES"; break;
		case QUERY_LOAD_PLAYERS:             Name = "LOAD_PLAYERS"; break;
		case QUERY_EXCLUDE_FROM_AUCTIONS:    Name = "EXCLUDE_FROM_AUCTIONS"; break;
		case QUERY_CANCEL_HOUSE_TRANSFER:    Name = "CANCEL_HOUSE_TRANSFER"; break;
		case QUERY_LOAD_WORLD_CONFIG:        Name = "LOAD_WORLD_CONFIG"; break;
		case QUERY_CREATE_ACCOUNT:           Name = "CREATE_ACCOUNT"; break;
		case QUERY_CREATE_CHARACTER:         Name = "CREATE_CHARACTER"; break;
		case QUERY_GET_ACCOUNT_SUMMARY:      Name = "GET_ACCOUNT_SUMMARY"; break;
		case QUERY_GET_CHARACTER_PROFILE:    Name = "GET_CHARACTER_PROFILE"; break;
		case QUERY_GET_WORLDS:               Name = "GET_WORLDS"; break;
		case QUERY_GET_ONLINE_CHARACTERS:    Name = "GET_ONLINE_CHARACTERS"; break;
		case QUERY_GET_KILL_STATISTICS:      Name = "GET_KILL_STATISTICS"; break;
		default:                             Name = "UNKNOWN"; break;
	}
	return Name;
}

// Query Queue and Workers
//==============================================================================
TQuery *QueryNew(void){
	TQuery *Query = (TQuery*)calloc(1, sizeof(TQuery));
	AtomicStore(&Query->RefCount, 1);
	Query->BufferSize = g_Config.QueryBufferSize;
	Query->Buffer = (uint8*)calloc(1, Query->BufferSize);
	Query->Request = TReadBuffer{};
	Query->Response = TWriteBuffer{};
	return Query;
}

void QueryDone(TQuery *Query){
	if(Query != NULL){
		int RefCount = AtomicFetchAdd(&Query->RefCount, -1);
		ASSERT(RefCount >= 1);
		if(RefCount == 1){
			free(Query->Buffer);
			free(Query);
		}
	}
}

int QueryRefCount(TQuery *Query){
	return AtomicLoad(&Query->RefCount);
}

void QueryEnqueue(TQuery *Query){
	ASSERT(g_QueryQueue != NULL);
	ASSERT(Query != NULL);

	// IMPORTANT(fusion): A query object should be referenced by a connection
	// and a query queue/worker at most. Anything else, we're gonna have a bad
	// time.
	int RefCount = 1;
	if(!AtomicCompareExchange(&Query->RefCount, &RefCount, 2)){
		LOG_ERR("Query already have %d references", RefCount);
		return;
	}

	pthread_mutex_lock(&g_QueryQueue->Mutex);
	uint32 NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
	uint32 MaxQueries = g_QueryQueue->MaxQueries;
	while(NumQueries >= MaxQueries){
		LOG_WARN("Execution stalled: queue is full (%u / %u)...",
				NumQueries, MaxQueries);
		pthread_cond_wait(&g_QueryQueue->RoomAvailable, &g_QueryQueue->Mutex);
		NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
		MaxQueries = g_QueryQueue->MaxQueries;
	}

	if(NumQueries == 0){
		pthread_cond_signal(&g_QueryQueue->WorkAvailable);
	}

	g_QueryQueue->Queries[g_QueryQueue->WritePos % MaxQueries] = Query;
	g_QueryQueue->WritePos += 1;
	pthread_mutex_unlock(&g_QueryQueue->Mutex);
}

TQuery *QueryDequeue(AtomicInt *Stop){
	ASSERT(g_QueryQueue != NULL);
	ASSERT(Stop != NULL);

	TQuery *Query = NULL;
	pthread_mutex_lock(&g_QueryQueue->Mutex);
	uint32 NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
	while(NumQueries == 0 && !AtomicLoad(Stop)){
		pthread_cond_wait(&g_QueryQueue->WorkAvailable, &g_QueryQueue->Mutex);
		NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
	}

	if(NumQueries > 0 && !AtomicLoad(Stop)){
		uint32 MaxQueries = g_QueryQueue->MaxQueries;
		if(NumQueries == MaxQueries){
			pthread_cond_signal(&g_QueryQueue->RoomAvailable);
		}

		Query = g_QueryQueue->Queries[g_QueryQueue->ReadPos % MaxQueries];
		g_QueryQueue->ReadPos += 1;
	}
	pthread_mutex_unlock(&g_QueryQueue->Mutex);
	return Query;
}

static void *WorkerThread(void *Data){
	ASSERT(Data != NULL);
	TWorker *Worker = (TWorker*)Data;
	if(AtomicLoad(&Worker->Stop)){
		LOG_WARN("Worker#%d: Stopping on entry...", Worker->WorkerID);
		AtomicStore(&Worker->Status, WORKER_STATUS_DONE);
		return NULL;
	}

	TDatabase *Database = DatabaseOpen();
	if(Database == NULL){
		LOG_ERR("Worker#%d: Failed to connect to database", Worker->WorkerID);
		AtomicStore(&Worker->Status, WORKER_STATUS_DONE);
		return NULL;
	}

	LOG("Worker#%d: ACTIVE...", Worker->WorkerID);
	AtomicStore(&Worker->Status, WORKER_STATUS_ACTIVE);
	while(TQuery *Query = QueryDequeue(&Worker->Stop)){
		void (*ProcessQuery)(TDatabase*, TQuery*) = NULL;
		Query->QueryType = Query->Request.Read8();
		switch(Query->QueryType){
			case QUERY_INTERNAL_RESOLVE_WORLD:		ProcessQuery = ProcessInternalResolveWorld; break;
			case QUERY_CHECK_ACCOUNT_PASSWORD:		ProcessQuery = ProcessCheckAccountPassword; break;
			case QUERY_LOGIN_ACCOUNT:				ProcessQuery = ProcessLoginAccount; break;
			case QUERY_LOGIN_GAME:					ProcessQuery = ProcessLoginGame; break;
			case QUERY_LOGOUT_GAME:					ProcessQuery = ProcessLogoutGame; break;
			case QUERY_SET_NAMELOCK:				ProcessQuery = ProcessSetNamelock; break;
			case QUERY_BANISH_ACCOUNT:				ProcessQuery = ProcessBanishAccount; break;
			case QUERY_SET_NOTATION:				ProcessQuery = ProcessSetNotation; break;
			case QUERY_REPORT_STATEMENT:			ProcessQuery = ProcessReportStatement; break;
			case QUERY_BANISH_IP_ADDRESS:			ProcessQuery = ProcessBanishIpAddress; break;
			case QUERY_LOG_CHARACTER_DEATH:			ProcessQuery = ProcessLogCharacterDeath; break;
			case QUERY_ADD_BUDDY:					ProcessQuery = ProcessAddBuddy; break;
			case QUERY_REMOVE_BUDDY:				ProcessQuery = ProcessRemoveBuddy; break;
			case QUERY_DECREMENT_IS_ONLINE:			ProcessQuery = ProcessDecrementIsOnline; break;
			case QUERY_FINISH_AUCTIONS:				ProcessQuery = ProcessFinishAuctions; break;
			case QUERY_TRANSFER_HOUSES:				ProcessQuery = ProcessTransferHouses; break;
			case QUERY_EVICT_FREE_ACCOUNTS:			ProcessQuery = ProcessEvictFreeAccounts; break;
			case QUERY_EVICT_DELETED_CHARACTERS:	ProcessQuery = ProcessEvictDeletedCharacters; break;
			case QUERY_EVICT_EX_GUILDLEADERS:		ProcessQuery = ProcessEvictExGuildleaders; break;
			case QUERY_INSERT_HOUSE_OWNER:			ProcessQuery = ProcessInsertHouseOwner; break;
			case QUERY_UPDATE_HOUSE_OWNER:			ProcessQuery = ProcessUpdateHouseOwner; break;
			case QUERY_DELETE_HOUSE_OWNER:			ProcessQuery = ProcessDeleteHouseOwner; break;
			case QUERY_GET_HOUSE_OWNERS:			ProcessQuery = ProcessGetHouseOwners; break;
			case QUERY_GET_AUCTIONS:				ProcessQuery = ProcessGetAuctions; break;
			case QUERY_START_AUCTION:				ProcessQuery = ProcessStartAuction; break;
			case QUERY_INSERT_HOUSES:				ProcessQuery = ProcessInsertHouses; break;
			case QUERY_CLEAR_IS_ONLINE:				ProcessQuery = ProcessClearIsOnline; break;
			case QUERY_CREATE_PLAYERLIST:			ProcessQuery = ProcessCreatePlayerlist; break;
			case QUERY_LOG_KILLED_CREATURES:		ProcessQuery = ProcessLogKilledCreatures; break;
			case QUERY_LOAD_PLAYERS:				ProcessQuery = ProcessLoadPlayers; break;
			case QUERY_EXCLUDE_FROM_AUCTIONS:		ProcessQuery = ProcessExcludeFromAuctions; break;
			case QUERY_CANCEL_HOUSE_TRANSFER:		ProcessQuery = ProcessCancelHouseTransfer; break;
			case QUERY_LOAD_WORLD_CONFIG:			ProcessQuery = ProcessLoadWorldConfig; break;
			case QUERY_CREATE_ACCOUNT:				ProcessQuery = ProcessCreateAccount; break;
			case QUERY_CREATE_CHARACTER:			ProcessQuery = ProcessCreateCharacter; break;
			case QUERY_GET_ACCOUNT_SUMMARY:			ProcessQuery = ProcessGetAccountSummary; break;
			case QUERY_GET_CHARACTER_PROFILE:		ProcessQuery = ProcessGetCharacterProfile; break;
			case QUERY_GET_WORLDS:					ProcessQuery = ProcessGetWorlds; break;
			case QUERY_GET_ONLINE_CHARACTERS:		ProcessQuery = ProcessGetOnlineCharacters; break;
			case QUERY_GET_KILL_STATISTICS:			ProcessQuery = ProcessGetKillStatistics; break;
		}

		Query->QueryStatus = QUERY_STATUS_PENDING;
		if(ProcessQuery != NULL && DatabaseCheckpoint(Database)){
			// NOTE(fusion): A minimum of 1 attempt is ASSUMED.
			int Attempts = g_Config.QueryMaxAttempts;
			while(true){
				ProcessQuery(Database, Query);
				if(Query->QueryStatus != QUERY_STATUS_PENDING
						|| Attempts <= 0
						|| !DatabaseCheckpoint(Database)){
					break;
				}

				Attempts -= 1;

				// NOTE(fusion): This one is important because we want to know
				// whether some query is failing too often, in which case there
				// may be a problem with it.
				LOG_WARN("Worker#%d: Query %s failed, retrying...",
						Worker->WorkerID, QueryName(Query->QueryType));
			}
		}

		if(Query->QueryStatus == QUERY_STATUS_PENDING){
			QueryFailed(Query);
		}

		QueryDone(Query);
		WakeConnections();
	}

	LOG("Worker#%d: DONE...", Worker->WorkerID);
	DatabaseClose(Database);
	AtomicStore(&Worker->Status, WORKER_STATUS_DONE);
	return NULL;
}

bool InitQuery(void){
	ASSERT(g_QueryQueue == NULL);
	ASSERT(g_Workers == NULL);

	// IMPORTANT(fusion): We'd ideally have a single query per connection at any
	// given time but, in reality, connections could be reset while their queries
	// are still in a query queue/worker, increasing the maximum number of queries
	// in flight.
	g_QueryQueue = (TQueryQueue*)calloc(1, sizeof(TQueryQueue));
	pthread_mutex_init(&g_QueryQueue->Mutex, NULL);
	pthread_cond_init(&g_QueryQueue->WorkAvailable, NULL);
	pthread_cond_init(&g_QueryQueue->RoomAvailable, NULL);
	g_QueryQueue->MaxQueries = 2 * g_Config.MaxConnections;
	g_QueryQueue->Queries = (TQuery**)calloc(g_QueryQueue->MaxQueries, sizeof(TQuery*));

	g_NumWorkers = g_Config.QueryWorkerThreads;
	if(g_NumWorkers > DatabaseMaxConcurrency()){
		g_NumWorkers = DatabaseMaxConcurrency();
	}

	g_Workers = (TWorker*)calloc(g_NumWorkers, sizeof(TWorker));
	for(int i = 0; i < g_NumWorkers; i += 1){
		TWorker *Worker = &g_Workers[i];
		Worker->WorkerID = i;
		AtomicStore(&Worker->Status, WORKER_STATUS_SPAWNING);
		AtomicStore(&Worker->Stop, 0);
		int ErrorCode = pthread_create(&Worker->Thread, NULL, WorkerThread, Worker);
		if(ErrorCode != 0){
			LOG_ERR("Failed to spawn worker thread %d: (%d) %s",
					i, ErrorCode, strerrordesc_np(ErrorCode));
			return false;
		}
	}

	while(true){
		int NumWorkersSpawning = 0;
		int NumWorkersActive = 0;
		int NumWorkersDone = 0;
		for(int i = 0; i < g_NumWorkers; i += 1){
			int Status = AtomicLoad(&g_Workers[i].Status);
			if(Status == WORKER_STATUS_SPAWNING){
				NumWorkersSpawning += 1;
			}else if(Status == WORKER_STATUS_ACTIVE){
				NumWorkersActive += 1;
			}else if(Status == WORKER_STATUS_DONE){
				NumWorkersDone += 1;
			}
		}

		if(NumWorkersSpawning > 0){
			LOG("Waiting on worker threads... (SPAWNING=%d, ACTIVE=%d, DONE=%d)",
					NumWorkersSpawning, NumWorkersActive, NumWorkersDone);
			SleepMS(500);
			continue;
		}

		if(NumWorkersDone > 0){
			LOG_ERR("%d worker thread%s failed to initialize",
					NumWorkersDone, (NumWorkersDone == 1 ? "" : "s"));
			return false;
		}

		ASSERT(NumWorkersActive == g_NumWorkers);
		break;
	}

	return true;
}

void ExitQuery(void){
	if(g_Workers != NULL){
		ASSERT(g_QueryQueue != NULL);

		for(int i = 0; i < g_NumWorkers; i += 1){
			AtomicStore(&g_Workers[i].Stop, 1);
		}

		pthread_cond_broadcast(&g_QueryQueue->WorkAvailable);
		for(int i = 0; i < g_NumWorkers; i += 1){
			// IMPORTANT(fusion): There is no "invalid" pthread handle so this
			// is non-standard behaviour. Nevertheless the game server uses it
			// and it seems to be consistent on Linux, which is what matters at
			// the end of the day.
			if(g_Workers[i].Thread != 0){
				pthread_join(g_Workers[i].Thread, NULL);
			}
		}

		free(g_Workers);
	}

	if(g_QueryQueue != NULL){
		pthread_mutex_destroy(&g_QueryQueue->Mutex);
		pthread_cond_destroy(&g_QueryQueue->WorkAvailable);
		pthread_cond_destroy(&g_QueryQueue->RoomAvailable);

		// TODO(fusion): Abort queries instead?
		uint32 MaxQueries = g_QueryQueue->MaxQueries;
		for(uint32 ReadPos = g_QueryQueue->ReadPos;
				ReadPos != g_QueryQueue->WritePos;
				ReadPos += 1){
			QueryDone(g_QueryQueue->Queries[ReadPos % MaxQueries]);
		}

		free(g_QueryQueue->Queries);
		free(g_QueryQueue);
	}
}

// Query Request
//==============================================================================
TWriteBuffer QueryBeginRequest(TQuery *Query, int QueryType){
	Query->Request = TReadBuffer{};
	TWriteBuffer WriteBuffer = TWriteBuffer(Query->Buffer, Query->BufferSize);
	WriteBuffer.Write8((uint8)QueryType);
	return WriteBuffer;
}

bool QueryFinishRequest(TQuery *Query, TWriteBuffer WriteBuffer){
	ASSERT(WriteBuffer.Buffer   == Query->Buffer
		&& WriteBuffer.Size     == Query->BufferSize
		&& WriteBuffer.Position >= 1);
	bool Result = !WriteBuffer.Overflowed();
	if(Result){
		Query->Request = TReadBuffer(WriteBuffer.Buffer, WriteBuffer.Position);
	}
	return Result;
}

bool QueryInternalResolveWorld(TQuery *Query, const char *World){
	TWriteBuffer WriteBuffer = QueryBeginRequest(Query, QUERY_INTERNAL_RESOLVE_WORLD);
	WriteBuffer.WriteString(World);
	return QueryFinishRequest(Query, WriteBuffer);
}

// Query Response
//==============================================================================
TWriteBuffer *QueryBeginResponse(TQuery *Query, int Status){
	ASSERT(Status != QUERY_STATUS_PENDING);
	Query->QueryStatus = Status;
	Query->Response = TWriteBuffer(Query->Buffer, Query->BufferSize);
	Query->Response.Write16(0);
	Query->Response.Write8((uint8)Status);
	return &Query->Response;
}

bool QueryFinishResponse(TQuery *Query){
	TWriteBuffer *Response = &Query->Response;
	if(Response->Position <= 2){
		LOG_ERR("Invalid response size");
		return false;
	}

	int PayloadSize = Response->Position - 2;
	if(PayloadSize < 0xFFFF){
		Response->Rewrite16(0, (uint16)PayloadSize);
	}else{
		Response->Rewrite16(0, 0xFFFF);
		Response->Insert32(2, (uint32)PayloadSize);
	}

	return !Response->Overflowed();
}

void QueryOk(TQuery *Query){
	QueryBeginResponse(Query, QUERY_STATUS_OK);
	QueryFinishResponse(Query);
}

void QueryError(TQuery *Query, int ErrorCode){
	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_ERROR);
	Response->Write8((uint8)ErrorCode);
	QueryFinishResponse(Query);
}

void QueryFailed(TQuery *Query){
	QueryBeginResponse(Query, QUERY_STATUS_FAILED);
	QueryFinishResponse(Query);
}

// Query Helpers
//==============================================================================
static void CompoundBanishment(TBanishmentStatus Status, int *Days, bool *FinalWarning){
	// TODO(fusion): We might want to add all these constants as config values.
	ASSERT(Days != NULL && FinalWarning != NULL);
	if(Status.FinalWarning){
		*FinalWarning = false;
		*Days = 0; // permanent
	}else if(Status.TimesBanished > 5 || *FinalWarning){
		*FinalWarning = true;
		if(*Days < 30){
			*Days = 30;
		}else{
			*Days *= 2;
		}
	}
}

// Query Processing
//==============================================================================
// IMPORTANT(fusion): Query processing functions are expected to signal their status
// by updating `Query->QueryStatus`. A query that is still `PENDING` at the end of
// processing is considered unfinished and may be retried.
//  The query response should only be written at the very end, when we know it has
// SUCCEEDED, so that request data is kept intact (because they share the same query
// buffer) if the caller decides to retry it.
//  Since using `QueryBeginResponse` automatically sets `Query->QueryStatus`, it
// should be unlikely that the request data is modified while keeping a `PENDING`
// status.

// NOTE(fusion): These should be used with query functions to reduce clutter.
#define QUERY_STOP_IF(Cond)								\
	do{													\
		if(Cond){										\
			Query->QueryStatus = QUERY_STATUS_PENDING;	\
			return;										\
		}												\
	}while(0)

#define QUERY_ERROR_IF(Cond, ErrorCode)					\
	do{													\
		if(Cond){										\
			QueryError(Query, ErrorCode);				\
			return;										\
		}												\
	}while(0)

#define QUERY_FAIL_IF(Cond) 							\
	do{													\
		if(Cond){										\
			QueryFailed(Query);							\
			return;										\
		}												\
	}while(0)

void ProcessInternalResolveWorld(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char World[30];
	Request.ReadString(World, sizeof(World));

	int WorldID;
	QUERY_STOP_IF(!GetWorldID(Database, World, &WorldID));
	QUERY_FAIL_IF(WorldID <= 0);

	Query->WorldID = WorldID;
	QueryOk(Query);
}

static void CheckAccountPasswordTx(TDatabase *Database, TQuery *Query,
		int AccountID, const char *Password, int IPAddress){
	TransactionScope Tx("CheckAccountPassword");
	QUERY_STOP_IF(!Tx.Begin(Database));

	TAccount Account;
	QUERY_STOP_IF(!GetAccountData(Database, AccountID, &Account));
	QUERY_ERROR_IF(Account.AccountID == 0, 1);
	QUERY_ERROR_IF(!TestPassword(Account.Auth, sizeof(Account.Auth), Password), 2);

	int FailedLoginAttempts;
	QUERY_STOP_IF(!GetAccountFailedLoginAttempts(Database, Account.AccountID, 5 * 60, &FailedLoginAttempts));
	QUERY_ERROR_IF(FailedLoginAttempts > 10, 3);
	QUERY_STOP_IF(!GetIPAddressFailedLoginAttempts(Database, IPAddress, 30 * 60, &FailedLoginAttempts));
	QUERY_ERROR_IF(FailedLoginAttempts > 20, 4);

	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessCheckAccountPassword(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char Password[30];
	char IPString[16];
	int AccountID = (int)Request.Read32();
	Request.ReadString(Password, sizeof(Password));
	Request.ReadString(IPString, sizeof(IPString));

	int IPAddress = 0;
	QUERY_FAIL_IF(!ParseIPAddress(&IPAddress, IPString));

	// NOTE(fusion): Same as `ProcessLoginGame`.
	CheckAccountPasswordTx(Database, Query, AccountID, Password, IPAddress);
	if(Query->QueryStatus != QUERY_STATUS_PENDING){
		InsertLoginAttempt(Database, AccountID, IPAddress,
				(Query->QueryStatus != QUERY_STATUS_OK));
	}
}

static void LoginAccountTx(TDatabase *Database, TQuery *Query,
		int AccountID, const char *Password, int IPAddress){
	TransactionScope Tx("LoginAccount");
	QUERY_STOP_IF(!Tx.Begin(Database));

	TAccount Account;
	QUERY_STOP_IF(!GetAccountData(Database, AccountID, &Account));
	QUERY_ERROR_IF(Account.AccountID == 0, 1);
	QUERY_ERROR_IF(!TestPassword(Account.Auth, sizeof(Account.Auth), Password), 2);

	int FailedLoginAttempts;
	QUERY_STOP_IF(!GetAccountFailedLoginAttempts(Database, Account.AccountID, 5 * 60, &FailedLoginAttempts));
	QUERY_ERROR_IF(FailedLoginAttempts > 10, 3);
	QUERY_STOP_IF(!GetIPAddressFailedLoginAttempts(Database, IPAddress, 30 * 60, &FailedLoginAttempts));
	QUERY_ERROR_IF(FailedLoginAttempts > 20, 4);

	bool IsBanished;
	QUERY_STOP_IF(!IsAccountBanished(Database, Account.AccountID, &IsBanished));
	QUERY_ERROR_IF(IsBanished, 5);
	QUERY_STOP_IF(!IsIPBanished(Database, IPAddress, &IsBanished));
	QUERY_ERROR_IF(IsBanished, 6);

	DynamicArray<TCharacterEndpoint> Characters;
	QUERY_STOP_IF(!GetCharacterEndpoints(Database, Account.AccountID, &Characters));
	QUERY_STOP_IF(!Tx.Commit());

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumCharacters = std::min<int>(Characters.Length(), UINT8_MAX);
	Response->Write8((uint8)NumCharacters);
	for(int i = 0; i < NumCharacters; i += 1){
		Response->WriteString(Characters[i].Name);
		Response->WriteString(Characters[i].WorldName);

		int WorldAddress;
		if(ResolveHostName(Characters[i].WorldHost, &WorldAddress)){
			Response->Write32BE(WorldAddress);
			Response->Write16(Characters[i].WorldPort);
		}else{
			LOG_ERR("Failed to resolve world \"%s\" host name \"%s\" for character \"%s\"",
					Characters[i].WorldName, Characters[i].WorldHost, Characters[i].Name);

			Response->Write32BE(0);
			Response->Write16(0);
		}
	}
	Response->Write16((uint16)(Account.PremiumDays + Account.PendingPremiumDays));
	QueryFinishResponse(Query);
}

void ProcessLoginAccount(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char Password[30];
	char IPString[16];
	int AccountID = (int)Request.Read32();
	Request.ReadString(Password, sizeof(Password));
	Request.ReadString(IPString, sizeof(IPString));

	int IPAddress = 0;
	QUERY_FAIL_IF(!ParseIPAddress(&IPAddress, IPString));

	// NOTE(fusion): Same as `ProcessLoginGame`.
	LoginAccountTx(Database, Query, AccountID, Password, IPAddress);
	if(Query->QueryStatus != QUERY_STATUS_PENDING){
		InsertLoginAttempt(Database, AccountID, IPAddress,
				(Query->QueryStatus != QUERY_STATUS_OK));
	}
}

static void LoginGameTx(TDatabase *Database, TQuery *Query,
		int AccountID, const char *CharacterName, const char *Password,
		int IPAddress, bool PrivateWorld, bool GamemasterRequired){
	TransactionScope Tx("LoginGame");
	QUERY_STOP_IF(!Tx.Begin(Database));

	TCharacterLoginData Character;
	QUERY_STOP_IF(!GetCharacterLoginData(Database, CharacterName, &Character));
	QUERY_ERROR_IF(Character.CharacterID == 0, 1);
	QUERY_ERROR_IF(Character.Deleted, 2);
	QUERY_ERROR_IF(Character.WorldID != Query->WorldID, 3);
	if(PrivateWorld){
		bool Invited;
		QUERY_STOP_IF(!GetWorldInvitation(Database, Query->WorldID, Character.CharacterID, &Invited));
		QUERY_ERROR_IF(!Invited, 4);
	}

	TAccount Account;
	QUERY_STOP_IF(!GetAccountData(Database, AccountID, &Account));
	// NOTE(fusion): This is correct, there is no error code 5.
	QUERY_ERROR_IF(Account.AccountID == 0 || Account.AccountID != Character.AccountID, 15);
	QUERY_ERROR_IF(Account.Deleted, 8);
	QUERY_ERROR_IF(!TestPassword(Account.Auth, sizeof(Account.Auth), Password), 6);

	int FailedLoginAttempts;
	QUERY_STOP_IF(!GetAccountFailedLoginAttempts(Database, Account.AccountID, 5 * 60, &FailedLoginAttempts));
	QUERY_ERROR_IF(FailedLoginAttempts > 10, 7);
	QUERY_STOP_IF(!GetIPAddressFailedLoginAttempts(Database, IPAddress, 30 * 60, &FailedLoginAttempts));
	QUERY_ERROR_IF(FailedLoginAttempts > 20, 9);

	bool IsBanished;
	QUERY_STOP_IF(!IsAccountBanished(Database, Account.AccountID, &IsBanished));
	QUERY_ERROR_IF(IsBanished, 10);
	QUERY_STOP_IF(!IsCharacterNamelocked(Database, Character.CharacterID, &IsBanished));
	QUERY_ERROR_IF(IsBanished, 11);
	QUERY_STOP_IF(!IsIPBanished(Database, IPAddress, &IsBanished));
	QUERY_ERROR_IF(IsBanished, 12);

	// TODO(fusion): Probably merge these into a single operation?
	bool MultiClient;
	QUERY_STOP_IF(!GetCharacterRight(Database, Character.CharacterID, "ALLOW_MULTICLIENT", &MultiClient));
	if(!MultiClient){
		int OnlineCharacters;
		QUERY_STOP_IF(!GetAccountOnlineCharacters(Database, Account.AccountID, &OnlineCharacters));
		if(OnlineCharacters > 0){
			bool IsOnline;
			QUERY_STOP_IF(!IsCharacterOnline(Database, Character.CharacterID, &IsOnline));
			QUERY_ERROR_IF(!IsOnline, 13);
		}
	}

	if(GamemasterRequired){
		bool GamemasterOutfit;
		QUERY_STOP_IF(!GetCharacterRight(Database, Character.CharacterID, "GAMEMASTER_OUTFIT", &GamemasterOutfit));
		QUERY_ERROR_IF(!GamemasterOutfit, 14);
	}

	DynamicArray<TAccountBuddy> Buddies;
	QUERY_STOP_IF(!GetBuddies(Database, Query->WorldID, Account.AccountID, &Buddies));

	DynamicArray<TCharacterRight> Rights;
	QUERY_STOP_IF(!GetCharacterRights(Database, Character.CharacterID, &Rights));

	bool PremiumAccountActivated = false;
	if(Account.PremiumDays == 0 && Account.PendingPremiumDays > 0){
		QUERY_STOP_IF(!ActivatePendingPremiumDays(Database, Account.AccountID));
		Account.PremiumDays += Account.PendingPremiumDays;
		Account.PendingPremiumDays = 0;
		PremiumAccountActivated = true;
	}

	if(Account.PremiumDays > 0){
		Rights.Push(TCharacterRight{"PREMIUM_ACCOUNT"});
	}

	QUERY_STOP_IF(!IncrementIsOnline(Database, Query->WorldID, Character.CharacterID));
	QUERY_STOP_IF(!Tx.Commit());

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->Write32((uint32)Character.CharacterID);
	Response->WriteString(Character.Name);
	Response->Write8((uint8)Character.Sex);
	Response->WriteString(Character.Guild);
	Response->WriteString(Character.Rank);
	Response->WriteString(Character.Title);

	int NumBuddies = std::min<int>(Buddies.Length(), UINT8_MAX);
	Response->Write8((uint8)NumBuddies);
	for(int i = 0; i < NumBuddies; i += 1){
		Response->Write32((uint32)Buddies[i].CharacterID);
		Response->WriteString(Buddies[i].Name);
	}

	int NumRights = std::min<int>(Rights.Length(), UINT8_MAX);
	Response->Write8((uint8)NumRights);
	for(int i = 0; i < NumRights; i += 1){
		Response->WriteString(Rights[i].Name);
	}

	Response->WriteFlag(PremiumAccountActivated);
	QueryFinishResponse(Query);
}

void ProcessLoginGame(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char CharacterName[30];
	char Password[30];
	char IPString[16];
	int AccountID = (int)Request.Read32();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	Request.ReadString(Password, sizeof(Password));
	Request.ReadString(IPString, sizeof(IPString));
	bool PrivateWorld = Request.ReadFlag();
	Request.ReadFlag(); // "PremiumAccountRequired" unused
	bool GamemasterRequired = Request.ReadFlag();

	int IPAddress;
	QUERY_FAIL_IF(!ParseIPAddress(&IPAddress, IPString));

	// IMPORTANT(fusion): We need to insert login attempts outside the login game
	// transaction or we could end up not having it recorded at all due to rollbacks.
	// It is also the reason the whole transaction had to be pulled to its own function.
	// IMPORTANT(fusion): Don't return if we fail to insert the login attempt as the
	// result of the whole operation was already determined by the transaction function.
	LoginGameTx(Database, Query, AccountID, CharacterName,
			 Password, IPAddress, PrivateWorld, GamemasterRequired);
	if(Query->QueryStatus != QUERY_STATUS_PENDING){
		InsertLoginAttempt(Database, AccountID, IPAddress,
				(Query->QueryStatus != QUERY_STATUS_OK));
	}
}

void ProcessLogoutGame(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char Profession[30];
	char Residence[30];
	int CharacterID = (int)Request.Read32();
	int Level = Request.Read16();
	Request.ReadString(Profession, sizeof(Profession));
	Request.ReadString(Residence, sizeof(Residence));
	int LastLoginTime = (int)Request.Read32();
	int TutorActivities = Request.Read16();

	QUERY_STOP_IF(!LogoutCharacter(Database, Query->WorldID, CharacterID,
			Level, Profession, Residence, LastLoginTime, TutorActivities));
	QueryOk(Query);
}

void ProcessSetNamelock(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = (int)Request.Read32();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	Request.ReadString(IPString, sizeof(IPString));
	Request.ReadString(Reason, sizeof(Reason));
	Request.ReadString(Comment, sizeof(Comment));

	int IPAddress = 0;
	QUERY_FAIL_IF(!StringEmpty(IPString) && !ParseIPAddress(&IPAddress, IPString));

	TransactionScope Tx("SetNamelock");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int CharacterID;
	QUERY_STOP_IF(!GetCharacterID(Database, Query->WorldID, CharacterName, &CharacterID));
	QUERY_ERROR_IF(CharacterID == 0, 1);

	// TODO(fusion): Might be `NO_BANISHMENT`.
	bool Namelock;
	QUERY_STOP_IF(!GetCharacterRight(Database, CharacterID, "NAMELOCK", &Namelock));
	QUERY_ERROR_IF(Namelock, 2);

	TNamelockStatus Status;
	QUERY_STOP_IF(!GetNamelockStatus(Database, CharacterID, &Status));
	QUERY_ERROR_IF(Status.Namelocked, (Status.Approved ? 4 : 3));

	QUERY_STOP_IF(!InsertNamelock(Database, CharacterID, IPAddress, GamemasterID, Reason, Comment));
	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessBanishAccount(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = (int)Request.Read32();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	Request.ReadString(IPString, sizeof(IPString));
	Request.ReadString(Reason, sizeof(Reason));
	Request.ReadString(Comment, sizeof(Comment));
	bool FinalWarning = Request.ReadFlag();

	int IPAddress = 0;
	QUERY_FAIL_IF(!StringEmpty(IPString) && !ParseIPAddress(&IPAddress, IPString));

	TransactionScope Tx("BanishAccount");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int CharacterID;
	QUERY_STOP_IF(!GetCharacterID(Database, Query->WorldID, CharacterName, &CharacterID));
	QUERY_ERROR_IF(CharacterID == 0, 1);

	// TODO(fusion): Might be `NO_BANISHMENT`.
	bool Banishment;
	QUERY_STOP_IF(!GetCharacterRight(Database, CharacterID, "BANISHMENT", &Banishment));
	QUERY_ERROR_IF(Banishment, 2);

	TBanishmentStatus Status;
	QUERY_STOP_IF(!GetBanishmentStatus(Database, CharacterID, &Status));
	QUERY_ERROR_IF(Status.Banished, 3);

	int Days = 7;
	int BanishmentID = 0;
	CompoundBanishment(Status, &Days, &FinalWarning);
	QUERY_STOP_IF(!InsertBanishment(Database, CharacterID, IPAddress,
			GamemasterID, Reason, Comment, FinalWarning, Days * 86400,
			&BanishmentID));
	QUERY_STOP_IF(!Tx.Commit());

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->Write32((uint32)BanishmentID);
	Response->Write8(Days > 0 ? Days : 0xFF);
	Response->WriteFlag(FinalWarning);
	QueryFinishResponse(Query);
}

void ProcessSetNotation(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = (int)Request.Read32();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	Request.ReadString(IPString, sizeof(IPString));
	Request.ReadString(Reason, sizeof(Reason));
	Request.ReadString(Comment, sizeof(Comment));

	int IPAddress = 0;
	QUERY_FAIL_IF(!StringEmpty(IPString) && !ParseIPAddress(&IPAddress, IPString));

	TransactionScope Tx("SetNotation");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int CharacterID;
	QUERY_STOP_IF(!GetCharacterID(Database, Query->WorldID, CharacterName, &CharacterID));
	QUERY_ERROR_IF(CharacterID == 0, 1);

	// TODO(fusion): Might be `NO_BANISHMENT`.
	bool Notation;
	QUERY_STOP_IF(!GetCharacterRight(Database, CharacterID, "NOTATION", &Notation));
	QUERY_ERROR_IF(Notation, 2);

	int Notations = 0;
	int BanishmentID = 0;
	QUERY_STOP_IF(!GetNotationCount(Database, CharacterID, &Notations));
	if(Notations >= 5){
		int BanishmentDays = 7;
		bool FinalWarning = false;
		TBanishmentStatus Status = {};
		QUERY_STOP_IF(!GetBanishmentStatus(Database, CharacterID, &Status));
		CompoundBanishment(Status, &BanishmentDays, &FinalWarning);
		QUERY_STOP_IF(!InsertBanishment(Database, CharacterID, IPAddress,
				0, "Excessive Notations", "", FinalWarning, BanishmentDays,
				&BanishmentID));
	}

	QUERY_STOP_IF(!InsertNotation(Database, CharacterID, IPAddress, GamemasterID, Reason, Comment));
	QUERY_STOP_IF(!Tx.Commit());

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->Write32((uint32)BanishmentID);
	QueryFinishResponse(Query);
}

void ProcessReportStatement(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char CharacterName[30];
	char Reason[200];
	char Comment[200];
	int ReporterID = (int)Request.Read32();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	Request.ReadString(Reason, sizeof(Reason));
	Request.ReadString(Comment, sizeof(Comment));
	int BanishmentID = (int)Request.Read32();
	int StatementID = (int)Request.Read32();
	int NumStatements = (int)Request.Read16();

	if(StatementID == 0){
		LOG_ERR("Missing statement id");
		QueryFailed(Query);
		return;
	}

	if(NumStatements == 0){
		LOG_ERR("Missing statement context");
		QueryFailed(Query);
		return;
	}

	TStatement *ReportedStatement = NULL;
	TStatement *Statements = (TStatement*)alloca(NumStatements * sizeof(TStatement));
	for(int i = 0; i < NumStatements; i += 1){
		Statements[i].StatementID = (int)Request.Read32();
		Statements[i].Timestamp = (int)Request.Read32();
		Statements[i].CharacterID = (int)Request.Read32();
		Request.ReadString(Statements[i].Channel, sizeof(Statements[i].Channel));
		Request.ReadString(Statements[i].Text, sizeof(Statements[i].Text));

		if(Statements[i].StatementID == StatementID){
			if(ReportedStatement != NULL){
				LOG_WARN("Reported statement (%d, %d, %d) appears multiple times",
						Query->WorldID, Statements[i].Timestamp, Statements[i].StatementID);
			}
			ReportedStatement = &Statements[i];
		}
	}

	if(ReportedStatement == NULL){
		LOG_ERR("Missing reported statement");
		QueryFailed(Query);
		return;
	}

	TransactionScope Tx("ReportStatement");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int CharacterID;
	QUERY_STOP_IF(!GetCharacterID(Database, Query->WorldID, CharacterName, &CharacterID));
	QUERY_ERROR_IF(CharacterID == 0, 1);

	if(ReportedStatement->CharacterID != CharacterID){
		LOG_ERR("Reported statement character mismatch");
		QueryFailed(Query);
		return;
	}

	bool IsReported;
	QUERY_STOP_IF(!IsStatementReported(Database, Query->WorldID, ReportedStatement, &IsReported));
	QUERY_ERROR_IF(IsReported, 2);

	QUERY_STOP_IF(!InsertStatements(Database, Query->WorldID, NumStatements, Statements));
	QUERY_STOP_IF(!InsertReportedStatement(Database, Query->WorldID, ReportedStatement,
											BanishmentID, ReporterID, Reason, Comment));
	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessBanishIpAddress(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = (int)Request.Read16();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	Request.ReadString(IPString, sizeof(IPString));
	Request.ReadString(Reason, sizeof(Reason));
	Request.ReadString(Comment, sizeof(Comment));

	int IPAddress;
	QUERY_FAIL_IF(!ParseIPAddress(&IPAddress, IPString));

	TransactionScope Tx("BanishIP");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int CharacterID;
	QUERY_STOP_IF(!GetCharacterID(Database, Query->WorldID, CharacterName, &CharacterID));
	QUERY_ERROR_IF(CharacterID == 0, 1);

	// TODO(fusion): Might be `NO_BANISHMENT`.
	bool IPBanishment;
	QUERY_STOP_IF(!GetCharacterRight(Database, CharacterID, "IP_BANISHMENT", &IPBanishment));
	QUERY_ERROR_IF(IPBanishment, 2);

	// IMPORTANT(fusion): It is not a good idea to ban IP addresses, specially V4,
	// as they may be dynamically assigned or represent the address of a public ISP
	// router that manages multiple clients.
	int BanishmentDays = 3;
	QUERY_STOP_IF(!InsertIPBanishment(Database, CharacterID, IPAddress,
			GamemasterID, Reason, Comment, BanishmentDays * 86400));
	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessLogCharacterDeath(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	char Remark[30];
	int CharacterID = (int)Request.Read32();
	int Level = (int)Request.Read16();
	int OffenderID = (int)Request.Read32();
	Request.ReadString(Remark, sizeof(Remark));
	bool Unjustified = Request.ReadFlag();
	int Timestamp = (int)Request.Read32();
	QUERY_STOP_IF(!InsertCharacterDeath(Database, Query->WorldID, CharacterID,
							Level, OffenderID, Remark, Unjustified, Timestamp));
	QueryOk(Query);

}

void ProcessAddBuddy(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int AccountID = (int)Request.Read32();
	int BuddyID = (int)Request.Read32();
	QUERY_STOP_IF(!InsertBuddy(Database, Query->WorldID, AccountID, BuddyID));
	QueryOk(Query);
}

void ProcessRemoveBuddy(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int AccountID = (int)Request.Read32();
	int BuddyID = (int)Request.Read32();
	QUERY_STOP_IF(!DeleteBuddy(Database, Query->WorldID, AccountID, BuddyID));
	QueryOk(Query);
}

void ProcessDecrementIsOnline(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int CharacterID = (int)Request.Read32();
	QUERY_STOP_IF(!DecrementIsOnline(Database, Query->WorldID, CharacterID));
	QueryOk(Query);
}

void ProcessFinishAuctions(TDatabase *Database, TQuery *Query){
	DynamicArray<THouseAuction> Auctions;
	QUERY_STOP_IF(!FinishHouseAuctions(Database, Query->WorldID, &Auctions));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumAuctions = std::min<int>(Auctions.Length(), UINT16_MAX);
	Response->Write16((uint16)NumAuctions);
	for(int i = 0; i < NumAuctions; i += 1){
		Response->Write16((uint16)Auctions[i].HouseID);
		Response->Write32((uint32)Auctions[i].BidderID);
		Response->WriteString(Auctions[i].BidderName);
		Response->Write32((uint32)Auctions[i].BidAmount);
	}
	QueryFinishResponse(Query);
}

void ProcessTransferHouses(TDatabase *Database, TQuery *Query){
	DynamicArray<THouseTransfer> Transfers;
	QUERY_STOP_IF(!FinishHouseTransfers(Database, Query->WorldID, &Transfers));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumTransfers = std::min<int>(Transfers.Length(), UINT16_MAX);
	Response->Write16((uint16)NumTransfers);
	for(int i = 0; i < NumTransfers; i += 1){
		Response->Write16((uint16)Transfers[i].HouseID);
		Response->Write32((uint32)Transfers[i].NewOwnerID);
		Response->WriteString(Transfers[i].NewOwnerName);
		Response->Write32((uint32)Transfers[i].Price);
	}
	QueryFinishResponse(Query);
}

void ProcessEvictFreeAccounts(TDatabase *Database, TQuery *Query){
	DynamicArray<THouseEviction> Evictions;
	QUERY_STOP_IF(!GetFreeAccountEvictions(Database, Query->WorldID, &Evictions));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumEvictions = std::min<int>(Evictions.Length(), UINT16_MAX);
	Response->Write16((uint16)NumEvictions);
	for(int i = 0; i < NumEvictions; i += 1){
		Response->Write16((uint16)Evictions[i].HouseID);
		Response->Write32((uint32)Evictions[i].OwnerID);
	}
	QueryFinishResponse(Query);
}

void ProcessEvictDeletedCharacters(TDatabase *Database, TQuery *Query){
	DynamicArray<THouseEviction> Evictions;
	QUERY_STOP_IF(!GetDeletedCharacterEvictions(Database, Query->WorldID, &Evictions));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumEvictions = std::min<int>(Evictions.Length(), UINT16_MAX);
	Response->Write16((uint16)NumEvictions);
	for(int i = 0; i < NumEvictions; i += 1){
		Response->Write16((uint16)Evictions[i].HouseID);
	}
	QueryFinishResponse(Query);
}

void ProcessEvictExGuildleaders(TDatabase *Database, TQuery *Query){
	// NOTE(fusion): This is a bit different from the other eviction functions.
	// The server doesn't maintain guild information for characters so it will
	// send a list of guild houses with their owners and we're supposed to check
	// whether the owner is still a guild leader. I don't think we should check
	// any other information as the server is authoritative on house information.
	DynamicArray<int> Evictions;
	TReadBuffer Request = Query->Request;
	int NumGuildHouses = (int)Request.Read16();
	for(int i = 0; i < NumGuildHouses; i += 1){
		int HouseID = (int)Request.Read16();
		int OwnerID = (int)Request.Read32();

		bool IsGuildLeader;
		QUERY_STOP_IF(!GetGuildLeaderStatus(Database, Query->WorldID, OwnerID, &IsGuildLeader));
		if(IsGuildLeader){
			Evictions.Push(HouseID);
		}
	}

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumEvictions = std::min<int>(Evictions.Length(), UINT16_MAX);
	Response->Write16((uint16)NumEvictions);
	for(int i = 0; i < NumEvictions; i += 1){
		Response->Write16((uint16)Evictions[i]);
	}
	QueryFinishResponse(Query);
}

void ProcessInsertHouseOwner(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int HouseID = (int)Request.Read16();
	int OwnerID = (int)Request.Read32();
	int PaidUntil = (int)Request.Read32();
	QUERY_STOP_IF(!InsertHouseOwner(Database, Query->WorldID, HouseID, OwnerID, PaidUntil));
	QueryOk(Query);
}

void ProcessUpdateHouseOwner(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int HouseID = (int)Request.Read16();
	int OwnerID = (int)Request.Read32();
	int PaidUntil = (int)Request.Read32();
	QUERY_STOP_IF(!UpdateHouseOwner(Database, Query->WorldID, HouseID, OwnerID, PaidUntil));
	QueryOk(Query);
}

void ProcessDeleteHouseOwner(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int HouseID = (int)Request.Read16();
	QUERY_STOP_IF(!DeleteHouseOwner(Database, Query->WorldID, HouseID));
	QueryOk(Query);
}

void ProcessGetHouseOwners(TDatabase *Database, TQuery *Query){
	DynamicArray<THouseOwner> Owners;
	QUERY_STOP_IF(!GetHouseOwners(Database, Query->WorldID, &Owners));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumOwners = std::min<int>(Owners.Length(), UINT16_MAX);
	Response->Write16((uint16)NumOwners);
	for(int i = 0; i < NumOwners; i += 1){
		Response->Write16((uint16)Owners[i].HouseID);
		Response->Write32((uint32)Owners[i].OwnerID);
		Response->WriteString(Owners[i].OwnerName);
		Response->Write32((uint32)Owners[i].PaidUntil);
	}
	QueryFinishResponse(Query);
}

void ProcessGetAuctions(TDatabase *Database, TQuery *Query){
	DynamicArray<int> Auctions;
	QUERY_STOP_IF(!GetHouseAuctions(Database, Query->WorldID, &Auctions));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumAuctions = std::min<int>(Auctions.Length(), UINT16_MAX);
	Response->Write16((uint16)NumAuctions);
	for(int i = 0; i < NumAuctions; i += 1){
		Response->Write16((uint16)Auctions[i]);
	}
	QueryFinishResponse(Query);
}

void ProcessStartAuction(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int HouseID = (int)Request.Read16();
	QUERY_STOP_IF(!StartHouseAuction(Database, Query->WorldID, HouseID));
	QueryOk(Query);
}

void ProcessInsertHouses(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	TransactionScope Tx("InsertHouses");
	QUERY_STOP_IF(!Tx.Begin(Database));
	QUERY_STOP_IF(!DeleteHouses(Database, Query->WorldID));

	int NumHouses = (int)Request.Read16();
	if(NumHouses > 0){
		THouse *Houses = (THouse*)alloca(NumHouses * sizeof(THouse));
		for(int i = 0; i < NumHouses; i += 1){
			Houses[i].HouseID = (int)Request.Read16();
			Request.ReadString(Houses[i].Name, sizeof(Houses[i].Name));
			Houses[i].Rent = (int)Request.Read32();
			Request.ReadString(Houses[i].Description, sizeof(Houses[i].Description));
			Houses[i].Size = (int)Request.Read16();
			Houses[i].PositionX = (int)Request.Read16();
			Houses[i].PositionY = (int)Request.Read16();
			Houses[i].PositionZ = (int)Request.Read8();
			Request.ReadString(Houses[i].Town, sizeof(Houses[i].Town));
			Houses[i].GuildHouse = Request.ReadFlag();
		}

		QUERY_STOP_IF(!InsertHouses(Database, Query->WorldID, NumHouses, Houses));
	}

	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessClearIsOnline(TDatabase *Database, TQuery *Query){
	int NumAffectedCharacters;
	QUERY_STOP_IF(!ClearIsOnline(Database, Query->WorldID, &NumAffectedCharacters));
	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->Write16((uint16)NumAffectedCharacters);
	QueryFinishResponse(Query);
}

void ProcessCreatePlayerlist(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	TransactionScope Tx("OnlineList");
	QUERY_STOP_IF(!Tx.Begin(Database));
	QUERY_STOP_IF(!DeleteOnlineCharacters(Database, Query->WorldID));

	// TODO(fusion): I think `NumCharacters` may be used to signal that the
	// server is going OFFLINE, in which case we'd have to add an `Online`
	// column to `Worlds` and update it here.

	bool NewRecord = false;
	int NumCharacters = (int)Request.Read16();
	if(NumCharacters != 0xFFFF && NumCharacters > 0){
		TOnlineCharacter *Characters = (TOnlineCharacter*)alloca(NumCharacters * sizeof(TOnlineCharacter));
		for(int i = 0; i < NumCharacters; i += 1){
			Request.ReadString(Characters[i].Name, sizeof(Characters[i].Name));
			Characters[i].Level = (int)Request.Read16();
			Request.ReadString(Characters[i].Profession, sizeof(Characters[i].Profession));
		}

		QUERY_STOP_IF(!InsertOnlineCharacters(Database, Query->WorldID, NumCharacters, Characters));
		QUERY_STOP_IF(!CheckOnlineRecord(Database, Query->WorldID, NumCharacters, &NewRecord));
	}

	QUERY_STOP_IF(!Tx.Commit());

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->WriteFlag(NewRecord);
	QueryFinishResponse(Query);
}

void ProcessLogKilledCreatures(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int NumStats = (int)Request.Read16();
	TKillStatistics *Stats = (TKillStatistics*)alloca(NumStats * sizeof(TKillStatistics));
	for(int i = 0; i < NumStats; i += 1){
		Request.ReadString(Stats[i].RaceName, sizeof(Stats[i].RaceName));
		Stats[i].PlayersKilled = (int)Request.Read32();
		Stats[i].TimesKilled = (int)Request.Read32();
	}

	if(NumStats > 0){
		TransactionScope Tx("LogKilledCreatures");
		QUERY_STOP_IF(!Tx.Begin(Database));
		QUERY_STOP_IF(!MergeKillStatistics(Database, Query->WorldID, NumStats, Stats));
		QUERY_STOP_IF(!Tx.Commit());
	}

	QueryOk(Query);
}

void ProcessLoadPlayers(TDatabase *Database, TQuery *Query){
	// IMPORTANT(fusion): The server expect 10K entries at most. It is probably
	// some shared hard coded constant.
	int NumEntries;
	TCharacterIndexEntry Entries[10000];
	TReadBuffer Request = Query->Request;
	int MinimumCharacterID = (int)Request.Read32();
	QUERY_STOP_IF(!GetCharacterIndexEntries(Database, Query->WorldID,
			MinimumCharacterID, NARRAY(Entries), &NumEntries, Entries));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->Write32((uint32)NumEntries);
	for(int i = 0; i < NumEntries; i += 1){
		Response->WriteString(Entries[i].Name);
		Response->Write32((uint32)Entries[i].CharacterID);
	}
	QueryFinishResponse(Query);
}

void ProcessExcludeFromAuctions(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	TransactionScope Tx("ExcludeFromAuctions");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int CharacterID = (int)Request.Read32();
	bool Banish = Request.ReadFlag();
	int ExclusionDays = 7;
	int BanishmentID = 0;
	if(Banish){
		int BanishmentDays = 7;
		bool FinalWarning = false;
		TBanishmentStatus Status;
		QUERY_STOP_IF(!GetBanishmentStatus(Database, CharacterID, &Status));
		CompoundBanishment(Status, &BanishmentDays, &FinalWarning);
		QUERY_STOP_IF(!InsertBanishment(Database, CharacterID, 0,
				0, "Spoiling Auction", "", FinalWarning, BanishmentDays * 86400,
				&BanishmentID));
	}

	QUERY_STOP_IF(!ExcludeFromAuctions(Database, Query->WorldID,
			CharacterID, ExclusionDays * 86400, BanishmentID));
	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessCancelHouseTransfer(TDatabase *Database, TQuery *Query){
	// TODO(fusion): Not sure what this is used for. Maybe house transfer rows
	// are kept permanently and this query is used to delete/flag it, in case
	// the it didn't complete. We might need to refine `FinishHouseTransfers`.
	//int HouseID = Buffer->Read16();
	QueryOk(Query);
}

void ProcessLoadWorldConfig(TDatabase *Database, TQuery *Query){
	TWorldConfig WorldConfig = {};
	QUERY_STOP_IF(!GetWorldConfig(Database, Query->WorldID, &WorldConfig));
	QUERY_FAIL_IF(WorldConfig.WorldID == 0);

	int IPAddress;
	QUERY_FAIL_IF(!ResolveHostName(WorldConfig.HostName, &IPAddress));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->Write8((uint8)WorldConfig.Type);
	Response->Write8((uint8)WorldConfig.RebootTime);
	Response->Write32BE((uint32)IPAddress);
	Response->Write16((uint16)WorldConfig.Port);
	Response->Write16((uint16)WorldConfig.MaxPlayers);
	Response->Write16((uint16)WorldConfig.PremiumPlayerBuffer);
	Response->Write16((uint16)WorldConfig.MaxNewbies);
	Response->Write16((uint16)WorldConfig.PremiumNewbieBuffer);
	QueryFinishResponse(Query);
}

void ProcessCreateAccount(TDatabase *Database, TQuery *Query){
	// TODO(fusion): We'd ideally want to automatically generate an account number
	// and return it in case of success but that would also require a more robust
	// website infrastructure with verification e-mails, etc...
	char Email[100];
	char Password[30];
	TReadBuffer Request = Query->Request;
	int AccountID = (int)Request.Read32();
	Request.ReadString(Email, sizeof(Email));
	Request.ReadString(Password, sizeof(Password));

	// NOTE(fusion): Inputs should be checked before hand.
	QUERY_FAIL_IF(AccountID <= 0);
	QUERY_FAIL_IF(StringEmpty(Email));
	QUERY_FAIL_IF(StringEmpty(Password));

	uint8 Auth[64];
	QUERY_FAIL_IF(!GenerateAuth(Password, Auth, sizeof(Auth)));

	TransactionScope Tx("CreateAccount");
	QUERY_STOP_IF(!Tx.Begin(Database));

	bool AccountExists;
	QUERY_STOP_IF(!AccountNumberExists(Database, AccountID, &AccountExists));
	QUERY_ERROR_IF(AccountExists, 1);
	QUERY_STOP_IF(!AccountEmailExists(Database, Email, &AccountExists));
	QUERY_ERROR_IF(AccountExists, 2);

	QUERY_STOP_IF(!CreateAccount(Database, AccountID, Email, Auth, sizeof(Auth)));
	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessCreateCharacter(TDatabase *Database, TQuery *Query){
	char WorldName[30];
	char CharacterName[30];
	TReadBuffer Request = Query->Request;
	Request.ReadString(WorldName, sizeof(WorldName));
	int AccountID = (int)Request.Read32();
	Request.ReadString(CharacterName, sizeof(CharacterName));
	int Sex = (int)Request.Read8();

	// NOTE(fusion): Inputs should be checked before hand.
	QUERY_FAIL_IF(AccountID <= 0);
	QUERY_FAIL_IF(Sex != 1 && Sex != 2);
	QUERY_FAIL_IF(StringEmpty(WorldName));
	QUERY_FAIL_IF(StringEmpty(CharacterName));

	TransactionScope Tx("CreateCharacter");
	QUERY_STOP_IF(!Tx.Begin(Database));

	int WorldID;
	QUERY_STOP_IF(!GetWorldID(Database, WorldName, &WorldID));
	QUERY_ERROR_IF(WorldID == 0, 1);

	bool AccountExists;
	QUERY_STOP_IF(!AccountNumberExists(Database, AccountID, &AccountExists));
	QUERY_ERROR_IF(!AccountExists, 2);

	bool CharacterExists;
	QUERY_STOP_IF(!CharacterNameExists(Database, CharacterName, &CharacterExists));
	QUERY_ERROR_IF(CharacterExists, 3);

	QUERY_STOP_IF(!CreateCharacter(Database, WorldID, AccountID, CharacterName, Sex));
	QUERY_STOP_IF(!Tx.Commit());
	QueryOk(Query);
}

void ProcessGetAccountSummary(TDatabase *Database, TQuery *Query){
	TReadBuffer Request = Query->Request;
	int AccountID = (int)Request.Read32();

	QUERY_FAIL_IF(AccountID <= 0);

	TAccount Account;
	QUERY_STOP_IF(!GetAccountData(Database, AccountID, &Account));
	QUERY_FAIL_IF(Account.AccountID == 0 || Account.AccountID != AccountID);

	DynamicArray<TCharacterSummary> Characters;
	QUERY_STOP_IF(!GetCharacterSummaries(Database, AccountID, &Characters));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->WriteString(Account.Email);
	Response->Write16((uint16)Account.PremiumDays);
	Response->Write16((uint16)Account.PendingPremiumDays);
	Response->WriteFlag(Account.Deleted);
	int NumCharacters = std::min<int>(Characters.Length(), UINT8_MAX);
	Response->Write8((uint8)NumCharacters);
	for(int i = 0; i < NumCharacters; i += 1){
		Response->WriteString(Characters[i].Name);
		Response->WriteString(Characters[i].World);
		Response->Write16((uint16)Characters[i].Level);
		Response->WriteString(Characters[i].Profession);
		Response->WriteFlag(Characters[i].Online);
		Response->WriteFlag(Characters[i].Deleted);
	}
	QueryFinishResponse(Query);
}

void ProcessGetCharacterProfile(TDatabase *Database, TQuery *Query){
	char CharacterName[30];
	TReadBuffer Request = Query->Request;
	Request.ReadString(CharacterName, sizeof(CharacterName));

	QUERY_FAIL_IF(StringEmpty(CharacterName));

	TCharacterProfile Character;
	QUERY_STOP_IF(!GetCharacterProfile(Database, CharacterName, &Character));
	QUERY_ERROR_IF(!StringEqCI(Character.Name, CharacterName), 1);

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	Response->WriteString(Character.Name);
	Response->WriteString(Character.World);
	Response->Write8((uint8)Character.Sex);
	Response->WriteString(Character.Guild);
	Response->WriteString(Character.Rank);
	Response->WriteString(Character.Title);
	Response->Write16((uint16)Character.Level);
	Response->WriteString(Character.Profession);
	Response->WriteString(Character.Residence);
	Response->Write32((uint32)Character.LastLogin);
	Response->Write16((uint16)Character.PremiumDays);
	Response->WriteFlag(Character.Online);
	Response->WriteFlag(Character.Deleted);
	QueryFinishResponse(Query);
}

void ProcessGetWorlds(TDatabase *Database, TQuery *Query){
	DynamicArray<TWorld> Worlds;
	QUERY_STOP_IF(!GetWorlds(Database, &Worlds));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumWorlds = std::min<int>(Worlds.Length(), UINT8_MAX);
	Response->Write8((uint8)NumWorlds);
	for(int i = 0; i < NumWorlds; i += 1){
		Response->WriteString(Worlds[i].Name);
		Response->Write8((uint8)Worlds[i].Type);
		Response->Write16((uint16)Worlds[i].NumPlayers);
		Response->Write16((uint16)Worlds[i].MaxPlayers);
		Response->Write16((uint16)Worlds[i].OnlineRecord);
		Response->Write32((uint32)Worlds[i].OnlineRecordTimestamp);
	}
	QueryFinishResponse(Query);
}

void ProcessGetOnlineCharacters(TDatabase *Database, TQuery *Query){
	char WorldName[30];
	TReadBuffer Request = Query->Request;
	Request.ReadString(WorldName, sizeof(WorldName));

	int WorldID;
	QUERY_STOP_IF(!GetWorldID(Database, WorldName, &WorldID));
	QUERY_FAIL_IF(WorldID == 0);

	DynamicArray<TOnlineCharacter> Characters;
	QUERY_STOP_IF(!GetOnlineCharacters(Database, WorldID, &Characters));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumCharacters = std::min<int>(Characters.Length(), UINT16_MAX);
	Response->Write16((uint16)NumCharacters);
	for(int i = 0; i < NumCharacters; i += 1){
		Response->WriteString(Characters[i].Name);
		Response->Write16((uint16)Characters[i].Level);
		Response->WriteString(Characters[i].Profession);
	}
	QueryFinishResponse(Query);
}

void ProcessGetKillStatistics(TDatabase *Database, TQuery *Query){
	char WorldName[30];
	TReadBuffer Request = Query->Request;
	Request.ReadString(WorldName, sizeof(WorldName));

	int WorldID;
	QUERY_STOP_IF(!GetWorldID(Database, WorldName, &WorldID));
	QUERY_FAIL_IF(WorldID == 0);

	DynamicArray<TKillStatistics> Stats;
	QUERY_STOP_IF(!GetKillStatistics(Database, WorldID, &Stats));

	TWriteBuffer *Response = QueryBeginResponse(Query, QUERY_STATUS_OK);
	int NumStats = std::min<int>(Stats.Length(), UINT16_MAX);
	Response->Write16((uint16)NumStats);
	for(int i = 0; i < NumStats; i += 1){
		Response->WriteString(Stats[i].RaceName);
		Response->Write32((uint32)Stats[i].PlayersKilled);
		Response->Write32((uint32)Stats[i].TimesKilled);
	}
	QueryFinishResponse(Query);
}

