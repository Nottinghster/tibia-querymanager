#include "querymanager.hh"

#include <pthread.h>

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
	AtomicInt Running;
	pthread_t Thread;
};

static int g_NumWorkers;
static TWorker *g_Workers;
static TQueryQueue *g_QueryQueue;

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

TQuery *QueryDequeue(AtomicInt *Running){
	ASSERT(g_QueryQueue != NULL);
	ASSERT(Running != NULL);

	TQuery *Query = NULL;
	pthread_mutex_lock(&g_QueryQueue->Mutex);
	uint32 NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
	while(NumQueries == 0 && AtomicLoad(Running)){
		pthread_cond_wait(&g_QueryQueue->WorkAvailable, &g_QueryQueue->Mutex);
		NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
	}

	if(NumQueries > 0 && AtomicLoad(Running)){
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
	if(!AtomicLoad(&Worker->Running)){
		LOG_WARN("%d: Exiting on entry...", Worker->WorkerID);
		return NULL;
	}

	TDatabase *Database = OpenDatabase();
	if(Database == NULL){
		LOG_ERR("%d: Failed to connect to database", Worker->WorkerID);
		return NULL;
	}

	LOG("%d: Running...", Worker->WorkerID);
	while(TQuery *Query = QueryDequeue(&Worker->Running)){
		Query->QueryType = Query->Request.Read8();
		//bool (*ProcessQuery)(TDatabase*, TQuery*) = NULL;
		switch(Query->QueryType){
			case QUERY_INTERNAL_RESOLVE_WORLD:		ProcessInternalResolveWorld(Database, Query); break;
			case QUERY_CHECK_ACCOUNT_PASSWORD:		ProcessCheckAccountPassword(Database, Query); break;
			case QUERY_LOGIN_ACCOUNT:				ProcessLoginAccount(Database, Query); break;
			case QUERY_LOGIN_ADMIN:					ProcessLoginAdmin(Database, Query); break;
			case QUERY_LOGIN_GAME:					ProcessLoginGame(Database, Query); break;
			case QUERY_LOGOUT_GAME:					ProcessLogoutGame(Database, Query); break;
			case QUERY_SET_NAMELOCK:				ProcessSetNamelock(Database, Query); break;
			case QUERY_BANISH_ACCOUNT:				ProcessBanishAccount(Database, Query); break;
			case QUERY_SET_NOTATION:				ProcessSetNotation(Database, Query); break;
			case QUERY_REPORT_STATEMENT:			ProcessReportStatement(Database, Query); break;
			case QUERY_BANISH_IP_ADDRESS:			ProcessBanishIpAddress(Database, Query); break;
			case QUERY_LOG_CHARACTER_DEATH:			ProcessLogCharacterDeath(Database, Query); break;
			case QUERY_ADD_BUDDY:					ProcessAddBuddy(Database, Query); break;
			case QUERY_REMOVE_BUDDY:				ProcessRemoveBuddy(Database, Query); break;
			case QUERY_DECREMENT_IS_ONLINE:			ProcessDecrementIsOnline(Database, Query); break;
			case QUERY_FINISH_AUCTIONS:				ProcessFinishAuctions(Database, Query); break;
			case QUERY_TRANSFER_HOUSES:				ProcessTransferHouses(Database, Query); break;
			case QUERY_EVICT_FREE_ACCOUNTS:			ProcessEvictFreeAccounts(Database, Query); break;
			case QUERY_EVICT_DELETED_CHARACTERS:	ProcessEvictDeletedCharacters(Database, Query); break;
			case QUERY_EVICT_EX_GUILDLEADERS:		ProcessEvictExGuildleaders(Database, Query); break;
			case QUERY_INSERT_HOUSE_OWNER:			ProcessInsertHouseOwner(Database, Query); break;
			case QUERY_UPDATE_HOUSE_OWNER:			ProcessUpdateHouseOwner(Database, Query); break;
			case QUERY_DELETE_HOUSE_OWNER:			ProcessDeleteHouseOwner(Database, Query); break;
			case QUERY_GET_HOUSE_OWNERS:			ProcessGetHouseOwners(Database, Query); break;
			case QUERY_GET_AUCTIONS:				ProcessGetAuctions(Database, Query); break;
			case QUERY_START_AUCTION:				ProcessStartAuction(Database, Query); break;
			case QUERY_INSERT_HOUSES:				ProcessInsertHouses(Database, Query); break;
			case QUERY_CLEAR_IS_ONLINE:				ProcessClearIsOnline(Database, Query); break;
			case QUERY_CREATE_PLAYERLIST:			ProcessCreatePlayerlist(Database, Query); break;
			case QUERY_LOG_KILLED_CREATURES:		ProcessLogKilledCreatures(Database, Query); break;
			case QUERY_LOAD_PLAYERS:				ProcessLoadPlayers(Database, Query); break;
			case QUERY_EXCLUDE_FROM_AUCTIONS:		ProcessExcludeFromAuctions(Database, Query); break;
			case QUERY_CANCEL_HOUSE_TRANSFER:		ProcessCancelHouseTransfer(Database, Query); break;
			case QUERY_LOAD_WORLD_CONFIG:			ProcessLoadWorldConfig(Database, Query); break;
			case QUERY_CREATE_ACCOUNT:				ProcessCreateAccount(Database, Query); break;
			case QUERY_CREATE_CHARACTER:			ProcessCreateCharacter(Database, Query); break;
			case QUERY_GET_ACCOUNT_SUMMARY:			ProcessGetAccountSummary(Database, Query); break;
			case QUERY_GET_CHARACTER_PROFILE:		ProcessGetCharacterProfile(Database, Query); break;
			case QUERY_GET_WORLDS:					ProcessGetWorlds(Database, Query); break;
			case QUERY_GET_ONLINE_CHARACTERS:		ProcessGetOnlineCharacters(Database, Query); break;
			case QUERY_GET_KILL_STATISTICS:			ProcessGetKillStatistics(Database, Query); break;
			default:{
				QueryFailed(Query);
				break;
			}
		}

		QueryDone(Query);
	}

	LOG("%d: Exiting...", Worker->WorkerID);
	CloseDatabase(Database);
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

	g_NumWorkers = 1;
	g_Workers = (TWorker*)calloc(g_NumWorkers, sizeof(TWorker));
	for(int i = 0; i < g_NumWorkers; i += 1){
		TWorker *Worker = &g_Workers[i];
		Worker->WorkerID = i;
		AtomicStore(&Worker->Running, 1);
		int ErrorCode = pthread_create(&Worker->Thread, NULL, WorkerThread, Worker);
		if(ErrorCode != 0){
			LOG_ERR("Failed to spawn worker thread %d: (%d) %s",
					i, ErrorCode, strerrordesc_np(ErrorCode));
			Worker->WorkerID = -1;
			return false;
		}
	}

	return true;
}

void ExitQuery(void){
	if(g_Workers != NULL){
		for(int i = 0; i < g_NumWorkers; i += 1){
			AtomicStore(&g_Workers[i].Running, 0);
		}

		if(g_QueryQueue != NULL){
			pthread_cond_broadcast(&g_QueryQueue->WorkAvailable);
		}

		for(int i = 0; i < g_NumWorkers; i += 1){
			// IMPORTANT(fusion): The `WorkerID` will be set to -1 if we fail
			// to spawn its thread.
			if(g_Workers[i].WorkerID != -1){
				pthread_join(g_Workers[i].Thread, NULL);
			}
		}

		g_NumWorkers = 0;
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

// Query Processing
//==============================================================================
void ProcessInternalResolveWorld(TDatabase *Database, TQuery *Query);
void ProcessCheckAccountPassword(TDatabase *Database, TQuery *Query);
void ProcessLoginAccount(TDatabase *Database, TQuery *Query);
void ProcessLoginAdmin(TDatabase *Database, TQuery *Query);
void ProcessLoginGame(TDatabase *Database, TQuery *Query);
void ProcessLogoutGame(TDatabase *Database, TQuery *Query);
void ProcessSetNamelock(TDatabase *Database, TQuery *Query);
void ProcessBanishAccount(TDatabase *Database, TQuery *Query);
void ProcessSetNotation(TDatabase *Database, TQuery *Query);
void ProcessReportStatement(TDatabase *Database, TQuery *Query);
void ProcessBanishIpAddress(TDatabase *Database, TQuery *Query);
void ProcessLogCharacterDeath(TDatabase *Database, TQuery *Query);
void ProcessAddBuddy(TDatabase *Database, TQuery *Query);
void ProcessRemoveBuddy(TDatabase *Database, TQuery *Query);
void ProcessDecrementIsOnline(TDatabase *Database, TQuery *Query);
void ProcessFinishAuctions(TDatabase *Database, TQuery *Query);
void ProcessTransferHouses(TDatabase *Database, TQuery *Query);
void ProcessEvictFreeAccounts(TDatabase *Database, TQuery *Query);
void ProcessEvictDeletedCharacters(TDatabase *Database, TQuery *Query);
void ProcessEvictExGuildleaders(TDatabase *Database, TQuery *Query);
void ProcessInsertHouseOwner(TDatabase *Database, TQuery *Query);
void ProcessUpdateHouseOwner(TDatabase *Database, TQuery *Query);
void ProcessDeleteHouseOwner(TDatabase *Database, TQuery *Query);
void ProcessGetHouseOwners(TDatabase *Database, TQuery *Query);
void ProcessGetAuctions(TDatabase *Database, TQuery *Query);
void ProcessStartAuction(TDatabase *Database, TQuery *Query);
void ProcessInsertHouses(TDatabase *Database, TQuery *Query);
void ProcessClearIsOnline(TDatabase *Database, TQuery *Query);
void ProcessCreatePlayerlist(TDatabase *Database, TQuery *Query);
void ProcessLogKilledCreatures(TDatabase *Database, TQuery *Query);
void ProcessLoadPlayers(TDatabase *Database, TQuery *Query);
void ProcessExcludeFromAuctions(TDatabase *Database, TQuery *Query);
void ProcessCancelHouseTransfer(TDatabase *Database, TQuery *Query);
void ProcessLoadWorldConfig(TDatabase *Database, TQuery *Query);
void ProcessCreateAccount(TDatabase *Database, TQuery *Query);
void ProcessCreateCharacter(TDatabase *Database, TQuery *Query);
void ProcessGetAccountSummary(TDatabase *Database, TQuery *Query);
void ProcessGetCharacterProfile(TDatabase *Database, TQuery *Query);
void ProcessGetWorlds(TDatabase *Database, TQuery *Query);
void ProcessGetOnlineCharacters(TDatabase *Database, TQuery *Query);
void ProcessGetKillStatistics(TDatabase *Database, TQuery *Query);

// TODO(fusion): These are the old query processing functions. The new ones will
// be very similar, except that we want a way to tell whether there was a database
// failure, such as a connection reset, so we can automatically retry them up to
// a certain amount of times before failing.
//		bool ProcessX(Database, Query){
//			//   Execute database queries, returning false on failure. The
//			// response should only be written at the very end when we know
//			// the query SUCCEEDED, to keep the request data intact if a retry
//			// is needed (because they share the query buffer).
//		}
//
//		...
//
//		void DatabaseCheckpoint(Database){
//			//   Check whether there was a database error or if the database is
//			// still connected and make sure it is ready for processing a query.
//		}
//
//		...
//
//		int NumAttempts = MaxAttempts;
//		DatabaseCheckpoint(Database);
//		while(!ProcessX(Database, Query)){
//			if(Attempts <= 0){
//				QueryFailed(Query);
//				break;
//			}
//			DatabaseCheckpoint(Database);
//			NumAttempts -= 1;
//		}


#if 0
// Connection Queries
//==============================================================================
void CompoundBanishment(TBanishmentStatus Status, int *Days, bool *FinalWarning){
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


void ProcessLoginQuery(TConnection *Connection, TReadBuffer *Buffer){
	char Password[30];
	char LoginData[30];
	int ApplicationType = Buffer->Read8();
	Buffer->ReadString(Password, sizeof(Password));
	if(ApplicationType == APPLICATION_TYPE_GAME){
		Buffer->ReadString(LoginData, sizeof(LoginData));
	}

	// TODO(fusion): Probably just disconnect on failed login attempt? Implement
	// write then disconnect?
	if(!StringEq(g_QueryManagerPassword, Password)){
		LOG_WARN("Invalid login attempt from %s", Connection->RemoteAddress);
		SendQueryStatusFailed(Connection);
		return;
	}

	int WorldID = 0;
	if(ApplicationType == APPLICATION_TYPE_GAME){
		WorldID = GetWorldID(LoginData);
		if(WorldID == 0){
			LOG_WARN("Rejecting connection %s from unknown game server \"%s\"",
					Connection->RemoteAddress, LoginData);
			SendQueryStatusFailed(Connection);
			return;
		}
		LOG("Connection %s AUTHORIZED to game server \"%s\" (%d)",
				Connection->RemoteAddress, LoginData, WorldID);
	}else if(ApplicationType == APPLICATION_TYPE_LOGIN){
		LOG("Connection %s AUTHORIZED to login server", Connection->RemoteAddress);
	}else if(ApplicationType == APPLICATION_TYPE_WEB){
		LOG("Connection %s AUTHORIZED to web server", Connection->RemoteAddress);
	}else{
		LOG_WARN("Rejecting connection %s from unknown application type %d",
				Connection->RemoteAddress, ApplicationType);
		SendQueryStatusFailed(Connection);
		return;
	}

	Connection->Authorized = true;
	Connection->ApplicationType = ApplicationType;
	Connection->WorldID = WorldID;
	SendQueryStatusOk(Connection);
}

static int CheckAccountPasswordTransaction(int AccountID, const char *Password, int IPAddress){
	TransactionScope Tx("CheckAccountPassword");
	if(!Tx.Begin()){
		return -1;
	}

	TAccount Account;
	if(!GetAccountData(AccountID, &Account)){
		return -1;
	}

	if(Account.AccountID == 0){
		return 1;
	}

	if(!TestPassword(Account.Auth, sizeof(Account.Auth), Password)){
		return 2;
	}

	if(GetAccountFailedLoginAttempts(Account.AccountID, 5 * 60) > 10){
		return 3;
	}

	if(GetIPAddressFailedLoginAttempts(IPAddress, 30 * 60) > 20){
		return 4;
	}

	if(!Tx.Commit()){
		return -1;
	}

	return 0;
}

void ProcessCheckAccountPasswordQuery(TConnection *Connection, TReadBuffer *Buffer){
	char Password[30];
	char IPString[16];
	int AccountID = (int)Buffer->Read32();
	Buffer->ReadString(Password, sizeof(Password));
	Buffer->ReadString(IPString, sizeof(IPString));

	int IPAddress = 0;
	if(!ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	// NOTE(fusion): Similar to `ProcessLoginAccountQuery`.
	int Result = CheckAccountPasswordTransaction(AccountID, Password, IPAddress);
	InsertLoginAttempt(AccountID, IPAddress, (Result != 0));
	if(Result == -1){
		SendQueryStatusFailed(Connection);
	}else if(Result != 0){
		SendQueryStatusError(Connection, Result);
	}else{
		SendQueryStatusOk(Connection);
	}
}

int LoginAccountTransaction(int AccountID, const char *Password, int IPAddress,
		DynamicArray<TCharacterEndpoint> *Characters, int *PremiumDays){
	TransactionScope Tx("LoginAccount");
	if(!Tx.Begin()){
		return -1;
	}

	TAccount Account;
	if(!GetAccountData(AccountID, &Account)){
		return -1;
	}

	if(Account.AccountID == 0){
		return 1;
	}

	if(!TestPassword(Account.Auth, sizeof(Account.Auth), Password)){
		return 2;
	}

	if(GetAccountFailedLoginAttempts(Account.AccountID, 5 * 60) > 10){
		return 3;
	}

	if(GetIPAddressFailedLoginAttempts(IPAddress, 30 * 60) > 20){
		return 4;
	}

	if(IsAccountBanished(Account.AccountID)){
		return 5;
	}

	if(IsIPBanished(IPAddress)){
		return 6;
	}

	if(!GetCharacterEndpoints(Account.AccountID, Characters)){
		return -1;
	}

	if(!Tx.Commit()){
		return -1;
	}

	*PremiumDays = Account.PremiumDays + Account.PendingPremiumDays;
	return 0;
}

void ProcessLoginAccountQuery(TConnection *Connection, TReadBuffer *Buffer){
	char Password[30];
	char IPString[16];
	int AccountID = (int)Buffer->Read32();
	Buffer->ReadString(Password, sizeof(Password));
	Buffer->ReadString(IPString, sizeof(IPString));

	int IPAddress = 0;
	if(!ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	int PremiumDays = 0;
	DynamicArray<TCharacterEndpoint> Characters;
	int Result = LoginAccountTransaction(AccountID, Password,
			IPAddress, &Characters, &PremiumDays);

	// NOTE(fusion): Similar to `ProcessLoginGameQuery` except we don't modify
	// any tables inside the login transaction.
	// TODO(fusion): Maybe have different login attempt tables or types?
	InsertLoginAttempt(AccountID, IPAddress, (Result != 0));

	if(Result == -1){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(Result != 0){
		SendQueryStatusError(Connection, Result);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumCharacters = std::min<int>(Characters.Length(), UINT8_MAX);
	WriteBuffer.Write8((uint8)NumCharacters);
	for(int i = 0; i < NumCharacters; i += 1){
		WriteBuffer.WriteString(Characters[i].Name);
		WriteBuffer.WriteString(Characters[i].WorldName);
		WriteBuffer.Write32BE((uint32)Characters[i].WorldAddress);
		WriteBuffer.Write16((uint16)Characters[i].WorldPort);
	}
	WriteBuffer.Write16((uint16)PremiumDays);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessLoginAdminQuery(TConnection *Connection, TReadBuffer *Buffer){
	// TODO(fusion): I thought for a second this could be the query used with
	// the login server but it doesn't take a password or ip address for basic
	// checks. Even if it's used in combination with `CheckAccountPassword`,
	// it doesn't make sense to split what should have been a single query which
	// is what the new `LoginAccount` query does.
	SendQueryStatusFailed(Connection);
}

static int LoginGameTransaction(int WorldID, int AccountID, const char *CharacterName,
		const char *Password, int IPAddress, bool PrivateWorld, bool GamemasterRequired,
		TCharacterLoginData *Character, DynamicArray<TAccountBuddy> *Buddies,
		DynamicArray<TCharacterRight> *Rights, bool *PremiumAccountActivated){
	TransactionScope Tx("LoginGame");
	if(!Tx.Begin()){
		return -1;
	}

	if(!GetCharacterLoginData(CharacterName, Character)){
		return -1;
	}

	if(Character->CharacterID == 0){
		return 1;
	}

	if(Character->Deleted){
		return 2;
	}

	if(Character->WorldID != WorldID){
		return 3;
	}

	if(PrivateWorld){
		if(!GetWorldInvitation(WorldID, Character->CharacterID)){
			return 4;
		}
	}

	TAccount Account;
	if(!GetAccountData(AccountID, &Account)){
		return -1;
	}

	if(Account.AccountID == 0 || Account.AccountID != Character->AccountID){
		// NOTE(fusion): This is correct, there is no error code 5.
		return 15;
	}

	if(Account.Deleted){
		return 8;
	}

	if(!TestPassword(Account.Auth, sizeof(Account.Auth), Password)){
		return 6;
	}

	if(GetAccountFailedLoginAttempts(Account.AccountID, 5 * 60) > 10){
		return 7;
	}

	if(GetIPAddressFailedLoginAttempts(IPAddress, 30 * 60) > 20){
		return 9;
	}

	if(IsAccountBanished(Account.AccountID)){
		return 10;
	}

	if(IsCharacterNamelocked(Character->CharacterID)){
		return 11;
	}

	if(IsIPBanished(IPAddress)){
		return 12;
	}

	// TODO(fusion): Probably merge these into a single operation?
	if(!GetCharacterRight(Character->CharacterID, "ALLOW_MULTICLIENT")
			&& GetAccountOnlineCharacters(Account.AccountID) > 0
			&& !IsCharacterOnline(Character->CharacterID)){
		return 13;
	}

	if(GamemasterRequired){
		if(!GetCharacterRight(Character->CharacterID, "GAMEMASTER_OUTFIT")){
			return 14;
		}
	}

	if(!GetBuddies(WorldID, Account.AccountID, Buddies)){
		return -1;
	}

	if(!GetCharacterRights(Character->CharacterID, Rights)){
		return -1;
	}

	if(Account.PremiumDays == 0 && Account.PendingPremiumDays > 0){
		if(!ActivatePendingPremiumDays(Account.AccountID)){
			return -1;
		}

		Account.PremiumDays += Account.PendingPremiumDays;
		Account.PendingPremiumDays = 0;
		*PremiumAccountActivated = true;
	}

	if(Account.PremiumDays > 0){
		Rights->Push(TCharacterRight{"PREMIUM_ACCOUNT"});
	}

	if(!IncrementIsOnline(WorldID, Character->CharacterID)){
		return -1;
	}

	if(!Tx.Commit()){
		return -1;
	}

	return 0;
}

void ProcessLoginGameQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char CharacterName[30];
	char Password[30];
	char IPString[16];
	int AccountID = (int)Buffer->Read32();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	Buffer->ReadString(Password, sizeof(Password));
	Buffer->ReadString(IPString, sizeof(IPString));
	bool PrivateWorld = Buffer->ReadFlag();
	Buffer->ReadFlag(); // "PremiumAccountRequired" unused
	bool GamemasterRequired = Buffer->ReadFlag();

	int IPAddress = 0;
	if(!ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TCharacterLoginData Character;
	DynamicArray<TAccountBuddy> Buddies;
	DynamicArray<TCharacterRight> Rights;
	bool PremiumAccountActivated = false;
	int Result = LoginGameTransaction(Connection->WorldID, AccountID,
			CharacterName, Password, IPAddress, PrivateWorld,
			GamemasterRequired, &Character, &Buddies, &Rights,
			&PremiumAccountActivated);

	// IMPORTANT(fusion): We need to insert login attempts outside the login game
	// transaction or we could end up not having it recorded at all due to rollbacks.
	// It is also the reason the whole transaction had to be pulled to its own function.
	// IMPORTANT(fusion): Don't return if we fail to insert the login attempt as the
	// result of the whole operation was already determined by the transaction function.
	InsertLoginAttempt(AccountID, IPAddress, (Result != 0));

	if(Result == -1){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(Result != 0){
		SendQueryStatusError(Connection, Result);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.Write32((uint32)Character.CharacterID);
	WriteBuffer.WriteString(Character.Name);
	WriteBuffer.Write8((uint8)Character.Sex);
	WriteBuffer.WriteString(Character.Guild);
	WriteBuffer.WriteString(Character.Rank);
	WriteBuffer.WriteString(Character.Title);

	int NumBuddies = std::min<int>(Buddies.Length(), UINT8_MAX);
	WriteBuffer.Write8((uint8)NumBuddies);
	for(int i = 0; i < NumBuddies; i += 1){
		WriteBuffer.Write32((uint32)Buddies[i].CharacterID);
		WriteBuffer.WriteString(Buddies[i].Name);
	}

	int NumRights = std::min<int>(Rights.Length(), UINT8_MAX);
	WriteBuffer.Write8((uint8)NumRights);
	for(int i = 0; i < NumRights; i += 1){
		WriteBuffer.WriteString(Rights[i].Name);
	}

	WriteBuffer.WriteFlag(PremiumAccountActivated);

	SendResponse(Connection, &WriteBuffer);
}

void ProcessLogoutGameQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char Profession[30];
	char Residence[30];
	int CharacterID = (int)Buffer->Read32();
	int Level = Buffer->Read16();
	Buffer->ReadString(Profession, sizeof(Profession));
	Buffer->ReadString(Residence, sizeof(Residence));
	int LastLoginTime = (int)Buffer->Read32();
	int TutorActivities = Buffer->Read16();

	if(!LogoutCharacter(Connection->WorldID, CharacterID, Level,
			Profession, Residence, LastLoginTime, TutorActivities)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessSetNamelockQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = (int)Buffer->Read32();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	Buffer->ReadString(IPString, sizeof(IPString));
	Buffer->ReadString(Reason, sizeof(Reason));
	Buffer->ReadString(Comment, sizeof(Comment));

	int IPAddress = 0;
	if(!StringEmpty(IPString) && !ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("SetNamelock");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = GetCharacterID(Connection->WorldID, CharacterName);
	if(CharacterID == 0){
		SendQueryStatusError(Connection, 1);
		return;
	}

	// TODO(fusion): Might be `NO_BANISHMENT`.
	if(GetCharacterRight(CharacterID, "NAMELOCK")){
		SendQueryStatusError(Connection, 2);
		return;
	}

	TNamelockStatus Status = GetNamelockStatus(CharacterID);
	if(Status.Namelocked){
		SendQueryStatusError(Connection, (Status.Approved ? 4 : 3));
		return;
	}

	if(!InsertNamelock(CharacterID, IPAddress, GamemasterID, Reason, Comment)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessBanishAccountQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = (int)Buffer->Read32();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	Buffer->ReadString(IPString, sizeof(IPString));
	Buffer->ReadString(Reason, sizeof(Reason));
	Buffer->ReadString(Comment, sizeof(Comment));
	bool FinalWarning = Buffer->ReadFlag();

	int IPAddress = 0;
	if(!StringEmpty(IPString) && !ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("BanishAccount");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = GetCharacterID(Connection->WorldID, CharacterName);
	if(CharacterID == 0){
		SendQueryStatusError(Connection, 1);
		return;
	}

	// TODO(fusion): Might be `NO_BANISHMENT`.
	if(GetCharacterRight(CharacterID, "BANISHMENT")){
		SendQueryStatusError(Connection, 2);
		return;
	}

	TBanishmentStatus Status = GetBanishmentStatus(CharacterID);
	if(Status.Banished){
		SendQueryStatusError(Connection, 3);
		return;
	}

	int BanishmentID = 0;
	int Days = 7;
	CompoundBanishment(Status, &Days, &FinalWarning);
	if(!InsertBanishment(CharacterID, IPAddress, GamemasterID,
			Reason, Comment, FinalWarning, Days * 86400, &BanishmentID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.Write32((uint32)BanishmentID);
	WriteBuffer.Write8(Days > 0 ? Days : 0xFF);
	WriteBuffer.WriteFlag(FinalWarning);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessSetNotationQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = Buffer->Read32();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	Buffer->ReadString(IPString, sizeof(IPString));
	Buffer->ReadString(Reason, sizeof(Reason));
	Buffer->ReadString(Comment, sizeof(Comment));

	int IPAddress = 0;
	if(!StringEmpty(IPString) && !ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("SetNotation");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = GetCharacterID(Connection->WorldID, CharacterName);
	if(CharacterID == 0){
		SendQueryStatusError(Connection, 1);
		return;
	}

	// TODO(fusion): Might be `NO_BANISHMENT`.
	if(GetCharacterRight(CharacterID, "NOTATION")){
		SendQueryStatusError(Connection, 2);
		return;
	}

	int BanishmentID = 0;
	if(GetNotationCount(CharacterID) >= 5){
		int BanishmentDays = 7;
		bool FinalWarning = false;
		TBanishmentStatus Status = GetBanishmentStatus(CharacterID);
		CompoundBanishment(Status, &BanishmentDays, &FinalWarning);
		if(!InsertBanishment(CharacterID, IPAddress, 0, "Excessive Notations",
				"", FinalWarning, BanishmentDays, &BanishmentID)){
			SendQueryStatusFailed(Connection);
			return;
		}
	}

	if(!InsertNotation(CharacterID, IPAddress, GamemasterID, Reason, Comment)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.Write32((uint32)BanishmentID);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessReportStatementQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char CharacterName[30];
	char Reason[200];
	char Comment[200];
	int ReporterID = Buffer->Read32();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	Buffer->ReadString(Reason, sizeof(Reason));
	Buffer->ReadString(Comment, sizeof(Comment));
	int BanishmentID = Buffer->Read32();
	int StatementID = Buffer->Read32();
	int NumStatements = Buffer->Read16();

	if(StatementID == 0){
		LOG_ERR("Missing reported statement id");
		SendQueryStatusFailed(Connection);
		return;
	}

	if(NumStatements == 0){
		LOG_ERR("Missing report statements");
		SendQueryStatusFailed(Connection);
		return;
	}

	TStatement *ReportedStatement = NULL;
	TStatement *Statements = (TStatement*)alloca(NumStatements * sizeof(TStatement));
	for(int i = 0; i < NumStatements; i += 1){
		Statements[i].StatementID = (int)Buffer->Read32();
		Statements[i].Timestamp = (int)Buffer->Read32();
		Statements[i].CharacterID = (int)Buffer->Read32();
		Buffer->ReadString(Statements[i].Channel, sizeof(Statements[i].Channel));
		Buffer->ReadString(Statements[i].Text, sizeof(Statements[i].Text));

		if(Statements[i].StatementID == StatementID){
			if(ReportedStatement != NULL){
				LOG_WARN("Reported statement (%d, %d, %d) appears multiple times",
						Connection->WorldID, Statements[i].Timestamp,
						Statements[i].StatementID);
			}
			ReportedStatement = &Statements[i];
		}
	}

	if(ReportedStatement == NULL){
		LOG_ERR("Missing reported statement");
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("ReportStatement");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = GetCharacterID(Connection->WorldID, CharacterName);
	if(CharacterID == 0){
		SendQueryStatusError(Connection, 1);
		return;
	}else if(ReportedStatement->CharacterID != CharacterID){
		LOG_ERR("Reported statement character mismatch");
		SendQueryStatusFailed(Connection);
		return;
	}

	if(IsStatementReported(Connection->WorldID, ReportedStatement)){
		SendQueryStatusError(Connection, 2);
		return;
	}

	if(!InsertStatements(Connection->WorldID, NumStatements, Statements)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!InsertReportedStatement(Connection->WorldID, ReportedStatement,
			BanishmentID, ReporterID, Reason, Comment)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessBanishIPAddressQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char CharacterName[30];
	char IPString[16];
	char Reason[200];
	char Comment[200];
	int GamemasterID = Buffer->Read16();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	Buffer->ReadString(IPString, sizeof(IPString));
	Buffer->ReadString(Reason, sizeof(Reason));
	Buffer->ReadString(Comment, sizeof(Comment));

	int IPAddress = 0;
	if(!ParseIPAddress(IPString, &IPAddress)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("BanishIP");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = GetCharacterID(Connection->WorldID, CharacterName);
	if(CharacterID == 0){
		SendQueryStatusError(Connection, 1);
		return;
	}

	// TODO(fusion): Might be `NO_BANISHMENT`.
	if(GetCharacterRight(CharacterID, "IP_BANISHMENT")){
		SendQueryStatusError(Connection, 2);
		return;
	}

	// IMPORTANT(fusion): It is not a good idea to ban an IP address, specially
	// V4 addresses, as they may be dynamically assigned or represent the address
	// of a public ISP router that manages multiple clients.
	int BanishmentDays = 3;
	if(!InsertIPBanishment(CharacterID, IPAddress, GamemasterID,
			Reason, Comment, BanishmentDays * 86400)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessLogCharacterDeathQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	char Remark[30];
	int CharacterID = (int)Buffer->Read32();
	int Level = Buffer->Read16();
	int OffenderID = (int)Buffer->Read32();
	Buffer->ReadString(Remark, sizeof(Remark));
	bool Unjustified = Buffer->ReadFlag();
	int Timestamp = (int)Buffer->Read32();
	if(!InsertCharacterDeath(Connection->WorldID, CharacterID, Level,
			OffenderID, Remark, Unjustified, Timestamp)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessAddBuddyQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int AccountID = (int)Buffer->Read32();
	int BuddyID = (int)Buffer->Read32();
	if(!InsertBuddy(Connection->WorldID, AccountID, BuddyID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessRemoveBuddyQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int AccountID = (int)Buffer->Read32();
	int BuddyID = (int)Buffer->Read32();
	if(!DeleteBuddy(Connection->WorldID, AccountID, BuddyID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessDecrementIsOnlineQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = (int)Buffer->Read32();
	if(!DecrementIsOnline(Connection->WorldID, CharacterID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessFinishAuctionsQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<THouseAuction> Auctions;
	if(!FinishHouseAuctions(Connection->WorldID, &Auctions)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumAuctions = std::min<int>(Auctions.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumAuctions);
	for(int i = 0; i < NumAuctions; i += 1){
		WriteBuffer.Write16((uint16)Auctions[i].HouseID);
		WriteBuffer.Write32((uint32)Auctions[i].BidderID);
		WriteBuffer.WriteString(Auctions[i].BidderName);
		WriteBuffer.Write32((uint32)Auctions[i].BidAmount);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessTransferHousesQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<THouseTransfer> Transfers;
	if(!FinishHouseTransfers(Connection->WorldID, &Transfers)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumTransfers = std::min<int>(Transfers.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumTransfers);
	for(int i = 0; i < NumTransfers; i += 1){
		WriteBuffer.Write16((uint16)Transfers[i].HouseID);
		WriteBuffer.Write32((uint32)Transfers[i].NewOwnerID);
		WriteBuffer.WriteString(Transfers[i].NewOwnerName);
		WriteBuffer.Write32((uint32)Transfers[i].Price);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessEvictFreeAccountsQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<THouseEviction> Evictions;
	if(!GetFreeAccountEvictions(Connection->WorldID, &Evictions)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumEvictions = std::min<int>(Evictions.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumEvictions);
	for(int i = 0; i < NumEvictions; i += 1){
		WriteBuffer.Write16((uint16)Evictions[i].HouseID);
		WriteBuffer.Write32((uint32)Evictions[i].OwnerID);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessEvictDeletedCharactersQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<THouseEviction> Evictions;
	if(!GetDeletedCharacterEvictions(Connection->WorldID, &Evictions)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumEvictions = std::min<int>(Evictions.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumEvictions);
	for(int i = 0; i < NumEvictions; i += 1){
		WriteBuffer.Write16((uint16)Evictions[i].HouseID);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessEvictExGuildleadersQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	// NOTE(fusion): This is a bit different from the other eviction functions.
	// The server doesn't maintain guild information for characters so it will
	// send a list of guild houses with their owners and we're supposed to check
	// whether the owner is still a guild leader. I don't think we should check
	// any other information as the server is authoritative on house information.
	DynamicArray<int> Evictions;
	int NumGuildHouses = Buffer->Read16();
	for(int i = 0; i < NumGuildHouses; i += 1){
		int HouseID = Buffer->Read16();
		int OwnerID = (int)Buffer->Read32();
		if(!GetGuildLeaderStatus(Connection->WorldID, OwnerID)){
			Evictions.Push(HouseID);
		}
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumEvictions = std::min<int>(Evictions.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumEvictions);
	for(int i = 0; i < NumEvictions; i += 1){
		WriteBuffer.Write16((uint16)Evictions[i]);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessInsertHouseOwnerQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int HouseID = Buffer->Read16();
	int OwnerID = (int)Buffer->Read32();
	int PaidUntil = (int)Buffer->Read32();
	if(!InsertHouseOwner(Connection->WorldID, HouseID, OwnerID, PaidUntil)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessUpdateHouseOwnerQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int HouseID = Buffer->Read16();
	int OwnerID = (int)Buffer->Read32();
	int PaidUntil = (int)Buffer->Read32();
	if(!UpdateHouseOwner(Connection->WorldID, HouseID, OwnerID, PaidUntil)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessDeleteHouseOwnerQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int HouseID = Buffer->Read16();
	if(!DeleteHouseOwner(Connection->WorldID, HouseID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessGetHouseOwnersQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<THouseOwner> Owners;
	if(!GetHouseOwners(Connection->WorldID, &Owners)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumOwners = std::min<int>(Owners.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumOwners);
	for(int i = 0; i < NumOwners; i += 1){
		WriteBuffer.Write16((uint16)Owners[i].HouseID);
		WriteBuffer.Write32((uint32)Owners[i].OwnerID);
		WriteBuffer.WriteString(Owners[i].OwnerName);
		WriteBuffer.Write32((uint32)Owners[i].PaidUntil);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessGetAuctionsQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<int> Auctions;
	if(!GetHouseAuctions(Connection->WorldID, &Auctions)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumAuctions = std::min<int>(Auctions.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumAuctions);
	for(int i = 0; i < NumAuctions; i += 1){
		WriteBuffer.Write16((uint16)Auctions[i]);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessStartAuctionQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int HouseID = Buffer->Read16();
	if(!StartHouseAuction(Connection->WorldID, HouseID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessInsertHousesQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("InsertHouses");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!DeleteHouses(Connection->WorldID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	int NumHouses = Buffer->Read16();
	if(NumHouses > 0){
G		THouse *Houses = (THouse*)alloca(NumHouses * sizeof(THouse));
		for(int i = 0; i < NumHouses; i += 1){
			Houses[i].HouseID = Buffer->Read16();
			Buffer->ReadString(Houses[i].Name, sizeof(Houses[i].Name));
			Houses[i].Rent = (int)Buffer->Read32();
			Buffer->ReadString(Houses[i].Description, sizeof(Houses[i].Description));
			Houses[i].Size = Buffer->Read16();
			Houses[i].PositionX = Buffer->Read16();
			Houses[i].PositionY = Buffer->Read16();
			Houses[i].PositionZ = Buffer->Read8();
			Buffer->ReadString(Houses[i].Town, sizeof(Houses[i].Town));
			Houses[i].GuildHouse = Buffer->ReadFlag();
		}

		if(!InsertHouses(Connection->WorldID, NumHouses, Houses)){
			SendQueryStatusFailed(Connection);
			return;
		}
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessClearIsOnlineQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int NumAffectedCharacters;
	if(!ClearIsOnline(Connection->WorldID, &NumAffectedCharacters)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.Write16((uint16)NumAffectedCharacters);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessCreatePlayerlistQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("OnlineList");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!DeleteOnlineCharacters(Connection->WorldID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	// TODO(fusion): I think `NumCharacters` may be used to signal that the
	// server is going OFFLINE, in which case we'd have to add an `Online`
	// column to `Worlds` and update it here.

	bool NewRecord = false;
	int NumCharacters = Buffer->Read16();
	if(NumCharacters != 0xFFFF && NumCharacters > 0){
		TOnlineCharacter *Characters = (TOnlineCharacter*)alloca(NumCharacters * sizeof(TOnlineCharacter));
		for(int i = 0; i < NumCharacters; i += 1){
			Buffer->ReadString(Characters[i].Name, sizeof(Characters[i].Name));
			Characters[i].Level = Buffer->Read16();
			Buffer->ReadString(Characters[i].Profession, sizeof(Characters[i].Profession));
		}

		if(!InsertOnlineCharacters(Connection->WorldID, NumCharacters, Characters)){
			SendQueryStatusFailed(Connection);
			return;
		}

		if(!CheckOnlineRecord(Connection->WorldID, NumCharacters, &NewRecord)){
			SendQueryStatusFailed(Connection);
			return;
		}
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.WriteFlag(NewRecord);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessLogKilledCreaturesQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	int NumStats = Buffer->Read16();
	TKillStatistics *Stats = (TKillStatistics*)alloca(NumStats * sizeof(TKillStatistics));
	for(int i = 0; i < NumStats; i += 1){
		Buffer->ReadString(Stats[i].RaceName, sizeof(Stats[i].RaceName));
		Stats[i].PlayersKilled = (int)Buffer->Read32();
		Stats[i].TimesKilled = (int)Buffer->Read32();
	}

	if(NumStats > 0){
		TransactionScope Tx("LogKilledCreatures");
		if(!Tx.Begin()){
			SendQueryStatusFailed(Connection);
			return;
		}

		if(!MergeKillStatistics(Connection->WorldID, NumStats, Stats)){
			SendQueryStatusFailed(Connection);
			return;
		}

		if(!Tx.Commit()){
			SendQueryStatusFailed(Connection);
			return;
		}
	}

	SendQueryStatusOk(Connection);
}

void ProcessLoadPlayersQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	// IMPORTANT(fusion): The server expect 10K entries at most. It is probably
	// some shared hard coded constant.
	int NumEntries;
	TCharacterIndexEntry Entries[10000];
	int MinimumCharacterID = (int)Buffer->Read32();
	if(!GetCharacterIndexEntries(Connection->WorldID,
			MinimumCharacterID, NARRAY(Entries), &NumEntries, Entries)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.Write32((uint32)NumEntries);
	for(int i = 0; i < NumEntries; i += 1){
		WriteBuffer.WriteString(Entries[i].Name);
		WriteBuffer.Write32((uint32)Entries[i].CharacterID);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessExcludeFromAuctionsQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("ExcludeFromAuctions");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int CharacterID = (int)Buffer->Read32();
	bool Banish = Buffer->ReadFlag();
	int ExclusionDays = 7;
	int BanishmentID = 0;
	if(Banish){
		int BanishmentDays = 7;
		bool FinalWarning = false;
		TBanishmentStatus Status = GetBanishmentStatus(CharacterID);
		CompoundBanishment(Status, &BanishmentDays, &FinalWarning);
		if(!InsertBanishment(CharacterID, 0, 0, "Spoiling Auction",
				"", FinalWarning, BanishmentDays * 86400, &BanishmentID)){
			SendQueryStatusFailed(Connection);
			return;
		}
	}

	if(!ExcludeFromAuctions(Connection->WorldID,
			CharacterID, ExclusionDays * 86400, BanishmentID)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessCancelHouseTransferQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	// TODO(fusion): Not sure what this is used for. Maybe house transfer rows
	// are kept permanently and this query is used to delete/flag it, in case
	// the it didn't complete. We might need to refine `FinishHouseTransfers`.
	//int HouseID = Buffer->Read16();
	SendQueryStatusOk(Connection);
}

void ProcessLoadWorldConfigQuery(TConnection *Connection, TReadBuffer *Buffer){
	if(Connection->ApplicationType != APPLICATION_TYPE_GAME){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWorldConfig WorldConfig = {};
	if(!GetWorldConfig(Connection->WorldID, &WorldConfig)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.Write8((uint8)WorldConfig.Type);
	WriteBuffer.Write8((uint8)WorldConfig.RebootTime);
	WriteBuffer.Write32BE((uint32)WorldConfig.IPAddress);
	WriteBuffer.Write16((uint16)WorldConfig.Port);
	WriteBuffer.Write16((uint16)WorldConfig.MaxPlayers);
	WriteBuffer.Write16((uint16)WorldConfig.PremiumPlayerBuffer);
	WriteBuffer.Write16((uint16)WorldConfig.MaxNewbies);
	WriteBuffer.Write16((uint16)WorldConfig.PremiumNewbieBuffer);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessCreateAccountQuery(TConnection *Connection, TReadBuffer *Buffer){
	// TODO(fusion): We'd ideally want to automatically generate an account number
	// and return it in case of success but that would also require a more robust
	// website infrastructure with verification e-mails, etc...
	char Email[100];
	char Password[30];
	int AccountID = (int)Buffer->Read32();
	Buffer->ReadString(Email, sizeof(Email));
	Buffer->ReadString(Password, sizeof(Password));

	// NOTE(fusion): Inputs should be checked before hand.
	if(AccountID <= 0 || StringEmpty(Email) || StringEmpty(Password)){
		SendQueryStatusFailed(Connection);
		return;
	}

	uint8 Auth[64];
	if(!GenerateAuth(Password, Auth, sizeof(Auth))){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("CreateAccount");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(AccountNumberExists(AccountID)){
		SendQueryStatusError(Connection, 1);
		return;
	}

	if(AccountEmailExists(Email)){
		SendQueryStatusError(Connection, 2);
		return;
	}

	if(!CreateAccount(AccountID, Email, Auth, sizeof(Auth))){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessCreateCharacterQuery(TConnection *Connection, TReadBuffer *Buffer){
	char WorldName[30];
	char CharacterName[30];
	Buffer->ReadString(WorldName, sizeof(WorldName));
	int AccountID = (int)Buffer->Read32();
	Buffer->ReadString(CharacterName, sizeof(CharacterName));
	int Sex = Buffer->Read8();

	// NOTE(fusion): Inputs should be checked before hand.
	if(AccountID <= 0 || (Sex != 1 && Sex != 2)
			|| StringEmpty(WorldName)
			|| StringEmpty(CharacterName)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TransactionScope Tx("CreateCharacter");
	if(!Tx.Begin()){
		SendQueryStatusFailed(Connection);
		return;
	}

	int WorldID = GetWorldID(WorldName);
	if(WorldID == 0){
		SendQueryStatusError(Connection, 1);
		return;
	}

	if(!AccountNumberExists(AccountID)){
		SendQueryStatusError(Connection, 2);
		return;
	}

	if(CharacterNameExists(CharacterName)){
		SendQueryStatusError(Connection, 3);
		return;
	}

	if(!CreateCharacter(WorldID, AccountID, CharacterName, Sex)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!Tx.Commit()){
		SendQueryStatusFailed(Connection);
		return;
	}

	SendQueryStatusOk(Connection);
}

void ProcessGetAccountSummaryQuery(TConnection *Connection, TReadBuffer *Buffer){
	int AccountID = (int)Buffer->Read32();

	if(AccountID <= 0){
		SendQueryStatusFailed(Connection);
		return;
	}

	TAccount Account;
	if(!GetAccountData(AccountID, &Account)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(Account.AccountID != AccountID){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<TCharacterSummary> Characters;
	if(!GetCharacterSummaries(AccountID, &Characters)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.WriteString(Account.Email);
	WriteBuffer.Write16((uint16)Account.PremiumDays);
	WriteBuffer.Write16((uint16)Account.PendingPremiumDays);
	WriteBuffer.WriteFlag(Account.Deleted);
	int NumCharacters = std::min<int>(Characters.Length(), UINT8_MAX);
	WriteBuffer.Write8((uint8)NumCharacters);
	for(int i = 0; i < NumCharacters; i += 1){
		WriteBuffer.WriteString(Characters[i].Name);
		WriteBuffer.WriteString(Characters[i].World);
		WriteBuffer.Write16((uint16)Characters[i].Level);
		WriteBuffer.WriteString(Characters[i].Profession);
		WriteBuffer.WriteFlag(Characters[i].Online);
		WriteBuffer.WriteFlag(Characters[i].Deleted);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessGetCharacterProfileQuery(TConnection *Connection, TReadBuffer *Buffer){
	char CharacterName[30];
	Buffer->ReadString(CharacterName, sizeof(CharacterName));

	if(StringEmpty(CharacterName)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TCharacterProfile Character;
	if(!GetCharacterProfile(CharacterName, &Character)){
		SendQueryStatusFailed(Connection);
		return;
	}

	if(!StringEqCI(Character.Name, CharacterName)){
		SendQueryStatusError(Connection, 1);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	WriteBuffer.WriteString(Character.Name);
	WriteBuffer.WriteString(Character.World);
	WriteBuffer.Write8((uint8)Character.Sex);
	WriteBuffer.WriteString(Character.Guild);
	WriteBuffer.WriteString(Character.Rank);
	WriteBuffer.WriteString(Character.Title);
	WriteBuffer.Write16((uint16)Character.Level);
	WriteBuffer.WriteString(Character.Profession);
	WriteBuffer.WriteString(Character.Residence);
	WriteBuffer.Write32((uint32)Character.LastLogin);
	WriteBuffer.Write16((uint16)Character.PremiumDays);
	WriteBuffer.WriteFlag(Character.Online);
	WriteBuffer.WriteFlag(Character.Deleted);
	SendResponse(Connection, &WriteBuffer);
}

void ProcessGetWorldsQuery(TConnection *Connection, TReadBuffer *Buffer){
	DynamicArray<TWorld> Worlds;
	if(!GetWorlds(&Worlds)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumWorlds = std::min<int>(Worlds.Length(), UINT8_MAX);
	WriteBuffer.Write8((uint8)NumWorlds);
	for(int i = 0; i < NumWorlds; i += 1){
		WriteBuffer.WriteString(Worlds[i].Name);
		WriteBuffer.Write8((uint8)Worlds[i].Type);
		WriteBuffer.Write16((uint16)Worlds[i].NumPlayers);
		WriteBuffer.Write16((uint16)Worlds[i].MaxPlayers);
		WriteBuffer.Write16((uint16)Worlds[i].OnlineRecord);
		WriteBuffer.Write32((uint32)Worlds[i].OnlineRecordTimestamp);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessGetOnlineCharactersQuery(TConnection *Connection, TReadBuffer *Buffer){
	char WorldName[30];
	Buffer->ReadString(WorldName, sizeof(WorldName));

	int WorldID = GetWorldID(WorldName);
	if(WorldID == 0){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<TOnlineCharacter> Characters;
	if(!GetOnlineCharacters(WorldID, &Characters)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumCharacters = std::min<int>(Characters.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumCharacters);
	for(int i = 0; i < NumCharacters; i += 1){
		WriteBuffer.WriteString(Characters[i].Name);
		WriteBuffer.Write16((uint16)Characters[i].Level);
		WriteBuffer.WriteString(Characters[i].Profession);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessGetKillStatisticsQuery(TConnection *Connection, TReadBuffer *Buffer){
	char WorldName[30];
	Buffer->ReadString(WorldName, sizeof(WorldName));

	int WorldID = GetWorldID(WorldName);
	if(WorldID == 0){
		SendQueryStatusFailed(Connection);
		return;
	}

	DynamicArray<TKillStatistics> Stats;
	if(!GetKillStatistics(WorldID, &Stats)){
		SendQueryStatusFailed(Connection);
		return;
	}

	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	int NumStats = std::min<int>(Stats.Length(), UINT16_MAX);
	WriteBuffer.Write16((uint16)NumStats);
	for(int i = 0; i < NumStats; i += 1){
		WriteBuffer.WriteString(Stats[i].RaceName);
		WriteBuffer.Write32((uint32)Stats[i].PlayersKilled);
		WriteBuffer.Write32((uint32)Stats[i].TimesKilled);
	}
	SendResponse(Connection, &WriteBuffer);
}

void ProcessConnectionQuery(TConnection *Connection){
	TReadBuffer Buffer(Connection->Buffer, Connection->RWSize);
	int Query = Buffer.Read8();
	if(!Connection->Authorized){
		if(Query == QUERY_LOGIN){
			ProcessLoginQuery(Connection, &Buffer);
		}else{
			LOG_ERR("Unauthorized query %d from %s", QueryType, Connection->RemoteAddress);
			CloseConnection(Connection);
		}
		return;
	}

	switch(Query){
		case QUERY_CHECK_ACCOUNT_PASSWORD:		ProcessCheckAccountPasswordQuery(Connection, &Buffer); break;
		case QUERY_LOGIN_ACCOUNT:				ProcessLoginAccountQuery(Connection, &Buffer); break;
		case QUERY_LOGIN_ADMIN:					ProcessLoginAdminQuery(Connection, &Buffer); break;
		case QUERY_LOGIN_GAME:					ProcessLoginGameQuery(Connection, &Buffer); break;
		case QUERY_LOGOUT_GAME:					ProcessLogoutGameQuery(Connection, &Buffer); break;
		case QUERY_SET_NAMELOCK:				ProcessSetNamelockQuery(Connection, &Buffer); break;
		case QUERY_BANISH_ACCOUNT:				ProcessBanishAccountQuery(Connection, &Buffer); break;
		case QUERY_SET_NOTATION:				ProcessSetNotationQuery(Connection, &Buffer); break;
		case QUERY_REPORT_STATEMENT:			ProcessReportStatementQuery(Connection, &Buffer); break;
		case QUERY_BANISH_IP_ADDRESS:			ProcessBanishIPAddressQuery(Connection, &Buffer); break;
		case QUERY_LOG_CHARACTER_DEATH:			ProcessLogCharacterDeathQuery(Connection, &Buffer); break;
		case QUERY_ADD_BUDDY:					ProcessAddBuddyQuery(Connection, &Buffer); break;
		case QUERY_REMOVE_BUDDY:				ProcessRemoveBuddyQuery(Connection, &Buffer); break;
		case QUERY_DECREMENT_IS_ONLINE:			ProcessDecrementIsOnlineQuery(Connection, &Buffer); break;
		case QUERY_FINISH_AUCTIONS:				ProcessFinishAuctionsQuery(Connection, &Buffer); break;
		case QUERY_TRANSFER_HOUSES:				ProcessTransferHousesQuery(Connection, &Buffer); break;
		case QUERY_EVICT_FREE_ACCOUNTS:			ProcessEvictFreeAccountsQuery(Connection, &Buffer); break;
		case QUERY_EVICT_DELETED_CHARACTERS:	ProcessEvictDeletedCharactersQuery(Connection, &Buffer); break;
		case QUERY_EVICT_EX_GUILDLEADERS:		ProcessEvictExGuildleadersQuery(Connection, &Buffer); break;
		case QUERY_INSERT_HOUSE_OWNER:			ProcessInsertHouseOwnerQuery(Connection, &Buffer); break;
		case QUERY_UPDATE_HOUSE_OWNER:			ProcessUpdateHouseOwnerQuery(Connection, &Buffer); break;
		case QUERY_DELETE_HOUSE_OWNER:			ProcessDeleteHouseOwnerQuery(Connection, &Buffer); break;
		case QUERY_GET_HOUSE_OWNERS:			ProcessGetHouseOwnersQuery(Connection, &Buffer); break;
		case QUERY_GET_AUCTIONS:				ProcessGetAuctionsQuery(Connection, &Buffer); break;
		case QUERY_START_AUCTION:				ProcessStartAuctionQuery(Connection, &Buffer); break;
		case QUERY_INSERT_HOUSES:				ProcessInsertHousesQuery(Connection, &Buffer); break;
		case QUERY_CLEAR_IS_ONLINE:				ProcessClearIsOnlineQuery(Connection, &Buffer); break;
		case QUERY_CREATE_PLAYERLIST:			ProcessCreatePlayerlistQuery(Connection, &Buffer); break;
		case QUERY_LOG_KILLED_CREATURES:		ProcessLogKilledCreaturesQuery(Connection, &Buffer); break;
		case QUERY_LOAD_PLAYERS:				ProcessLoadPlayersQuery(Connection, &Buffer); break;
		case QUERY_EXCLUDE_FROM_AUCTIONS:		ProcessExcludeFromAuctionsQuery(Connection, &Buffer); break;
		case QUERY_CANCEL_HOUSE_TRANSFER:		ProcessCancelHouseTransferQuery(Connection, &Buffer); break;
		case QUERY_LOAD_WORLD_CONFIG:			ProcessLoadWorldConfigQuery(Connection, &Buffer); break;
		case QUERY_CREATE_ACCOUNT:				ProcessCreateAccountQuery(Connection, &Buffer); break;
		case QUERY_CREATE_CHARACTER:			ProcessCreateCharacterQuery(Connection, &Buffer); break;
		case QUERY_GET_ACCOUNT_SUMMARY:			ProcessGetAccountSummaryQuery(Connection, &Buffer); break;
		case QUERY_GET_CHARACTER_PROFILE:		ProcessGetCharacterProfileQuery(Connection, &Buffer); break;
		case QUERY_GET_WORLDS:					ProcessGetWorldsQuery(Connection, &Buffer); break;
		case QUERY_GET_ONLINE_CHARACTERS:		ProcessGetOnlineCharactersQuery(Connection, &Buffer); break;
		case QUERY_GET_KILL_STATISTICS:			ProcessGetKillStatisticsQuery(Connection, &Buffer); break;
		default:{
			LOG_ERR("Unknown query %d from %s", Query, Connection->RemoteAddress);
			SendQueryStatusFailed(Connection);
			break;
		}
	}
}
#endif
