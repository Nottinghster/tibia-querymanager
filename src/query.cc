#include "querymanager.hh"

#include <pthread.h>

struct TQueryQueue{
	pthread_mutex_t Mutex;
	pthread_cond_t EmptyCond;
	pthread_cond_t FullCond;
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
	Query->BufferSize = g_MaxConnectionPacketSize;
	Query->Buffer = (uint8*)calloc(1, Query->BufferSize);
	return Query;
}

void QueryDone(TQuery *Query){
	int RefCount = AtomicFetchAdd(&Query->RefCount, -1);
	ASSERT(RefCount >= 1);
	if(RefCount == 1){
		free(Query->Buffer);
		free(Query);
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
		pthread_cond_wait(&g_QueryQueue->FullCond, &g_QueryQueue->Mutex);
		NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
		MaxQueries = g_QueryQueue->MaxQueries;
	}

	if(NumQueries == 0){
		pthread_cond_signal(&g_QueryQueue->EmptyCond);
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
		pthread_cond_wait(&g_QueryQueue->EmptyCond, &g_QueryQueue->Mutex);
		NumQueries = g_QueryQueue->WritePos - g_QueryQueue->ReadPos;
	}

	if(NumQueries > 0 && AtomicLoad(Running)){
		uint32 MaxQueries = g_QueryQueue->MaxQueries;
		if(NumQueries == MaxQueries){
			pthread_cond_signal(&g_QueryQueue->FullCond);
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
		//TODO
		switch(Query->QueryType){
			default:{
				//
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
	pthread_cond_init(&g_QueryQueue->EmptyCond, NULL);
	pthread_cond_init(&g_QueryQueue->FullCond, NULL);
	g_QueryQueue->MaxQueries = 2 * g_MaxConnections;
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
		pthread_cond_destroy(&g_QueryQueue->EmptyCond);
		pthread_cond_destroy(&g_QueryQueue->FullCond);

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

// Queries
//==============================================================================
TWriteBuffer *QueryBeginResponse(TQuery *Query, int Status){
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

