#if DATABASE_POSTGRESQL
#include "querymanager.hh"
#include "libpq-fe.h"
#include <netinet/ip.h>

// IMPORTANT(fusion): With PostgreSQL being a distributed database, we cannot
// rely on automatic schema upgrades like in the case of SQLite. It must be
// managed manually and there must be an agreement on the current version
// which is why there is a `SchemaInfo` table.
#define POSTGRESQL_SCHEMA_VERSION 1

// IMPORTANT(fusion): PostgreSQL timestamps will count the number of microseconds
// since 2000-01-01 00:00:00, with negative values for timestamps before it. To be
// able to convert between PostgreSQL and UNIX timestamps we need the EPOCH of one
// represented as a timestamp of the other, which is exactly what this is. This is
// the PostgreSQL EPOCH represented as an UNIX timestamp.
#define POSTGRESQL_EPOCH 946684800

// IMPORTANT(fusion): Address families used with INET and CIDR binary format.
// They're taken from `utils/inet.h` which is not included with libpq but should
// be stable across different systems, mostly because AF_INET should be stable.
#define POSTGRESQL_AF_INET  (AF_INET + 0)
#define POSTGRESQL_AF_INET6 (AF_INET + 1)
STATIC_ASSERT(POSTGRESQL_AF_INET  == 2);
STATIC_ASSERT(POSTGRESQL_AF_INET6 == 3);

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
#define CIDROID 650
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

// Param Buffer
//==============================================================================
struct ParamBuffer{
	const char **Values;
	int *Lengths;
	int *Formats;
	int NumParams;
	int MaxParams;
	int PreferredFormat;

	// NOTE(fusion): 8KB should be more than enough for all case scenarios. We'll
	// know if it's not.
	int ArenaPos;
	uint8 Arena[KB(8)];
};

static void *ParamAllocImpl(ParamBuffer *Params, int Size, int Alignment){
	usize ArenaStart = (usize)Params->Arena;
	usize ArenaEnd   = ArenaStart + sizeof(Params->Arena);
	usize ArenaPos   = ArenaStart + Params->ArenaPos;
	usize AllocStart = AlignUp(ArenaPos, Alignment);
	usize AllocEnd   = AllocStart + Size;
	if(AllocEnd > ArenaEnd){
		PANIC("Param buffer is FULL");
	}

	Params->ArenaPos = (int)(AllocEnd - ArenaStart);
	return (void*)AllocStart;
}

template<typename T>
static T *ParamAlloc(ParamBuffer *Params, int Count){
	ASSERT(Count > 0);
	return (T*)ParamAllocImpl(Params, sizeof(T) * Count, alignof(T));
}

static void ParamBegin(ParamBuffer *Params, int MaxParams, int PreferredFormat){
	// NOTE(fusion): Reset arena.
	memset(Params->Arena, 0, sizeof(Params->Arena));
	Params->ArenaPos = 0;

	// NOTE(fusion): Reset params.
	if(MaxParams > 0){
		Params->Values = ParamAlloc<const char*>(Params, MaxParams);
		Params->Lengths = ParamAlloc<int>(Params, MaxParams);
		Params->Formats = ParamAlloc<int>(Params, MaxParams);
	}else{
		Params->Values = NULL;
		Params->Lengths = NULL;
		Params->Formats = NULL;
	}
	Params->NumParams = 0;
	Params->MaxParams = MaxParams;
	Params->PreferredFormat = PreferredFormat;
}

static void InsertParam(ParamBuffer *Params, const char *Param, int Length, int Format){
	if(Params->NumParams >= Params->MaxParams){
		PANIC("Too many parameters specified (%d/%d)",
				Params->NumParams + 1, Params->MaxParams);
	}

	Params->Values[Params->NumParams] = Param;
	Params->Lengths[Params->NumParams] = Length;
	Params->Formats[Params->NumParams] = Format;
	Params->NumParams += 1;
}

static void InsertTextParam(ParamBuffer *Params, const char *Text){
	int TextLength = (int)strlen(Text);
	char *Copy = ParamAlloc<char>(Params, TextLength + 1);
	memcpy(Copy, Text, TextLength + 1);
	InsertParam(Params, Copy, TextLength, 0);
}

static void InsertBinaryParam(ParamBuffer *Params, const uint8 *Data, int Length){
	uint8 *Copy = ParamAlloc<uint8>(Params, Length);
	memcpy(Copy, Data, Length);
	InsertParam(Params, (const char*)Copy, Length, 1);
}

static void ParamBool(ParamBuffer *Params, bool Value){
	if(Params->PreferredFormat == 1){ // BINARY FORMAT
		uint8 Data = (Value ? 0x01 : 0x00);
		InsertBinaryParam(Params, &Data, 1);
	}else{                            // TEXT FORMAT
		InsertTextParam(Params, (Value ? "TRUE" : "FALSE"));
	}
}

static void ParamInteger(ParamBuffer *Params, int Value){
	if(Params->PreferredFormat == 1){ // BINARY FORMAT
		uint8 Data[4];
		BufferWrite32BE(Data, (uint32)Value);
		InsertBinaryParam(Params, Data, 4);
	}else{
		char Text[16];
		StringBufFormat(Text, "%d", Value);
		InsertTextParam(Params, Text);
	}
}

static void ParamText(ParamBuffer *Params, const char *Text){
	// NOTE(fusion): Always use TEXT format.
	InsertTextParam(Params, Text);
}

static void ParamByteA(ParamBuffer *Params, const uint8 *Data, int Length){
	// TODO(fusion): Always use BINARY format?
	InsertBinaryParam(Params, Data, Length);
}

static void ParamIPAddress(ParamBuffer *Params, int IPAddress){
	if(Params->PreferredFormat == 1){ // BINARY FORMAT
		uint8 Data[8];
		Data[0] = POSTGRESQL_AF_INET; // AddressFamily
		Data[1] = 32;                 // MaskBits
		Data[2] = 0;                  // IsCIDR
		Data[3] = 4;                  // AddressSize
		BufferWrite32BE(Data + 4, (uint32)IPAddress);
		InsertBinaryParam(Params, Data, 8);
	}else{
		char Text[16];
		StringBufFormat(Text, "%d.%d.%d.%d",
				((IPAddress >> 24) & 0xFF),
				((IPAddress >> 16) & 0xFF),
				((IPAddress >>  8) & 0xFF),
				((IPAddress >>  0) & 0xFF));
		InsertTextParam(Params, Text);
	}
}

static void ParamTimestamp(ParamBuffer *Params, int Timestamp){
	if(Params->PreferredFormat == 1){ // BINARY FORMAT
		// NOTE(fusion): See `POSTGRESQL_EPOCH`.
		uint8 Data[8];
		int64 PGTimestamp = (int64)(Timestamp - POSTGRESQL_EPOCH) * 1000000;
		BufferWrite64BE(Data, (uint64)PGTimestamp);
		InsertBinaryParam(Params, Data, 8);
	}else{
		char Text[32];
		struct tm tm = GetGMTime((time_t)Timestamp);
		strftime(Text, sizeof(Text), "%Y-%m-%d %H:%M:%S+00", &tm);
		InsertTextParam(Params, Text);
	}
}

// Result Helpers
//==============================================================================
struct AutoResultClear{
private:
	PGresult *m_Result;

public:
	AutoResultClear(PGresult *Result){
		m_Result = Result;
	}

	~AutoResultClear(void){
		if(m_Result != NULL){
			PQclear(m_Result);
			m_Result = NULL;
		}
	}
};

static bool GetResultBool(PGresult *Result, int Row, int Col){
	bool Value = false;
	int Format = PQfformat(Result, Col);
	Oid Type = PQftype(Result, Col);
	if(Format == 0){       // TEXT FORMAT
		if(!ParseBoolean(&Value, PQgetvalue(Result, Row, Col))){
			LOG_ERR("Failed to properly parse column (%d) %s as BOOLEAN",
					Col, PQfname(Result, Col));
		}
	}else if(Format == 1){ // BINARY FORMAT
		switch(Type){
			case BOOLOID:{
				ASSERT(PQgetlength(Result, Row, Col) == 1);
				Value = (BufferRead8((const uint8*)PQgetvalue(Result, Row, Col)) != 0);
				break;
			}

			case INT8OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 8);
				Value = (BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col)) != 0);
				break;
			}

			case INT2OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 2);
				Value = (BufferRead16BE((const uint8*)PQgetvalue(Result, Row, Col)) != 0);
				break;
			}

			case INT4OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 4);
				Value = (BufferRead32BE((const uint8*)PQgetvalue(Result, Row, Col)) != 0);
				break;
			}

			case TEXTOID:
			case VARCHAROID:{
				if(!ParseBoolean(&Value, PQgetvalue(Result, Row, Col))){
					LOG_WARN("Failed to properly convert column (%d) %s from TEXT to BOOLEAN",
							Col, PQfname(Result, Col));
				}
				break;
			}

			default:{
				LOG_ERR("Column (%d) %s has OID %d which is not convertible to BOOLEAN",
						Col, PQfname(Result, Col), Type);
				break;
			}
		}
	}
	return Value;
}

static int GetResultInt(PGresult *Result, int Row, int Col){
	int Value = 0;
	int Format = PQfformat(Result, Col);
	Oid Type = PQftype(Result, Col);
	ASSERT(Format == 0 || Format == 1);
	if(Format == 0){       // TEXT FORMAT
		if(!ParseInteger(&Value, PQgetvalue(Result, Row, Col))){
			LOG_ERR("Failed to properly parse column (%d) %s as INT4",
					Col, PQfname(Result, Col));
		}
	}else if(Format == 1){ // BINARY FORMAT
		switch(Type){
			case BOOLOID:{
				ASSERT(PQgetlength(Result, Row, Col) == 1);
				Value = BufferRead8((const uint8*)PQgetvalue(Result, Row, Col));
				break;
			}

			case INT8OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 8);
				int64 Temp = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(Temp < INT_MIN || Temp > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to INT4",
							Col, PQfname(Result, Col));
				}

				Value = (int)Temp;
				break;
			}

			case INT2OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 2);
				Value = (int16)BufferRead16BE((const uint8*)PQgetvalue(Result, Row, Col));
				break;
			}

			case INT4OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 4);
				Value = (int)BufferRead32BE((const uint8*)PQgetvalue(Result, Row, Col));
				break;
			}

			case TEXTOID:
			case VARCHAROID:{
				if(!ParseInteger(&Value, PQgetvalue(Result, Row, Col))){
					LOG_WARN("Failed to properly convert column (%d) %s from TEXT to INT4",
							Col, PQfname(Result, Col));
				}
				break;
			}

			default:{
				LOG_ERR("Column (%d) %s has OID %d which is not convertible to INT4",
						Col, PQfname(Result, Col), Type);
				break;
			}
		}
	}
	return Value;
}

static const char *GetResultText(PGresult *Result, int Row, int Col){
	const char *Text = "";
	int Format = PQfformat(Result, Col);
	Oid Type = PQftype(Result, Col);
	ASSERT(Format == 0 || Format == 1);
	if(Format == 0){       // TEXT FORMAT
		Text = PQgetvalue(Result, Row, Col);
	}else if(Format == 1){ // BINARY FORMAT
		switch(Type){
			case TEXTOID:
			case VARCHAROID:{
				Text = PQgetvalue(Result, Row, Col);
				break;
			}

			default:{
				// IMPORTANT(fusion): There is no trivial way to convert whatever
				// value we received back to string. We'd either need to allocate
				// or modify the prototype of this function to accept an output
				// buffer.
				//  The fact is, we shouldn't expect implicit conversions to work
				// when using the binary format, PERIOD.
				LOG_ERR("Column (%d) %s has OID %d which is not trivially convertible to TEXT",
						Col, PQfname(Result, Col), Type);
				break;
			}
		}
	}
	return Text;
}

static int GetResultByteA(PGresult *Result, int Row, int Col, uint8 *Buffer, int BufferSize){
	int Size = -1;
	int Format = PQfformat(Result, Col);
	ASSERT(Format == 0 || Format == 1);
	if(Format == 0){       // TEXT FORMAT
		const char *String = PQgetvalue(Result, Row, Col);
		if(String[0] == '\\' && String[1] == 'x'){
			Size = ParseHexString(Buffer, BufferSize, String + 2);
		}else{
			LOG_ERR("Column (%d) %s (OID %d) doesn't contain a valid BYTEA literal",
					Col, PQfname(Result, Col), PQftype(Result, Col));
		}
	}else if(Format == 1){ // BINARY FORMAT
		Size = PQgetlength(Result, Row, Col);
		if(Size > 0 && Size < BufferSize){
			memcpy(Buffer, PQgetvalue(Result, Row, Col), Size);
		}
	}

	ASSERT(Size <= BufferSize);
	return Size;
}

static int GetResultIPAddress(PGresult *Result, int Row, int Col){
	int IPAddress = 0;
	int Format = PQfformat(Result, Col);
	Oid Type = PQftype(Result, Col);
	ASSERT(Format == 0 || Format == 1);
	if(Format == 0){       // TEXT FORMAT
		if(!ParseIPAddress(&IPAddress, PQgetvalue(Result, Row, Col))){
			LOG_ERR("Failed to parse column (%d) %s as IPV4",
					Col, PQfname(Result, Col));
		}
	}else if(Format == 1){ // BINARY FORMAT
		switch(Type){
			case INT8OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 8);
				int64 Temp = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(Temp < INT_MIN || Temp > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to IPV4",
							Col, PQfname(Result, Col));
				}

				IPAddress = (int)Temp;
				break;
			}

			case INT4OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 4);
				IPAddress = (int)BufferRead32BE((const uint8*)PQgetvalue(Result, Row, Col));
				break;
			}


			case TEXTOID:
			case VARCHAROID:{
				if(!ParseIPAddress(&IPAddress, PQgetvalue(Result, Row, Col))){
					LOG_ERR("Failed to convert column (%d) %s from TEXT to IPV4",
							Col, PQfname(Result, Col));
				}
				break;
			}

			case CIDROID:
			case INETOID:{
				int Size = PQgetlength(Result, Row, Col);
				const uint8 *Data = (const uint8*)PQgetvalue(Result, Row, Col);
				if(Size >= 4){
					int AddressFamily = (int)Data[0];
					// Data[1]; // MaskBits
					// Data[2]; // IsCIDR
					int AddressSize = (int)Data[3];
					if(AddressFamily == POSTGRESQL_AF_INET && AddressSize == 4 && Size >= 8){
						IPAddress = (int)BufferRead32BE(Data + 4);
					}else{
						LOG_ERR("CIDR/INET column (%d) %s doesn't contain IPV4 address",
								Col, PQfname(Result, Col));
					}
				}else{
					LOG_ERR("CIDR/INET column (%d) %s has unexpected binary format",
							Col, PQfname(Result, Col));
				}
				break;
			}

			default:{
				LOG_ERR("Column (%d) %s has OID %d which is not convertible to IPV4",
						Col, PQfname(Result, Col), Type);
				break;
			}
		}
	}
	return IPAddress;

}

static bool ParseTimestamp(int *Dest, const char *String){
	// TODO(fusion): I don't think this function exists on Windows but neither
	// are we running on Windows so does it even matter? There might be better
	// ways to properly parse this timestamp format.
	struct tm tm = {};
	const char *Rem = strptime(String, "%Y-%m-%d %H:%M:%S", &tm);
	if(Rem == NULL){
		LOG_ERR("Invalid timestamp format \"%s\"", String);
		return false;
	}

	// NOTE(fusion): Skip optional milliseconds/microseconds.
	if(Rem[0] == '.'){
		Rem += 1;
		while(isdigit(Rem[0])){
			Rem += 1;
		}
	}

	// NOTE(fusion): Parse optional timezone.
	int GMTOffset = 0;
	if((Rem[0] == '-' || Rem[0] == '+') && isdigit(Rem[1]) && isdigit(Rem[2])){
		// NOTE(fusion): Hours.
		GMTOffset += ((Rem[1] - '0') * 10 + (Rem[2] - '0')) * 3600;

		// NOTE(fusion): Optional minutes.
		if(isdigit(Rem[3]) && isdigit(Rem[4])){
			GMTOffset += ((Rem[3] - '0') * 10 + (Rem[4] - '0')) * 60;
		}

		if(Rem[0] == '+'){
			GMTOffset = -GMTOffset;
		}
	}

	*Dest = (int)timegm(&tm) + GMTOffset;
	return true;
}

static int GetResultTimestamp(PGresult *Result, int Row, int Col){
	int Timestamp = 0;
	int Format = PQfformat(Result, Col);
	Oid Type = PQftype(Result, Col);
	ASSERT(Format == 0 || Format == 1);
	if(Format == 0){       // TEXT FORMAT
		if(!ParseTimestamp(&Timestamp, PQgetvalue(Result, Row, Col))){
			LOG_ERR("Failed to parse column (%d) %s as TIMESTAMP",
					Col, PQfname(Result, Col));
		}
	}else if(Format == 1){ // BINARY FORMAT
		switch(Type){
			case INT8OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 8);
				int64 Temp = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(Temp < INT_MIN || Temp > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to TIMESTAMP",
							Col, PQfname(Result, Col));
				}

				Timestamp = (int)Temp;
				break;
			}

			case INT4OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 4);
				Timestamp = (int)BufferRead32BE((const uint8*)PQgetvalue(Result, Row, Col));
				break;
			}

			case TEXTOID:
			case VARCHAROID:{
				if(!ParseTimestamp(&Timestamp, PQgetvalue(Result, Row, Col))){
					LOG_ERR("Failed to convert column (%d) %s from TEXT to TIMESTAMP",
							Col, PQfname(Result, Col));
				}
				break;
			}

			case TIMESTAMPOID:
			case TIMESTAMPTZOID:{
				ASSERT(PQgetlength(Result, Row, Col) == 8);
				// NOTE(fusion): See `POSTGRESQL_EPOCH`.
				int64 PGTimestamp = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				int64 Timestamp64 = ((PGTimestamp / 1000000) + POSTGRESQL_EPOCH);
				if(Timestamp64 < INT_MIN){
					Timestamp = INT_MIN;
				}else if(Timestamp64 > INT_MAX){
					Timestamp = INT_MAX;
				}else{
					Timestamp = (int)Timestamp64;
				}
				break;
			}

			default:{
				LOG_ERR("Column (%d) %s has OID %d which is not convertible to TIMESTAMP",
						Col, PQfname(Result, Col), Type);
				break;
			}
		}
	}
	return Timestamp;
}

// Internal Helpers
//==============================================================================
static bool ExecInternal(TDatabase *Database, const char *Format, ...) ATTR_PRINTF(2, 3);
static bool ExecInternal(TDatabase *Database, const char *Format, ...){
	va_list ap;
	va_start(ap, Format);
	char Text[1024];
	int Written = vsnprintf(Text, sizeof(Text), Format, ap);
	va_end(ap);

	if(Written >= (int)sizeof(Text)){
		LOG_ERR("Query is too long");
		return false;
	}

	PGresult *Result = PQexec(Database->Handle, Text);
	AutoResultClear ResultGuard(Result);
	bool Status = PQresultStatus(Result) == PGRES_COMMAND_OK
			|| PQresultStatus(Result) == PGRES_TUPLES_OK;
	if(!Status){
		char Preview[30];
		StringBufCopyEllipsis(Preview, Text);
		LOG_ERR("Failed to execute query \"%s\": %s",
				Preview, PQerrorMessage(Database->Handle));
	}
	return Status;
}

static bool GetSchemaVersion(TDatabase *Database, int *Version){
	PGresult *Result = PQexec(Database->Handle,
			"SELECT Value FROM SchemaInfo WHERE Key = 'VERSION'");
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s",
				PQerrorMessage(Database->Handle));
		return false;
	}

	if(PQntuples(Result) == 0){
		LOG_ERR("Query returned no rows");
		return false;
	}

	*Version = GetResultInt(Result, 0, 0);
	return true;
}

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
			if(Entry->Text != NULL){
				free(Entry->Text);
				Entry->LastUsed = 0;
				Entry->Hash = 0;
				Entry->Text = NULL;
			}
		}

		// NOTE(fusion): This function would usually be called along with `PQreset` or
		// `PQfinish` but it's probably a good idea to close all prepared statements if
		// the connection is still going. There is no libpq wrapper but we can execute
		// `DEALLOCATE ALL`.
		if(PQstatus(Database->Handle) == CONNECTION_OK){
			if(!ExecInternal(Database, "DEALLOCATE ALL")){
				LOG_WARN("Failed to close all prepared statements");
			}
		}

		free(Database->CachedStatements);
		Database->MaxCachedStatements = 0;
		Database->CachedStatements = NULL;
	}
}

// IMPORTANT(fusion): Even though it is possible to declare parameter types
// with OIDs, it is simpler to use explicit casts such as `$1::INTEGER` to
// enforce types. It also makes so all relevant information about the query
// is packed into `Text` so we don't need to track anything else to ensure
// queries with with different types are kept separate.
//  Keep in mind that using the same parameter multiple times with different
// explicit type casts will make so only the first one is used when inferring
// the actual parameter type. Others are considered casts from it.
//  For example, `SELECT $1::TIMESTAMP, $1::TIMESTAMPTZ` will make so $1 is
// inferred as `TIMESTAMP`, so `$1::TIMESTAMPTZ` will actually be a cast from
// `TIMESTAMP` into `TIMESTAMPTZ`, which will most likely yield unexpected
// results.
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
			AutoResultClear ResultGuard(Result);
			if(PQresultStatus(Result) != PGRES_COMMAND_OK){
				char OldPreview[30];
				StringBufCopyEllipsis(OldPreview, Stmt->Text);
				LOG_ERR("Failed to close prepared query \"%s\": %s",
						OldPreview, PQerrorMessage(Database->Handle));
			}
			free(Stmt->Text);
		}

		{
			PGresult *Result = PQprepare(Database->Handle, Stmt->Name, Text, 0, NULL);
			AutoResultClear ResultGuard(Result);
			if(PQresultStatus(Result) != PGRES_COMMAND_OK){
				char NewPreview[30];
				StringBufCopyEllipsis(NewPreview, Text);
				LOG_ERR("Failed to prepare query \"%s\": %s",
						NewPreview, PQerrorMessage(Database->Handle));
				return NULL;
			}
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
			AutoResultClear ResultGuard(Result);
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
		}
#endif
	}

	return Stmt->Name;
}

// TransactionScope
//==============================================================================
TransactionScope::TransactionScope(const char *Context){
	m_Context = (Context != NULL ? Context : "NOCONTEXT");
	m_Database = NULL;
}

TransactionScope::~TransactionScope(void){
	if(m_Database != NULL){
		if(!ExecInternal(m_Database, "ROLLBACK")){
			LOG_ERR("Failed to rollback transaction (%s)", m_Context);
		}

		m_Database = NULL;
	}
}

bool TransactionScope::Begin(TDatabase *Database){
	if(m_Database != NULL){
		LOG_ERR("Transaction (%s) already running", m_Context);
		return false;
	}

	if(!ExecInternal(Database, "BEGIN")){
		LOG_ERR("Failed to begin transaction (%s)", m_Context);
		return false;
	}

	m_Database = Database;
	return true;
}

bool TransactionScope::Commit(void){
	if(m_Database == NULL){
		LOG_ERR("Transaction (%s) not running", m_Context);
		return false;
	}

	if(!ExecInternal(m_Database, "COMMIT")){
		LOG_ERR("Failed to commit transaction (%s)", m_Context);
		return false;
	}

	m_Database = NULL;
	return true;
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

	int SchemaVersion;
	if(!GetSchemaVersion(Database, &SchemaVersion)){
		LOG_ERR("Failed to retrieve schema version..."
				" Database schema may not have been initialized");
		DatabaseClose(Database);
		return NULL;
	}

	if(SchemaVersion != POSTGRESQL_SCHEMA_VERSION){
		LOG_ERR("Schema version MISMATCH (expected %d, got %d)",
				POSTGRESQL_SCHEMA_VERSION, SchemaVersion);
		DatabaseClose(Database);
		return NULL;
	}

#if 1
	{
		// TODO(fusion): REMOVE. This was for testing TIMESTAMP input/output, to make
		// sure they were consistent across different formats (text/binary).
		const char *Stmt = PrepareQuery(Database, "SELECT $1::TIMESTAMP, $2::TIMESTAMPTZ");
		ASSERT(Stmt != NULL);

		int Timestamp = (int)time(NULL);
		LOG("TIMESTAMP: %d", Timestamp);

		for(int i = 0; i <= 1; i += 1)
		for(int j = 0; j <= 1; j += 1){
			LOG("TEST (%d, %d)", i, j);
			ParamBuffer Params;
			ParamBegin(&Params, 2, i);
			ParamTimestamp(&Params, Timestamp);
			ParamTimestamp(&Params, Timestamp);
			PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
										Params.Values, Params.Lengths, Params.Formats, j);
			AutoResultClear ResultGuard(Result);
			if(PQresultStatus(Result) == PGRES_TUPLES_OK){
				LOG("0: %d", GetResultTimestamp(Result, 0, 0));
				LOG("1: %d", GetResultTimestamp(Result, 0, 1));
			}else{
				LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
			}
		}
	}
#endif

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

// Primary Tables
//==============================================================================
bool GetWorldID(TDatabase *Database, const char *World, int *WorldID){
	ASSERT(Database != NULL && World != NULL && WorldID != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT WorldID FROM Worlds WHERE Name = $1::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	//	TODO(fusion): In this specific case it doesn't make a difference but
	// we'll probably need some helper struct to organize query parameters.
	// It would have an internal buffer which would be used as an arena.
	//ParamBuffer Params = {};
	//ParamBegin(&Params, 1, 1);
	//ParamText(&Params, World);
	//PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
	//						Params.Values, Params.Lengths, Params.Formats, 1);

	const char *ParamValues[] = { World };
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, 1, ParamValues, NULL, NULL, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*WorldID = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

bool GetWorlds(TDatabase *Database, DynamicArray<TWorld> *Worlds){
	ASSERT(Database != NULL && Worlds != NULL);
	const char *Stmt = PrepareQuery(Database,
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

	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, 0, NULL, NULL, NULL, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		TWorld World = {};
		StringBufCopy(World.Name, GetResultText(Result, Row, 0));
		World.Type = GetResultInt(Result, Row, 1);
		World.NumPlayers = GetResultInt(Result, Row, 2);
		World.MaxPlayers = GetResultInt(Result, Row, 3);
		World.OnlineRecord = GetResultInt(Result, Row, 4);
		World.OnlineRecordTimestamp = GetResultTimestamp(Result, Row, 5);
		Worlds->Push(World);
	}

	return true;
}

bool GetWorldConfig(TDatabase *Database, int WorldID, TWorldConfig *WorldConfig){
	ASSERT(Database != NULL && WorldConfig != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT WorldID, Type, RebootTime, Host, Port, MaxPlayers,"
				" PremiumPlayerBuffer, MaxNewbies, PremiumNewbieBuffer"
			" FROM Worlds WHERE WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInteger(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	// TODO(fusion): We probably need a way to differentiate a failure from an
	// empty result set.
	memset(WorldConfig, 0, sizeof(TWorldConfig));
	if(PQntuples(Result) > 0){
		WorldConfig->WorldID = GetResultInt(Result, 0, 0);
		WorldConfig->Type = GetResultInt(Result, 0, 1);
		WorldConfig->RebootTime = GetResultInt(Result, 0, 2);
		StringBufCopy(WorldConfig->HostName, GetResultText(Result, 0, 3));
		WorldConfig->Port = GetResultInt(Result, 0, 4);
		WorldConfig->MaxPlayers = GetResultInt(Result, 0, 5);
		WorldConfig->PremiumPlayerBuffer = GetResultInt(Result, 0, 6);
		WorldConfig->MaxNewbies = GetResultInt(Result, 0, 7);
		WorldConfig->PremiumNewbieBuffer = GetResultInt(Result, 0, 8);
	}

	return true;
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
