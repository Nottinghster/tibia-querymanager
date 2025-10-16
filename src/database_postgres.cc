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
	}else{
		InsertTextParam(Params, (Value ? "TRUE" : "FALSE"));
	}
}

static void ParamInt(ParamBuffer *Params, int Value){
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

static void ParamInterval(ParamBuffer *Params, int Interval){
	int OneDay  = 60 * 60 * 24;
	int Years   = (Interval / (OneDay * 365)); Interval -= Years * (OneDay * 365);
	int Months  = (Interval / (OneDay * 30));  Interval -= Months * (OneDay * 30);
	int Days    = (Interval / OneDay);         Interval -= Days * OneDay;
	int Seconds = Interval;                    Interval -= Seconds;

	if(Params->PreferredFormat == 1){ // BINARY FORMAT
		uint8 Data[16];
		int64 Microseconds = (int64)Seconds * 1000000;
		BufferWrite64BE(Data +  0, (uint64)Microseconds);
		BufferWrite32BE(Data +  8, (uint32)Days);
		BufferWrite32BE(Data + 12, (uint32)(Months + Years * 12));
		InsertBinaryParam(Params, Data, 16);
	}else{
		StringBuffer<256> Text;

		if(Years != 0){
			if(!Text.Empty()) Text.Append(" ");
			Text.FormatAppend("%d years", Years);
		}

		if(Months != 0){
			if(!Text.Empty()) Text.Append(" ");
			Text.FormatAppend("%d mons", Months);
		}

		if(Days != 0){
			if(!Text.Empty()) Text.Append(" ");
			Text.FormatAppend("%d days", Days);
		}

		if(Seconds != 0){
			bool Negative = false;
			if(Seconds < 0){
				Negative = true;
				Seconds = -Seconds;
			}

			int OneMinute = 60;
			int Hours     = (Seconds / (OneMinute * 60)); Seconds -= Hours * (OneMinute * 60);
			int Minutes   = (Seconds / OneMinute);        Seconds -= Minutes * OneMinute;

			if(!Text.Empty()) Text.Append(" ");
			if(Negative) Text.Append("-");
			Text.FormatAppend("%02d:%02d:%02d", Hours, Minutes, Seconds);
		}

		ASSERT(!Text.Overflowed());
		InsertTextParam(Params, Text.CString());
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

static int ResultAffectedRows(PGresult *Result){
	int AffectedRows = 0;
	ParseInteger(&AffectedRows, PQcmdTuples(Result));
	return AffectedRows;
}

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
				int64 Value64 = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(Value64 < INT_MIN || Value64 > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to INT4",
							Col, PQfname(Result, Col));
				}

				Value = (int)Value64;
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
		if(Size > 0 && Size <= BufferSize){
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
				int64 IPAddress64 = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(IPAddress64 < INT_MIN || IPAddress64 > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to IPV4",
							Col, PQfname(Result, Col));
				}

				IPAddress = (int)IPAddress64;
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
	ASSERT(Dest != NULL && String != NULL);

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
				int64 Timestamp64 = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(Timestamp64 < INT_MIN || Timestamp64 > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to TIMESTAMP",
							Col, PQfname(Result, Col));
				}

				Timestamp = (int)Timestamp64;
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

static void SkipWhitespace(int *Cursor, const char *String){
	while(isspace(String[*Cursor])){
		*Cursor += 1;
	}
}

static bool SkipNextChar(int Ch, int *Cursor, const char *String){
	ASSERT(Ch != 0);
	if(String[*Cursor] != Ch){
		return false;
	}

	*Cursor += 1;
	return true;
}

static bool ReadNextNumber(int *Dest, int *Cursor, const char *String){
	SkipWhitespace(Cursor, String);

	bool Negative = false;
	if(!isdigit(String[*Cursor])){
		if(String[*Cursor] != '+' && String[*Cursor] != '-'){
			return false;
		}

		if(!isdigit(String[*Cursor + 1])){
			return false;
		}

		if(String[*Cursor] == '-'){
			Negative = true;
		}

		*Cursor += 1;
	}

	int Number = 0;
	while(isdigit(String[*Cursor])){
		Number = (Number * 10) + (String[*Cursor] - '0');
		*Cursor += 1;
	}

	if(Negative){
		Number = -Number;
	}

	*Dest = Number;
	return true;
}

static bool ReadNextWord(char *Dest, int DestCapacity, int *Cursor, const char *String){
	ASSERT(DestCapacity > 0);

	SkipWhitespace(Cursor, String);
	if(!isalpha(String[*Cursor])){
		return false;
	}

	int WritePos = 0;
	while(isalpha(String[*Cursor])){
		if(WritePos < DestCapacity){
			Dest[WritePos] = String[*Cursor];
			WritePos += 1;
		}
		*Cursor += 1;
	}

	bool Result = (WritePos < DestCapacity);
	if(Result){
		Dest[WritePos] = 0;
	}else{
		Dest[DestCapacity - 1] = 0;
	}

	return Result;
}

static bool ParseInterval(int *Dest, const char *String){
	// TODO(fusion): This is rather rudimentary but should work at least with
	// the values that the server is currently returning. I also assume this
	// is stable enough or it would break other client libraries as well.
	ASSERT(Dest != NULL && String != NULL);
	int Interval = 0;
	int Cursor = 0;
	bool Negate = false;
	while(true){
		SkipWhitespace(&Cursor, String);
		if(String[Cursor] == 0){
			break;
		}

		int Number;
		if(!ReadNextNumber(&Number, &Cursor, String)){
			// "Expected number"
			return false;
		}

		if(SkipNextChar(':', &Cursor, String)){
			int Minutes, Seconds;
			if(!ReadNextNumber(&Minutes, &Cursor, String)
			|| !SkipNextChar(':', &Cursor, String)
			|| !ReadNextNumber(&Seconds, &Cursor, String)){
				// "Expected HH:MM:SS.FFFFFF"
				return false;
			}

			if(Minutes < 0 || Minutes > 59){
				// "Expected minutes to be within [0, 59]"
				return false;
			}

			if(Seconds < 0 || Seconds > 59){
				// "Expected seconds to be within [0, 59]"
				return false;
			}

			// NOTE(fusion): Parse microseconds but ignore it.
			if(SkipNextChar('.', &Cursor, String)){
				int Frac;
				int PrevCursor = Cursor;
				if(!ReadNextNumber(&Frac, &Cursor, String)){
					// "Expected fractional part"
					return false;
				}

				int FracDigits = Cursor - PrevCursor;
				if(FracDigits > 6){
					// "Too many fractional digits"
					return false;
				}
			}

			Interval += (Number * 3600 + Minutes * 60 + Seconds);
		}else{
			char Unit[16];
			if(!ReadNextWord(Unit, sizeof(Unit), &Cursor, String)){
				// "Expected unit"
				return false;
			}

			// NOTE(fusion): Remove ending 's' to allow for plurals. This probably
			// doesn't work for "centuries" or "millennia" but we can adjust later.
			// Not to mention that either a single century or millennium will already
			// cause an integer overflow.
			int UnitLength = strlen(Unit);
			if(UnitLength > 1 && Unit[UnitLength - 1] == 's'){
				Unit[UnitLength - 1] = 0;
			}

			if(StringStartsWithCI("microsecond", Unit)){
				Interval += Number / 1000000;
			}else if(StringStartsWithCI("millisecond", Unit)){
				Interval += Number / 1000;
			}else if(StringStartsWithCI("second", Unit)){
				Interval += Number;
			}else if(StringStartsWithCI("minute", Unit)){
				Interval += Number * 60;
			}else if(StringStartsWithCI("hour", Unit)){
				Interval += Number * 60 * 60;
			}else if(StringStartsWithCI("day", Unit)){
				Interval += Number * 60 * 60 * 24;
			}else if(StringStartsWithCI("week", Unit)){
				Interval += Number * 60 * 60 * 24 * 7;
			}else if(StringStartsWithCI("month", Unit)){
				Interval += Number * 60 * 60 * 24 * 30;
			}else if(StringStartsWithCI("year", Unit)){
				Interval += Number * 60 * 60 * 24 * 365;
			}else if(StringStartsWithCI("decade", Unit)){
				Interval += Number * 60 * 60 * 24 * 365 * 10;
			}else if(StringStartsWithCI("century", Unit)){
				Interval += Number * 60 * 60 * 24 * 365 * 100;
			}else if(StringStartsWithCI("millennium", Unit)){
				Interval += Number * 60 * 60 * 24 * 365 * 1000;
			}else{
				// "Invalid unit"
				return false;
			}
		}

		char Direction[8];
		if(ReadNextWord(Direction, sizeof(Direction), &Cursor, String)){
			if(!StringEqCI(Direction, "ago")){
				// "Invalid interval direction"
				return false;
			}

			SkipWhitespace(&Cursor, String);
			if(String[Cursor] != 0){
				// "Interval direction is expected only at the very end"
				return false;
			}

			Negate = true;
		}
	}

	if(Negate){
		Interval = -Interval;
	}

	*Dest = Interval;
	return true;
}

static int GetResultInterval(PGresult *Result, int Row, int Col){
	int Interval = 0;
	int Format = PQfformat(Result, Col);
	Oid Type = PQftype(Result, Col);
	ASSERT(Format == 0 || Format == 1);
	if(Format == 0){       // TEXT FORMAT
		if(!ParseInterval(&Interval, PQgetvalue(Result, Row, Col))){
			LOG_ERR("Failed to parse column (%d) %s as INTERVAL",
					Col, PQfname(Result, Col));
		}
	}else if(Format == 1){ // BINARY FORMAT
		switch(Type){
			case INT8OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 8);
				int64 Interval64 = (int64)BufferRead64BE((const uint8*)PQgetvalue(Result, Row, Col));
				if(Interval64 < INT_MIN || Interval64 > INT_MAX){
					LOG_WARN("Lossy conversion of column (%d) %s from INT8 to INTERVAL",
							Col, PQfname(Result, Col));
				}

				Interval = (int)Interval64;
				break;
			}

			case INT4OID:{
				ASSERT(PQgetlength(Result, Row, Col) == 4);
				Interval = (int)BufferRead32BE((const uint8*)PQgetvalue(Result, Row, Col));
				break;
			}

			case TEXTOID:
			case VARCHAROID:{
				if(!ParseInterval(&Interval, PQgetvalue(Result, Row, Col))){
					LOG_ERR("Failed to convert column (%d) %s from TEXT to INTERVAL",
							Col, PQfname(Result, Col));
				}
				break;
			}

			case INTERVALOID:{
				ASSERT(PQgetlength(Result, Row, Col) == 16);
				const uint8 *Data = (const uint8*)PQgetvalue(Result, Row, Col);
				int64 Microseconds = (int64)BufferRead64BE(Data + 0);
				int Days = (int)BufferRead32BE(Data + 8);
				int Months = (int)BufferRead32BE(Data + 12);

				// NOTE(fusion): Split months into years for better precision. Using 30 days
				// months will reduce the number of year days to 360.
				int Years = Months / 12;
				Months = Months % 12;

				// NOTE(fusion): We just can't support the whole range of intervals
				// with a simple integer but the vast majority won't need more than
				// that. Anything else we'll just call "undefined behaviour" lol.
				int64 Interval64 = (Microseconds / 1000000)
								+ ((int64)Days * 60 * 60 * 24)
								+ ((int64)Months * 60 * 60 * 24 * 30)
								+ ((int64)Years * 60 * 60 * 24 * 365);
				if(Interval64 < INT_MIN){
					Interval = INT_MIN;
				}else if(Interval64 > INT_MAX){
					Interval = INT_MAX;
				}else{
					Interval = (int)Interval64;
				}
				break;
			}

			default:{
				LOG_ERR("Column (%d) %s has OID %d which is not convertible to INTERVAL",
						Col, PQfname(Result, Col), Type);
				break;
			}
		}
	}
	return Interval;
}

// Other Helpers
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

#if 0
	{
		// TODO(fusion): REMOVE. This is for testing query input/output, to make sure
		// they're consitent across different formats (text/binary).
		const char *Stmt = PrepareQuery(Database,
				"SELECT $1::INTERVAL, $2::INTERVAL");
		ASSERT(Stmt != NULL);

		for(int i = 0; i <= 1; i += 1)
		for(int j = 0; j <= 1; j += 1){
			LOG("TEST (%d, %d)", i, j);
			ParamBuffer Params;
			ParamBegin(&Params, 2, i);
			ParamInterval(&Params, 86400 + 3600);
			ParamInterval(&Params, - 86400 * 4 + 7 * 3600);
			PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
										Params.Values, Params.Lengths, Params.Formats, j);
			AutoResultClear ResultGuard(Result);
			if(PQresultStatus(Result) == PGRES_TUPLES_OK){
				LOG("0: %d", GetResultInterval(Result, 0, 0));
				LOG("1: %d", GetResultInterval(Result, 0, 1));
			}else{
				LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
			}
		}

		DatabaseClose(Database);
		return NULL;
	}
#endif

	return Database;
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
	ParamInt(&Params, WorldID);
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

bool AccountExists(TDatabase *Database, int AccountID, const char *Email, bool *Exists){
	ASSERT(Database != NULL && Email != NULL && Exists != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM Accounts"
			" WHERE AccountID = $1::INTEGER OR Email = $2::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, AccountID);
	ParamText(&Params, Email);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Exists = (PQntuples(Result) > 0);
	return true;
}

bool AccountNumberExists(TDatabase *Database, int AccountID, bool *Exists){
	ASSERT(Database != NULL && Exists != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM Accounts WHERE AccountID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Exists = (PQntuples(Result) > 0);
	return true;
}

bool AccountEmailExists(TDatabase *Database, const char *Email, bool *Exists){
	ASSERT(Database != NULL && Email != NULL && Exists != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM Accounts WHERE Email = $1::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamText(&Params, Email);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Exists = (PQntuples(Result) > 0);
	return true;
}

bool CreateAccount(TDatabase *Database, int AccountID, const char *Email, const uint8 *Auth, int AuthSize){
	ASSERT(Database != NULL && Email != NULL
			&& Auth != NULL && AuthSize > 0);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Accounts (AccountID, Email, Auth)"
			" VALUES ($1::INTEGER, $2::TEXT, $3::BYTEA)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 3, 1);
	ParamInt(&Params, AccountID);
	ParamText(&Params, Email);
	ParamByteA(&Params, Auth, AuthSize);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetAccountData(TDatabase *Database, int AccountID, TAccount *Account){
	ASSERT(Database != NULL && Account != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT AccountID, Email, Auth,"
				" GREATEST(PremiumEnd - CURRENT_TIMESTAMP, '0'),"
				" PendingPremiumDays, Deleted"
			" FROM Accounts WHERE AccountID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}


	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	memset(Account, 0, sizeof(TAccount));
	if(PQntuples(Result) > 0){
		uint8 Auth[sizeof(Account->Auth)];
		Account->AccountID = GetResultInt(Result, 0, 0);
		StringBufCopy(Account->Email, GetResultText(Result, 0, 1));
		if(GetResultByteA(Result, 0, 2, Auth, sizeof(Auth)) == sizeof(Auth)){
			memcpy(Account->Auth, Auth, sizeof(Auth));
		}
		Account->PremiumDays = RoundSecondsToDays(GetResultInterval(Result, 0, 3));
		Account->PendingPremiumDays = GetResultInt(Result, 0, 4);
		Account->Deleted = GetResultBool(Result, 0, 5);
	}

	return true;
}

bool GetAccountOnlineCharacters(TDatabase *Database, int AccountID, int *OnlineCharacters){
	ASSERT(Database != NULL && OnlineCharacters != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT COUNT(*) FROM Characters"
			" WHERE AccountID = $1::INTEGER AND IsOnline != 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*OnlineCharacters = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

bool IsCharacterOnline(TDatabase *Database, int CharacterID, bool *Online){
	ASSERT(Database != NULL && Online != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT IsOnline FROM Characters WHERE CharacterID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Online = (PQntuples(Result) > 0 && GetResultInt(Result, 0, 0) != 0);
	return true;
}

bool ActivatePendingPremiumDays(TDatabase *Database, int AccountID){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"UPDATE Accounts"
			" SET PremiumEnd = GREATEST(PremiumEnd, CURRENT_TIMESTAMP)"
						" + MAKE_INTERVAL(days => PendingPremiumDays),"
				" PendingPremiumDays = 0"
			" WHERE AccountID = $1::INTEGER AND PendingPremiumDays > 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetCharacterEndpoints(TDatabase *Database, int AccountID, DynamicArray<TCharacterEndpoint> *Characters){
	ASSERT(Database != NULL && Characters != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT C.Name, W.Name, W.Host, W.Port"
			" FROM Characters AS C"
			" INNER JOIN Worlds AS W ON W.WorldID = C.WorldID"
			" WHERE C.AccountID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		int WorldAddress;
		const char *CharacterName = GetResultText(Result, Row, 0);
		const char *WorldName = GetResultText(Result, Row, 1);
		const char *HostName = GetResultText(Result, Row, 2);
		if(HostName == NULL || !ResolveHostName(HostName, &WorldAddress)){
			LOG_ERR("Failed to resolve world \"%s\" host name \"%s\" for character \"%s\"",
					WorldName, HostName, CharacterName);
			continue;
		}

		TCharacterEndpoint Character = {};
		StringBufCopy(Character.Name, CharacterName);
		StringBufCopy(Character.WorldName, WorldName);
		Character.WorldAddress = WorldAddress;
		Character.WorldPort = GetResultInt(Result, Row, 3);
		Characters->Push(Character);

	}

	return true;
}

bool GetCharacterSummaries(TDatabase *Database, int AccountID, DynamicArray<TCharacterSummary> *Characters){
	ASSERT(Database != NULL && Characters != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT C.Name, W.Name, C.Level, C.Profession, C.IsOnline, C.Deleted"
			" FROM Characters AS C"
			" LEFT JOIN Worlds AS W ON W.WorldID = C.WorldID"
			" WHERE C.AccountID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		TCharacterSummary Character = {};
		StringBufCopy(Character.Name, GetResultText(Result, Row, 0));
		StringBufCopy(Character.World, GetResultText(Result, Row, 1));
		Character.Level = GetResultInt(Result, Row, 2);
		StringBufCopy(Character.Profession, GetResultText(Result, Row, 3));
		Character.Online = GetResultBool(Result, Row, 4);
		Character.Deleted = GetResultBool(Result, Row, 5);;
		Characters->Push(Character);
	}

	return true;
}

bool CharacterNameExists(TDatabase *Database, const char *Name, bool *Exists){
	ASSERT(Database != NULL && Exists != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM Characters WHERE Name = $1::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamText(&Params, Name);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Exists = (PQntuples(Result) > 0);
	return true;
}

bool CreateCharacter(TDatabase *Database, int WorldID, int AccountID, const char *Name, int Sex){
	ASSERT(Database != NULL && Name != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Characters (WorldID, AccountID, Name, Sex)"
			" VALUES ($1::INTEGER, $2::INTEGER, $3::TEXT, $4::INTEGER)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 4, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, AccountID);
	ParamText(&Params, Name);
	ParamInt(&Params, Sex);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetCharacterID(TDatabase *Database, int WorldID, const char *CharacterName, int *CharacterID){
	ASSERT(Database != NULL && CharacterName != NULL && CharacterID != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT CharacterID FROM Characters"
			" WHERE WorldID = $1::INTEGER AND Name = $2::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamText(&Params, CharacterName);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*CharacterID = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

bool GetCharacterLoginData(TDatabase *Database, const char *CharacterName, TCharacterLoginData *Character){
	ASSERT(Database != NULL && CharacterName != NULL && Character != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT WorldID, CharacterID, AccountID, Name,"
				" Sex, Guild, Rank, Title, Deleted"
			" FROM Characters WHERE Name = $1::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamText(&Params, CharacterName);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	memset(Character, 0, sizeof(TCharacterLoginData));
	if(PQntuples(Result) > 0){
		Character->WorldID = GetResultInt(Result, 0, 0);
		Character->CharacterID = GetResultInt(Result, 0, 1);
		Character->AccountID = GetResultInt(Result, 0, 2);
		StringBufCopy(Character->Name, GetResultText(Result, 0, 3));
		Character->Sex = GetResultInt(Result, 0, 4);
		StringBufCopy(Character->Guild, GetResultText(Result, 0, 5));
		StringBufCopy(Character->Rank, GetResultText(Result, 0, 6));
		StringBufCopy(Character->Title, GetResultText(Result, 0, 7));
		Character->Deleted = GetResultBool(Result, 0, 8);
	}

	return true;
}

bool GetCharacterProfile(TDatabase *Database, const char *CharacterName, TCharacterProfile *Character){
	ASSERT(Database != NULL && CharacterName != NULL && Character != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT C.Name, W.Name, C.Sex, C.Guild, C.Rank, C.Title, C.Level,"
				" C.Profession, C.Residence, C.LastLoginTime, C.IsOnline,"
				" C.Deleted, GREATEST(A.PremiumEnd - CURRENT_TIMESTAMP, '0')"
			" FROM Characters AS C"
			" LEFT JOIN Worlds AS W ON W.WorldID = C.WorldID"
			" LEFT JOIN Accounts AS A ON A.AccountID = C.AccountID"
			" LEFT JOIN CharacterRights AS R"
				" ON R.CharacterID = C.CharacterID"
				" AND R.Name = 'NO_STATISTICS'"
			" WHERE C.Name = $1::TEXT AND R.Name IS NULL");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamText(&Params, CharacterName);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	memset(Character, 0, sizeof(TCharacterProfile));
	if(PQntuples(Result) > 0){
		StringBufCopy(Character->Name, GetResultText(Result, 0, 0));
		StringBufCopy(Character->World, GetResultText(Result, 0, 1));
		Character->Sex = GetResultInt(Result, 0, 2);
		StringBufCopy(Character->Guild, GetResultText(Result, 0, 3));
		StringBufCopy(Character->Rank, GetResultText(Result, 0, 4));
		StringBufCopy(Character->Title, GetResultText(Result, 0, 5));
		Character->Level = GetResultInt(Result, 0, 6);
		StringBufCopy(Character->Profession, GetResultText(Result, 0, 7));
		StringBufCopy(Character->Residence, GetResultText(Result, 0, 8));
		Character->LastLogin = GetResultTimestamp(Result, 0, 9);
		Character->Online = GetResultBool(Result, 0, 10);
		Character->Deleted = GetResultBool(Result, 0, 11);
		Character->PremiumDays = RoundSecondsToDays(GetResultInterval(Result, 0, 12));
	}

	return true;
}

bool GetCharacterRight(TDatabase *Database, int CharacterID, const char *Right, bool *HasRight){
	ASSERT(Database != NULL && Right != NULL && HasRight != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM CharacterRights"
			" WHERE CharacterID = $1::INTEGER AND Name = $2::TEXT");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, CharacterID);
	ParamText(&Params, Right);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*HasRight = (PQntuples(Result) > 0);
	return true;
}

bool GetCharacterRights(TDatabase *Database, int CharacterID, DynamicArray<TCharacterRight> *Rights){
	ASSERT(Database != NULL && Rights != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT Name FROM CharacterRights WHERE CharacterID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		TCharacterRight Right = {};
		StringBufCopy(Right.Name, GetResultText(Result, Row, 0));
		Rights->Push(Right);
	}

	return true;
}

bool GetGuildLeaderStatus(TDatabase *Database, int WorldID, int CharacterID, bool *GuildLeader){
	ASSERT(Database != NULL && GuildLeader != NULL);
	// NOTE(fusion): Same as `DecrementIsOnline`.
	const char *Stmt = PrepareQuery(Database,
			"SELECT Guild, Rank FROM Characters"
			" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*GuildLeader = false;
	if(PQntuples(Result) > 0){
		const char *Guild = GetResultText(Result, 0, 0);
		const char *Rank = GetResultText(Result, 0, 1);
		if(Guild != NULL && !StringEmpty(Guild) && Rank != NULL && StringEqCI(Rank, "Leader")){
			*GuildLeader = true;
		}
	}

	return false;
}

bool IncrementIsOnline(TDatabase *Database, int WorldID, int CharacterID){
	ASSERT(Database != NULL);
	// NOTE(fusion): Same as `DecrementIsOnline`.
	const char *Stmt = PrepareQuery(Database,
			"UPDATE Characters SET IsOnline = IsOnline + 1"
			" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool DecrementIsOnline(TDatabase *Database, int WorldID, int CharacterID){
	ASSERT(Database != NULL);
	// NOTE(fusion): A character is uniquely identified by its id. The world id
	// check is purely to avoid a world from modifying a character from another
	// world.
	const char *Stmt = PrepareQuery(Database,
			"UPDATE Characters SET IsOnline = IsOnline - 1"
			" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool ClearIsOnline(TDatabase *Database, int WorldID, int *NumAffectedCharacters){
	ASSERT(Database != NULL && NumAffectedCharacters != NULL);
	const char *Stmt = PrepareQuery(Database,
			"UPDATE Characters SET IsOnline = 0"
			" WHERE WorldID = $1::INTEGER AND IsOnline != 0");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*NumAffectedCharacters = ResultAffectedRows(Result);
	return true;
}

bool LogoutCharacter(TDatabase *Database, int WorldID, int CharacterID, int Level,
		const char *Profession, const char *Residence, int LastLoginTime, int TutorActivities){
	ASSERT(Database != NULL && Profession != NULL && Residence != NULL);
	const char *Stmt = PrepareQuery(Database,
			"UPDATE Characters"
			" SET Level = $3::INTEGER,"
				" Profession = $4::TEXT,"
				" Residence = $5::TEXT,"
				" LastLoginTime = $6::TIMESTAMPTZ,"
				" TutorActivities = $7::INTEGER,"
				" IsOnline = IsOnline - 1"
			" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 7, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	ParamInt(&Params, Level);
	ParamText(&Params, Profession);
	ParamText(&Params, Residence);
	ParamTimestamp(&Params, LastLoginTime);
	ParamInt(&Params, TutorActivities);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetCharacterIndexEntries(TDatabase *Database, int WorldID, int MinimumCharacterID,
		int MaxEntries, int *NumEntries, TCharacterIndexEntry *Entries){
	ASSERT(Database != NULL && MaxEntries > 0 && NumEntries != NULL && Entries != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT CharacterID, Name FROM Characters"
			" WHERE WorldID = $1::INTEGER AND CharacterID >= $2::INTEGER"
			" ORDER BY CharacterID ASC LIMIT $3::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 3, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, MinimumCharacterID);
	ParamInt(&Params, MaxEntries);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	if(NumRows > MaxEntries){
		LOG_WARN("Query returned too many rows (expected %d, got %d)", MaxEntries, NumRows);
		NumRows = MaxEntries;
	}

	for(int Row = 0; Row < NumRows; Row += 1){
		Entries[Row].CharacterID = GetResultInt(Result, Row, 0);
		StringBufCopy(Entries[Row].Name, GetResultText(Result, Row, 1));
	}

	*NumEntries = NumRows;
	return true;
}

bool InsertCharacterDeath(TDatabase *Database, int WorldID, int CharacterID, int Level,
		int OffenderID, const char *Remark, bool Unjustified, int Timestamp){
	ASSERT(Database != NULL && Remark != NULL);
	// NOTE(fusion): Same as `DecrementIsOnline`.
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO CharacterDeaths (CharacterID, Level,"
				" OffenderID, Remark, Unjustified, Timestamp)"
			" SELECT $2::INTEGER, $3::INTEGER, $4::INTEGER,"
					"$5::TEXT, $6::BOOLEAN, $7::TIMESTAMPTZ"
				" FROM Characters"
				" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 7, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	ParamInt(&Params, Level);
	ParamInt(&Params, OffenderID);
	ParamText(&Params, Remark);
	ParamBool(&Params, Unjustified);
	ParamTimestamp(&Params, Timestamp);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool InsertBuddy(TDatabase *Database, int WorldID, int AccountID, int BuddyID){
	ASSERT(Database != NULL);
	// NOTE(fusion): Same as `DecrementIsOnline`.
	// NOTE(fusion): Use the `DO NOTHING` conflict resolution to make duplicate
	// row errors appear as successful insertions.
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Buddies (WorldID, AccountID, BuddyID)"
			" SELECT $1::INTEGER, $2::INTEGER, $3::INTEGER FROM Characters"
				" WHERE WorldID = $1::INTEGER AND CharacterID = $3::INTEGER"
			" ON CONFLICT DO NOTHING");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 3, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, AccountID);
	ParamInt(&Params, BuddyID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool DeleteBuddy(TDatabase *Database, int WorldID, int AccountID, int BuddyID){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"DELETE FROM Buddies"
			" WHERE WorldID = $1::INTEGER"
				" AND AccountID = $2::INTEGER"
				" AND BuddyID = $3::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 3, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, AccountID);
	ParamInt(&Params, BuddyID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetBuddies(TDatabase *Database, int WorldID, int AccountID, DynamicArray<TAccountBuddy> *Buddies){
	ASSERT(Database != NULL && Buddies != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT B.BuddyID, C.Name"
			" FROM Buddies AS B"
			" INNER JOIN Characters AS C"
				" ON C.WorldID = B.WorldID AND C.CharacterID = B.BuddyID"
			" WHERE B.WorldID = $1::INTEGER AND B.AccountID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		TAccountBuddy Buddy = {};
		Buddy.CharacterID = GetResultInt(Result, Row, 0);
		StringBufCopy(Buddy.Name, GetResultText(Result, Row, 1));
		Buddies->Push(Buddy);
	}

	return true;
}

bool GetWorldInvitation(TDatabase *Database, int WorldID, int CharacterID, bool *Invited){
	ASSERT(Database != NULL && Invited != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM WorldInvitations"
			" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Invited = (PQntuples(Result) > 0);
	return true;
}

bool InsertLoginAttempt(TDatabase *Database, int AccountID, int IPAddress, bool Failed){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO LoginAttempts (AccountID, IPAddress, Timestamp, Failed)"
			" VALUES ($1::INTEGER, $2::INET, CURRENT_TIMESTAMP, $3::BOOLEAN)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 3, 1);
	ParamInt(&Params, AccountID);
	ParamIPAddress(&Params, IPAddress);
	ParamBool(&Params, Failed);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetAccountFailedLoginAttempts(TDatabase *Database, int AccountID, int TimeWindow, int *FailedAttempts){
	ASSERT(Database != NULL && FailedAttempts != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT COUNT(*) FROM LoginAttempts"
			" WHERE AccountID = $1::INTEGER"
				" AND (CURRENT_TIMESTAMP - Timestamp) <= $2::INTERVAL"
				" AND Failed");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, AccountID);
	ParamInterval(&Params, TimeWindow);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*FailedAttempts = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

bool GetIPAddressFailedLoginAttempts(TDatabase *Database, int IPAddress, int TimeWindow, int *FailedAttempts){
	ASSERT(Database != NULL && FailedAttempts != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT COUNT(*) FROM LoginAttempts"
			" WHERE IPAddress = $1::INET"
				" AND (CURRENT_TIMESTAMP - Timestamp) <= $2::INTERVAL"
				" AND Failed");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamIPAddress(&Params, IPAddress);
	ParamInterval(&Params, TimeWindow);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*FailedAttempts = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

// House Tables
//==============================================================================
bool FinishHouseAuctions(TDatabase *Database, int WorldID, DynamicArray<THouseAuction> *Auctions){
	ASSERT(Database != NULL && Auctions != NULL);
	// TODO(fusion): If the application crashes while processing finished auctions,
	// non processed auctions will be lost but with no other side-effects. It could
	// be an inconvenience but it's not a big problem.
	const char *Stmt = PrepareQuery(Database,
			"DELETE FROM HouseAuctions"
			" WHERE WorldID = $1::INTEGER"
				" AND FinishTime IS NOT NULL"
				" AND FinishTime <= CURRENT_TIMESTAMP"
			" RETURNING HouseID, BidderID, BidAmount, FinishTime,"
				" (SELECT Name FROM Characters WHERE CharacterID = BidderID)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		THouseAuction Auction = {};
		Auction.HouseID = GetResultInt(Result, Row, 0);
		Auction.BidderID = GetResultInt(Result, Row, 1);
		Auction.BidAmount = GetResultInt(Result, Row, 2);
		Auction.FinishTime = GetResultTimestamp(Result, Row, 3);
		StringBufCopy(Auction.BidderName, GetResultText(Result, Row, 4));
		Auctions->Push(Auction);
	}

	return true;
}

bool FinishHouseTransfers(TDatabase *Database, int WorldID, DynamicArray<THouseTransfer> *Transfers){
	ASSERT(Database != NULL && Transfers != NULL);
	// TODO(fusion): Same as `FinishHouseAuctions` but with house transfers.
	const char *Stmt = PrepareQuery(Database,
			"DELETE FROM HouseTransfers"
			" WHERE WorldID = $1::INTEGER"
			" RETURNING HouseID, NewOwnerID, Price,"
				" (SELECT Name FROM Characters WHERE CharacterID = NewOwnerID)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		THouseTransfer Transfer = {};
		Transfer.HouseID = GetResultInt(Result, Row, 0);
		Transfer.NewOwnerID = GetResultInt(Result, Row, 1);
		Transfer.Price = GetResultInt(Result, Row, 2);
		StringBufCopy(Transfer.NewOwnerName, GetResultText(Result, Row, 4));
		Transfers->Push(Transfer);
	}

	return true;
}

bool GetFreeAccountEvictions(TDatabase *Database, int WorldID, DynamicArray<THouseEviction> *Evictions){
	ASSERT(Database != NULL && Evictions != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT O.HouseID, O.OwnerID"
			" FROM HouseOwners AS O"
			" LEFT JOIN Characters AS C ON C.CharacterID = O.OwnerID"
			" LEFT JOIN Accounts AS A ON A.AccountID = C.AccountID"
			" WHERE O.WorldID = $1::INTEGER"
				" AND (A.PremiumEnd IS NULL OR A.PremiumEnd < CURRENT_TIMESTAMP)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		THouseEviction Eviction = {};
		Eviction.HouseID = GetResultInt(Result, Row, 0);
		Eviction.OwnerID = GetResultInt(Result, Row, 1);
		Evictions->Push(Eviction);
	}

	return true;
}

bool GetDeletedCharacterEvictions(TDatabase *Database, int WorldID, DynamicArray<THouseEviction> *Evictions){
	ASSERT(Database != NULL && Evictions != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT O.HouseID, O.OwnerID"
			" FROM HouseOwners AS O"
			" LEFT JOIN Characters AS C ON C.CharacterID = O.OwnerID"
			" WHERE O.WorldID = $1::INTEGER"
				" AND (C.CharacterID IS NULL OR C.Deleted)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		THouseEviction Eviction = {};
		Eviction.HouseID = GetResultInt(Result, Row, 0);
		Eviction.OwnerID = GetResultInt(Result, Row, 1);
		Evictions->Push(Eviction);
	}

	return true;
}

bool InsertHouseOwner(TDatabase *Database, int WorldID, int HouseID, int OwnerID, int PaidUntil){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO HouseOwners (WorldID, HouseID, OwnerID, PaidUntil)"
			" VALUES ($1::INTEGER, $2::INTEGER, $3::INTEGER, $4::TIMESTAMPTZ)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 4, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, HouseID);
	ParamInt(&Params, OwnerID);
	ParamTimestamp(&Params, PaidUntil);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool UpdateHouseOwner(TDatabase *Database, int WorldID, int HouseID, int OwnerID, int PaidUntil){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"UPDATE HouseOwners"
			" SET OwnerID = $3::INTEGER, PaidUntil = $4::TIMESTAMPTZ"
			" WHERE WorldID = $1::INTEGER AND HouseID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 4, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, HouseID);
	ParamInt(&Params, OwnerID);
	ParamTimestamp(&Params, PaidUntil);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool DeleteHouseOwner(TDatabase *Database, int WorldID, int HouseID){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"DELETE FROM HouseOwners"
			" WHERE WorldID = $1::INTEGER AND HouseID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, HouseID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool GetHouseOwners(TDatabase *Database, int WorldID, DynamicArray<THouseOwner> *Owners){
	ASSERT(Database != NULL && Owners != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT O.HouseID, O.OwnerID, C.Name, O.PaidUntil"
			" FROM HouseOwners AS O"
			" LEFT JOIN Characters AS C ON C.CharacterID = O.OwnerID"
			" WHERE O.WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		THouseOwner Owner = {};
		Owner.HouseID = GetResultInt(Result, Row, 0);
		Owner.OwnerID = GetResultInt(Result, Row, 1);
		StringBufCopy(Owner.OwnerName, GetResultText(Result, Row, 2));
		Owner.PaidUntil = GetResultTimestamp(Result, Row, 3);
		Owners->Push(Owner);
	}

	return true;
}

bool GetHouseAuctions(TDatabase *Database, int WorldID, DynamicArray<int> *Auctions){
	ASSERT(Database != NULL && Auctions != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT HouseID FROM HouseAuctions WHERE WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		Auctions->Push(GetResultInt(Result, Row, 0));
	}

	return true;
}

bool StartHouseAuction(TDatabase *Database, int WorldID, int HouseID){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO HouseAuctions (WorldID, HouseID)"
			" VALUES ($1::INTEGER, $2::INTEGER)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, HouseID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool DeleteHouses(TDatabase *Database, int WorldID){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"DELETE FROM Houses WHERE WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool InsertHouses(TDatabase *Database, int WorldID, int NumHouses, THouse *Houses){
	ASSERT(Database != NULL && NumHouses > 0 && Houses != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Houses (WorldID, HouseID, Name, Rent, Description,"
				" Size, PositionX, PositionY, PositionZ, Town, GuildHouse)"
			" VALUES ($1::INTEGER, $2::INTEGER, $3::TEXT, $4::INTEGER, $5::TEXT,"
				" $6::INTEGER, $7::INTEGER, $8::INTEGER, $9::INTEGER, $10::TEXT,"
				" $11::BOOLEAN)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	// TODO(fusion): We could use pipelining to speed up multiple inserts but I'm
	// not sure the complexity increase would warrant any speed-ups.
	ParamBuffer Params = {};
	for(int i = 0; i < NumHouses; i += 1){
		ParamBegin(&Params, 11, 1);
		ParamInt(&Params, WorldID);
		ParamInt(&Params, Houses[i].HouseID);
		ParamText(&Params, Houses[i].Name);
		ParamInt(&Params, Houses[i].Rent);
		ParamText(&Params, Houses[i].Description);
		ParamInt(&Params, Houses[i].Size);
		ParamInt(&Params, Houses[i].PositionX);
		ParamInt(&Params, Houses[i].PositionY);
		ParamInt(&Params, Houses[i].PositionZ);
		ParamText(&Params, Houses[i].Town);
		ParamBool(&Params, Houses[i].GuildHouse);
		PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
								Params.Values, Params.Lengths, Params.Formats, 1);
		AutoResultClear ResultGuard(Result);
		if(PQresultStatus(Result) != PGRES_COMMAND_OK){
			LOG_ERR("Failed to insert house %d: %s",
					Houses[i].HouseID,
					PQerrorMessage(Database->Handle));
			return false;
		}
	}

	return true;
}

bool ExcludeFromAuctions(TDatabase *Database, int WorldID, int CharacterID, int Duration, int BanishmentID){
	ASSERT(Database != NULL);
	// NOTE(fusion): Same as `DecrementIsOnline`.
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO HouseAuctionExclusions (CharacterID, Issued, Until, BanishmentID)"
			" SELECT $2::INTEGER, CURRENT_TIMESTAMP, (CURRENT_TIMESTAMP + $3::INTERVAL), $4::INTEGER"
				" FROM Characters"
				" WHERE WorldID = $1::INTEGER AND CharacterID = $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 4, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, CharacterID);
	ParamInterval(&Params, Duration);
	ParamInt(&Params, BanishmentID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

// Banishment Tables
//==============================================================================
bool IsCharacterNamelocked(TDatabase *Database, int CharacterID, bool *Namelocked){
	ASSERT(Database != NULL && Namelocked != NULL);
	TNamelockStatus Status;
	if(!GetNamelockStatus(Database, CharacterID, &Status)){
		return false;
	}

	*Namelocked = Status.Namelocked && !Status.Approved;
	return true;
}

bool GetNamelockStatus(TDatabase *Database, int CharacterID, TNamelockStatus *Status){
	ASSERT(Database != NULL && Status != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT Approved FROM Namelocks WHERE CharacterID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	memset(Status, 0, sizeof(TNamelockStatus));
	Status->Namelocked = (PQntuples(Result) > 0);
	if(Status->Namelocked){
		Status->Approved = GetResultBool(Result, 0, 0);
	}

	return true;
}

bool InsertNamelock(TDatabase *Database, int CharacterID, int IPAddress,
		int GamemasterID, const char *Reason, const char *Comment){
	ASSERT(Database != NULL && Reason != NULL && Comment != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Namelocks (CharacterID, IPAddress, GamemasterID, Reason, Comment)"
			" VALUES ($1::INTEGER, $2::INET, $3::INTEGER, $4::TEXT, $5::TEXT)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 5, 1);
	ParamInt(&Params, CharacterID);
	ParamIPAddress(&Params, IPAddress);
	ParamInt(&Params, GamemasterID);
	ParamText(&Params, Reason);
	ParamText(&Params, Comment);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool IsAccountBanished(TDatabase *Database, int AccountID, bool *Banished){
	ASSERT(Database != NULL && Banished != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM Banishments"
			" WHERE AccountID = $1::INTEGER"
				" AND (Until = Issued OR Until > CURRENT_TIMESTAMP)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, AccountID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Banished = (PQntuples(Result) > 0);
	return true;
}

bool GetBanishmentStatus(TDatabase *Database, int CharacterID, TBanishmentStatus *Status){
	ASSERT(Database != NULL && Status != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT B.FinalWarning, (B.Until = B.Issued OR B.Until > CURRENT_TIMESTAMP)"
			" FROM Banishments AS B"
			" LEFT JOIN Characters AS C ON C.AccountID = B.AccountID"
			" WHERE C.CharacterID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = 0;
	memset(Status, 0, sizeof(TBanishmentStatus));
	for(int Row = 0; Row < NumRows; Row += 1){
		Status->TimesBanished += 1;

		if(GetResultBool(Result, Row, 0)){
			Status->FinalWarning = true;
		}

		if(GetResultBool(Result, Row, 1)){
			Status->Banished = true;
		}
	}

	return true;
}

bool InsertBanishment(TDatabase *Database, int CharacterID, int IPAddress, int GamemasterID,
		const char *Reason, const char *Comment, bool FinalWarning, int Duration, int *BanishmentID){
	ASSERT(Database != NULL && Reason != NULL && Comment != NULL && BanishmentID != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Banishments (AccountID, IPAddress, GamemasterID,"
				" Reason, Comment, FinalWarning, Issued, Until)"
			" SELECT AccountID, $2::INET, $3::INTEGER, $4::TEXT, $5::TEXT,"
					" $6::BOOLEAN, CURRENT_TIMESTAMP, (CURRENT_TIMESTAMP + $7::INTERVAL)"
				" FROM Characters WHERE CharacterID = $1::INTEGER"
			" RETURNING BanishmentID");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 7, 1);
	ParamInt(&Params, CharacterID);
	ParamIPAddress(&Params, IPAddress);
	ParamInt(&Params, GamemasterID);
	ParamText(&Params, Reason);
	ParamText(&Params, Comment);
	ParamBool(&Params, FinalWarning);
	ParamInterval(&Params, Duration);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*BanishmentID = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

bool GetNotationCount(TDatabase *Database, int CharacterID, int *Notations){
	ASSERT(Database != NULL && Notations != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT COUNT(*) FROM Notations WHERE CharacterID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, CharacterID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Notations = (PQntuples(Result) > 0 ? GetResultInt(Result, 0, 0) : 0);
	return true;
}

bool InsertNotation(TDatabase *Database, int CharacterID, int IPAddress,
		int GamemasterID, const char *Reason, const char *Comment){
	ASSERT(Database != NULL && Reason != NULL && Comment != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Notations (CharacterID, IPAddress, GamemasterID, Reason, Comment)"
			" VALUES ($1::INTEGER, $2::INET, $3::INTEGER, $4::TEXT, $5::TEXT)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 5, 1);
	ParamInt(&Params, CharacterID);
	ParamIPAddress(&Params, IPAddress);
	ParamInt(&Params, GamemasterID);
	ParamText(&Params, Reason);
	ParamText(&Params, Comment);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool IsIPBanished(TDatabase *Database, int IPAddress, bool *Banished){
	ASSERT(Database != NULL && Banished != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM IPBanishments"
			" WHERE IPAddress = $1::INET"
				" AND (Until = Issued OR Until > CURRENT_TIMESTAMP)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamIPAddress(&Params, IPAddress);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Banished = (PQntuples(Result) > 0);
	return true;
}

bool InsertIPBanishment(TDatabase *Database, int CharacterID, int IPAddress,
		int GamemasterID, const char *Reason, const char *Comment, int Duration){
	ASSERT(Database != NULL && Reason != NULL && Comment != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO IPBanishments (CharacterID, IPAddress,"
				" GamemasterID, Reason, Comment, Issued, Until)"
			" VALUES ($1::INTEGER, $2::INET, $3::INTEGER, $4::TEXT,"
				" $5::TEXT, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + $6::INTERVAL)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 6, 1);
	ParamInt(&Params, CharacterID);
	ParamIPAddress(&Params, IPAddress);
	ParamInt(&Params, GamemasterID);
	ParamText(&Params, Reason);
	ParamText(&Params, Comment);
	ParamInterval(&Params, Duration);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool IsStatementReported(TDatabase *Database, int WorldID, TStatement *Statement, bool *Reported){
	ASSERT(Database != NULL && Statement != NULL && Reported != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT 1 FROM Statements"
			" WHERE WorldID = $1::INTEGER"
				" AND Timestamp = $2::TIMESTAMPTZ"
				" AND StatementID = $3::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 3, 1);
	ParamInt(&Params, WorldID);
	ParamTimestamp(&Params, Statement->Timestamp);
	ParamInt(&Params, Statement->StatementID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*Reported = (PQntuples(Result) > 0);
	return true;
}

bool InsertStatements(TDatabase *Database, int WorldID, int NumStatements, TStatement *Statements){
	// NOTE(fusion): Use the `DO NOTHING` conflict resolution because different
	// reports may include the same statements for context and I assume it's not
	// uncommon to see overlaps.
	ASSERT(Database != NULL && NumStatements > 0 && Statements != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO Statements (WorldID, Timestamp, StatementID, CharacterID, Channel, Text)"
			" VALUES ($1::INTEGER, $2::TIMESTAMPTZ, $3::INTEGER, $4::INTEGER, $5::TEXT, $6::TEXT)"
			" ON CONFLICT DO NOTHING");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	for(int i = 0; i < NumStatements; i += 1){
		if(Statements[i].StatementID == 0){
			LOG_WARN("Skipping statement without id");
			continue;
		}

		ParamBegin(&Params, 6, 1);
		ParamInt(&Params, WorldID);
		ParamTimestamp(&Params, Statements[i].Timestamp);
		ParamInt(&Params, Statements[i].StatementID);
		ParamInt(&Params, Statements[i].CharacterID);
		ParamText(&Params, Statements[i].Channel);
		ParamText(&Params, Statements[i].Text);
		PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
								Params.Values, Params.Lengths, Params.Formats, 1);
		AutoResultClear ResultGuard(Result);
		if(PQresultStatus(Result) != PGRES_COMMAND_OK){
			LOG_ERR("Failed to insert statement %d: %s",
					Statements[i].StatementID,
					PQerrorMessage(Database->Handle));
			return false;
		}
	}

	return true;
}

bool InsertReportedStatement(TDatabase *Database, int WorldID, TStatement *Statement,
		int BanishmentID, int ReporterID, const char *Reason, const char *Comment){
	ASSERT(Database != NULL && Statement != NULL && Reason != NULL && Comment != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO ReportedStatements (WorldID, Timestamp, StatementID,"
				" CharacterID, BanishmentID, ReporterID, Reason, Comment)"
			" VALUES ($1::INTEGER, $2::TIMESTAMPTZ, $3::INTEGER, $4::INTEGER,"
				" $5::INTEGER, $6::INTEGER, $7::TEXT, $8::TEXT)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 8, 1);
	ParamInt(&Params, WorldID);
	ParamTimestamp(&Params, Statement->Timestamp);
	ParamInt(&Params, Statement->StatementID);
	ParamInt(&Params, Statement->CharacterID);
	ParamInt(&Params, BanishmentID);
	ParamInt(&Params, ReporterID);
	ParamText(&Params, Reason);
	ParamText(&Params, Comment);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

// Info Tables
//==============================================================================
bool GetKillStatistics(TDatabase *Database, int WorldID, DynamicArray<TKillStatistics> *Stats){
	ASSERT(Database != NULL && Stats != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT RaceName, TimesKilled, PlayersKilled"
			" FROM KillStatistics WHERE WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		TKillStatistics Entry = {};
		StringBufCopy(Entry.RaceName, GetResultText(Result, Row, 0));
		Entry.TimesKilled = GetResultInt(Result, Row, 1);
		Entry.PlayersKilled = GetResultInt(Result, Row, 2);
		Stats->Push(Entry);
	}

	return true;
}

bool MergeKillStatistics(TDatabase *Database, int WorldID, int NumStats, TKillStatistics *Stats){
	ASSERT(Database != NULL && NumStats > 0 && Stats != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO KillStatistics (WorldID, RaceName, TimesKilled, PlayersKilled)"
			" VALUES ($1::INTEGER, $2::TEXT, $3::INTEGER, $4::INTEGER)"
			" ON CONFLICT (WorldID, RaceName)"
				" DO UPDATE SET TimesKilled = KillStatistics.TimesKilled + EXCLUDED.TimesKilled,"
						" PlayersKilled = KillStatistics.PlayersKilled + EXCLUDED.PlayersKilled");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	for(int i = 0; i < NumStats; i += 1){
		ParamBegin(&Params, 4, 1);
		ParamInt(&Params, WorldID);
		ParamText(&Params, Stats[i].RaceName);
		ParamInt(&Params, Stats[i].TimesKilled);
		ParamInt(&Params, Stats[i].PlayersKilled);
		PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
								Params.Values, Params.Lengths, Params.Formats, 1);
		AutoResultClear ResultGuard(Result);
		if(PQresultStatus(Result) != PGRES_COMMAND_OK){
			LOG_ERR("Failed to merge \"%s\" stats: %s",
		   			Stats[i].RaceName,
					PQerrorMessage(Database->Handle));
			return false;
		}
	}

	return true;
}

bool GetOnlineCharacters(TDatabase *Database, int WorldID, DynamicArray<TOnlineCharacter> *Characters){
	ASSERT(Database != NULL && Characters != NULL);
	const char *Stmt = PrepareQuery(Database,
			"SELECT Name, Level, Profession"
			" FROM OnlineCharacters WHERE WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_TUPLES_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	int NumRows = PQntuples(Result);
	for(int Row = 0; Row < NumRows; Row += 1){
		TOnlineCharacter Character = {};
		StringBufCopy(Character.Name, GetResultText(Result, Row, 0));
		Character.Level = GetResultInt(Result, Row, 1);
		StringBufCopy(Character.Profession, GetResultText(Result, Row, 2));
		Characters->Push(Character);
	}

	return true;
}

bool DeleteOnlineCharacters(TDatabase *Database, int WorldID){
	ASSERT(Database != NULL);
	const char *Stmt = PrepareQuery(Database,
			"DELETE FROM OnlineCharacters WHERE WorldID = $1::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 1, 1);
	ParamInt(&Params, WorldID);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	return true;
}

bool InsertOnlineCharacters(TDatabase *Database, int WorldID,
		int NumCharacters, TOnlineCharacter *Characters){
	ASSERT(Database != NULL && Characters != NULL);
	const char *Stmt = PrepareQuery(Database,
			"INSERT INTO OnlineCharacters (WorldID, Name, Level, Profession)"
			" VALUES ($1::INTEGER, $2::TEXT, $3::INTEGER, $4::TEXT)");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	for(int i = 0; i < NumCharacters; i += 1){
		ParamBegin(&Params, 4, 1);
		ParamInt(&Params, WorldID);
		ParamText(&Params, Characters[i].Name);
		ParamInt(&Params, Characters[i].Level);
		ParamText(&Params, Characters[i].Profession);
		PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
								Params.Values, Params.Lengths, Params.Formats, 1);
		AutoResultClear ResultGuard(Result);
		if(PQresultStatus(Result) != PGRES_COMMAND_OK){
			LOG_ERR("Failed to insert character \"%s\": %s",
					Characters[i].Name,
					PQerrorMessage(Database->Handle));
			return false;
		}
	}

	return true;
}

bool CheckOnlineRecord(TDatabase *Database, int WorldID, int NumCharacters, bool *NewRecord){
	ASSERT(Database != NULL && NewRecord != NULL);
	const char *Stmt = PrepareQuery(Database,
			"UPDATE Worlds SET OnlineRecord = $2::INTEGER,"
				" OnlineRecordTimestamp = CURRENT_TIMESTAMP"
			" WHERE WorldID = $1::INTEGER AND OnlineRecord < $2::INTEGER");
	if(Stmt == NULL){
		LOG_ERR("Failed to prepare query");
		return false;
	}

	ParamBuffer Params = {};
	ParamBegin(&Params, 2, 1);
	ParamInt(&Params, WorldID);
	ParamInt(&Params, NumCharacters);
	PGresult *Result = PQexecPrepared(Database->Handle, Stmt, Params.NumParams,
							Params.Values, Params.Lengths, Params.Formats, 1);
	AutoResultClear ResultGuard(Result);
	if(PQresultStatus(Result) != PGRES_COMMAND_OK){
		LOG_ERR("Failed to execute query: %s", PQerrorMessage(Database->Handle));
		return false;
	}

	*NewRecord = (ResultAffectedRows(Result) > 0);
	return true;
}

#endif //DATABASE_POSTGRESQL
