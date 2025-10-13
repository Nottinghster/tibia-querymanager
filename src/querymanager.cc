#include "querymanager.hh"

// TODO(fusion): Support windows eventually?
#if OS_LINUX
#	include <errno.h>
#	include <signal.h>
#	include <sys/random.h>
#else
#	error "Operating system not currently supported."
#endif

int64     g_StartTimeMS    = 0;
AtomicInt g_ShutdownSignal = {};
TConfig   g_Config         = {};

void LogAdd(const char *Prefix, const char *Format, ...){
	char Entry[4096];
	va_list ap;
	va_start(ap, Format);
	vsnprintf(Entry, sizeof(Entry), Format, ap);
	va_end(ap);

	// NOTE(fusion): Trim trailing whitespace.
	int Length = (int)strlen(Entry);
	while(Length > 0 && isspace(Entry[Length - 1])){
		Entry[Length - 1] = 0;
		Length -= 1;
	}

	if(Length > 0){
		struct tm LocalTime = GetLocalTime(time(NULL));
		fprintf(stdout, "%04d/%02d/%02d %02d:%02d:%02d [%s] %s\n",
				LocalTime.tm_year + 1900, LocalTime.tm_mon + 1, LocalTime.tm_mday,
				LocalTime.tm_hour, LocalTime.tm_min, LocalTime.tm_sec,
				Prefix, Entry);
		fflush(stdout);
	}
}

void LogAddVerbose(const char *Prefix, const char *Function,
		const char *File, int Line, const char *Format, ...){
	char Entry[4096];
	va_list ap;
	va_start(ap, Format);
	vsnprintf(Entry, sizeof(Entry), Format, ap);
	va_end(ap);

	// NOTE(fusion): Trim trailing whitespace.
	int Length = (int)strlen(Entry);
	while(Length > 0 && isspace(Entry[Length - 1])){
		Entry[Length - 1] = 0;
		Length -= 1;
	}

	if(Length > 0){
		(void)File;
		(void)Line;
		struct tm LocalTime = GetLocalTime(time(NULL));
		fprintf(stdout, "%04d/%02d/%02d %02d:%02d:%02d [%s] %s: %s\n",
				LocalTime.tm_year + 1900, LocalTime.tm_mon + 1, LocalTime.tm_mday,
				LocalTime.tm_hour, LocalTime.tm_min, LocalTime.tm_sec,
				Prefix, Function, Entry);
		fflush(stdout);
	}
}

struct tm GetLocalTime(time_t t){
	struct tm result;
#if COMPILER_MSVC
	localtime_s(&result, &t);
#else
	localtime_r(&t, &result);
#endif
	return result;
}

int64 GetClockMonotonicMS(void){
#if OS_WINDOWS
	LARGE_INTEGER Counter, Frequency;
	QueryPerformanceCounter(&Counter);
	QueryPerformanceFrequency(&Frequency);
	return (int64)((Counter.QuadPart * 1000) / Frequency.QuadPart);
#else
	// NOTE(fusion): The coarse monotonic clock has a larger resolution but is
	// supposed to be faster, even avoiding system calls in some cases. It should
	// be fine for millisecond precision which is what we're using.
	struct timespec Time;
	clock_gettime(CLOCK_MONOTONIC_COARSE, &Time);
	return ((int64)Time.tv_sec * 1000)
		+ ((int64)Time.tv_nsec / 1000000);
#endif
}

int GetMonotonicUptimeMS(void){
	return (int)(GetClockMonotonicMS() - g_StartTimeMS);
}

void SleepMS(int DurationMS){
#if OS_WINDOWS
	Sleep((DWORD)DurationMS);
#else
	struct timespec Duration;
	Duration.tv_sec = (time_t)(DurationMS / 1000);
	Duration.tv_nsec = (long)((DurationMS % 1000) * 1000000);
	nanosleep(&Duration, NULL);
#endif
}

void CryptoRandom(uint8 *Buffer, int Count){
#if 0 && OS_WINDOWS
	// TODO(fusion): Not sure about this one.
	if(BCryptGenRandom(NULL, Buffer, (ULONG)Count, BCRYPT_USE_SYSTEM_PREFERRED_RNG) != STATUS_SUCCESS){
		PANIC("Failed to generate cryptographically safe random data.");
	}
#else
	// NOTE(fusion): This shouldn't fail unless the kernel doesn't implement the
	// required system call, in which case we should have a fallback method. See
	// `getrandom(2)` for the whole story.
	if((int)getrandom(Buffer, Count, 0) != Count){
		PANIC("Failed to generate cryptographically safe random data.");
	}
#endif
}

int RoundSecondsToDays(int Seconds){
	return (Seconds + 86399) / 86400;
}

bool StringEmpty(const char *String){
	return String[0] == 0;
}

bool StringEq(const char *A, const char *B){
	int Index = 0;
	while(true){
		if(A[Index] != B[Index]){
			return false;
		}else if(A[Index] == 0){
			return true;
		}
		Index += 1;
	}
}

bool StringEqCI(const char *A, const char *B){
	int Index = 0;
	while(true){
		if(tolower(A[Index]) != tolower(B[Index])){
			return false;
		}else if(A[Index] == 0){
			return true;
		}
		Index += 1;
	}
}

bool StringCopyN(char *Dest, int DestCapacity, const char *Src, int SrcLength){
	ASSERT(DestCapacity > 0);
	bool Result = (SrcLength < DestCapacity);
	if(Result && SrcLength > 0){
		memcpy(Dest, Src, SrcLength);
		Dest[SrcLength] = 0;
	}else{
		Dest[0] = 0;
	}
	return Result;
}

bool StringCopy(char *Dest, int DestCapacity, const char *Src){
	// IMPORTANT(fusion): `sqlite3_column_text` may return NULL if the column is
	// also NULL so we have an incentive to properly handle the case where `Src`
	// is NULL.
	int SrcLength = (Src != NULL ? (int)strlen(Src) : 0);
	return StringCopyN(Dest, DestCapacity, Src, SrcLength);
}

void StringCopyEllipsis(char *Dest, int DestCapacity, const char *Src){
	ASSERT(DestCapacity > 0);
	int SrcLength = (Src != NULL ? (int)strlen(Src) : 0);
	if(SrcLength < DestCapacity){
		memcpy(Dest, Src, SrcLength);
		Dest[SrcLength] = 0;
	}else{
		memcpy(Dest, Src, DestCapacity);
		if(DestCapacity >= 4){
			Dest[DestCapacity - 4] = '.';
			Dest[DestCapacity - 3] = '.';
			Dest[DestCapacity - 2] = '.';
		}
		Dest[DestCapacity - 1] = 0;
	}
}

bool StringFormat(char *Dest, int DestCapacity, const char *Format, ...){
	va_list ap;
	va_start(ap, Format);
	int Written = vsnprintf(Dest, DestCapacity, Format, ap);
	va_end(ap);
	return Written >= 0 && Written < DestCapacity;
}

uint32 HashString(const char *String){
	// FNV1a 32-bits
	uint32 Hash = 0x811C9DC5U;
	for(int i = 0; String[i] != 0; i += 1){
		Hash ^= (uint32)String[i];
		Hash *= 0x01000193U;
	}
	return Hash;
}

int HexDigit(int Ch){
	if(Ch >= '0' && Ch <= '9'){
		return (Ch - '0');
	}else if(Ch >= 'A' && Ch <= 'F'){
		return (Ch - 'A') + 10;
	}else if(Ch >= 'a' && Ch <= 'f'){
		return (Ch - 'a') + 10;
	}else{
		return -1;
	}
}

int ParseHexString(uint8 *Dest, int DestCapacity, const char *String){
	int StringLen = (int)strlen(String);
	if(StringLen % 2 != 0){
		LOG_ERR("Expected even number of characters");
		return -1;
	}

	int NumBytes = (StringLen / 2);
	if(NumBytes > DestCapacity){
		LOG_ERR("Supplied buffer is too small (Size: %d, Required: %d)",
				DestCapacity, NumBytes);
		return -1;
	}

	for(int i = 0; i < StringLen; i += 2){
		int DigitHi = HexDigit(String[i + 0]);
		int DigitLo = HexDigit(String[i + 1]);
		if(DigitHi == -1 || DigitLo == -1){
			LOG_ERR("Invalid hex digit at offset %d", i);
			return -1;
		}

		Dest[i/2] = ((uint8)DigitHi << 4) | (uint8)DigitLo;
	}

	return NumBytes;
}

bool ParseIPAddress(int *Dest, const char *String){
	if(StringEmpty(String)){
		LOG_ERR("Empty IP Address");
		return false;
	}

	int Addr[4];
	if(sscanf(String, "%d.%d.%d.%d", &Addr[0], &Addr[1], &Addr[2], &Addr[3]) != 4){
		LOG_ERR("Invalid IP Address format \"%s\"", String);
		return false;
	}

	if(Addr[0] < 0 || Addr[0] > 0xFF
	|| Addr[1] < 0 || Addr[1] > 0xFF
	|| Addr[2] < 0 || Addr[2] > 0xFF
	|| Addr[3] < 0 || Addr[3] > 0xFF){
		LOG_ERR("Invalid IP Address \"%s\"", String);
		return false;
	}

	if(Dest){
		*Dest = ((int)Addr[0] << 24)
				| ((int)Addr[1] << 16)
				| ((int)Addr[2] << 8)
				| ((int)Addr[3] << 0);
	}

	return true;
}

bool ParseBoolean(bool *Dest, const char *String){
	ASSERT(Dest && String);
	*Dest = StringEqCI(String, "true")
			|| StringEqCI(String, "on")
			|| StringEqCI(String, "yes");
	return *Dest
			|| StringEqCI(String, "false")
			|| StringEqCI(String, "off")
			|| StringEqCI(String, "no");
}

bool ParseInteger(int *Dest, const char *String){
	ASSERT(Dest && String);
	const char *StringEnd;
	*Dest = (int)strtol(String, (char**)&StringEnd, 0);
	return StringEnd > String;
}

bool ParseDuration(int *Dest, const char *String){
	ASSERT(Dest && String);
	const char *Suffix;
	*Dest = (int)strtol(String, (char**)&Suffix, 0);
	if(Suffix == String){
		return false;
	}

	while(Suffix[0] != 0 && isspace(Suffix[0])){
		Suffix += 1;
	}

	if(Suffix[0] == 'S' || Suffix[0] == 's'){
		*Dest *= (1000);
	}else if(Suffix[0] == 'M' || Suffix[0] == 'm'){
		*Dest *= (60 * 1000);
	}else if(Suffix[0] == 'H' || Suffix[0] == 'h'){
		*Dest *= (60 * 60 * 1000);
	}

	return true;
}

bool ParseSize(int *Dest, const char *String){
	ASSERT(Dest && String);
	const char *Suffix;
	*Dest = (int)strtol(String, (char**)&Suffix, 0);
	if(Suffix == String){
		return false;
	}

	while(Suffix[0] != 0 && isspace(Suffix[0])){
		Suffix += 1;
	}

	if(Suffix[0] == 'K' || Suffix[0] == 'k'){
		*Dest *= (1024);
	}else if(Suffix[0] == 'M' || Suffix[0] == 'm'){
		*Dest *= (1024 * 1024);
	}

	return true;
}

bool ParseString(char *Dest, int DestCapacity, const char *String){
	ASSERT(Dest && DestCapacity > 0 && String);
	int StringStart = 0;
	int StringEnd = (int)strlen(String);
	if(StringEnd >= 2){
		if((String[0] == '"' && String[StringEnd - 1] == '"')
		|| (String[0] == '\'' && String[StringEnd - 1] == '\'')
		|| (String[0] == '`' && String[StringEnd - 1] == '`')){
			StringStart += 1;
			StringEnd -= 1;
		}
	}

	return StringCopyN(Dest, DestCapacity,
			&String[StringStart], (StringEnd - StringStart));
}

bool ReadConfig(const char *FileName, TConfig *Config){
	FILE *File = fopen(FileName, "rb");
	if(File == NULL){
		LOG_ERR("Failed to open config file \"%s\"", FileName);
		return false;
	}

	bool EndOfFile = false;
	for(int LineNumber = 1; !EndOfFile; LineNumber += 1){
		char Line[1024];
		int MaxLineSize = (int)sizeof(Line);
		int LineSize = 0;
		int KeyStart = -1;
		int EqualPos = -1;
		while(true){
			int ch = fgetc(File);
			if(ch == EOF || ch == '\n'){
				if(ch == EOF){
					EndOfFile = true;
				}
				break;
			}

			if(LineSize < MaxLineSize){
				Line[LineSize] = (char)ch;
			}

			if(KeyStart == -1 && !isspace(ch)){
				KeyStart = LineSize;
			}

			if(EqualPos == -1 && ch == '='){
				EqualPos = LineSize;
			}

			LineSize += 1;
		}

		// NOTE(fusion): Check line size limit.
		if(LineSize > MaxLineSize){
			LOG_WARN("%s:%d: Exceeded line size limit of %d characters",
					FileName, LineNumber, MaxLineSize);
			continue;
		}

		// NOTE(fusion): Check empty line or comment.
		if(KeyStart == -1 || Line[KeyStart] == '#'){
			continue;
		}

		// NOTE(fusion): Check assignment.
		if(EqualPos == -1){
			LOG_WARN("%s:%d: No assignment found on non empty line",
					FileName, LineNumber);
			continue;
		}

		// NOTE(fusion): Check empty key.
		int KeyEnd = EqualPos;
		while(KeyEnd > KeyStart && isspace(Line[KeyEnd - 1])){
			KeyEnd -= 1;
		}

		if(KeyStart == KeyEnd){
			LOG_WARN("%s:%d: Empty key", FileName, LineNumber);
			continue;
		}

		// NOTE(fusion): Check empty value.
		int ValStart = EqualPos + 1;
		int ValEnd = LineSize;
		while(ValStart < ValEnd && isspace(Line[ValStart])){
			ValStart += 1;
		}

		while(ValEnd > ValStart && isspace(Line[ValEnd - 1])){
			ValEnd -= 1;
		}

		if(ValStart == ValEnd){
			LOG_WARN("%s:%d: Empty value", FileName, LineNumber);
			continue;
		}

		// NOTE(fusion): Parse KV pair.
		char Key[256];
		if(!StringBufCopyN(Key, &Line[KeyStart], (KeyEnd - KeyStart))){
			LOG_WARN("%s:%d: Exceeded key size limit of %d characters",
					FileName, LineNumber, (int)(sizeof(Key) - 1));
			continue;
		}

		char Val[256];
		if(!StringBufCopyN(Val, &Line[ValStart], (ValEnd - ValStart))){
			LOG_WARN("%s:%d: Exceeded value size limit of %d characters",
					FileName, LineNumber, (int)(sizeof(Val) - 1));
			continue;
		}

		if(StringEqCI(Key, "MaxCachedHostNames")){
			ParseInteger(&Config->MaxCachedHostNames, Val);
		}else if(StringEqCI(Key, "HostNameExpireTime")){
			ParseDuration(&Config->HostNameExpireTime, Val);
#if DATABASE_SQLITE
		}else if(StringEqCI(Key, "SQLite.File")){
			ParseStringBuf(Config->SQLite.File, Val);
		}else if(StringEqCI(Key, "SQLite.MaxCachedStatements")){
			ParseInteger(&Config->SQLite.MaxCachedStatements, Val);
#elif DATABASE_POSTGRESQL
		}else if(StringEqCI(Key, "PostgreSQL.Host")){
			ParseStringBuf(Config->PostgreSQL.Host, Val);
		}else if(StringEqCI(Key, "PostgreSQL.Port")){
			ParseStringBuf(Config->PostgreSQL.Port, Val);
		}else if(StringEqCI(Key, "PostgreSQL.DBName")){
			ParseStringBuf(Config->PostgreSQL.DBName, Val);
		}else if(StringEqCI(Key, "PostgreSQL.User")){
			ParseStringBuf(Config->PostgreSQL.User, Val);
		}else if(StringEqCI(Key, "PostgreSQL.Password")){
			ParseStringBuf(Config->PostgreSQL.Password, Val);
		}else if(StringEqCI(Key, "PostgreSQL.ConnectTimeout")){
			ParseStringBuf(Config->PostgreSQL.ConnectTimeout, Val);
		}else if(StringEqCI(Key, "PostgreSQL.ClientEncoding")){
			ParseStringBuf(Config->PostgreSQL.ClientEncoding, Val);
		}else if(StringEqCI(Key, "PostgreSQL.ApplicationName")){
			ParseStringBuf(Config->PostgreSQL.ApplicationName, Val);
		}else if(StringEqCI(Key, "PostgreSQL.SSLMode")){
			ParseStringBuf(Config->PostgreSQL.SSLMode, Val);
		}else if(StringEqCI(Key, "PostgreSQL.SSLRootCert")){
			ParseStringBuf(Config->PostgreSQL.SSLRootCert, Val);
		}else if(StringEqCI(Key, "PostgreSQL.MaxCachedStatements")){
			ParseInteger(&Config->PostgreSQL.MaxCachedStatements, Val);
#elif DATABASE_MYSQL
		}else if(StringEqCI(Key, "MySQL.Host")){
			ParseStringBuf(Config->MySQL.Host, Val);
		}else if(StringEqCI(Key, "MySQL.Port")){
			ParseStringBuf(Config->MySQL.Port, Val);
		}else if(StringEqCI(Key, "MySQL.DBName")){
			ParseStringBuf(Config->MySQL.DBName, Val);
		}else if(StringEqCI(Key, "MySQL.User")){
			ParseStringBuf(Config->MySQL.User, Val);
		}else if(StringEqCI(Key, "MySQL.Password")){
			ParseStringBuf(Config->MySQL.Password, Val);
		}else if(StringEqCI(Key, "MySQL.UnixSocket")){
			ParseStringBuf(Config->MySQL.UnixSocket, Val);
		}else if(StringEqCI(Key, "MySQL.MaxCachedStatements")){
			ParseInteger(&Config->MySQL.MaxCachedStatements, Val);
#endif
		}else if(StringEqCI(Key, "QueryManagerPort")){
			ParseInteger(&Config->QueryManagerPort, Val);
		}else if(StringEqCI(Key, "QueryManagerPassword")){
			ParseStringBuf(Config->QueryManagerPassword, Val);
		}else if(StringEqCI(Key, "QueryWorkerThreads")){
			ParseInteger(&Config->QueryWorkerThreads, Val);
		}else if(StringEqCI(Key, "QueryBufferSize")
				|| StringEqCI(Key, "MaxConnectionPacketSize")){
			ParseSize(&Config->QueryBufferSize, Val);
		}else if(StringEqCI(Key, "QueryMaxAttempts")){
			ParseInteger(&Config->QueryMaxAttempts, Val);
		}else if(StringEqCI(Key, "MaxConnections")){
			ParseInteger(&Config->MaxConnections, Val);
		}else if(StringEqCI(Key, "MaxConnectionIdleTime")){
			ParseDuration(&Config->MaxConnectionIdleTime, Val);
		}else{
			LOG_WARN("Unknown config \"%s\"", Key);
		}
	}

	fclose(File);
	return true;
}

static bool SigHandler(int SigNr, sighandler_t Handler){
	struct sigaction Action = {};
	Action.sa_handler = Handler;
	sigfillset(&Action.sa_mask);
	if(sigaction(SigNr, &Action, NULL) == -1){
		LOG_ERR("Failed to change handler for signal %d (%s): (%d) %s",
				SigNr, sigdescr_np(SigNr), errno, strerrordesc_np(errno));
		return false;
	}
	return true;
}

static void ShutdownHandler(int SigNr){
	AtomicStore(&g_ShutdownSignal, SigNr);
	WakeConnections();
}

int main(int argc, const char **argv){
	(void)argc;
	(void)argv;

	g_StartTimeMS = GetClockMonotonicMS();
	AtomicStore(&g_ShutdownSignal, 0);
	if(!SigHandler(SIGPIPE, SIG_IGN)
	|| !SigHandler(SIGINT, ShutdownHandler)
	|| !SigHandler(SIGTERM, ShutdownHandler)){
		return EXIT_FAILURE;
	}

	// HostCache Config
	g_Config.MaxCachedHostNames = 100;
	g_Config.HostNameExpireTime = 30 * 60 * 1000; // milliseconds

	// Database Config
#if DATABASE_SQLITE
	StringBufCopy(g_Config.SQLite.File, "tibia.db");
	g_Config.SQLite.MaxCachedStatements = 100;
#elif DATABASE_POSTGRESQL
	StringBufCopy(g_Config.PostgreSQL.Host,            "localhost");
	StringBufCopy(g_Config.PostgreSQL.Port,            "5432");
	StringBufCopy(g_Config.PostgreSQL.DBName,          "tibia");
	StringBufCopy(g_Config.PostgreSQL.User,            "tibia");
	StringBufCopy(g_Config.PostgreSQL.Password,        "");
	StringBufCopy(g_Config.PostgreSQL.ConnectTimeout,  "");
	StringBufCopy(g_Config.PostgreSQL.ClientEncoding,  "UTF8");
	StringBufCopy(g_Config.PostgreSQL.ApplicationName, "QueryManager");
	StringBufCopy(g_Config.PostgreSQL.SSLMode,         "");
	StringBufCopy(g_Config.PostgreSQL.SSLRootCert,     "");
	g_Config.PostgreSQL.MaxCachedStatements = 100;
#elif DATABASE_MYSQL
	StringBufCopy(g_Config.MySQL.Host,       "localhost");
	StringBufCopy(g_Config.MySQL.Port,       "3306");
	StringBufCopy(g_Config.MySQL.DBName,     "tibia");
	StringBufCopy(g_Config.MySQL.User,       "tibia");
	StringBufCopy(g_Config.MySQL.Password,   "");
	StringBufCopy(g_Config.MySQL.UnixSocket, "");
	g_Config.MySQL.MaxCachedStatements = 100;
#endif

	// Connection Config
	g_Config.QueryManagerPort = 7174;
	StringBufCopy(g_Config.QueryManagerPassword, "");
	g_Config.QueryWorkerThreads = 1;
	g_Config.QueryBufferSize = (int)MB(1);
	g_Config.QueryMaxAttempts = 3;
	g_Config.MaxConnections = 25;
	g_Config.MaxConnectionIdleTime = 60 * 1000; // milliseconds

	LOG("Tibia Query Manager v0.2 (%s)", DATABASE_SYSTEM_NAME);
	if(!ReadConfig("config.cfg", &g_Config)){
		return EXIT_FAILURE;
	}

	// NOTE(fusion): Print config values for debugging purposes.
	LOG("Max cached host names:            %d",     g_Config.MaxCachedHostNames);
	LOG("Host name expire time:            %dms",   g_Config.HostNameExpireTime);
#if DATABASE_SQLITE
	LOG("SQLite file:                      \"%s\"", g_Config.SQLite.File);
	LOG("SQLite max cached statements:     %d",     g_Config.SQLite.MaxCachedStatements);
#elif DATABASE_POSTGRESQL
	LOG("PostgreSQL host:                  \"%s\"", g_Config.PostgreSQL.Host);
	LOG("PostgreSQL port:                  \"%s\"", g_Config.PostgreSQL.Port);
	LOG("PostgreSQL dbname:                \"%s\"", g_Config.PostgreSQL.DBName);
	LOG("PostgreSQL user:                  \"%s\"", g_Config.PostgreSQL.User);
	LOG("PostgreSQL connect_timeout:       \"%s\"", g_Config.PostgreSQL.ConnectTimeout);
	LOG("PostgreSQL client_encoding:       \"%s\"", g_Config.PostgreSQL.ClientEncoding);
	LOG("PostgreSQL application_name:      \"%s\"", g_Config.PostgreSQL.ApplicationName);
	LOG("PostgreSQL sslmode:               \"%s\"", g_Config.PostgreSQL.SSLMode);
	LOG("PostgreSQL sslrootcert:           \"%s\"", g_Config.PostgreSQL.SSLRootCert);
	LOG("PostgreSQL max cached statements: %d",     g_Config.PostgreSQL.MaxCachedStatements);
#elif DATABASE_MYSQL
	LOG("MySQL host:                       \"%s\"", g_Config.MySQL.Host);
	LOG("MySQL port:                       \"%s\"", g_Config.MySQL.Port);
	LOG("MySQL dbname:                     \"%s\"", g_Config.MySQL.DBName);
	LOG("MySQL user:                       \"%s\"", g_Config.MySQL.User);
	LOG("MySQL unix socket:                \"%s\"", g_Config.MySQL.UnixSocket);
	LOG("MySQL max cached statements:      %d",     g_Config.MySQL.MaxCachedStatements);
#endif
	LOG("Query manager port:               %d",     g_Config.QueryManagerPort);
	LOG("Query worker threads:             %d",     g_Config.QueryWorkerThreads);
	LOG("Query buffer size:                %dB",    g_Config.QueryBufferSize);
	LOG("Query max attempts:               %d",     g_Config.QueryMaxAttempts);
	LOG("Max connections:                  %d",     g_Config.MaxConnections);
	LOG("Max connection idle time:         %dms",   g_Config.MaxConnectionIdleTime);

	if(!CheckSHA256()){
		return EXIT_FAILURE;
	}

	atexit(ExitHostCache);
	atexit(ExitQuery);
	atexit(ExitConnections);
	if(!InitHostCache()
			|| !InitQuery()
			|| !InitConnections()){
		return EXIT_FAILURE;
	}

	LOG("Running...");
	while(AtomicLoad(&g_ShutdownSignal) == 0){
		// NOTE(fusion): `ProcessConnections` will do a blocking `poll` which
		// prevents this from being a hot loop, while still being reactive.
		ProcessConnections();
	}

	int ShutdownSignal = AtomicLoad(&g_ShutdownSignal);
	LOG("Received signal %d (%s), shutting down...",
			ShutdownSignal, sigdescr_np(ShutdownSignal));

	return EXIT_SUCCESS;
}
