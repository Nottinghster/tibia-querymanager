#include "querymanager.hh"

// TODO(fusion): Support windows eventually?
#if OS_LINUX
#	include <errno.h>
#	include <signal.h>
#	include <sys/random.h>
#else
#	error "Operating system not currently supported."
#endif

AtomicInt g_ShutdownSignal = {};
int       g_StartTimeMS    = 0;
TConfig   g_Config         = {};

void LogAdd(const char *Prefix, const char *Format, ...){
	char Entry[4096];
	va_list ap;
	va_start(ap, Format);
	vsnprintf(Entry, sizeof(Entry), Format, ap);
	va_end(ap);

	if(!StringEmpty(Entry)){
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

	if(!StringEmpty(Entry)){
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

uint32 HashString(const char *String){
	// FNV1a 32-bits
	uint32 Hash = 0x811C9DC5U;
	for(int i = 0; String[i] != 0; i += 1){
		Hash ^= (uint32)String[i];
		Hash *= 0x01000193U;
	}
	return Hash;
}

bool ParseIPAddress(const char *String, int *OutAddr){
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

	if(OutAddr){
		*OutAddr = ((int)Addr[0] << 24)
				| ((int)Addr[1] << 16)
				| ((int)Addr[2] << 8)
				| ((int)Addr[3] << 0);
	}

	return true;
}

bool ReadBooleanConfig(bool *Dest, const char *Val){
	ASSERT(Dest && Val);
	*Dest = StringEqCI(Val, "true");
	return *Dest || StringEqCI(Val, "false");
}

bool ReadIntegerConfig(int *Dest, const char *Val){
	ASSERT(Dest && Val);
	const char *ValEnd;
	*Dest = (int)strtol(Val, (char**)&ValEnd, 0);
	return ValEnd > Val;
}

bool ReadDurationConfig(int *Dest, const char *Val){
	ASSERT(Dest && Val);
	const char *Suffix;
	*Dest = (int)strtol(Val, (char**)&Suffix, 0);
	if(Suffix == Val){
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

bool ReadSizeConfig(int *Dest, const char *Val){
	ASSERT(Dest && Val);
	const char *Suffix;
	*Dest = (int)strtol(Val, (char**)&Suffix, 0);
	if(Suffix == Val){
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

bool ReadStringConfig(char *Dest, int DestCapacity, const char *Val){
	ASSERT(Dest && DestCapacity > 0 && Val);
	int ValStart = 0;
	int ValEnd = (int)strlen(Val);
	if(ValEnd >= 2){
		if((Val[0] == '"' && Val[ValEnd - 1] == '"')
		|| (Val[0] == '\'' && Val[ValEnd - 1] == '\'')
		|| (Val[0] == '`' && Val[ValEnd - 1] == '`')){
			ValStart += 1;
			ValEnd -= 1;
		}
	}

	return StringCopyN(Dest, DestCapacity,
			&Val[ValStart], (ValEnd - ValStart));
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
			ReadIntegerConfig(&Config->MaxCachedHostNames, Val);
		}else if(StringEqCI(Key, "HostNameExpireTime")){
			ReadDurationConfig(&Config->HostNameExpireTime, Val);
#if DATABASE_SQLITE
		}else if(StringEqCI(Key, "SQLite.File")){
			ReadStringBufConfig(Config->SQLite.File, Val);
		}else if(StringEqCI(Key, "SQLite.MaxCachedStatements")){
			ReadIntegerConfig(&Config->SQLite.MaxCachedStatements, Val);
#elif DATABASE_POSTGRESQL
		}else if(StringEqCI(Key, "PostgreSQL.UnixSocket")){
			ReadStringBufConfig(Config->PostgreSQL.UnixSocket, Val);
		}else if(StringEqCI(Key, "PostgreSQL.Host")){
			ReadStringBufConfig(Config->PostgreSQL.Host, Val);
		}else if(StringEqCI(Key, "PostgreSQL.Port")){
			ReadIntegerConfig(&Config->PostgreSQL.Port, Val);
		}else if(StringEqCI(Key, "PostgreSQL.User")){
			ReadStringBufConfig(Config->PostgreSQL.User, Val);
		}else if(StringEqCI(Key, "PostgreSQL.Password")){
			ReadStringBufConfig(Config->PostgreSQL.Password, Val);
		}else if(StringEqCI(Key, "PostgreSQL.Database")){
			ReadStringBufConfig(Config->PostgreSQL.Database, Val);
		}else if(StringEqCI(Key, "PostgreSQL.TLS")){
			ReadBooleanConfig(&Config->PostgreSQL.TLS, Val);
		}else if(StringEqCI(Key, "PostgreSQL.MaxCachedStatements")){
			ReadIntegerConfig(&Config->PostgreSQL.MaxCachedStatements, Val);
#elif DATABASE_MYSQL
		}else if(StringEqCI(Key, "MySQL.UnixSocket")){
			ReadStringBufConfig(Config->MySQL.UnixSocket, Val);
		}else if(StringEqCI(Key, "MySQL.Host")){
			ReadStringBufConfig(Config->MySQL.Host, Val);
		}else if(StringEqCI(Key, "MySQL.Port")){
			ReadIntegerConfig(&Config->MySQL.Port, Val);
		}else if(StringEqCI(Key, "MySQL.User")){
			ReadStringBufConfig(Config->MySQL.User, Val);
		}else if(StringEqCI(Key, "MySQL.Password")){
			ReadStringBufConfig(Config->MySQL.Password, Val);
		}else if(StringEqCI(Key, "MySQL.Database")){
			ReadStringBufConfig(Config->MySQL.Database, Val);
		}else if(StringEqCI(Key, "MySQL.TLS")){
			ReadBooleanConfig(&Config->MySQL.TLS, Val);
		}else if(StringEqCI(Key, "MySQL.MaxCachedStatements")){
			ReadIntegerConfig(&Config->MySQL.MaxCachedStatements, Val);
#endif
		}else if(StringEqCI(Key, "QueryManagerPort")){
			ReadIntegerConfig(&Config->QueryManagerPort, Val);
		}else if(StringEqCI(Key, "QueryManagerPassword")){
			ReadStringBufConfig(Config->QueryManagerPassword, Val);
		}else if(StringEqCI(Key, "QueryWorkerThreads")){
			ReadIntegerConfig(&Config->QueryWorkerThreads, Val);
		}else if(StringEqCI(Key, "QueryBufferSize")
				|| StringEqCI(Key, "MaxConnectionPacketSize")){
			ReadSizeConfig(&Config->QueryBufferSize, Val);
		}else if(StringEqCI(Key, "QueryMaxAttempts")){
			ReadIntegerConfig(&Config->QueryMaxAttempts, Val);
		}else if(StringEqCI(Key, "MaxConnections")){
			ReadIntegerConfig(&Config->MaxConnections, Val);
		}else if(StringEqCI(Key, "MaxConnectionIdleTime")){
			ReadDurationConfig(&Config->MaxConnectionIdleTime, Val);
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

	AtomicStore(&g_ShutdownSignal, 0);
	if(!SigHandler(SIGPIPE, SIG_IGN)
	|| !SigHandler(SIGINT, ShutdownHandler)
	|| !SigHandler(SIGTERM, ShutdownHandler)){
		return EXIT_FAILURE;
	}

	g_StartTimeMS = GetClockMonotonicMS();

	// HostCache Config
	g_Config.MaxCachedHostNames = 100;
	g_Config.HostNameExpireTime = 30 * 60 * 1000; // milliseconds

	// Database Config
#if DATABASE_SQLITE
	StringBufCopy(g_Config.SQLite.File, "tibia.db");
	g_Config.SQLite.MaxCachedStatements = 100;
#elif DATABASE_POSTGRESQL
	StringBufCopy(g_Config.PostgreSQL.UnixSocket, "");
	StringBufCopy(g_Config.PostgreSQL.Host, "localhost");
	g_Config.PostgreSQL.Port = 5432;
	StringBufCopy(g_Config.PostgreSQL.User, "postgres");
	StringBufCopy(g_Config.PostgreSQL.Password, "");
	StringBufCopy(g_Config.PostgreSQL.Database, "tibia");
	g_Config.PostgreSQL.TLS = true;
	g_Config.PostgreSQL.MaxCachedStatements = 100;
#elif DATABASE_MYSQL
	StringBufCopy(g_Config.MySQL.UnixSocket, "");
	StringBufCopy(g_Config.MySQL.Host, "localhost");
	g_Config.MySQL.Port = 5432;
	StringBufCopy(g_Config.MySQL.User, "postgres");
	StringBufCopy(g_Config.MySQL.Password, "");
	StringBufCopy(g_Config.MySQL.Database, "tibia");
	g_Config.MySQL.TLS = true;
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

	LOG("Tibia Query Manager v0.2");
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
	LOG("PostgreSQL unix socket:           \"%s\"", g_Config.PostgreSQL.UnixSocket);
	LOG("PostgreSQL host:                  \"%s\"", g_Config.PostgreSQL.Host);
	LOG("PostgreSQL port:                  %d",     g_Config.PostgreSQL.Port);
	LOG("PostgreSQL user:                  \"%s\"", g_Config.PostgreSQL.User);
	LOG("PostgreSQL database:              \"%s\"", g_Config.PostgreSQL.Database);
	LOG("PostgreSQL TLS:                   %s",     g_Config.PostgreSQL.TLS ? "true" : "false");
	LOG("PostgreSQL max cached statements: %d",     g_Config.PostgreSQL.MaxCachedStatements);
#elif DATABASE_MYSQL
	LOG("MySQL unix socket:                \"%s\"", g_Config.MySQL.UnixSocket);
	LOG("MySQL host:                       \"%s\"", g_Config.MySQL.Host);
	LOG("MySQL port:                       %d",     g_Config.MySQL.Port);
	LOG("MySQL user:                       \"%s\"", g_Config.MySQL.User);
	LOG("MySQL database:                   \"%s\"", g_Config.MySQL.Database);
	LOG("MySQL TLS:                        %s",     g_Config.MySQL.TLS ? "true" : "false");
	LOG("MySQL max cached statements:      %d",     g_Config.MySQL.MaxCachedStatements);
#endif
	LOG("Query manager port:               %d",     g_Config.QueryManagerPort);
	LOG("Query worker threads:             %d",     g_Config.QueryWorkerThreads);
	LOG("Query buffer size:                %d",     g_Config.QueryBufferSize);
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
