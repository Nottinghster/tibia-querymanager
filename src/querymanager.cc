#include "querymanager.hh"

// TODO(fusion): Support windows eventually?
#if OS_LINUX
#	include <errno.h>
#	include <signal.h>
#	include <sys/random.h>
#else
#	error "Operating system not currently supported."
#endif

// Shutdown Signal
int  g_ShutdownSignal			= 0;

// Time
int  g_MonotonicTimeMS			= 0;

// Database Config
char g_DatabaseFile[1024]		= "tibia.db";
int  g_MaxCachedStatements		= 100;

// HostCache Config
int  g_MaxCachedHostNames		= 100;
int  g_HostNameExpireTime       = 30 * 60 * 1000; // milliseconds

// Connection Config
int  g_UpdateRate				= 20;
int  g_QueryManagerPort			= 7174;
char g_QueryManagerPassword[30]	= "";
int  g_MaxConnections			= 50;
int  g_MaxConnectionIdleTime	= 60 * 1000; // milliseconds
int  g_MaxConnectionPacketSize	= (int)MB(1);

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
	struct timespec Time;
	clock_gettime(CLOCK_MONOTONIC, &Time);
	return ((int64)Time.tv_sec * 1000)
		+ ((int64)Time.tv_nsec / 1000000);
#endif
}

void SleepMS(int64 DurationMS){
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

bool ParseOptionalIPAddress(const char *String, int *OutAddr){
	if(String == NULL || StringEmpty(String)){
		return true;
	}

	return ParseIPAddress(String, OutAddr);
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

bool ReadConfig(const char *FileName){
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
		if(!StringCopyN(Key, (int)sizeof(Key), &Line[KeyStart], (KeyEnd - KeyStart))){
			LOG_WARN("%s:%d: Exceeded key size limit of %d characters",
					FileName, LineNumber, (int)(sizeof(Key) - 1));
			continue;
		}

		char Val[256];
		if(!StringCopyN(Val, (int)sizeof(Val), &Line[ValStart], (ValEnd - ValStart))){
			LOG_WARN("%s:%d: Exceeded value size limit of %d characters",
					FileName, LineNumber, (int)(sizeof(Val) - 1));
			continue;
		}

		if(StringEqCI(Key, "DatabaseFile")){
			ReadStringConfig(g_DatabaseFile, (int)sizeof(g_DatabaseFile), Val);
		}else if(StringEqCI(Key, "MaxCachedStatements")){
			ReadIntegerConfig(&g_MaxCachedStatements, Val);
		}else if(StringEqCI(Key, "MaxCachedHostNames")){
			ReadIntegerConfig(&g_MaxCachedHostNames, Val);
		}else if(StringEqCI(Key, "HostNameExpireTime")){
			ReadDurationConfig(&g_HostNameExpireTime, Val);
		}else if(StringEqCI(Key, "UpdateRate")){
			ReadIntegerConfig(&g_UpdateRate, Val);
		}else if(StringEqCI(Key, "QueryManagerPort")){
			ReadIntegerConfig(&g_QueryManagerPort, Val);
		}else if(StringEqCI(Key, "QueryManagerPassword")){
			ReadStringConfig(g_QueryManagerPassword, (int)sizeof(g_QueryManagerPassword), Val);
		}else if(StringEqCI(Key, "MaxConnections")){
			ReadIntegerConfig(&g_MaxConnections, Val);
		}else if(StringEqCI(Key, "MaxConnectionIdleTime")){
			ReadDurationConfig(&g_MaxConnectionIdleTime, Val);
		}else if(StringEqCI(Key, "MaxConnectionPacketSize")){
			ReadSizeConfig(&g_MaxConnectionPacketSize, Val);
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
	g_ShutdownSignal = SigNr;
}

int main(int argc, const char **argv){
	(void)argc;
	(void)argv;

	g_ShutdownSignal = 0;
	if(!SigHandler(SIGPIPE, SIG_IGN)
	|| !SigHandler(SIGINT, ShutdownHandler)
	|| !SigHandler(SIGTERM, ShutdownHandler)){
		return EXIT_FAILURE;
	}

	int64 StartTime = GetClockMonotonicMS();
	g_MonotonicTimeMS = 0;

	LOG("Tibia Query Manager v0.1");
	if(!ReadConfig("config.cfg")){
		return EXIT_FAILURE;
	}

	if(!CheckSHA256()){
		return EXIT_FAILURE;
	}

	atexit(ExitHostCache);
	atexit(ExitDatabase);
	atexit(ExitConnections);
	if(!InitHostCache()
			|| !InitDatabase()
			|| !InitConnections()){
		return EXIT_FAILURE;
	}

	LOG("Running at %d updates per second...", g_UpdateRate);
	int64 UpdateInterval = 1000 / (int64)g_UpdateRate;
	while(g_ShutdownSignal == 0){
		int64 UpdateStart = GetClockMonotonicMS();
		g_MonotonicTimeMS = (int)(UpdateStart - StartTime);
		ProcessConnections();
		int64 UpdateEnd = GetClockMonotonicMS();
		int64 NextUpdate = UpdateStart + UpdateInterval;
		if(NextUpdate > UpdateEnd){
			SleepMS(NextUpdate - UpdateEnd);
		}
	}

	LOG("Received signal %d (%s), shutting down...",
			g_ShutdownSignal, sigdescr_np(g_ShutdownSignal));

	return EXIT_SUCCESS;
}
