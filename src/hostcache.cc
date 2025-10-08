#include "querymanager.hh"

// TODO(fusion): Support windows eventually?
#if OS_LINUX
#	include <netdb.h>
#else
#	error "Operating system not currently supported."
#endif

struct THostCacheEntry{
	char HostName[100];
	bool Resolved;
	int IPAddress;
	int ResolveTime;
};

static THostCacheEntry *g_CachedHostNames;

bool InitHostCache(void){
	ASSERT(g_CachedHostNames == NULL);
	g_CachedHostNames = (THostCacheEntry*)calloc(
			g_Config.MaxCachedHostNames, sizeof(THostCacheEntry));
	return true;
}

void ExitHostCache(void){
	if(g_CachedHostNames != NULL){
		free(g_CachedHostNames);
		g_CachedHostNames = NULL;
	}
}

static bool DoResolveHostName(const char *HostName, int *OutAddr){
	ASSERT(HostName != NULL && OutAddr != NULL);
	addrinfo *Result = NULL;
	addrinfo Hints = {};
	Hints.ai_family = AF_INET;
	Hints.ai_socktype = SOCK_STREAM;
	int ErrCode = getaddrinfo(HostName, NULL, &Hints, &Result);
	if(ErrCode != 0){
		LOG_ERR("Failed to resolve hostname \"%s\": %s", HostName, gai_strerror(ErrCode));
		return false;
	}

	bool Resolved = false;
	for(addrinfo *AddrInfo = Result;
			AddrInfo != NULL;
			AddrInfo = AddrInfo->ai_next){
		if(AddrInfo->ai_family == AF_INET && AddrInfo->ai_socktype == SOCK_STREAM){
			ASSERT(AddrInfo->ai_addrlen == sizeof(sockaddr_in));
			*OutAddr = ntohl(((sockaddr_in*)AddrInfo->ai_addr)->sin_addr.s_addr);
			Resolved = true;
			break;
		}
	}
	freeaddrinfo(Result);
	return Resolved;
}

bool ResolveHostName(const char *HostName, int *OutAddr){
	ASSERT(HostName != NULL && !StringEmpty(HostName));
	THostCacheEntry *Entry = NULL;
	int LeastRecentlyUsedIndex = 0;
	int LeastRecentlyUsedTime = g_CachedHostNames[0].ResolveTime;
	for(int i = 0; i < g_Config.MaxCachedHostNames; i += 1){
		THostCacheEntry *Current = &g_CachedHostNames[i];

		if((g_MonotonicTimeMS - Current->ResolveTime) >= g_Config.HostNameExpireTime){
			memset(Current, 0, sizeof(THostCacheEntry));
		}

		if(Current->ResolveTime < LeastRecentlyUsedTime){
			LeastRecentlyUsedIndex = i;
			LeastRecentlyUsedTime = Current->ResolveTime;
		}

		if(StringEq(HostName, Current->HostName)){
			Entry = Current;
			break;
		}
	}

	if(Entry == NULL){
		// NOTE(fusion): We also cache failures.
		Entry = &g_CachedHostNames[LeastRecentlyUsedIndex];
		if(!StringBufCopy(Entry->HostName, HostName)){
			LOG_WARN("Hostname \"%s\" was improperly cached because it was"
					" too long (Length: %d, MaxLength: %d)", HostName,
					(int)strlen(HostName), (int)sizeof(Entry->HostName));
		}
		Entry->Resolved = DoResolveHostName(HostName, &Entry->IPAddress);
		Entry->ResolveTime = g_MonotonicTimeMS;
	}

	if(Entry && Entry->Resolved){
		if(OutAddr){
			*OutAddr = Entry->IPAddress;
		}
		return true;
	}else{
		return false;
	}
}
