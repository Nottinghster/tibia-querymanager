#include "querymanager.hh"

// TODO(fusion): Eventually support Windows? It's not that difficult to wrap the
// few OS specific calls but it can be annoying plus the socket type is slightly
// different.
#if OS_LINUX
#	include <errno.h>
#	include <fcntl.h>
#	include <netinet/in.h>
#	include <poll.h>
#	include <sys/socket.h>
#	include <unistd.h>
#	include <time.h>
#else
#	error "Operating system not currently supported."
#endif

static int g_Listener = -1;
static TConnection *g_Connections;

// Connection Handling
//==============================================================================
int ListenerBind(uint16 Port){
	int Socket = socket(AF_INET, SOCK_STREAM, 0);
	if(Socket == -1){
		LOG_ERR("Failed to create listener socket: (%d) %s", errno, strerrordesc_np(errno));
		return -1;
	}

	int ReuseAddr = 1;
	if(setsockopt(Socket, SOL_SOCKET, SO_REUSEADDR, &ReuseAddr, sizeof(ReuseAddr)) == -1){
		LOG_ERR("Failed to set SO_REUSADDR: (%d) %s", errno, strerrordesc_np(errno));
		close(Socket);
		return -1;
	}

	int Flags = fcntl(Socket, F_GETFL);
	if(Flags == -1){
		LOG_ERR("Failed to get socket flags: (%d) %s", errno, strerrordesc_np(errno));
		close(Socket);
		return -1;
	}

	if(fcntl(Socket, F_SETFL, Flags | O_NONBLOCK) == -1){
		LOG_ERR("Failed to set socket flags: (%d) %s", errno, strerrordesc_np(errno));
		close(Socket);
		return -1;
	}

	// IMPORTANT(fusion): Binding the socket to the LOOPBACK address should allow
	// only local connections to be accepted. This is VERY important as the protocol
	// IS NOT encrypted.
	sockaddr_in Addr = {};
	Addr.sin_family = AF_INET;
	Addr.sin_port = htons(Port);
	Addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if(bind(Socket, (sockaddr*)&Addr, sizeof(Addr)) == -1){
		LOG_ERR("Failed to bind socket to port %d: (%d) %s", Port, errno, strerrordesc_np(errno));
		close(Socket);
		return -1;
	}

	if(listen(Socket, 128) == -1){
		LOG_ERR("Failed to listen to port %d: (%d) %s", Port, errno, strerrordesc_np(errno));
		close(Socket);
		return -1;
	}

	return Socket;
}

int ListenerAccept(int Listener, uint32 *OutAddr, uint16 *OutPort){
	while(true){
		sockaddr_in SocketAddr = {};
		socklen_t SocketAddrLen = sizeof(SocketAddr);
		int Socket = accept(Listener, (sockaddr*)&SocketAddr, &SocketAddrLen);
		if(Socket == -1){
			if(errno != EAGAIN){
				LOG_ERR("Failed to accept connection: (%d) %s", errno, strerrordesc_np(errno));
			}
			return -1;
		}

		// IMPORTANT(fusion): It should be impossible to spoof the loopback
		// address so this comparison should be safe. We're also binding the
		// listening socket to the loopback address which should prevent any
		// remote addresses from showing up here.
		uint32 Addr = ntohl(SocketAddr.sin_addr.s_addr);
		uint16 Port = ntohs(SocketAddr.sin_port);
		if(Addr != INADDR_LOOPBACK){
			LOG_ERR("Rejecting remote connection from %08X:%d.", Addr, Port);
			close(Socket);
			continue;
		}

		int Flags = fcntl(Socket, F_GETFL);
		if(Flags == -1){
			LOG_ERR("Failed to get socket flags: (%d) %s", errno, strerrordesc_np(errno));
			close(Socket);
			continue;
		}

		if(fcntl(Socket, F_SETFL, Flags | O_NONBLOCK) == -1){
			LOG_ERR("Failed to set socket flags: (%d) %s", errno, strerrordesc_np(errno));
			close(Socket);
			continue;
		}

		if(OutAddr){
			*OutAddr = Addr;
		}

		if(OutPort){
			*OutPort = Port;
		}

		return Socket;
	}
}

void CloseConnection(TConnection *Connection){
	if(Connection->Socket != -1){
		close(Connection->Socket);
		Connection->Socket = -1;
	}
}

void EnsureConnectionBuffer(TConnection *Connection){
	if(Connection->Buffer == NULL){
		Connection->Buffer = (uint8*)malloc(g_MaxConnectionPacketSize);
	}
}

void DeleteConnectionBuffer(TConnection *Connection){
	if(Connection->Buffer != NULL){
		free(Connection->Buffer);
		Connection->Buffer = NULL;
	}
}

TConnection *AssignConnection(int Socket, uint32 Addr, uint16 Port){
	int ConnectionIndex = -1;
	for(int i = 0; i < g_MaxConnections; i += 1){
		if(g_Connections[i].State == CONNECTION_FREE){
			ConnectionIndex = i;
			break;
		}
	}

	TConnection *Connection = NULL;
	if(ConnectionIndex != -1){
		Connection = &g_Connections[ConnectionIndex];
		Connection->State = CONNECTION_READING;
		Connection->Socket = Socket;
		Connection->LastActive = g_MonotonicTimeMS;
		snprintf(Connection->RemoteAddress,
				sizeof(Connection->RemoteAddress),
				"%d.%d.%d.%d:%d",
				((int)(Addr >> 24) & 0xFF),
				((int)(Addr >> 16) & 0xFF),
				((int)(Addr >>  8) & 0xFF),
				((int)(Addr >>  0) & 0xFF),
				(int)Port);

		LOG("Connection %s assigned to slot %d",
				Connection->RemoteAddress, ConnectionIndex);
	}
	return Connection;
}

void ReleaseConnection(TConnection *Connection){
	if(Connection->State != CONNECTION_FREE){
		LOG("Connection %s released", Connection->RemoteAddress);
		CloseConnection(Connection);
		DeleteConnectionBuffer(Connection);
		memset(Connection, 0, sizeof(TConnection));
		Connection->State = CONNECTION_FREE;
	}
}

void CheckConnectionInput(TConnection *Connection, int Events){
	if((Events & POLLIN) == 0 || Connection->Socket == -1){
		return;
	}

	if(Connection->State != CONNECTION_READING){
		LOG_ERR("Connection %s (State: %d) sending out-of-order data",
				Connection->RemoteAddress, Connection->State);
		CloseConnection(Connection);
		return;
	}

	EnsureConnectionBuffer(Connection);
	while(true){
		int ReadSize = Connection->RWSize;
		if(ReadSize == 0){
			if(Connection->RWPosition < 2){
				ReadSize = 2 - Connection->RWPosition;
			}else{
				ReadSize = 6 - Connection->RWPosition;
			}
			ASSERT(ReadSize > 0);
		}

		int BytesRead = read(Connection->Socket,
				(Connection->Buffer + Connection->RWPosition),
				(ReadSize           - Connection->RWPosition));
		if(BytesRead == -1){
			if(errno != EAGAIN){
				// NOTE(fusion): Connection error.
				CloseConnection(Connection);
			}
			break;
		}else if(BytesRead == 0){
			// NOTE(fusion): Graceful close.
			CloseConnection(Connection);
			break;
		}

		Connection->RWPosition += BytesRead;
		if(Connection->RWPosition >= ReadSize){
			if(Connection->RWSize != 0){
				Connection->State = CONNECTION_PROCESSING;
				Connection->LastActive = g_MonotonicTimeMS;
				break;
			}else if(Connection->RWPosition == 2){
				int PayloadSize = BufferRead16LE(Connection->Buffer);
				if(PayloadSize <= 0 || PayloadSize > g_MaxConnectionPacketSize){
					CloseConnection(Connection);
					break;
				}

				if(PayloadSize != 0xFFFF){
					Connection->RWSize = PayloadSize;
					Connection->RWPosition = 0;
				}
			}else if(Connection->RWPosition == 6){
				int PayloadSize = (int)BufferRead32LE(Connection->Buffer + 2);
				if(PayloadSize <= 0 || PayloadSize > g_MaxConnectionPacketSize){
					CloseConnection(Connection);
					break;
				}

				Connection->RWSize = PayloadSize;
				Connection->RWPosition = 0;
			}else{
				PANIC("Invalid input state (State: %d, RWSize: %d, RWPosition: %d)",
						Connection->State, Connection->RWSize, Connection->RWPosition);
			}
		}
	}

	if(Connection->State == CONNECTION_PROCESSING){
		ProcessConnectionQuery(Connection);
	}
}

void CheckConnectionOutput(TConnection *Connection, int Events){
	if((Events & POLLOUT) == 0 || Connection->Socket == -1){
		return;
	}

	if(Connection->State != CONNECTION_WRITING){
		return;
	}

	while(true){
		int BytesWritten = write(Connection->Socket,
				(Connection->Buffer + Connection->RWPosition),
				(Connection->RWSize - Connection->RWPosition));
		if(BytesWritten == -1){
			if(errno != EAGAIN){
				CloseConnection(Connection);
			}
			break;
		}

		Connection->RWPosition += BytesWritten;
		if(Connection->RWPosition >= Connection->RWSize){
			Connection->State = CONNECTION_READING;
			Connection->RWSize = 0;
			Connection->RWPosition = 0;
			break;
		}
	}
}

void CheckConnection(TConnection *Connection, int Events){
	ASSERT((Events & POLLNVAL) == 0);

	if((Events & (POLLERR | POLLHUP)) != 0){
		CloseConnection(Connection);
	}

	if(g_MaxConnectionIdleTime > 0){
		int IdleTime = (g_MonotonicTimeMS - Connection->LastActive);
		if(IdleTime >= g_MaxConnectionIdleTime){
			LOG_WARN("Dropping connection %s due to inactivity",
					Connection->RemoteAddress);
			CloseConnection(Connection);
		}
	}

	if(Connection->Socket == -1){
		ReleaseConnection(Connection);
	}
}

void ProcessConnections(void){
	// NOTE(fusion): Accept new connections.
	while(true){
		uint32 Addr;
		uint16 Port;
		int Socket = ListenerAccept(g_Listener, &Addr, &Port);
		if(Socket == -1){
			break;
		}

		if(AssignConnection(Socket, Addr, Port) == NULL){
			LOG_ERR("Rejecting connection %08X:%d due to max number of"
					" connections being reached (%d)", Addr, Port, g_MaxConnections);
			close(Socket);
		}
	}

	// NOTE(fusion): Gather active connections.
	int NumConnections = 0;
	int *ConnectionIndices = (int*)alloca(g_MaxConnections * sizeof(int));
	pollfd *ConnectionFds  = (pollfd*)alloca(g_MaxConnections * sizeof(pollfd));
	for(int i = 0; i < g_MaxConnections; i += 1){
		if(g_Connections[i].State == CONNECTION_FREE || g_Connections[i].Socket == -1){
			continue;
		}

		ConnectionIndices[NumConnections] = i;
		ConnectionFds[NumConnections].fd = g_Connections[i].Socket;
		ConnectionFds[NumConnections].events = POLLIN | POLLOUT;
		ConnectionFds[NumConnections].revents = 0;
		NumConnections += 1;
	}

	if(NumConnections <= 0){
		return;
	}

	// NOTE(fusion): Poll connections.
	int NumEvents = poll(ConnectionFds, NumConnections, 0);
	if(NumEvents == -1){
		LOG_ERR("Failed to poll connections: (%d) %s", errno, strerrordesc_np(errno));
		return;
	}

	// NOTE(fusion): Process connections.
	for(int i = 0; i < NumConnections; i += 1){
		TConnection *Connection = &g_Connections[ConnectionIndices[i]];
		int Events = (int)ConnectionFds[i].revents;
		CheckConnectionInput(Connection, Events);
		CheckConnectionOutput(Connection, Events);
		CheckConnection(Connection, Events);
	}
}

bool InitConnections(void){
	ASSERT(g_Listener == -1);
	ASSERT(g_Connections == NULL);

	LOG("Query manager port: %d", g_QueryManagerPort);
	LOG("Max connections: %d", g_MaxConnections);
	LOG("Max connection idle time: %dms", g_MaxConnectionIdleTime);
	LOG("Max connection packet size: %d", g_MaxConnectionPacketSize);

	g_Listener = ListenerBind((uint16)g_QueryManagerPort);
	if(g_Listener == -1){
		LOG_ERR("Failed to bind listener");
		return false;
	}

	g_Connections = (TConnection*)calloc(
			g_MaxConnections, sizeof(TConnection));
	for(int i = 0; i < g_MaxConnections; i += 1){
		g_Connections[i].State = CONNECTION_FREE;
	}

	return true;
}

void ExitConnections(void){
	if(g_Listener != -1){
		close(g_Listener);
		g_Listener = -1;
	}

	if(g_Connections != NULL){
		for(int i = 0; i < g_MaxConnections; i += 1){
			ReleaseConnection(&g_Connections[i]);
		}

		free(g_Connections);
		g_Connections = NULL;
	}
}

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

TWriteBuffer PrepareResponse(TConnection *Connection, int Status){
	if(Connection->State != CONNECTION_PROCESSING){
		LOG_ERR("Connection %s is not processing query (State: %d)",
				Connection->RemoteAddress, Connection->State);
		CloseConnection(Connection);
		return TWriteBuffer(NULL, 0);
	}

	TWriteBuffer WriteBuffer(Connection->Buffer, g_MaxConnectionPacketSize);
	WriteBuffer.Write16(0);
	WriteBuffer.Write8((uint8)Status);
	return WriteBuffer;
}

void SendResponse(TConnection *Connection, TWriteBuffer *WriteBuffer){
	if(Connection->State != CONNECTION_PROCESSING){
		LOG_ERR("Connection %s is not processing query (State: %d)",
				Connection->RemoteAddress, Connection->State);
		CloseConnection(Connection);
		return;
	}

	ASSERT(WriteBuffer != NULL
		&& WriteBuffer->Buffer == Connection->Buffer
		&& WriteBuffer->Size == g_MaxConnectionPacketSize
		&& WriteBuffer->Position > 2);

	int PayloadSize = WriteBuffer->Position - 2;
	if(PayloadSize < 0xFFFF){
		WriteBuffer->Rewrite16(0, (uint16)PayloadSize);
	}else{
		WriteBuffer->Rewrite16(0, 0xFFFF);
		WriteBuffer->Insert32(2, (uint32)PayloadSize);
	}

	if(!WriteBuffer->Overflowed()){
		Connection->State = CONNECTION_WRITING;
		Connection->RWSize = WriteBuffer->Position;
		Connection->RWPosition = 0;
	}else{
		LOG_ERR("Write buffer overflowed when writing response to %s",
				Connection->RemoteAddress);
		CloseConnection(Connection);
	}
}

void SendQueryStatusOk(TConnection *Connection){
	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_OK);
	SendResponse(Connection, &WriteBuffer);
}

void SendQueryStatusError(TConnection *Connection, int ErrorCode){
	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_ERROR);
	WriteBuffer.Write8((uint8)ErrorCode);
	SendResponse(Connection, &WriteBuffer);
}

void SendQueryStatusFailed(TConnection *Connection){
	TWriteBuffer WriteBuffer = PrepareResponse(Connection, QUERY_STATUS_FAILED);
	SendResponse(Connection, &WriteBuffer);
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
		THouse *Houses = (THouse*)alloca(NumHouses * sizeof(THouse));
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
	// TODO(fusion): Ideally we'd create a new query and dispatch it to the
	// database thread for processing. Realistically, it wouldn't make a big
	// difference since we're already handling connections asynchronously and
	// the only blocking system calls are done by SQLite when interacting with
	// the disk.

	TReadBuffer Buffer(Connection->Buffer, Connection->RWSize);
	uint8 Query = Buffer.Read8();
	if(!Connection->Authorized){
		if(Query == QUERY_LOGIN){
			ProcessLoginQuery(Connection, &Buffer);
		}else{
			LOG_ERR("Expected login query");
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
