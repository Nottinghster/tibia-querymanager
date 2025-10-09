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
			LOG_ERR("Rejecting connection %08X:%d: remote connection", Addr, Port);
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

TConnection *AssignConnection(int Socket, uint32 Addr, uint16 Port){
	int ConnectionIndex = -1;
	for(int i = 0; i < g_Config.MaxConnections; i += 1){
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
		QueryDone(Connection->Query);
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

	if(Connection->Query == NULL){
		Connection->Query = QueryNew();
	}

	uint8 *Buffer = Connection->Query->Buffer;
	int BufferSize = Connection->Query->BufferSize;
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
				(Buffer   + Connection->RWPosition),
				(ReadSize - Connection->RWPosition));
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
				Connection->State = CONNECTION_REQUEST;
				Connection->LastActive = g_MonotonicTimeMS;
				Connection->Query->Request = TReadBuffer(Buffer, Connection->RWSize);
				break;
			}else if(Connection->RWPosition == 2){
				int PayloadSize = BufferRead16LE(Buffer);
				if(PayloadSize <= 0 || PayloadSize > BufferSize){
					CloseConnection(Connection);
					break;
				}

				if(PayloadSize != 0xFFFF){
					Connection->RWSize = PayloadSize;
					Connection->RWPosition = 0;
				}
			}else if(Connection->RWPosition == 6){
				int PayloadSize = (int)BufferRead32LE(Buffer + 2);
				if(PayloadSize <= 0 || PayloadSize > BufferSize){
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
}

void ProcessQuery(TConnection *Connection){
	ASSERT(Connection->Query != NULL);
	QueryEnqueue(Connection->Query);
	Connection->State = CONNECTION_RESPONSE;
}

void SendQueryResponse(TConnection *Connection){
	ASSERT(Connection->Query != NULL);
	if(Connection->State != CONNECTION_RESPONSE){
		LOG_ERR("Connection %s is not in a RESPONSE state (State: %d)",
				Connection->RemoteAddress, Connection->State);
		CloseConnection(Connection);
		return;
	}

	TWriteBuffer *Response = &Connection->Query->Response;
	if(!Response->Overflowed()){
		Connection->State = CONNECTION_WRITING;
		Connection->RWSize = Response->Position;
		Connection->RWPosition = 0;
	}else{
		LOG_ERR("Query buffer overflowed when writing to %s",
				Connection->RemoteAddress);
		CloseConnection(Connection);
	}
}

void SendQueryOk(TConnection *Connection){
	ASSERT(Connection->Query != NULL);
	QueryOk(Connection->Query);
	SendQueryResponse(Connection);
}

void SendQueryError(TConnection *Connection, int ErrorCode){
	ASSERT(Connection->Query != NULL);
	QueryError(Connection->Query, ErrorCode);
	SendQueryResponse(Connection);
}

void SendQueryFailed(TConnection *Connection){
	ASSERT(Connection->Query != NULL);
	QueryFailed(Connection->Query);
	SendQueryResponse(Connection);
}

void CheckConnectionQueryRequest(TConnection *Connection){
	if(Connection->State != CONNECTION_REQUEST){
		return;
	}

	ASSERT(Connection->Query != NULL);
	TQuery *Query = Connection->Query;
	TReadBuffer Request = Query->Request;
	int QueryType = Request.Read8();
	if(!Connection->Authorized){
		if(QueryType != QUERY_LOGIN){
			LOG_ERR("Unauthorized query (%d) %s from %s",
					QueryType, QueryName(QueryType),
					Connection->RemoteAddress);
			CloseConnection(Connection);
			return;
		}

		char Password[30] = {};
		char LoginData[30] = {};
		int ApplicationType = Request.Read8();
		Request.ReadString(Password, sizeof(Password));
		if(ApplicationType == APPLICATION_TYPE_GAME){
			Request.ReadString(LoginData, sizeof(LoginData));
		}

		if(!StringEq(g_Config.QueryManagerPassword, Password)){
			LOG_WARN("Invalid login attempt from %s", Connection->RemoteAddress);
			SendQueryFailed(Connection);
			return;
		}

		// NOTE(fusion): The connection is AUTHORIZED at this point but we still
		// need to check whether the application type is valid, and for the case
		// of a game server, whether the world is valid.
		if(ApplicationType == APPLICATION_TYPE_GAME){
			if(QueryInternalResolveWorld(Query, LoginData)){
				Connection->ApplicationType = APPLICATION_TYPE_GAME;
				StringBufCopy(Connection->LoginData, LoginData);
				ProcessQuery(Connection);
			}else{
				// TODO(fusion): This should probably be a PANIC?
				LOG_ERR("Rejecting connection %s: unable to rewrite login query..."
						" Try increasing the query buffer size", Connection->RemoteAddress);
				SendQueryFailed(Connection);
			}
		}else if(ApplicationType == APPLICATION_TYPE_LOGIN){
			LOG("Connection %s AUTHORIZED to login server", Connection->RemoteAddress);
			Connection->Authorized = true;
			Connection->ApplicationType = APPLICATION_TYPE_LOGIN;
			SendQueryOk(Connection);
		}else if(ApplicationType == APPLICATION_TYPE_WEB){
			LOG("Connection %s AUTHORIZED to web server", Connection->RemoteAddress);
			Connection->Authorized = true;
			Connection->ApplicationType = APPLICATION_TYPE_WEB;
			SendQueryOk(Connection);
		}else{
			LOG_WARN("Rejecting connection %s: unknown application type %d",
					Connection->RemoteAddress, ApplicationType);
			SendQueryFailed(Connection);
		}
	}else if(Connection->ApplicationType == APPLICATION_TYPE_GAME){
		if(QueryType == QUERY_LOGIN_GAME
				|| QueryType == QUERY_LOGOUT_GAME
				|| QueryType == QUERY_SET_NAMELOCK
				|| QueryType == QUERY_BANISH_ACCOUNT
				|| QueryType == QUERY_SET_NOTATION
				|| QueryType == QUERY_REPORT_STATEMENT
				|| QueryType == QUERY_BANISH_IP_ADDRESS
				|| QueryType == QUERY_LOG_CHARACTER_DEATH
				|| QueryType == QUERY_ADD_BUDDY
				|| QueryType == QUERY_REMOVE_BUDDY
				|| QueryType == QUERY_DECREMENT_IS_ONLINE
				|| QueryType == QUERY_FINISH_AUCTIONS
				|| QueryType == QUERY_TRANSFER_HOUSES
				|| QueryType == QUERY_EVICT_FREE_ACCOUNTS
				|| QueryType == QUERY_EVICT_DELETED_CHARACTERS
				|| QueryType == QUERY_EVICT_EX_GUILDLEADERS
				|| QueryType == QUERY_INSERT_HOUSE_OWNER
				|| QueryType == QUERY_UPDATE_HOUSE_OWNER
				|| QueryType == QUERY_DELETE_HOUSE_OWNER
				|| QueryType == QUERY_GET_HOUSE_OWNERS
				|| QueryType == QUERY_GET_AUCTIONS
				|| QueryType == QUERY_START_AUCTION
				|| QueryType == QUERY_INSERT_HOUSES
				|| QueryType == QUERY_CLEAR_IS_ONLINE
				|| QueryType == QUERY_CREATE_PLAYERLIST
				|| QueryType == QUERY_LOG_KILLED_CREATURES
				|| QueryType == QUERY_LOAD_PLAYERS
				|| QueryType == QUERY_EXCLUDE_FROM_AUCTIONS
				|| QueryType == QUERY_CANCEL_HOUSE_TRANSFER
				|| QueryType == QUERY_LOAD_WORLD_CONFIG){
			ProcessQuery(Connection);
		}else{
			LOG_ERR("Invalid GAME query (%d) %s from %s",
					QueryType, QueryName(QueryType),
					Connection->RemoteAddress);
			SendQueryFailed(Connection);
		}
	}else if(Connection->ApplicationType == APPLICATION_TYPE_LOGIN){
		if(QueryType == QUERY_LOGIN_ACCOUNT){
			ProcessQuery(Connection);
		}else{
			LOG_ERR("Invalid LOGIN query %d (%s) from %s",
					QueryType, QueryName(QueryType),
					Connection->RemoteAddress);
			SendQueryFailed(Connection);
		}
	}else if(Connection->ApplicationType == APPLICATION_TYPE_WEB){
		if(QueryType == QUERY_CHECK_ACCOUNT_PASSWORD
				|| QueryType == QUERY_CREATE_ACCOUNT
				|| QueryType == QUERY_CREATE_CHARACTER
				|| QueryType == QUERY_GET_ACCOUNT_SUMMARY
				|| QueryType == QUERY_GET_CHARACTER_PROFILE
				|| QueryType == QUERY_GET_WORLDS
				|| QueryType == QUERY_GET_ONLINE_CHARACTERS
				|| QueryType == QUERY_GET_KILL_STATISTICS){
			ProcessQuery(Connection);
		}else{
			LOG_ERR("Invalid WEB query (%d) %s from %s",
					QueryType, QueryName(QueryType),
					Connection->RemoteAddress);
			SendQueryFailed(Connection);
		}
	}
}

void CheckConnectionQueryResponse(TConnection *Connection){
	if(Connection->State != CONNECTION_RESPONSE){
		return;
	}

	ASSERT(Connection->Query != NULL);
	TQuery *Query = Connection->Query;
	if(QueryRefCount(Connection->Query) != 1){
		return;
	}

	if(Query->QueryType == QUERY_INTERNAL_RESOLVE_WORLD){
		if(Query->QueryStatus == QUERY_STATUS_OK){
			ASSERT(Query->WorldID > 0);
			LOG("Connection %s AUTHORIZED to game server \"%s\"",
					Connection->RemoteAddress, Connection->LoginData);
			Connection->Authorized = true;
			SendQueryOk(Connection);
		}else{
			// NOTE(fusion): The connection is automatically dropped if it
			// hasn't been authorized by the end of the first query.
			LOG_WARN("Rejecting connection %s: unknown game server \"%s\"",
					Connection->RemoteAddress, Connection->LoginData);
			SendQueryFailed(Connection);
		}
	}else{
		if(Query->QueryStatus == QUERY_STATUS_FAILED){
			LOG_WARN("Query (%d) %s from %s has FAILED",
					Query->QueryType,
					QueryName(Query->QueryType),
					Connection->RemoteAddress);
		}

		SendQueryResponse(Connection);
	}
}


void CheckConnectionOutput(TConnection *Connection, int Events){
	if((Events & POLLOUT) == 0 || Connection->Socket == -1){
		return;
	}

	if(Connection->State != CONNECTION_WRITING){
		return;
	}

	ASSERT(Connection->Query != NULL);
	uint8 *Buffer = Connection->Query->Buffer;
	while(true){
		int BytesWritten = write(Connection->Socket,
				(Buffer             + Connection->RWPosition),
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

			// NOTE(fusion): Close the connection if it's not authorized after
			// the first query.
			if(!Connection->Authorized){
				CloseConnection(Connection);
			}
			break;
		}
	}
}

void CheckConnection(TConnection *Connection, int Events){
	ASSERT((Events & POLLNVAL) == 0);

	if((Events & (POLLERR | POLLHUP)) != 0){
		CloseConnection(Connection);
	}

	if(g_Config.MaxConnectionIdleTime > 0){
		int IdleTime = (g_MonotonicTimeMS - Connection->LastActive);
		if(IdleTime >= g_Config.MaxConnectionIdleTime){
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
			LOG_ERR("Rejecting connection %08X:%d:"
					" max number of connections reached (%d)",
					Addr, Port, g_Config.MaxConnections);
			close(Socket);
		}
	}

	// NOTE(fusion): Gather active connections.
	int NumConnections = 0;
	int *ConnectionIndices = (int*)alloca(g_Config.MaxConnections * sizeof(int));
	pollfd *ConnectionFds  = (pollfd*)alloca(g_Config.MaxConnections * sizeof(pollfd));
	for(int i = 0; i < g_Config.MaxConnections; i += 1){
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
		CheckConnectionQueryRequest(Connection);
		CheckConnectionQueryResponse(Connection);
		CheckConnectionOutput(Connection, Events);
		CheckConnection(Connection, Events);
	}
}

bool InitConnections(void){
	ASSERT(g_Listener == -1);
	ASSERT(g_Connections == NULL);

	g_Listener = ListenerBind((uint16)g_Config.QueryManagerPort);
	if(g_Listener == -1){
		LOG_ERR("Failed to bind listener");
		return false;
	}

	g_Connections = (TConnection*)calloc(
			g_Config.MaxConnections, sizeof(TConnection));
	for(int i = 0; i < g_Config.MaxConnections; i += 1){
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
		for(int i = 0; i < g_Config.MaxConnections; i += 1){
			ReleaseConnection(&g_Connections[i]);
		}

		free(g_Connections);
		g_Connections = NULL;
	}
}

