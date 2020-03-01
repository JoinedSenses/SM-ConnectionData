#pragma semicolon 1
#pragma newdecls required

#include <sourcemod>
#include <geoip>

#define PLUGIN_VERSION "0.1.0"
#define PLUGIN_DESCRIPTION "Stores player connection data"

bool g_bLateLoad;

Database g_Database;
char g_sCurrentMap[80];
char g_sServerIP[22];
int g_iMapStart;
int g_iMapId;

enum struct PlayerData {
	char name[32];
	bool initial;
	char auth2[32];
	bool inserted;
	int id;

	void Clear() {
		this.name[0] = '\0';
		this.initial = false;
		this.auth2[0] = '\0';
		this.inserted = false;
		this.id = 0;
	}
}

PlayerData g_PData[MAXPLAYERS+1];

// ---------------

public Plugin myinfo = {
	name = "Connection Data",
	description = PLUGIN_DESCRIPTION,
	author = "JoinedSenses",
	version = PLUGIN_VERSION,
	url = "https://github.com/JoinedSenses"
};

// ---------------

public APLRes AskPluginLoad2(Handle myself, bool late, char[] error, int err_max) {
	g_bLateLoad = late;

	return APLRes_Success;
}

public void OnPluginStart() {
	CreateConVar(
		  "sm_connectiondata_version"
		, PLUGIN_VERSION, PLUGIN_DESCRIPTION
		, FCVAR_SPONLY|FCVAR_REPLICATED|FCVAR_NOTIFY|FCVAR_DONTRECORD
	).SetString(PLUGIN_VERSION);

	Database.Connect(dbConnect, "connectiondata");

	HookEvent("player_connect", eventPlayerConnect);
	HookEvent("player_disconnect", eventPlayerDisconnect);

	int ip = FindConVar("hostip").IntValue;

	Format(g_sServerIP, sizeof g_sServerIP, "%d.%d.%d.%d:%d"
		, ((ip & 0xFF000000) >> 24) & 0xFF
		, ((ip & 0x00FF0000) >> 16) & 0xFF
		, ((ip & 0x0000FF00) >>  8) & 0xFF
		, ((ip & 0x000000FF) >>  0) & 0xFF
		, FindConVar("hostport").IntValue
	);

	if (g_bLateLoad) {
		for (int i = 1; i <= MaxClients; ++i) {
			if (IsClientInGame(i) && !IsFakeClient(i)) {
				GetClientAuthId(i, AuthId_Steam2, g_PData[i].auth2, sizeof PlayerData::auth2);
			}
		}
	}
}

public void OnMapStart() {
	GetCurrentMap(g_sCurrentMap, sizeof g_sCurrentMap);
	g_iMapStart = RoundFloat(GetEngineTime());
	g_iMapId = 0;

	if (g_bLateLoad) {
		return;
	}

	startMapSession();
}

public void OnMapEnd() {
	if (g_bLateLoad) {
		g_bLateLoad = false;
		return;
	}

	endMapSession();
}

// ---------------

public void eventPlayerConnect(Event event, const char[] name, bool dontBroadcast) {
	if (!g_Database) {
		return;
	}

	if (event.GetBool("bot")) {
		return;
	}

	int idx = event.GetInt("index") + 1;
	g_PData[idx].initial = true;
	event.GetString("name", g_PData[idx].name, sizeof PlayerData::name);
}

public void OnClientConnected(int client) {
	if (!g_PData[client].initial) {
		return;
	}

	g_PData[client].initial = false;

	startClientSession(client);
}

public void OnClientAuthorized(int client, const char[] auth) {
	if (!g_Database || IsFakeClient(client)) {
		return;
	}

	GetClientAuthId(client, AuthId_Steam2, g_PData[client].auth2, sizeof PlayerData::auth2);

	if (g_PData[client].inserted) {
		if (g_PData[client].id) {
			runAuthQuery(client);
		}
	}
	else {
		CreateTimer(2.0, timerWaitForInsertion, GetClientUserId(client), TIMER_REPEAT);
	}
}

public Action timerWaitForInsertion(Handle timer, int userid) {
	int client = GetClientOfUserId(userid);
	if (client) {	
		if (!g_PData[client].inserted) {
			return Plugin_Continue;
		}

		if (g_PData[client].id) {
			runAuthQuery(client);
		}
	}

	return Plugin_Stop;
}

public void eventPlayerDisconnect(Event event, const char[] name, bool dontBroadcast) {
	if (!g_Database) {
		return;
	}

	int client = GetClientOfUserId(event.GetInt("userid"));
	if (!client || IsFakeClient(client) || !g_PData[client].inserted) {
		return;
	}

	endClientSession(client);
}

// --------------- Connection and Table Creation

public void dbConnect(Database db, const char[] error, any data) {
	if (db == null || error[0]) {
		LogError("Unable to connect to database (%s)", error);
		return;
	}

	g_Database = db;

	char dbType[16];
	g_Database.Driver.GetProduct(dbType, sizeof dbType);

	char increment[16];
	strcopy(increment, sizeof increment
		, (StrEqual(dbType, "mysql", false) ? "AUTO_INCREMENT" : "AUTOINCREMENT")
	);

	char query[1024];
	g_Database.Format(
		  query
		, sizeof query
		, "CREATE TABLE IF NOT EXISTS `connect_sessions` "
		... "("
			... "`id` INT UNSIGNED NOT NULL %s PRIMARY KEY, "
			... "`serverip` VARCHAR(39) NOT NULL, "
			... "`playerCount` TINYINT NOT NULL, "
			... "`map` VARCHAR(80) NOT NULL, "
			... "`name` VARCHAR(64) NOT NULL, "
			... "`authid2` VARCHAR(32) DEFAULT NULL, "
			... "`method` VARCHAR(64) DEFAULT NULL, "
			... "`date` DATE NOT NULL, "
			... "`time` TIME(0) NOT NULL, "
			... "`day` TINYINT NOT NULL, "
			... "`dateString` VARCHAR(32) NOT NULL, "
			... "`duration` INT DEFAULT NULL, "
			... "`ip` VARCHAR(39) NOT NULL, "
			... "`city` VARCHAR(64) NOT NULL, "
			... "`region` VARCHAR(64) NOT NULL, "
			... "`country` VARCHAR(64) NOT NULL"
		... ") ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4"
		, increment
	);

	g_Database.Query(dbCreateTable, query);

	g_Database.Format(
		  query
		, sizeof query
		, "CREATE TABLE IF NOT EXISTS `connect_totals` "
		... "("
			... "`authid2` VARCHAR(32) NOT NULL PRIMARY KEY, "
			... "`totalTime` INT NOT NULL, "
			... "`totalConnects` INT NOT NULL"
		... ")"
	);

	g_Database.Query(dbCreateTable, query);

	g_Database.Format(
		  query
		, sizeof query
		, "CREATE TABLE IF NOT EXISTS `map_sessions` "
		... "("
			... "`id` INT UNSIGNED NOT NULL %s PRIMARY KEY, "
			... "`serverip` VARCHAR(39) NOT NULL, "
			... "`map` VARCHAR(80), "
			... "`date` DATE NOT NULL, "
			... "`time` TIME(0) NOT NULL, "
			... "`day` TINYINT NOT NULL, "
			... "`dateString` VARCHAR(32) NOT NULL, "
			... "`duration` INT DEFAULT NULL"
		... ") ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4"
		, increment
	);

	g_Database.Query(dbCreateTable, query);

	g_Database.Format(
		  query
		, sizeof query
		, "CREATE TABLE IF NOT EXISTS `map_totals` "
		... "("
			... "`map` VARCHAR(80) NOT NULL PRIMARY KEY, "
			... "`totalTime` INT NOT NULL, "
			... "`totalSessions` INT NOT NULL"
		... ")"
	);

	g_Database.Query(dbCreateTable, query);
}

public void dbCreateTable(Database db, DBResultSet results, const char[] error, any data) {
	if (!db || !results || error[0]) {
		LogError("Table creation query failed. (%s)", error);
		return;
	}
}

// --------------- Map Queries

void startMapSession() {
	char date[16];
	FormatTime(date, sizeof(date), "%Y-%m-%d");

	char time[32];
	FormatTime(time, sizeof time, "%X");

	char day[2];
	FormatTime(day, sizeof day, "%w");

	char timeString[64];
	FormatTime(timeString, sizeof timeString, "%H:%M:%S %p %a %d %b %Y");

	char query[2048];
	g_Database.Format(query, sizeof query,
		"INSERT INTO `map_sessions` "
	... "("
		... "`serverip`, "
		... "`map`, "
		... "`date`, "
		... "`time`, "
		... "`day`, "
		... "`dateString`"
	... ")"
	... "VALUES ('%s', '%s', '%s', '%s', '%s', '%s')"
		, g_sServerIP
		, g_sCurrentMap
		, date
		, time
		, day
		, timeString
	);

	g_Database.Query(dbMapSession, query);
}

public void dbMapSession(Database db, DBResultSet results, const char[] error, int userid) {
	if (!db || !results || error[0]) {
		LogError("Map Session query failed. (%s)", error);
		return;
	}

	g_iMapId = results.InsertId;
}

void endMapSession() {
	int duration = RoundFloat(GetEngineTime()) - g_iMapStart;
	
	char query[128];

	g_Database.Format(query, sizeof query,
		"UPDATE `map_sessions` "
	... "SET `duration` = %i "
	... "WHERE `id` = %i",
		duration,
		g_iMapId
	);

	g_Database.Query(dbUpdateMapSession, query);

	DataPack dp = new DataPack();
	dp.WriteCell(duration);
	dp.WriteString(g_sCurrentMap);

	g_Database.Format(query, sizeof query,
		"SELECT `totalTime`, `totalSessions` FROM `map_totals` WHERE `map` = '%s'",
		g_sCurrentMap
	);

	g_Database.Query(dbSelectMapTotals, query, dp);
}

public void dbUpdateMapSession(Database db, DBResultSet results, const char[] error, any data) {
	if (!db || !results || error[0]) {
		LogError("Update Map Session query failed. (%s)", error);
		return;
	}
}

public void dbSelectMapTotals(Database db, DBResultSet results, const char[] error, DataPack dp) {
	if (!db || !results || error[0]) {
		delete dp;
		LogError("Select Map Totals query failed. (%s)", error);
		return;
	}

	dp.Reset();

	int current = dp.ReadCell();

	char map[32];
	dp.ReadString(map, sizeof map);

	delete dp;

	char query[256];
	if (results.FetchRow()) {	
		int time = results.FetchInt(0);
		int count = results.FetchInt(1);

		g_Database.Format(query, sizeof query,
			"UPDATE `map_totals` "
		... "SET `totalTime` = %i, `totalSessions` = %i "
		... "WHERE `map` = '%s'",
			time + current, count + 1,
			map
		);
	}
	else {
		g_Database.Format(query, sizeof query,
			"INSERT INTO `map_totals` (`map`, `totalTime`, `totalSessions`) VALUES ('%s', %i, 1)",
			map,
			current
		);
	}

	g_Database.Query(dbUpdateMapTotals, query);
}

public void dbUpdateMapTotals(Database db, DBResultSet results, const char[] error, any data) {
	if (!db || !results || error[0]) {
		LogError("Update Map Totals query failed. (%s)", error);
		return;
	}
}

// --------------- Connection Queries

void startClientSession(int client) {
	char name[64];
	g_Database.Escape(g_PData[client].name, name, sizeof name);

	char method[64];
	if (GetClientInfo(client, "cl_connectmethod", method, sizeof method)) {
		Format(method, sizeof method, "'%s'", method);
	}
	else {
		strcopy(method, sizeof method, "NULL");
	}

	char date[16];
	FormatTime(date, sizeof(date), "%Y-%m-%d");

	char time[32];
	FormatTime(time, sizeof time, "%X");

	char day[2];
	FormatTime(day, sizeof day, "%w");

	char timeString[64];
	FormatTime(timeString, sizeof timeString, "%H:%M:%S %p %a %d %b %Y");

	char ip[40];
	GetClientIP(client, ip, sizeof ip);

	char city[64];
	GeoipCity(ip, city, sizeof city);

	char region[64];
	GeoipRegion(ip, region, sizeof region);

	char country[64];
	GeoipCountry(ip, country, sizeof country);

	char query[2048];
	FormatEx(query, sizeof query,
		"INSERT INTO `connect_sessions` "
	... "("
		... "`serverip`, "
		... "`playercount`, "
		... "`map`, "
		... "`name`, "
		... "`method`, "
		... "`date`, "
		... "`time`, "
		... "`day`, "
		... "`dateString`, "
		... "`ip`, "
		... "`city`, "
		... "`region`, "
		... "`country`"
	... ")"
	... "VALUES ('%s', %i, '%s', '%s', %s, '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s')"
		, g_sServerIP
		, GetCurrentPlayerCount(client)
		, g_sCurrentMap
		, g_PData[client].name
		, method
		, date
		, time
		, day
		, timeString
		, ip
		, city
		, region
		, country
	);

	g_Database.Query(dbClientConnect, query, GetClientUserId(client));
}

public void dbClientConnect(Database db, DBResultSet results, const char[] error, int userid) {
	int client = GetClientOfUserId(userid);
	if (!client || IsFakeClient(client)) {
		return;
	}

	g_PData[client].inserted = true;

	if (!db || !results || error[0]) {
		LogError("Client connection query failed. (%s)", error);
		return;
	}

	g_PData[client].id = results.InsertId;
}

void runAuthQuery(int client) {
	char query[256];
	g_Database.Format(query, sizeof query,
		"UPDATE `connect_sessions` "
	... "SET `authid2` = '%s'"
	... "WHERE `id` = %i",
		g_PData[client].auth2,
		g_PData[client].id
	);

	g_Database.Query(dbSetAuth, query);
}

public void dbSetAuth(Database db, DBResultSet results, const char[] error, any data) {
	if (!db || !results || error[0]) {
		LogError("Client auth query failed. (%s)", error);
		return;
	}	
}

void endClientSession(int client) {
	int duration = RoundFloat(GetClientTime(client));
	
	char query[128];

	g_Database.Format(query, sizeof query,
		"UPDATE `connect_sessions` "
	... "SET `duration` = %i "
	... "WHERE `id` = %i",
		duration,
		g_PData[client].id
	);

	g_Database.Query(dbUpdatePlayerSession, query);

	if (g_PData[client].auth2[0]) {
		DataPack dp = new DataPack();
		dp.WriteCell(duration);
		dp.WriteString(g_PData[client].auth2);

		g_Database.Format(query, sizeof query,
			"SELECT `totalTime`, `totalConnects` FROM `connect_totals` WHERE `authid2` = '%s'",
			g_PData[client].auth2
		);

		g_Database.Query(dbSelectPlayerTotals, query, dp);
	}

	g_PData[client].Clear();
}

public void dbUpdatePlayerSession(Database db, DBResultSet results, const char[] error, any data) {
	if (!db || !results || error[0]) {
		LogError("Update Player Session query failed. (%s)", error);
		return;
	}
}

public void dbSelectPlayerTotals(Database db, DBResultSet results, const char[] error, DataPack dp) {
	if (!db || !results || error[0]) {
		delete dp;
		LogError("Select Player Totals query failed. (%s)", error);
		return;
	}

	dp.Reset();

	int current = dp.ReadCell();

	char authid2[32];
	dp.ReadString(authid2, sizeof authid2);

	delete dp;

	char query[256];
	if (results.FetchRow()) {	
		int time = results.FetchInt(0);
		int count = results.FetchInt(1);

		g_Database.Format(query, sizeof query,
			"UPDATE `connect_totals` "
		... "SET `totalTime` = %i, `totalConnects` = %i "
		... "WHERE `authid2` = '%s'",
			time + current, count + 1,
			authid2
		);
	}
	else {
		g_Database.Format(query, sizeof query,
			"INSERT INTO `connect_totals` (`authid2`, `totalTime`, `totalConnects`) VALUES ('%s', %i, 1)",
			authid2,
			current
		);
	}

	g_Database.Query(dbUpdatePlayerTotals, query);
}

public void dbUpdatePlayerTotals(Database db, DBResultSet results, const char[] error, any data) {
	if (!db || !results || error[0]) {
		LogError("Update Player Totals query failed. (%s)", error);
		return;
	}
}

int GetCurrentPlayerCount(int client) {
	int count = 0;
	for (int i = 1; i <= MaxClients; ++i) {
		if (IsClientInGame(i) && i != client) {
			++count;
		}
	}

	return count;
}
