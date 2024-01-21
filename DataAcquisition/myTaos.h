#ifndef MYTAOS_H
#define MYTAOS_H

void taosConn(const char *ip, const char *username, const char *password, const char *database, uint16_t port);

void executeSQL(const char *sql);

void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg);

void create_stable(int len, const char *tn, const char *type);

char *myQuery();

#endif // MYTAOS_H