#ifndef MYREDIS_H
#define MYREDIS_H

void redisConn(const char *ip, int port);

void r_push(float *analogs, uint8_t *bools, const char *listName);

#endif // MYREDIS_H