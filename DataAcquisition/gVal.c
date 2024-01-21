#include "gVal.h"

modbus_t *ctx;
TAOS *taos;
MQTTAsync client;

void clean()
{
    modbus_close(ctx);
    modbus_free(ctx);
    taos_close(taos);
    taos_cleanup();
    MQTTAsync_destroy(&client);
}

void removeQuotes(char *str)
{
    size_t len = strlen(str) - 2;
    assert(len > 0);
    memmove(str, str + 1, len);
    str[len] = '\0';
}
