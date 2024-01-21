#include <string.h> // 包含 memset 函数的头文件
#include <stdio.h>  // 包含 puts 函数的头文件
#include <stdlib.h> // 包含 malloc 函数的头文件
#include <assert.h>
#include "taos.h"

#define ANALOG_COLS 307
#define BOOL_COLS 560
#define STABLE_NAME1 "Analog"
#define STABLE_NAME2 "Bool"

TAOS *taos;

void clean()
{
    taos_close(taos);
    taos_cleanup();
}

template <typename T>
T *myMalloc(int size)
{
    T *array = (T *)malloc(size * sizeof(T));
    if (array == NULL)
    {
        puts("Malloc Error: Out of memory!");
        clean();
        exit(EXIT_FAILURE);
    }
    return array;
}

void taosConn(const char *ip, const char *username, const char *password, const char *database, uint16_t port)
{
    taos = taos_connect(ip, username, password, database, port);
    if (taos == NULL)
    {
        puts("Taos failed to connect");
        exit(EXIT_FAILURE);
    }
    else
    {
        puts("Taos successed to connect");
    }
}

void executeSQL(const char *sql)
{
    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0)
    {
        printf("Error code: %d; Message: %s\n", code, taos_errstr(res));
        taos_free_result(res);
        clean();
        exit(EXIT_FAILURE);
    }
    int affectedRows = taos_affected_rows(res);
    printf("Affected rows %d\n", affectedRows);
    taos_free_result(res);
}

void create_stable(int len, const char *tn, const char *type)
{
    int size = len * 12;
    char *str2 = myMalloc<char>(size);
    memset(str2, 0, size);
    for (int i = 0; i < len; ++i)
    {
        char num_str[16] = {0};
        sprintf(num_str, "c%d %s,", i, type);
        assert((strlen(str2) + strlen(num_str) < size));
        strcat(str2, num_str);
    }
    str2[strlen(str2) - 1] = '\0';

    char *sql2 = myMalloc<char>(size + 100);
    char stn[16] = "s_";
    strcat(stn, tn);
    sprintf(sql2, "DROP STABLE IF EXISTS %s", stn);
    // taos_query_a(taos, sql2, select_callback, taos);
    executeSQL(sql2);

    memset(sql2, 0, size + 100);
    sprintf(sql2, "CREATE STABLE IF NOT EXISTS %s (ts TIMESTAMP, %s) TAGS (dev INT)", stn, str2);
    free(str2);
    executeSQL(sql2);
    free(sql2);
}

int main()
{
    const char *taos_ip = "127.0.0.1";
    const char *taos_username = "root";
    const char *taos_password = "taosdata";
    const char *taos_database = "test1";
    uint16_t taos_port = 6030;
    taosConn(taos_ip, taos_username, taos_password, taos_database, taos_port);
    create_stable(ANALOG_COLS, STABLE_NAME1, "FLOAT");
    create_stable(BOOL_COLS, STABLE_NAME2, "BOOL");
    return 0;
}
