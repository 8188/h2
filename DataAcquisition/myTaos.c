#include "gVal.h"
#include "taos.h"
#include "myTaos.h"
#include "easylogging++.h"

void taosConn(const char *ip, const char *username, const char *password, const char *database, uint16_t port)
{
    taos = taos_connect(ip, username, password, database, port);
    if (taos == NULL)
    {
        puts("Taos failed to connect");
        LOG(ERROR) << "Taos failed to connect";
        exit(EXIT_FAILURE);
    }
    else
    {
        puts("Taos successed to connect");
        LOG(INFO) << "Taos successed to connect";
    }
}

void select_callback(void *param, TAOS_RES *res, int code)
{
    TAOS *_taos = (TAOS *)param;
    if (code == 0 && res)
    {
        // printHeader(res);
        // taos_fetch_rows_a(res, fetch_row_callback, _taos);
    }
    else
    {
        printf("Failed to execute taos_query. error: %s\n", taos_errstr(res));
        std::stringstream ss;
        ss << taos_errstr(res);
        LOG(ERROR) << "Failed to execute taos_query. error: " << ss.str();
        taos_free_result(res);
        clean();
        exit(EXIT_FAILURE);
    }
    int affectedRows = taos_affected_rows(res);
    printf("Affected rows %d\n", affectedRows);
    LOG(INFO) << "Affected rows " << affectedRows;
    taos_free_result(res);
}

void executeSQL(const char *sql)
{
    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0)
    {
        printf("Error code: %d; Message: %s\n", code, taos_errstr(res));
        std::stringstream ss;
	    ss << taos_errstr(res);
        LOG(ERROR) << "Error code: " << code << "; Message: " << ss.str();
        taos_free_result(res);
        clean();
        exit(EXIT_FAILURE);
    }
    int affectedRows = taos_affected_rows(res);
    printf("Affected rows %d\n", affectedRows);
    LOG(INFO) << "Affected rows " << affectedRows;
    taos_free_result(res);
}

void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg)
{
    if (code != 0)
    {
        printf("%s. error: %s\n", msg, taos_stmt_errstr(stmt));
        std::stringstream ss1;
	    ss1 << msg;
        std::stringstream ss2;
	    ss2 << taos_stmt_errstr(stmt);
        LOG(ERROR) << ss1.str() << ". error: " << ss2.str();
        taos_stmt_close(stmt);
        clean();
        exit(EXIT_FAILURE);
    }
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
        // assert((strlen(str2) + strlen(num_str) < size));
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
    // taos_query_a(taos, sql2, select_callback, taos); // 异步还没执行完，下面已经开始执行，报错？
    executeSQL(sql2);
    free(sql2);
}

char *myQuery()
{
    const char *sql = "SELECT * FROM s_bool LIMIT 1";
    TAOS_RES *res = taos_query(taos, sql);
    int code;
    if ((code = taos_errno(res)) != 0)
    {
        printf("Taos error code: %d; Message: %s\n", code, taos_errstr(res));
        std::stringstream ss;
	    ss << taos_errstr(res);
        LOG(ERROR) << "Taos error code: " << code << "; Message: " << ss.str();
        taos_free_result(res);
        clean();
        exit(EXIT_FAILURE);
    }

    int numFields = taos_num_fields(res);
    TAOS_FIELD *fields = taos_fetch_fields(res);
    TAOS_ROW row = taos_fetch_row(res);
    json jsonMsg;

    for (int i = 0; i < numFields; ++i)
    {
        const char *fieldName = fields[i].name;

        if (row != nullptr && row[i] != nullptr)
        {
            switch (fields[i].type)
            {
            case TSDB_DATA_TYPE_INT:
            {
                jsonMsg[fieldName] = *((int32_t *)row[i]);
                break;
            }
            case TSDB_DATA_TYPE_FLOAT:
            {
                jsonMsg[fieldName] = *((float *)row[i]);
                break;
            }
            case TSDB_DATA_TYPE_BOOL:
            {
                printf("%d ", *((int8_t *)row[i]));
                jsonMsg[fieldName] = *((int8_t *)row[i]);
                break;
            }
            case TSDB_DATA_TYPE_TIMESTAMP:
            {
                jsonMsg[fieldName] = *((int64_t *)row[i]);
                break;
            }
            default:
                break;
            }
        }
    }
    std::string str = jsonMsg.dump();
    char* value = new char[str.length() + 1];
    std::strcpy(value, str.c_str());
    
    taos_free_result(res);
    return value;
}
