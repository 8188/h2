#ifndef DATA_ACQUISITION_SAVE_H
#define DATA_ACQUISITION_SAVE_H

// #include "gVal.h"

#define LOG_FILE_NAME "logs/info.%datetime{%Y%M%d}.log"
#define MAX_LOG_FILE_SIZE "1000000"

uint16_t *modbusReadData;
uint8_t *readBools;
float *readAnalogs;

INITIALIZE_EASYLOGGINGPP

void setLogger()
{
    el::Logger* logger = el::Loggers::getLogger("myLog");
    el::Configurations conf;
    conf.setToDefault();
    conf.setGlobally(
        el::ConfigurationType::Format,
        "%datetime [%level] [%func] [%loc] %msg");
    conf.setGlobally(
        el::ConfigurationType::ToFile,
        "true");
    conf.setGlobally(
        el::ConfigurationType::Filename,
        LOG_FILE_NAME);
    conf.setGlobally(
        el::ConfigurationType::ToStandardOutput,
        "false");
    conf.setGlobally(
        el::ConfigurationType::MillisecondsWidth,
        "3");
    conf.setGlobally(
        el::ConfigurationType::MaxLogFileSize,
        MAX_LOG_FILE_SIZE);
    el::Loggers::reconfigureAllLoggers(conf);

    LOG(INFO) << "--------The program has started--------";
}

void insert(redisReply *reply, int len, int dev, const char *taosTableName, const char* type)
{
    char *str1 = myMalloc<char>(len * 2 + 1);
    int j = 0;
    for (int i = 0; i < len; ++i, j += 2)
    {
        str1[j] = '?';
        str1[j + 1] = ',';
    }

    str1[j - 1] = '\0';
    char stn[16] = "s_";
    strcat(stn, taosTableName);

    char *sql1 = myMalloc<char>(len * 2 + 100);
    sprintf(sql1, "INSERT INTO ? USING %s TAGS(?) VALUES(?, %s)", stn, str1);
    free(str1);
    
    TAOS_STMT *stmt = taos_stmt_init(taos);
    int code = taos_stmt_prepare(stmt, sql1, 0);
    free(sql1);
    checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");

    TAOS_MULTI_BIND tags[1];
    tags[0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[0].buffer_length = sizeof(int);
    tags[0].is_null = NULL;
    tags[0].buffer = &dev;

    TAOS_MULTI_BIND *values = new TAOS_MULTI_BIND[len + 1];
    values[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    values[0].buffer_length = sizeof(int64_t);
    values[0].is_null = NULL;
    values[0].num = 1;

    int insertNums = reply->elements;
    json j_obj;
    long long now;
    time_t t;
    struct tm *tm_now;
    char buf[64];
    char tableName[100];
    float *fdata = new float[len];
    uint8_t *bdata = new uint8_t[len];

    for (int i = 0; i < insertNums; i++)
    {
        j_obj = json::parse(reply->element[i]->str);
        now = j_obj.at("timestamp");
        t = now / 1000;
        tm_now = localtime(&t);
        memset(buf, 0, 64);
        strftime(buf, 64, "%Y%m%d", tm_now);
        memset(tableName, 0, 100);
        sprintf(tableName, "%s%s", taosTableName, buf);

        code = taos_stmt_set_tbname_tags(stmt, tableName, tags);
        checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

        values[0].buffer = &now;

        if (type == "FLOAT")
        {
            std::vector<float> data = j_obj.at("analogs");

            for (int i = 1; i < len + 1; ++i)
            {
                values[i].buffer_type = TSDB_DATA_TYPE_FLOAT;
                fdata[i - 1] = data[i - 1] * 1.0f;
                values[i].buffer = &fdata[i - 1];
                values[i].buffer_length = sizeof(float);
                values[i].is_null = NULL;
                values[i].num = 1;
            }
        }
        else
        {
            std::vector<int8_t> data = j_obj.at("bools");

            for (int i = 1; i < len + 1; ++i)
            {
                values[i].buffer_type = TSDB_DATA_TYPE_BOOL;
                // printf("%d ", data[i - 1]);
                bdata[i - 1] = data[i - 1];
                values[i].buffer = &bdata[i - 1];
                values[i].buffer_length = sizeof(int8_t);
                values[i].is_null = NULL;
                values[i].num = 1;
            }
        }

        code = taos_stmt_bind_param(stmt, values);
        checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param");

        code = taos_stmt_add_batch(stmt);
        checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
    }
    delete[] values;
    delete[] fdata;
    delete[] bdata;

    code = taos_stmt_execute(stmt);
    checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");

    int affectedRows = taos_stmt_affected_rows(stmt);
    printf("successfully inserted dev:%d, table:%s, %d rows\n", dev, taosTableName, affectedRows);

    taos_stmt_close(stmt);
}

void lrange_and_insert(const char* redisListName, const char* taosTableName, int nb, int dev, const char* type)
{
    redisReply *reply = (redisReply *)redisCommand(c, "LRANGE %s 0 -1", redisListName);

    if (reply->type == REDIS_REPLY_ARRAY)
    {
        insert(reply, nb, dev, taosTableName, type);
        printf("Get elements from %s: %lu\n", redisListName, reply->elements);
    }

    freeReplyObject(reply);
}

#endif // DATA_ACQUISITION_SAVE_H