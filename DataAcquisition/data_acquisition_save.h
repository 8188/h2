#ifndef DATA_ACQUISITION_SAVE_H
#define DATA_ACQUISITION_SAVE_H

// #include "gVal.h"

#define LOG_FILE_NAME "logs/info.%datetime{%Y%M%d}.log"
#define MAX_LOG_FILE_SIZE "1000000"

uint16_t *modbusReadData;
uint8_t *readBools;
float *readAnalogs;
std::vector<uint64_t> timestamps;
std::vector<std::vector<float>> gAnalogs;
std::vector<std::vector<uint8_t>> gBools;

std::unordered_map<std::string, int> typeMap = {
    {"BOOL", TSDB_DATA_TYPE_BOOL},
    {"FLOAT", TSDB_DATA_TYPE_FLOAT},
    {"INT", TSDB_DATA_TYPE_INT}
};

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

void saveTimestamps()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    timestamps.push_back(millis);
}

template <typename T>
void saveDatum(T *readData, int cols, std::vector<std::vector<T>>& saveData)
{
    std::vector<T> bools_vector(readData, readData + cols);
    saveData.push_back(bools_vector);
    free(readData);
}

template <typename T>
void newInsert(std::vector<std::vector<T>>& data, int len, int dev, const char *taosTableName, const std::string& type)
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

    int insertNums = timestamps.size();
    long long now;
    time_t t;
    struct tm *tm_now;
    char buf[64];
    char tableName[100];

    int bufferType{0};
    auto iter = typeMap.find(type);
    if (iter != typeMap.end())
    {
        bufferType = iter->second;
    }

    for (int i = 0; i < insertNums; i++)
    {
        now = timestamps[i];
        t = now / 1000;
        tm_now = localtime(&t);
        memset(buf, 0, 64);
        strftime(buf, 64, "%Y%m%d", tm_now);
        memset(tableName, 0, 100);
        sprintf(tableName, "%s%s", taosTableName, buf);

        code = taos_stmt_set_tbname_tags(stmt, tableName, tags);
        checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

        values[0].buffer = &now;

        for (int j = 1; j < len + 1; ++j)
        {
            values[j].buffer_type = bufferType;
            values[j].buffer = &data[i][j - 1];
            values[j].buffer_length = sizeof(T);
            values[j].is_null = NULL;
            values[j].num = 1;
        }

        code = taos_stmt_bind_param(stmt, values);
        checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param");

        code = taos_stmt_add_batch(stmt);
        checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
    }
    delete[] values;

    code = taos_stmt_execute(stmt);
    checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");

    int affectedRows = taos_stmt_affected_rows(stmt);
    printf("successfully inserted dev:%d, table:%s, %d rows\n", dev, taosTableName, affectedRows);

    taos_stmt_close(stmt);
}

#endif // DATA_ACQUISITION_SAVE_H