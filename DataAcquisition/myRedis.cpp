#include "gVal.h"
#include <hiredis.h>
#include "myRedis.h"
#include "easylogging++.h"

void redisConn(const char *ip, int port)
{
    c = redisConnect(ip, port);
    if (c == NULL || c->err) {
        if (c) {
            printf("Redis connection error: %s\n", c->errstr);
            std::stringstream ss;
	        ss << c->errstr;
            LOG(ERROR) << "Redis connection error: " << ss.str();
        } else {
            puts("Redis connection error: can't allocate redis context");
            LOG(ERROR) << "Redis connection error: can't allocate redis context";
        }
        clean();
        exit(EXIT_FAILURE);
    } 
    else
    {
        puts("Redis successed to connect");
        LOG(INFO) << "Redis successed to connect";
    }
}

void r_push(float *analogs, uint8_t *bools, const char *redisListName)
{
    auto now = std::chrono::system_clock::now();  // 获取当前系统时钟时间
    auto duration = now.time_since_epoch();  // 转换为时间段（自 epoch 以来的时间）
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();  // 转换为毫秒级时间戳

    std::vector<float> analogs_vector(analogs, analogs + ANALOG_COLS);
    std::vector<uint8_t> bools_vector(bools, bools + BOOL_COLS);
    json j_vec;
    j_vec["timestamp"] = millis;
    j_vec["analogs"] = analogs_vector;
    j_vec["bools"] = bools_vector;
    std::string str = j_vec.dump();
    char* cstr = new char[str.length() + 1];
    std::strcpy(cstr, str.c_str());

    redisCommand(c, "RPUSH %s %s", redisListName, cstr);
    delete[] cstr;
}
