#include <taskflow/taskflow.hpp>
#include "gVal.h"
#include "modbus_read.h"
#include "myTaos.h"
#include "myRedis.h"
#include "MQTTAsync_publish.h"
#include "data_acquisition_save.h"
#include <inttypes.h>

int main(int argc, char *argv[])
{
    setLogger();

    std::mutex gMutex;

    std::ifstream config_file("config.json");
    if (!config_file.is_open()) {
        std::cerr << "Failed to open config file\n";
        LOG(ERROR) << "Failed to open config file";
        exit(EXIT_FAILURE);
    }

    json config_data;
    config_file >> config_data;
    char *taos_ip = config_data["taos_ip"].dump().data();
    removeQuotes(taos_ip);
    char *taos_username = config_data["taos_user"].dump().data();
    removeQuotes(taos_username);
    char *taos_password = config_data["taos_pw"].dump().data();
    removeQuotes(taos_password);
    char *taos_database = config_data["taos_db"].dump().data();
    removeQuotes(taos_database);
    uint16_t taos_port = config_data["taos_port"];

    char *modbus_ip = config_data["modbus_ip"].dump().data();
    removeQuotes(modbus_ip);
    int modbus_port = config_data["modbus_port"];
    int modbus_slave_id = config_data["modbus_id"];

    char *redis_ip = config_data["redis_ip"].dump().data();
    removeQuotes(redis_ip);
    int redis_port = config_data["redis_port"];

    // char *mqtt_address = config_data["mqtt_addr"].dump().data();
    // removeQuotes(mqtt_address);
    // char *mqtt_clientid = config_data["mqtt_id"].dump().data();
    // removeQuotes(mqtt_clientid);
    // char *mqtt_topic = config_data["mqtt_topic"].dump().data();
    // removeQuotes(mqtt_topic);
    // unsigned int mqtt_qos = config_data["mqtt_qos"];
    config_file.close();

    const char *mqtt_address = "mqtt://10.64.28.28:1883";
    const char *mqtt_clientid = "dee3fe90-d917-4961-b63a-4bc2f724242a";
    const char *mqtt_topic = "3D";
    int mqtt_qos = 1;
    int device = 1;

    taosConn(taos_ip, taos_username, taos_password, taos_database, taos_port);

    modbusConn(modbus_ip, modbus_port, modbus_slave_id);

    redisConn(redis_ip, redis_port);

    MQTTConn(mqtt_address, mqtt_clientid);

    create_stable(BOOL_COLS, "Bool", "BOOL");
    create_stable(ANALOG_COLS, "Analog", "FLOAT");

    tf::Taskflow f1("F1");

    tf::Task f1A = f1.emplace([&]() {
        modbusReadData = read_registers(START_REGISTERS, NB_REGISTERS);
    }).name("modbus_read");

    tf::Task f1B = f1.emplace([&]() {
        readAnalogs = extractAnalog(modbusReadData);
    }).name("extract_analogs");

    tf::Task f1C = f1.emplace([&]() {
        readBools = extractBool(modbusReadData);
    }).name("extract_bools");

    tf::Task f1D = f1.emplace([&]() {
        r_push(readAnalogs, readBools, "holdreg");
        free(modbusReadData);
        free(readBools);
        free(readAnalogs);
    }).name("rpush");

    f1A.precede(f1B, f1C);
    f1D.succeed(f1B, f1C);

    tf::Taskflow f2("F2");

    tf::Task f2A = f2.emplace([&] {
        std::lock_guard<std::mutex> lock(gMutex);
        lrange_and_insert("holdreg", "analog", ANALOG_COLS, device, "FLOAT");
    }).name("insert_holdreg");

    tf::Task f2B = f2.emplace([&] {
        std::lock_guard<std::mutex> lock(gMutex);
        lrange_and_insert("holdreg", "bool", BOOL_COLS, device, "INT");
    }).name("insert_holdreg");

    tf::Taskflow f3("F3");

    tf::Task f3A = f3.emplace([&] {
        char *str = myQuery();
        myPublish(str, mqtt_topic, mqtt_qos);
    }).name("publish");
    
    tf::Executor executor;
    int count = 0;
    while (1)
    {
        auto start = std::chrono::steady_clock::now();

        executor.run(f1).wait();
        if (++count % 3 == 0) {
            executor.run(f2).wait();
            redisCommand(c, "LTRIM %s 1 0", "holdreg");
        }
        if (count % 5 == 0) {
            executor.run(f3).wait();
        }
        if (count % 15 == 0) {
            count = 0;
        }

        auto end = std::chrono::steady_clock::now();
        auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        printf("Total time used: %ld microseconds\n", elapsed_time.count());
        std::this_thread::sleep_for(std::chrono::microseconds(1000000 - elapsed_time.count()));
    }

    clean();
    return 0;
}