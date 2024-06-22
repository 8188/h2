#include <chrono>
#include <cmath>
#include <ctime>
// #include <execution>
#include <iostream>
#include <math.h>
#include <memory>
#include <random>
#include <stdexcept>
#include <string_view>

#include "dotenv.h"
#include "nlohmann/json.hpp"
#include "taos.h"
#include "taskflow/taskflow.hpp"
#include <mqtt/async_client.h>
#include <sw/redis++/redis++.h>

using json = nlohmann::json;

constexpr const long long INTERVAL { 5000000 };
constexpr const int QOS { 1 };
constexpr const int DECIMALS { 3 };
constexpr const auto TIMEOUT { std::chrono::seconds(10) };
constexpr const char* DATE_FORMAT { "%Y-%m-%d %H:%M:%S" };

const std::vector<std::string> codes_with_unit(const std::string& unit, const std::vector<std::string>& codes)
{
    std::vector<std::string> result;
    result.reserve(codes.size());
    // std::for_each(std::execution::par, codes.begin(), codes.end(), [&](const auto& item) {
    //     result.emplace_back(unit + item);
    // });
    for (const auto& item : codes) {
        result.emplace_back(unit + item);
    }
    return result;
}

const std::string get_now()
{
    constexpr int BUFFER_SIZE { 20 };

    auto now { std::chrono::system_clock::now() };
    auto now_time { std::chrono::system_clock::to_time_t(now) };
    char buffer[BUFFER_SIZE];
    std::strftime(buffer, BUFFER_SIZE, DATE_FORMAT, std::localtime(&now_time));
    return std::string(buffer);
}

std::time_t string2time(const std::string& timeStr)
{
    std::tm tm = {};
    strptime(timeStr.c_str(), DATE_FORMAT, &tm);
    return std::mktime(&tm);
}

bool fileExists(const std::string& filename)
{
    std::ifstream file(filename);
    return file.good();
}

std::string generate_random_string_with_hyphens()
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::stringstream ss;
    const char* hex_chars = "0123456789abcdef";
    for (int i = 0; i < 32; ++i) {
        if (i == 8 || i == 12 || i == 16 || i == 20) {
            ss << "-";
        }
        int index = dis(gen);
        ss << hex_chars[index];
    }

    return ss.str();
}

template <typename T>
std::string myRound(T value, int precision = DECIMALS)
{
    std::ostringstream out;
    if constexpr (std::is_integral<T>::value) {
        out << value;
    } else {
        out << std::fixed << std::setprecision(precision) << value;
    }
    return out.str();
}

class MyRedis {
private:
    sw::redis::ConnectionOptions makeConnectionOptions(const std::string& ip, int port, int db, const std::string& user, const std::string& password)
    {
        sw::redis::ConnectionOptions opts;
        opts.host = ip;
        opts.port = port;
        opts.db = db;
        if (!user.empty()) {
            opts.user = user;
        }
        if (!password.empty()) {
            opts.password = password;
        }
        opts.socket_timeout = std::chrono::milliseconds(50);
        return opts;
    }

    sw::redis::ConnectionPoolOptions makePoolOptions()
    {
        sw::redis::ConnectionPoolOptions pool_opts;
        pool_opts.size = 3;
        pool_opts.wait_timeout = std::chrono::milliseconds(50);
        return pool_opts;
    }

public:
    sw::redis::Redis redis;

    MyRedis(const std::string& ip, int port, int db, const std::string& user, const std::string& password)
        : redis(makeConnectionOptions(ip, port, db, user, password), makePoolOptions())
    {
        std::cout << "Connected to Redis.\n";
    }
};

class MyMQTT {
private:
    mqtt::async_client client;
    mqtt::connect_options connOpts;

    mqtt::connect_options buildConnectOptions(const std::string& username, const std::string& password,
        const std::string& caCerts, const std::string& certfile,
        const std::string& keyFile, const std::string& keyFilePassword) const
    {
        // mqtt::connect_options_builder()对应mqtt:/ip:port, ::ws()对应ws:/ip:port
        auto connBuilder = mqtt::connect_options_builder()
                               .user_name(username)
                               .password(password)
                               .keep_alive_interval(std::chrono::seconds(45));

        if (!caCerts.empty()) {
            mqtt::ssl_options ssl;
            ssl.set_trust_store(caCerts);
            ssl.set_key_store(certfile);
            ssl.set_private_key(keyFile);
            ssl.set_private_key_password(keyFilePassword);

            connBuilder.ssl(ssl);
        }

        return connBuilder.finalize();
    }

    void disconnect()
    {
        if (client.is_connected()) {
            client.disconnect()->wait();
            std::cout << "Disconnected from MQTT broker.\n";
        }
    }

public:
    MyMQTT(const std::string& address, const std::string& clientId,
        const std::string& username, const std::string& password,
        const std::string& caCerts, const std::string& certfile,
        const std::string& keyFile, const std::string& keyFilePassword)
        : client(address, clientId)
        , connOpts { buildConnectOptions(username, password, caCerts, certfile, keyFile, keyFilePassword) }
    {
    }

    ~MyMQTT()
    {
        disconnect();
    }

    void connect()
    {
        client.connect(connOpts)->wait();
        std::cout << "Connected to MQTT broker.\n";
    }

    void publish(const std::string& topic, const std::string& payload, int qos, bool retained = false)
    {
        auto msg = mqtt::make_message(topic, payload, qos, retained);
        bool ok = client.publish(msg)->wait_for(TIMEOUT);
        if (!ok) {
            std::cerr << "Error: Publishing message timed out." << std::endl;
        }
    }
};

class MyTaos {
private:
    const char* TAOS_IP;
    const char* TAOS_USERNAME;
    const char* TAOS_PASSWORD;
    const char* TAOS_DATABASE;
    uint16_t TAOS_PORT;
    TAOS* taos;

    void connectToTaos()
    {
        taos = taos_connect(TAOS_IP, TAOS_USERNAME, TAOS_PASSWORD, TAOS_DATABASE, TAOS_PORT);
        if (taos == NULL) {
            throw std::runtime_error("Failed to connect to Taos database");
        }
    }

public:
    MyTaos()
        : taos(nullptr)
    {
        TAOS_IP = std::getenv("TAOS_IP");
        TAOS_USERNAME = std::getenv("TAOS_USERNAME");
        TAOS_PASSWORD = std::getenv("TAOS_PASSWORD");
        TAOS_DATABASE = std::getenv("TAOS_DATABASE");
        TAOS_PORT = static_cast<uint16_t>(std::atoi(std::getenv("TAOS_PORT")));
        connectToTaos();
        puts("Connected to Taos.");
    }

    ~MyTaos()
    {
        if (taos) {
            taos_close(taos);
            taos_cleanup();
            puts("Taos connection closed");
        }
    }

    json query(const std::string& sql)
    {
        if (taos == nullptr) {
            throw std::runtime_error("Taos connection not initialized");
        }
        // std::cout << sql << '\n';
        TAOS_RES* res = taos_query(taos, sql.c_str());
        int code = taos_errno(res);
        if (code != 0) {
            printf("Taos error code: %d; Message: %s\n", code, taos_errstr(res));
            taos_free_result(res);
            throw std::runtime_error("Query execution error");
        }

        int numFields = taos_num_fields(res);
        TAOS_FIELD* fields = taos_fetch_fields(res);
        TAOS_ROW row;
        json jsonMsg;
        int rowNum { 0 };

        while ((row = taos_fetch_row(res))) {
            for (std::size_t i = 0; i < numFields; ++i) {
                const char* fieldName = fields[i].name;
                // printf("%s %d\n", fieldName, fields[i].type);
                if (row != nullptr && row[i] != nullptr) {
                    switch (fields[i].type) {
                    case TSDB_DATA_TYPE_INT: {
                        jsonMsg[std::to_string(rowNum)][fieldName] = *((int32_t*)row[i]);
                        break;
                    }
                    case TSDB_DATA_TYPE_BIGINT: // count(*)
                    {
                        jsonMsg[std::to_string(rowNum)][fieldName] = *((int64_t*)row[i]);
                        break;
                    }
                    case TSDB_DATA_TYPE_FLOAT: {
                        jsonMsg[std::to_string(rowNum)][fieldName] = *((float_t*)row[i]);
                        break;
                    }
                    case TSDB_DATA_TYPE_DOUBLE: {
                        jsonMsg[std::to_string(rowNum)][fieldName] = *((double_t*)row[i]);
                        break;
                    }
                    case TSDB_DATA_TYPE_BOOL: {
                        // printf("%d ", *((int8_t *)row[i]));
                        jsonMsg[std::to_string(rowNum)][fieldName] = *((int8_t*)row[i]);
                        break;
                    }
                    case TSDB_DATA_TYPE_TIMESTAMP: {
                        jsonMsg[std::to_string(rowNum)][fieldName] = *((int64_t*)row[i]);
                        break;
                    }
                    default:
                        break;
                    }
                }
            }
            rowNum++;
        }
        taos_free_result(res);
        return jsonMsg;
    }

    void execute(const std::string& sql)
    {
        if (taos == nullptr) {
            throw std::runtime_error("Taos connection not initialized");
        }
        // std::cout << sql << '\n';
        TAOS_RES* res = taos_query(taos, sql.c_str());
        int code = taos_errno(res);
        if (code != 0) {
            printf("Taos error code: %d; Message: %s\n", code, taos_errstr(res));
            taos_free_result(res);
            throw std::runtime_error("Query execution error");
        }
        taos_free_result(res);
    }

    template <typename T>
    T query_single(const std::string& key, const std::string& table)
    {
        const std::string sql { "select " + key + " from " + table + " limit 1" };
        json result = query(sql);
        T val {};

        if (result.is_null()) {
            puts("Taos query return empty");
        } else {
            val = result["0"][key];
        }
        return val;
    }

    std::vector<std::string> query_multi_row(const std::string& key, const std::string& table, int limit)
    {
        const std::string sql { "select " + key + " from " + table + " limit " + std::to_string(limit) };
        json result = query(sql);
        std::vector<std::string> vals {};
        if (result.is_null()) {
            puts("Taos query return empty");
        } else {
            for (const auto& item : result.items()) {
                std::string val = myRound(item.value()[key]);
                vals.emplace_back(val);
            }
        }
        return vals;
    }
};

class MechanismBase {
private:
    struct Alarm {
        std::string_view code;
        std::string desc;
        std::string_view advice;
        std::string startTime;
    };

public:
    std::shared_ptr<MyRedis> m_redis;
    std::shared_ptr<MyMQTT> m_MQTTCli;
    std::shared_ptr<MyTaos> m_taosCli;
    const std::string m_unit;
    std::unordered_map<std::string_view, std::unordered_map<std::string_view, std::vector<Alarm>>> alerts {};

    MechanismBase(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, std::shared_ptr<MyTaos> taosCli)
        : m_unit { unit }
        , m_redis { redis }
        , m_MQTTCli { MQTTCli }
        , m_taosCli { taosCli }
    {
        if (unit < "1" || unit > "9") {
            throw std::invalid_argument("unit must be in the range from '1' to '9'");
        }
    }

    virtual int logic() = 0;

    void send_message(const std::string& topic)
    {
        json j;
        for (const auto& pair : alerts[m_unit]) {
            json alarms;
            for (const auto& alarm : pair.second) {
                json alarmJson;
                alarmJson["code"] = alarm.code;
                alarmJson["desc"] = alarm.desc;
                alarmJson["advice"] = alarm.advice;
                alarmJson["startTime"] = alarm.startTime;
                // std::cout << alarm.startTime << '\n';
                alarms.push_back(alarmJson);
                // std::cout << "Code: " << alarmJson["code"] << ", Desc: " << alarmJson["desc"] << ", Advice: " << alarmJson["advice"] << ", Start Time: " << alarmJson["startTime"] << '\n';
            }
            j[std::string(pair.first)] = alarms;
        }

        const std::string jsonString = j.dump();
        m_MQTTCli->publish(topic, jsonString, QOS, false);
        alerts.clear();
    }

    void trigger(const std::string_view& key, const std::string_view& field, const std::string_view& tag,
        const std::string_view& content, const std::string_view& st, const std::string_view& now)
    {
        Alarm newAlarm;
        newAlarm.code = tag;
        newAlarm.desc = content;
        newAlarm.advice = "";
        newAlarm.startTime = st;

        alerts[m_unit]["alarms"].emplace_back(newAlarm);

        if (st == "0") {
            m_redis->redis.hset(key, field, now);
        }
    }

    void revert(const std::string_view& key, const std::string_view& field, const std::string_view& st) const
    {
        if (!st.empty()) {
            m_redis->redis.hset(key, field, "0");
        }
    }

    const std::string generate_select_query(const std::vector<std::string>& columns, const std::string& tableName, int limit) const
    {
        std::string query = "SELECT ";

        for (std::size_t i = 0; i < columns.size(); ++i) {
            query += columns[i];
            if (i != columns.size() - 1) {
                query += ", ";
            }
        }

        query += " FROM ";
        query += tableName;

        if (limit > 0) {
            query += " LIMIT ";
            query += std::to_string(limit);
        }

        return query;
    }
};

class H2Quality : public MechanismBase {
private:
    const std::vector<std::string> m_cols;
    int iUnit;

public:
    H2Quality(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, std::shared_ptr<MyTaos> taosCli)
        : MechanismBase(unit, redis, MQTTCli, taosCli)
        , m_cols { "c34", "c35" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "H2_" + m_unit + ":Mechanism:H2Quality" };
        const std::string content { "氢气品质差" };
        const std::string now { get_now() };
        const std::string tableName { "s_analog" };
        int limit { 1 };

        const std::string sql = generate_select_query(m_cols, tableName, limit);
        json result = m_taosCli->query(sql);
        if (result.is_null()) {
            puts("Taos query return empty");
            return flag;
        }

        std::optional<std::string> optional_str;
        for (const std::string& tag : m_cols) {
            optional_str = m_redis->redis.hget(key, tag);
            const std::string st = optional_str.value_or("0");

            if (result["0"][tag] != nullptr) {
                if (result["0"][tag] < 0.96) {
                    trigger(key, tag, tag, content, st, now);
                    flag = 1;
                } else {
                    revert(key, tag, st);
                }
            }
        }
        return flag;
    }
};

class H2Leakage : public MechanismBase {
private:
    const std::vector<std::string> m_cols;
    int iUnit;

public:
    H2Leakage(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, std::shared_ptr<MyTaos> taosCli)
        : MechanismBase(unit, redis, MQTTCli, taosCli)
        , m_cols { "c158", "c173" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "H2_" + m_unit + ":Mechanism:H2Leakage" };
        const std::string content { "发电机漏氢" };
        const std::string now { get_now() };
        const std::string tableName { "s_bool" };
        int limit { 1 };

        const std::string sql = generate_select_query(m_cols, tableName, limit);
        json result = m_taosCli->query(sql);
        // std::cout << result.dump(4) << '\n';
        if (result.is_null()) {
            puts("Taos query return empty");
            return flag;
        }

        std::optional<std::string> optional_str;
        for (const std::string& tag : m_cols) {
            optional_str = m_redis->redis.hget(key, tag);
            const std::string st = optional_str.value_or("0");

            if (result["0"][tag] != nullptr) {
                if (result["0"][tag] == true) {
                    trigger(key, tag, tag, content, st, now);
                    flag = 1;
                } else {
                    revert(key, tag, st);
                }
            }
        }
        return flag;
    }
};

class LiquidLeakage : public MechanismBase {
private:
    const std::vector<std::string> m_cols;
    int iUnit;

public:
    LiquidLeakage(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, std::shared_ptr<MyTaos> taosCli)
        : MechanismBase(unit, redis, MQTTCli, taosCli)
        , m_cols { "c36", "c37", "c38", "c39", "c27", "c28", "c29", "c30" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "H2_" + m_unit + ":Mechanism:liquidLeakage" };
        const std::string content { "发电机漏液" };
        const std::string now { get_now() };
        const std::string tableName { "s_analog" };
        int limit { 1 };

        const std::string sql = generate_select_query(m_cols, tableName, limit);
        json result = m_taosCli->query(sql);
        // std::cout << result.dump(4) << '\n';
        if (result.is_null()) {
            puts("Taos query return empty");
            return flag;
        }

        std::optional<std::string> optional_str;
        for (std::size_t i = 0; i < m_cols.size(); ++i) {
            const std::string& tag = m_cols[i];
            optional_str = m_redis->redis.hget(key, tag);
            const std::string st = optional_str.value_or("0");

            if (result["0"][tag] != nullptr) {
                if ((i < 4 && result["0"][tag] > 650) || result["0"][tag] > 10) {
                    trigger(key, tag, tag, content, st, now);
                    flag = 1;
                } else {
                    revert(key, tag, st);
                }
            }
        }
        return flag;
    }
};

class AlertStatistics {
private:
    std::shared_ptr<MyMQTT> m_MQTTCli;
    std::shared_ptr<MyTaos> m_taosCli;
    const std::string m_unit;

    std::string generate_alert_stat_sql(
        const std::vector<std::string>& cols, const std::string& alias,
        const std::string& table, const std::string& timeCondition = "ts > now() - 1h") const
    {
        std::ostringstream diff_columns_stream;
        std::ostringstream conditions_stream;
        for (const auto& col : cols) {
            diff_columns_stream << "diff(" << col << ") as diff" << col;
            conditions_stream << "diff" << col << " = 1";
            if (&col != &cols.back()) {
                diff_columns_stream << ", ";
                conditions_stream << " OR ";
            }
        }

        std::ostringstream query_stream;
        query_stream << "SELECT COUNT(*) as " << alias << " FROM (SELECT ts, "
                     << diff_columns_stream.str() << " FROM " << table << ") WHERE "
                     << conditions_stream.str() << " and " << timeCondition;

        return query_stream.str();
    }

public:
    const std::string tableName;
    const std::string alias_A;
    const std::string alias_PEM;
    const std::string alias_PG;
    const std::vector<std::string> cols_A;
    const std::vector<std::string> cols_PEM;
    const std::vector<std::string> cols_PG;

    AlertStatistics(const std::string& unit, std::shared_ptr<MyMQTT> MQTTCli, std::shared_ptr<MyTaos> taosCli)
        : m_unit { unit }
        , m_MQTTCli { MQTTCli }
        , m_taosCli { taosCli }
        , tableName { "s_bool" }
        , alias_A { "`控制`" }
        , alias_PEM { "`制氢`" }
        , alias_PG { "`提纯`" }
        , cols_A {
            "c0", "c1", "c42", "c43", "c44", "c45", "c46", "c89", "c94", "c95", "c96", "c97",
            "c98", "c99", "c100", "c101", "c102", "c103", "c104", "c105", "c106", "c107",
            "c108", "c109", "c110", "c111", "c112", "c113", "c114", "c115", "c116",
            "c117", "c118", "c119", "c120", "c121", "c122", "c123", "c124", "c125",
            "c126", "c127", "c128", "c129", "c130", "c131", "c132", "c133", "c134",
            "c135", "c136", "c137", "c138", "c139", "c140", "c141", "c142", "c143",
            "c144", "c145", "c146", "c147", "c148", "c149", "c150", "c151", "c152",
            "c153", "c154", "c155", "c156", "c157", "c158", "c159", "c160", "c161",
            "c162", "c163", "c164", "c165", "c166", "c167", "c168", "c169", "c170",
            "c171", "c172", "c173"
        }
        , cols_PEM {
            "c421", "c422", "c424", "c426", "c430", "c431", "c432", "c433", "c434",
            "c435", "c436", "c437", "c438", "c439", "c440", "c441", "c442", "c443",
            "c444", "c445", "c446", "c447", "c448", "c449", "c464", "c465", "c466",
            "c467", "c468", "c469", "c470", "c471", "c472", "c473", "c474", "c475",
            "c476", "c477", "c478", "c479", "c480", "c481", "c482", "c483", "c484",
            "c485", "c486", "c487", "c488", "c489", "c490"
        }
        , cols_PG {
            "c357", "c359", "c360", "c362", "c363", "c364", "c365", "c366", "c367",
            "c368", "c369", "c370", "c384", "c385", "c386", "c387", "c388", "c389",
            "c390", "c391", "c392"
        }
    {
        if (unit < "1" || unit > "9") {
            throw std::invalid_argument("unit must be in the range from '1' to '9'");
        }
    }

    std::string alert_query(const std::vector<std::string>& cols, const std::string& alias)
    {
        long long value { 0 };
        const std::string sql { generate_alert_stat_sql(cols, alias, tableName) };
        json result = m_taosCli->query(sql);
        // std::cout << result.dump(4) << '\n';
        if (result.is_null()) {
            puts("Taos query return empty");
        } else {
            if (result["0"][alias] != nullptr) {
                value = result["0"][alias];
            }
        }
        return myRound(value);
    }

    void alert_insert(const std::string& dataA, const std::string& dataPEM, const std::string& dataPG)
    {
        json j;
        j["A"] = dataA;
        j["PEM"] = dataPEM;
        j["PG"] = dataPG;

        const std::string jsonString = j.dump();
        m_MQTTCli->publish("H2_" + m_unit + "/AlertCount", jsonString, QOS);

        std::string sql = "insert into alert values (now, " + dataA + ", " + dataPEM + ", " + dataPG + ")";
        m_taosCli->execute(sql);
    }
};

class Task {
private:
    const std::string m_unit;
    std::string countA {};
    std::string countPEM {};
    std::string countPG {};

    const std::string H2_quality_topic;
    const std::string H2_leakage_topic;
    const std::string liquid_leakage_topic;

    H2Quality H2_quality;
    H2Leakage H2_leakage;
    LiquidLeakage liquid_leakage;
    AlertStatistics alertStat;

    std::shared_ptr<MyMQTT> m_MQTTCli;
    std::shared_ptr<MyTaos> m_taosCli;

    template <typename T>
    void test(T& mechanism, const std::string& topic) const
    {
        int flag = mechanism.logic();
        std::cout << flag << '\n';
        if (flag == 1) {
            mechanism.send_message(topic);
        }
    }

    void homeInfo() const
    {
        std::map<std::string, std::string> animation {
            { "fillCO2", "1" },
            { "exhaustH2", "1" },
            { "fillH2fromPowerPlant", "1" },
            { "fillH2fromConfluence", "1" },
            { "operationNormally", "1" },
            { "purificationH2", "1" },
            { "makeH2", "1" },
        };

        double sysStatus { m_taosCli->query_single<double>("c97", "s_analog") };
        double unitLoad = 960.18;
        std::map<std::string, std::string> status {
            { "unitStatus", "1" },
            { "sysStatus", myRound(sysStatus) },
            { "unitLoad", myRound(unitLoad) + "MW" }
        };

        double PEMPower = 1;
        int PEMSys { m_taosCli->query_single<int>("cast(c159 as int)", "s_bool") };
        int PGSys { m_taosCli->query_single<int>("cast(c160 as int)", "s_bool") };
        std::map<std::string, std::string> healthLevel {
            { "PEMPower", myRound(PEMPower) + "MW" },
            { "PEMSys", std::to_string(PEMSys) },
            { "PGSys", std::to_string(PGSys) }
        };

        double pressure { m_taosCli->query_single<double>("c1", "s_analog") };
        double purity { m_taosCli->query_single<double>("c34", "s_analog") };
        double pew { m_taosCli->query_single<double>("c5", "s_analog") };
        double makeFlow { m_taosCli->query_single<double>("c201", "s_analog") };
        std::map<std::string, std::string> operationData {
            { "pressure", myRound(pressure) + "MPa" },
            { "purity", myRound(purity) + "%" },
            { "dew", myRound(pew) + "%" },
            { "makeFlow", myRound(makeFlow) + "m3/h" }
        };

        std::vector<std::string> avgPressure { m_taosCli->query_multi_row("avg(c1)", "s_analog interval(1h)", 7) };
        std::vector<std::string> avgPurity { m_taosCli->query_multi_row("avg(c34)", "s_analog interval(1h)", 7) };
        std::vector<std::string> avgDew { m_taosCli->query_multi_row("avg(c5)", "s_analog interval(1h)", 7) };
        std::vector<std::string> avgMakeFlow { m_taosCli->query_multi_row("avg(c201)", "s_analog interval(1h)", 7) };
        std::map<std::string, std::vector<std::string>> average {
            { "pressure", avgPressure },
            { "purity", avgPurity },
            { "dew", avgDew },
            { "makeFlow", avgMakeFlow }
        };

        std::map<std::string, std::string> cost {
            { "energy", "85.16" },
            { "water", "2.84" }
        };

        std::map<std::string, std::map<std::string, std::string>> economy {
            { "supplement", { { "mon", "22.06" }, { "tue", "23.18" }, { "wed", "23.35" }, { "thu", "22.98" }, { "fri", "23.01" }, { "sat", "22.19" }, { "sun", "22.23" } } },
            { "purification", { { "mon", "49.36" }, { "tue", "55.19" }, { "wed", "55.23" }, { "thu", "54.42" }, { "fri", "55.41" }, { "sat", "48.27" }, { "sun", "49.36" } } }
        };

        std::vector<std::map<std::string, std::string>> alertsList {
            { { "name", "氢气纯度低" }, { "diagnosis", "氢气泄露" }, { "advice", "检查S1断管道法兰面" }, { "datetime", "2024-01-01T12:00:00.123" } }
        };

        json j;
        j["animation"] = animation;
        j["status"] = status;
        j["healthLevel"] = healthLevel;
        j["operationData"] = operationData;
        j["average"] = average;
        j["cost"] = cost;
        j["economy"] = economy;
        j["alert"] = alertsList;

        const std::string jsonString = j.dump();
        // std::cout << j.dump(4) << '\n';
        m_MQTTCli->publish("H2_" + m_unit + "/HomeInfo", jsonString, QOS);
    }

public:
    Task(const std::string& unit, std::shared_ptr<MyRedis> redisCli, std::shared_ptr<MyMQTT> MQTTCli, std::shared_ptr<MyTaos> taosCli)
        : m_unit { unit }
        , H2_quality_topic { "H2_" + unit + "/Mechanism/H2Quality" }
        , H2_leakage_topic { "H2_" + unit + "/Mechanism/H2Leakage" }
        , liquid_leakage_topic { "H2_" + unit + "/Mechanism/LiquidLeakage" }
        , H2_quality { unit, redisCli, MQTTCli, taosCli }
        , H2_leakage { unit, redisCli, MQTTCli, taosCli }
        , liquid_leakage { unit, redisCli, MQTTCli, taosCli }
        , alertStat { unit, MQTTCli, taosCli }
        , m_MQTTCli { MQTTCli }
        , m_taosCli { taosCli }
    {
    }

    tf::Taskflow flow(int& count)
    {
        tf::Taskflow f1("F1");

        tf::Task f1A = f1.emplace([&]() {
                             test(H2_quality, H2_quality_topic);
                         }).name("test_H2_quality");

        tf::Task f1B = f1.emplace([&]() {
                             test(H2_leakage, H2_leakage_topic);
                         }).name("test_H2_leakage");

        tf::Task f1C = f1.emplace([&]() {
                             test(liquid_leakage, liquid_leakage_topic);
                         }).name("test_liquid_leakage");

        tf::Task f1D = f1.emplace([&]() {
                             countA = alertStat.alert_query(alertStat.cols_A, alertStat.alias_A);
                         }).name("alert_query_A");

        tf::Task f1E = f1.emplace([&]() {
                             countPEM = alertStat.alert_query(alertStat.cols_PEM, alertStat.alias_PEM);
                         }).name("alert_query_PEM");

        tf::Task f1F = f1.emplace([&]() {
                             countPG = alertStat.alert_query(alertStat.cols_PG, alertStat.alias_PG);
                         }).name("alert_query_PG");

        tf::Task f1G = f1.emplace([&]() {
                             if (count % 720 == 719) {
                                 alertStat.alert_insert(countA, countPEM, countPG);
                             }
                         }).name("alert_insert");

        tf::Task f1H = f1.emplace([&]() {
                             if (count % 2 == 1) {
                                 homeInfo();
                             }
                         }).name("homeinfo");

        f1G.succeed(f1D, f1E, f1F);
        return f1;
    }
};

int main()
{
    if (!fileExists(".env")) {
        throw std::runtime_error("File .env does not exist!");
    }

    dotenv::init();
    const std::string MQTT_ADDRESS { std::getenv("MQTT_ADDRESS") };
    const std::string MQTT_USERNAME { std::getenv("MQTT_USERNAME") };
    const std::string MQTT_PASSWORD { std::getenv("MQTT_PASSWORD") };
    const std::string MQTT_CA_CERTS { std::getenv("MQTT_CA_CERTS") };
    const std::string MQTT_CERTFILE { std::getenv("MQTT_CERTFILE") };
    const std::string MQTT_KEYFILE { std::getenv("MQTT_KEYFILE") };
    const std::string MQTT_KEYFILE_PASSWORD { std::getenv("MQTT_KEYFILE_PASSWORD") };
    const std::string CLIENT_ID { generate_random_string_with_hyphens() };

    const std::string REDIS_IP { std::getenv("REDIS_IP") };
    const int REDIS_PORT = std::atoi(std::getenv("REDIS_PORT"));
    const int REDIS_DB = std::atoi(std::getenv("REDIS_DB"));
    const std::string REDIS_USER { std::getenv("REDIS_USER") };
    const std::string REDIS_PASSWORD { std::getenv("REDIS_PASSWORD") };

    auto MQTTCli = std::make_shared<MyMQTT>(MQTT_ADDRESS, CLIENT_ID, MQTT_USERNAME, MQTT_PASSWORD,
        MQTT_CA_CERTS, MQTT_CERTFILE, MQTT_KEYFILE, MQTT_KEYFILE_PASSWORD);
    MQTTCli->connect();

    auto redisCli = std::make_shared<MyRedis>(REDIS_IP, REDIS_PORT, REDIS_DB, REDIS_USER, REDIS_PASSWORD);

    auto taosCli = std::make_shared<MyTaos>();

    const std::string unit1 { "1" };

    Task task1(unit1, redisCli, MQTTCli, taosCli);

    tf::Executor executor;
    int count { 0 };

    while (1) {
        auto start = std::chrono::steady_clock::now();

        executor.run(task1.flow(count)).wait();
        // executor.run(task2.flow()).wait();

        auto end = std::chrono::steady_clock::now();
        auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        printf("Loop %d time used: %ld microseconds\n", ++count, elapsed_time.count());
        std::this_thread::sleep_for(std::chrono::microseconds(INTERVAL - elapsed_time.count()));
    }

    return 0;
}