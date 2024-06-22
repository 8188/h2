[Dependencies]
sw/redis++, paho.mqtt.cpp, taosdata/TDengine
nlphmannJson, taskflow, dotenv-cpp	-- header-only

[Run]
make
(TF_ENABLE_PROFILER=utils.json) ./utils