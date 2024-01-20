### Powerplant Hydrogen Cooling System Intelligent Project

#### DataAcquisition
- Reliance
Hiredis, Paho.mqtt.c, Libmodbus, Tdengine, Easyloggingpp, Nlohmann, TaskFlow
- Compile
```
g++ -c easylogging++.cc -o easylogging++.o -DELPP_NO_DEFAULT_LOG_FILE
g++ easylogging++.o gVal.c myRedis.cpp myTaos.c modbus_read.cpp MQTTAsync_publish.c data_acquisition_save.cpp -o xxx -I/your/reliance/headfile -L/your/reliance/lib -ltaos -lmodbus -lhiredis -lpaho-mqtt3a -pthread
```
#### Algorithm
```
celery -A tasks worker -P eventlet -f logs/celery.log -c 4
celery -A tasks beat
```