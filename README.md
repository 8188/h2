### Powerplant Hydrogen Cooling System Intelligent Project

#### DataAcquisition
- Reliance
Paho.mqtt.c, Libmodbus, Tdengine, Easyloggingpp, Nlohmann, TaskFlow
- Create Data Table
```
g++ createStable.cpp -o createStable -I/reliance/headfile -L/reliance/lib -ltaos
```
- Compile
```
g++ -c easylogging++.cc -o easylogging++.o -DELPP_NO_DEFAULT_LOG_FILE
g++ easylogging++.o gVal.c myTaos.c modbus_read.cpp MQTTAsync_publish.c data_acquisition_save.cpp -o xxx -I/reliance/headfile -L/reliance/lib -ltaos -lmodbus -lpaho-mqtt3a
```
#### Algorithm
- Reliance
```
pip install -r requirements.txt
```
- Run
```
celery -A tasks worker -P eventlet -f logs/celery.log -c 4
celery -A tasks beat
```