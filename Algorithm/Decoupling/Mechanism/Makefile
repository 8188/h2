CXX = g++
CXXFLAGS = -pthread -std=c++17 -I.. -Wall -Wextra
LIBS = -lredis++ -lhiredis -lpaho-mqttpp3 -ltaos
MQTT_LIB = $(shell ./detect_mqtt.sh)

OUT = utils
SRC = utils.cpp
OBJ = $(SRC:.cpp=.o)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

all: $(OUT)

$(OUT): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS) $(MQTT_LIB)

debug: CXXFLAGS += -g
debug: $(OUT)

release: CXXFLAGS += -O3
release: $(OUT)

clean:
	rm -f $(OUT) $(OBJ)

.PHONY: all debug release clean
