#ifndef GVAL_H
#define GVAL_H

#include "modbus.h"
#include "taos.h"
#include "MQTTAsync.h"
#include <json.hpp>
#include "easylogging++.h"

using json = nlohmann::json;

#define ANALOG_COLS 307
#define BOOL_COLS 560

#define START_REGISTERS 0
#define NB_REGISTERS 3022 + 1

extern modbus_t *ctx;
extern TAOS *taos;
extern MQTTAsync client;

void clean();

void removeQuotes(char *str);

template <typename T>
T *myMalloc(int size)
{
    T *array = (T *)malloc(size * sizeof(T));
    if (array == NULL)
    {
        puts("Malloc Error: Out of memory!");
        LOG(ERROR) << "Malloc Error: Out of memory!";
        clean();
        exit(EXIT_FAILURE);
    }
    return array;
}

#endif // GVAL_H