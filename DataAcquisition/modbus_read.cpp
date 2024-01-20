#include "modbus.h"
#include "gVal.h"
#include "easylogging++.h"

void modbusConn(const char* ip, int port, int slave_id)
{
    ctx = modbus_new_tcp(ip, port);
    if (modbus_connect(ctx) == -1)
    {
        printf("Modbus connection failed: %s\n", modbus_strerror(errno));
        std::stringstream ss;
	    ss << modbus_strerror(errno);
        LOG(ERROR) << "Modbus connection failed: " << ss.str();
        clean();
        exit(EXIT_FAILURE);
    }
    else
    {
        puts("Modbus successed to connect");
        LOG(INFO) << "Modbus successed to connect";
    }
    modbus_set_slave(ctx, slave_id);

    modbus_set_response_timeout(ctx, 0, 200000);
}

uint16_t *read_registers(int start_registers, int nb_registers)
{
    uint16_t *holding_registers = myMalloc<uint16_t>(nb_registers);
    int blocks = nb_registers / MODBUS_MAX_READ_REGISTERS + (nb_registers % MODBUS_MAX_READ_REGISTERS != 0);

    for (int rc = 0, i = 0; i < blocks; ++i)
    {
        int addr = start_registers + i * MODBUS_MAX_READ_REGISTERS;
        int nb = (i == blocks - 1) ? nb_registers : MODBUS_MAX_READ_REGISTERS;
        rc = modbus_read_registers(ctx, addr, nb, holding_registers + i * MODBUS_MAX_READ_REGISTERS);
        if (rc == -1) {
            fprintf(stderr, "Read Holding Registers failed: %s\n", modbus_strerror(errno));
            free(holding_registers);
            clean();
            exit(EXIT_FAILURE);
        }
        nb_registers -= MODBUS_MAX_READ_REGISTERS;
    }

    return holding_registers;
}

float *extractAnalog(const uint16_t* c)
{
    float *new_array = myMalloc<float>(ANALOG_COLS);
    int index = 0;

    for (int i = 1; i <= 107; i += 3)
    {
        new_array[index++] = c[i] * 0.01f;
    }

    for (int i = 108; i <= 119; i += 3)
    {
        new_array[index++] = c[i] * 0.1f;
    }

    new_array[index++] = c[120];

    new_array[index++] = c[121] * 0.1f;

    new_array[index++] = c[122] * 0.1f;

    new_array[index++] = c[123] * 0.01f;

    new_array[index++] = c[124] * 0.1f;

    for (int i = 125; i <= 162; i++)
    {
        new_array[index++] = c[i] * 0.01f;
    }

    for (int i = 163; i <= 174; i++)
    {
        new_array[index++] = c[i];
    }

    new_array[index++] = c[175] * 0.1f;

    new_array[index++] = c[176] * 0.01f;

    for (int i = 300; i <= 304; i++)
    {
        new_array[index++] = c[i];
    }

    for (int i = 350; i <= 353; i++)
    {
        new_array[index++] = c[i];
    }

    for (int i = 355; i <= 373; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1050; i <= 1104; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1110; i <= 1184; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1250; i <= 1344; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1350; i <= 1356; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1362; i <= 1364; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1370; i <= 1372; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1378; i <= 1380; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1386; i <= 1456; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 1470; i <= 1510; i += 2)
    {
        new_array[index++] = modbus_get_float_abcd(&c[i]);
    }

    for (int i = 3004; i <= 3020; i += 2)
    {
        int32_t dint = (c[i + 1] << 16) | c[i];
        new_array[index++] = (float)dint;
    }

    for (int i = 3022; i <= 3022; i++)
    {
        new_array[index++] = c[i];
    }

    // for (size_t i = 0; i < ANALOG_COLS; i++) {
    //     printf("arr[%zu]=%f\t", i, new_array[i]);
    // }
    // puts("");
    return new_array;
}

uint8_t *extractBool(const uint16_t* c)
{
    uint8_t *new_array = myMalloc<uint8_t>(BOOL_COLS);
    int index = 0;
    uint16_t temp;

    for (int i = 500; i <= 505; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    for (int i = 200; i <= 204; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    for (int i = 250; i <= 256; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }
    
    for (int i = 275; i <= 276; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    for (int i = 1000; i <= 1001; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    for (int i = 1005; i <= 1008; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    for (int i = 1015; i <= 1021; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    for (int i = 1025; i <= 1026; ++i) {
        temp = (c[i] >> 8) | (c[i] << 8);

        for (int j = 0; j <= 15; ++j) {
            new_array[index++] = ((temp >> (15 - j)) & 1);
        }
    }

    // for (size_t i = 0; i < BOOL_COLS; i++) {
    //     printf("arr[%zu]=%d\t", i, new_array[i]);
    // }
    // puts("");
    return new_array;
}