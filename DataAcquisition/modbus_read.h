#ifndef MODBUS_READ_H
#define MODBUS_READ_H

void modbusConn(const char *ip, int port, int slave_id);

uint16_t *read_registers(int start_registers, int nb_registers);

float *extractAnalog(const uint16_t *c);

uint8_t *extractBool(const uint16_t *c);

#endif // MODBUS_READ_H