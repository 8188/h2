#ifndef MQTTASYNC_PUBLISH_H
#define MQTTASYNC_PUBLISH_H

#include "MQTTAsync.h"

#ifdef __cplusplus
extern "C" {
#endif

void connlost(void *context, char *cause);

void onDisconnectFailure(void *context, MQTTAsync_failureData *response);

void onDisconnect(void *context, MQTTAsync_successData *response);

void onSendFailure(void *context, MQTTAsync_failureData *response);

void onSend(void *context, MQTTAsync_successData *response);

void onConnectFailure(void *context, MQTTAsync_failureData *response);

void onConnect(void *context, MQTTAsync_successData *response);

int messageArrived(void *context, char *topicName, int topicLen, MQTTAsync_message *m);

void MQTTConn(const char *address, const char *clientid);

void myPublish(char *value, const char *topic, unsigned int qos);

#ifdef __cplusplus
}
#endif

#endif // MQTTASYNC_PUBLISH_H