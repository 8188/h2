#include "MQTTAsync_publish.h"
#include <stdio.h>
#include "gVal.h"
#include "easylogging++.h"

#if !defined(_WIN32)
#include <unistd.h>
#else
#include <windows.h>
#include <Minwinbase.h>
#endif

#if defined(_WRS_KERNEL)
#include <OsWrapper.h>
#endif

#if defined(_WIN32) || defined(_WIN64)
#define snprintf _snprintf
#endif

volatile int connected = 0;
MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
MQTTAsync_responseOptions pub_opts = MQTTAsync_responseOptions_initializer;

void connlost(void *context, char *cause)
{
	printf("MQTT connection lost, cause: %s, Reconnecting\n", cause);
	std::stringstream ss;
	ss << cause;
	LOG(WARNING) << "MQTT connection lost, cause: " << ss.str() << ", Reconnecting";

	MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
	conn_opts.keepAliveInterval = 20;
	conn_opts.cleansession = 1;

	while (1) 
	{
		#if defined(_WIN32)
			Sleep(1000);
		#else
			usleep(1000000L);
		#endif
		int rc;
		if ((rc = MQTTAsync_connect(client, &conn_opts)) != MQTTASYNC_SUCCESS)
		{
			printf("MQTT failed to restart connect, return code %d\n", rc);
			LOG(ERROR) << "MQTT failed to restart connect, return code " << rc;
		}
		else
		{
			puts("MQTT successed to reconnect");
			LOG(ERROR) << "MQTT successed to reconnect";
			break;
		}
	}
}

void onDisconnectFailure(void* context, MQTTAsync_failureData* response)
{
	puts("MQTT disconnect failed");
	LOG(ERROR) << "MQTT disconnect failed";
}

void onDisconnect(void* context, MQTTAsync_successData* response)
{
	puts("MQTT successed to disconnect");
	LOG(INFO) << "MQTT successed to disconnect";
}

void onSendFailure(void* context, MQTTAsync_failureData* response)
{
	MQTTAsync client = (MQTTAsync)context;
	MQTTAsync_disconnectOptions opts = MQTTAsync_disconnectOptions_initializer;
	int rc;

	printf("MQTT message send failed token %d error code %d\n", response->token, response->code);
	LOG(ERROR) << "MQTT message send failed token " << response->token << " error code " << response->code;
	opts.onSuccess = onDisconnect;
	opts.onFailure = onDisconnectFailure;
	opts.context = client;
	if ((rc = MQTTAsync_disconnect(client, &opts)) != MQTTASYNC_SUCCESS)
	{
		printf("MQTT failed to start disconnect, return code %d\n", rc);
		LOG(ERROR) << "MQTT failed to start disconnect, return code " << rc;
		exit(EXIT_FAILURE);
	}
}

void onSend(void* context, MQTTAsync_successData* response)
{
	// This gets called when a message is acknowledged successfully.
}


void onConnectFailure(void* context, MQTTAsync_failureData* response)
{
	printf("MQTT connect failed, rc %d\n", response ? response->code : 0);
	LOG(ERROR) << "MQTT connect failed, rc " << response ? response->code : 0;
}


void onConnect(void* context, MQTTAsync_successData* response)
{
	puts("MQTT Successed to connecte");
	LOG(INFO) << "MQTT Successed to connecte";
	connected = 1;
}

int messageArrived(void* context, char* topicName, int topicLen, MQTTAsync_message* m)
{
	/* not expecting any messages */
	return 1;
}

void MQTTConn(const char *address, const char *clientid)
{
	MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
	int rc;

	if ((rc = MQTTAsync_create(&client, address, clientid, MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTASYNC_SUCCESS)
	{
		printf("MQTT failed to create client object, return code %d\n", rc);
		LOG(ERROR) << "MQTT failed to create client object, return code " << rc;
		clean();
		exit(EXIT_FAILURE);
	}

	if ((rc = MQTTAsync_setCallbacks(client, NULL, connlost, messageArrived, NULL)) != MQTTASYNC_SUCCESS)
	{
		printf("MQTT failed to set callback, return code %d\n", rc);
		LOG(ERROR) << "MQTT failed to set callback, return code " << rc;
		clean();
		exit(EXIT_FAILURE);
	}

	conn_opts.keepAliveInterval = 20;
	conn_opts.cleansession = 1;
	conn_opts.onSuccess = onConnect;
	conn_opts.onFailure = onConnectFailure;
	conn_opts.context = client;
	if ((rc = MQTTAsync_connect(client, &conn_opts)) != MQTTASYNC_SUCCESS)
	{
		printf("MATT failed to start connect, return code %d\n", rc);
		LOG(ERROR) << "MATT failed to start connect, return code " << rc;
		clean();
		exit(EXIT_FAILURE);
	}

	while (!connected) {
		puts("MQTT Waiting for connection");
		LOG(INFO) << "MQTT Waiting for connection";
		#if defined(_WIN32)
			Sleep(100);
		#else
			usleep(100000L);
		#endif
	}

	pub_opts.onSuccess = onSend;
	pub_opts.onFailure = onSendFailure;
	pub_opts.context = client;
}

void myPublish(char *value, const char* topic, unsigned int qos)
{
    pubmsg.payload = value;
	pubmsg.payloadlen = strlen(value);
	pubmsg.qos = qos;
	pubmsg.retained = 0;

	if (MQTTAsync_isConnected(client) == 1)
	{
		int rc;
		if ((rc = MQTTAsync_sendMessage(client, topic, &pubmsg, &pub_opts)) != MQTTASYNC_SUCCESS)
		{
		printf("MQTT failed to start sendMessage, return code %d\n", rc);
		LOG(WARNING) << "MQTT failed to start sendMessage, return code " << rc;
		}
	}
	delete[] value;
}
