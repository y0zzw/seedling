#include <WiFi.h>
#include <WiFiUdp.h>
#include <NTPClient.h>
#include <GTimer.h>
#include <PubSubClient.h>
#include "cJSON.h"
#include "sys_api.h"
#include "rtc.h"
#include "AmebaFatFS.h"
#include <FlashMemory.h>
#include "OTA.h"

#define FW_VER			"1.0.17"
#define FW_DATE			"2026/3/27"

#define DEBUG			0
bool enableTestMode = false;

#define SUPPORT_FLOW_SENSOR         	0
#define SUPPORT_ENVIRONMENT_SENSOR  	1
#define SUPPORT_TEMP_HUMIDITY_SENSOR	0
#define SUPPORT_LIGHT_SENSOR			0
#define SUPPORT_SOIL_MOISTURE_SENSOR	1
#define SUPPORT_WATER_LEVEL_SENSOR  	1
#define SUPPORT_WATCH_DOG				1
#define SUPPORT_NCU_CLOUD           	1
#define SUPPORT_LED						0
#define SUPPORT_CAMERA  				1
#define SUPPORT_SAVE_ENV_DATA			1

#if SUPPORT_SOIL_MOISTURE_SENSOR
#define SUPPORT_WATERING_MOISTURE_TYPE	0
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

/* GPIO definitions	*/
#define PUMP_GPIO_PIN			8
#define SV_GPIO_PIN				7		// SV = Solenoid Valve

#define RELAY_ON				HIGH
#define RELAY_OFF				LOW

#if SUPPORT_LED
#define LED_GPIO_PIN			21

#define LED_ON					HIGH
#define LED_OFF					LOW
#endif // SUPPORT_LED

#if SUPPORT_ENVIRONMENT_SENSOR

#define MINIMUM_READING_TIME  	300000	//5 minutes

#if SUPPORT_TEMP_HUMIDITY_SENSOR

#define SUPPORT_DHT11		0
#define SUPPORT_DHT22		0
#define SUPPORT_SHT2X		1

#if SUPPORT_DHT11 || SUPPORT_DHT22
#include "DHT.h"

#if SUPPORT_DHT11
#define DHT_TYPE                DHT11   // DHT 11
#endif // SUPPORT_DHT11

#if SUPPORT_DHT22
#define DHT_TYPE                DHT22   // DHT 22
#endif // SUPPORT_DHT22

#define DHT_DATA_PIN            6
#endif // SUPPORT_DHT11 || SUPPORT_DHT22

#if SUPPORT_SHT2X
#include "uFire_SHT20.h"
#endif // SUPPORT_SHT2X

#endif // SUPPORT_TEMP_HUMIDITY_SENSOR

#if SUPPORT_LIGHT_SENSOR
#include <BH1750.h>
#endif // SUPPORT_LIGHT_SENSOR

#if SUPPORT_SOIL_MOISTURE_SENSOR
#define SOIL_DATA_PIN				11
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

#endif // SUPPORT_ENVIRONMENT_SENSOR

#if SUPPORT_FLOW_SENSOR
#define FLOW_GPIO_PIN			5
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATCH_DOG
#include "WDT.h"

#define WATCHDOG_TIMEOUT		(MQTT_SOCKET_TIMEOUT*2*1000)	// must greater than MQTT_SOCKET_TIMEOUT

#define MAX_UPDATE_WDT_INTERVAL		30000	// 30 seconds
#define MAX_WATCHDOG_MSG_LEN		32
#endif // SUPPORT_WATCH_DOG

/* NCU cloud definitions */
#if SUPPORT_NCU_CLOUD
#define DEFAULT_CLIENT_ID				"seedlingCamClient"
#define MQTT_TLS_SERVER_AUTH			1
#define MQTT_RECONNECTION_INTERVAL		60000	// 60 seconds
#define REPORT_TYPE     				8
#define STATS_TYPE      				10
#define NOTIFY_TYPE						500
#define HEARTBEAT_INTERVAL				600		// 10 minutes
#define RETRIEVE_UUID_INTERVAL			60000	// 60 seconds
#define MAX_HEARTBEAT_LEN				150
#define MAX_CLIENTID_LEN				25
#define MAX_RANDOM_CLIENTID_LEN			30
#define MAX_DEVICE_UUID_LEN         	40
#define MAX_UUID_TOPIC_LEN				60
#define MAX_SUBSCRIBE_TOPIC_LEN     	128
#define MAX_PAYLOAD_LEN					255
#define MAX_LED_PAYLOAD_LEN         	80
#define MAX_WARNING_MSG_LEN         	255
#define MAX_CHECK_RESP_ID_LEN			12
#define MAX_CHECK_MQTT_LOOP_INTERVAL	10000	// 10 seconds
#define MAX_CHECK_NUC_TASK_INTERVAL		1000

#define SUCCESS_RESP_CODE					200
#define PAYLOAD_MALFORMED_RESP_CODE			400
#define OVERSIZE_RESP_CODE					413
#define MALFORMED_IMAGE_RESP_CODE			422
#define INTERNAL_SERVER_ERROR_RESP_CODE		500

// for cJson
#define DEVICE_UUID_ITEM				"device_uuid"
#define REPORT_AT_ITEM					"reported_at"
#define	SERVER_STATUS_ITEM				"server_status"

bool retrieveUUID = false;
bool disablePublish = false;
bool send_hb = false;
bool receive_hb_resp = false;
bool isSubscribeReportResp = false;
bool waitforresp = false;
bool lastConnState = false;
bool needCleanup = false;

int tw_offset = 28800;

uint8_t prevMqttConnState = 0;	// 0: power on, 1: connected successfully
								// 2: connection failed

uint16_t max_mqtt_buf_size = 2048;
uint16_t mqtt_keep_alive = 120;
uint16_t max_socket_timeout = 10;
uint16_t max_stream_timeout = 180;

unsigned long last_reconn_attempt = 0;
unsigned long last_heart_beat = 0;
unsigned long last_retrieve_uuid = 0;
unsigned long last_check_mqtt_loop = 0;
unsigned long last_check_ncu_task = 0;

const static char mqttServer[] = "172.20.10.4";
const static char uuidAPI[] = "uuid-lookup";
const static char reportAPI[] = "report";
const static char uplinkTopic[] = "uplink/%s/%s";
const static char downlinkTopic[] = "downlink/%s/%s";
const static char reportPayload[] = "{\"type\":%d,\"level\":0,\"info\":\"%s\",\"reported_at\":%lu}";
const static char heartbeatPayload[] = "{\\\"seed_name\\\":\\\"%s\\\",\\\"working_day\\\":%d,\\\"watering_day\\\":%d,\\\"schedule\\\":%d,\\\"rssi\\\":%d}";

char clientId[MAX_CLIENTID_LEN + 1] = {0};
char device_uuid[MAX_DEVICE_UUID_LEN + 1] = {0};
char reportTopic[MAX_SUBSCRIBE_TOPIC_LEN + 1] = {0};
char reportRespTopic[MAX_SUBSCRIBE_TOPIC_LEN + 1] = {0};
char commandTopic[MAX_SUBSCRIBE_TOPIC_LEN + 1] = {0};
char report_hb_id[MAX_CHECK_RESP_ID_LEN + 1] = {0};
char start_pumping_payload[MAX_PAYLOAD_LEN + 1] = {0};
char stop_pumping_payload[MAX_PAYLOAD_LEN + 1] = {0};
char water_level_payload[MAX_PAYLOAD_LEN + 1] = {0};
char open_sv_payload[MAX_PAYLOAD_LEN + 1] = {0};
char close_sv_payload[MAX_PAYLOAD_LEN + 1] = {0};
char powerup_payload[MAX_PAYLOAD_LEN + 1] = {0};
char shift_sch_payload[MAX_PAYLOAD_LEN + 1] = {0};
char led_payload[MAX_LED_PAYLOAD_LEN + 1] = {0};
char warning_msg[MAX_WARNING_MSG_LEN + 1] = {0};

#if SUPPORT_ENVIRONMENT_SENSOR
char env_payload[MAX_PAYLOAD_LEN + 1] = {0};

#if SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_LIGHT_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

const static char readingPayload[] = "{\\\"temperature\\\":%.1f,\\\"humidity\\\":%.1f,\\\"light\\\":%d,\\\"soil_humidity\\\":%d}";

#elif SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_LIGHT_SENSOR

const static char readingPayload[] = "{\\\"temperature\\\":%.1f,\\\"humidity\\\":%.1f,\\\"light\\\":%d}";

#elif SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

const static char readingPayload[] = "{\\\"temperature\\\":%.1f,\\\"humidity\\\":%.1f,\\\"soil_humidity\\\":%d}";

#elif SUPPORT_LIGHT_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

const static char readingPayload[] = "{\\\"light\\\":%d,\\\"soil_humidity\\\":%d}";

#elif SUPPORT_TEMP_HUMIDITY_SENSOR
const static char readingPayload[] = "{\\\"temperature\\\":%.1f,\\\"humidity\\\":%.1f}";

#elif SUPPORT_LIGHT_SENSOR
const static char readingPayload[] = "{\\\"light\\\":%d}";

#elif SUPPORT_SOIL_MOISTURE_SENSOR

const static char readingPayload[] = "{\\\"soil_humidity\\\":%d}";

#endif // SUPPORT_LIGHT_SENSOR

#endif // SUPPORT_ENVIRONMENT_SENSOR

void cloud_callback(char* topic, byte* payload, unsigned int length);
void save_log_msg(char *msg);
bool safeMQTTconnected();

WiFiSSLClient sslClient;
PubSubClient mqttClient(sslClient);

char* rootCABuff = \
"-----BEGIN CERTIFICATE-----\n" \
"MIIE/zCCAuegAwIBAgIUVQ6p4MDagOS0q6XLzblEx3oXV/AwDQYJKoZIhvcNAQEL\n" \
"BQAwDzENMAsGA1UEAwwEbXdubDAeFw0yNTA4MDYwNzI1MjZaFw0zNTA4MDQwNzI1\n" \
"MjZaMA8xDTALBgNVBAMMBG13bmwwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIK\n" \
"AoICAQDALY2FTXxLFHhvE9IMLUnF7cndh+CFNBZ4b49Zta9BFQzBHnwDkUr3qKSr\n" \
"JGqUJ/UCwh34Xx4Ob+kzV9rRt5lFUKm1GDEL/26j2AqzWP/OshNAMoVP5tzm85+q\n" \
"SQStNtgUw1KnILZTeqpgUdebvWoBip2YSePxQ3TXp57dFzVh0nuzurL2jn1QR7y6\n" \
"k4L/i2rBK8msY8yZL+gpM9YZkzdEceEhSZYLgZwOSEtBT2n19qqhZ/pYbvBE2iCS\n" \
"H3XleYh/HqcKvDoJycyrE/kYcghRctFvm5cmbEfkIkClkpYG8LSrb7ftDGba8Ln6\n" \
"jf9Cl+A2mcpH2gI+95NdqzDc++kmGYx7r2m9RizTkIL0R3u5gbp8ruXc+jwfZAzD\n" \
"i7Idy75hPlTB7w2kOCLvOwJq9Z5P8kno2ZM9HlQ95PzSLnAL4zPMXfJ+iENWJVpw\n" \
"JABRghiW4T4PTAxkaeolWPmt5kd/aWCUmMntV7hYgW7vfNCCeQeFKq4PCne5fotU\n" \
"gp55BBRctgeivy+vZe/HA9detAyJBjdVIKxG8DIXTtHm2U6xLwMrNl/8RoyGUEOE\n" \
"uqpIB6wzUDDHOmzyDbfAg0nV+gLBP6Xh7iX2l3e4nELLNYooQfh900cPixfwwgJf\n" \
"DhKLVQ9HXi87p1DoyxbUrbVy0/alc9haCBoUsPCAqsKcjbCASQIDAQABo1MwUTAd\n" \
"BgNVHQ4EFgQUuw4PbnLetUfT/wok5EyjMLfGi7cwHwYDVR0jBBgwFoAUuw4PbnLe\n" \
"tUfT/wok5EyjMLfGi7cwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC\n" \
"AgEAL5r2T3c9vaITCq2CV3ZO7BPztQE2a/Rn7lhCy38ZQfji4OK6ytrQka8AxEp6\n" \
"9qi8iBqKc5voAmXks8UzG5WG1LUWAEJWv7rxlN78OIA5aEVRnLGzrZKIWshLAGRc\n" \
"WJ8QJ+eO0cfaz7u2MAOlOo0AxYH/ja2g6kryUhi1Ho3g627quvgcHJ14T8mcqlrL\n" \
"HwP27U5s+Vw69qGtB9YTtoYOSlYTqgGp8+S/E7X0+wXnSvLzaZR0GweQL+AQolVf\n" \
"JUwmxricvEJJE+IvUtQ0PhizCNV/i7TwEZgBBFl7ZpX7V/RgRxIcjG65ZONTQi/5\n" \
"i8rq8RTbd4syFnweVrkkvpS4IPsFHSIG0AOGu2xu7wDl9PuCYPlwfr3c53lcrh5Y\n" \
"JVPfQfECiojb9k5h6tFADSA6HGhHeq7rirLbvX0SHEf/JIy5jrkDS2mqGRpTgrWt\n" \
"e1+0jNLijdMKoArnVoOAqkjZax2yz8AHQhORdhWEtE6btCjm/ce+i5Lk+lNjL5oE\n" \
"9S07GV6R5petwvpgKwZYy0U2jZ1nPe43KjMR1y8DsAw4xfXGRVVz6RvsorZCLJd0\n" \
"1AYCXaiKEzPVe/BYtrtW4Bn4SA949NBW82jtrqgdiOJdqnNuM9aoXnmb71hkc8ge\n" \
"TT0u2m2Zy6Jqze4q752Y/xL8BJ2fusgMzmYCRctsyA0A9JY=\n" \
"-----END CERTIFICATE-----\n";

#if MQTT_TLS_SERVER_AUTH
// notBefore=Feb 12 03:22:09 2026 GMT
// notAfter=Feb 10 03:22:09 2036 GMT
char* certificateBuff = \
"-----BEGIN CERTIFICATE-----\n" \
"MIIEYDCCAkigAwIBAgIUKp3pc+dFtU/Ll/iFeLkK4dAYNQ8wDQYJKoZIhvcNAQEL\n" \
"BQAwDzENMAsGA1UEAwwEbXdubDAeFw0yNjA1MjgxMDA4NDVaFw0yNzA1MjgxMDA4\n" \
"NDVaMGYxCzAJBgNVBAYTAlRXMQ8wDQYDVQQIDAZUYWlwZWkxDzANBgNVBAcMBlRh\n" \
"aXBlaTEUMBIGA1UECgwLRXhhbXBsZSBJbmMxDDAKBgNVBAsMA0RldjERMA8GA1UE\n" \
"AwwIdGVzdDA1MjgwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCGmuGX\n" \
"j0Jg+nQfnDumzIVmeMkv9lEc0CEcCLA15N3GA6wAfZsqVJ6VsZCH3qM2yofViMpb\n" \
"VtuRiorsfPjE9ScVRahSHGasbQ7G2VCKKEYH4RGYCz8FSQ5WeXEsN03OxxdAr6Gh\n" \
"GZi4jhXwuQY7xQfbNEE50pMem8LejwluvbeYyXYWHjkW3vsscCipNwxHc87PMXm7\n" \
"c4gMkjWEpNJKXqq+3jLQXqEgNobR9NYTsadKZiw0+PswmEM+Io+jJOV9zUbIR/3x\n" \
"ti/Eko9GyRkIt3NJz5BbDw1TiX6WL6NlTX6BaxnVI7qDVvezGHcuZYRIlf3M1gVv\n" \
"Hojlqe0Lg6SmyY99AgMBAAGjXTBbMBkGA1UdEQQSMBCCCHRlc3QwNTI4hwSsFAoE\n" \
"MB0GA1UdDgQWBBQq0NXjKdCt5ftWtodb9e2GgnDPTDAfBgNVHSMEGDAWgBS7Dg9u\n" \
"ct61R9P/CiTkTKMwt8aLtzANBgkqhkiG9w0BAQsFAAOCAgEAiTf4MiKIUtxIjoGe\n" \
"EgjRr2GWpDDOzzSH0rziE7qZrZto50AdW75bnCBiiHxGkkgjQzNl0sN+rCthaVfj\n" \
"AkaeQc7vOQA8/CxvjIvze0UJD5qyb+L4D1jVL+NWxmfLMQ+nFG6erJu7AP/FK3sF\n" \
"wsQe025sjGI9a2dM2Y1L3bUZ7ynnxIcY/M/iCOfDPATE+mgg1YWdPOQhm1LJtWzR\n" \
"86CcQziIr1iUBtPGCXvIjVcG1fu7s46OlvGGednHliDR9YAi4j7utGdcQuhPKDsL\n" \
"yclDiAqugEiHQcP+eEDWPYyeBU83yN1h1Hc4Vk3b2sFxb/fJL5gV2l1L4YL6eGmS\n" \
"2I5ZtUCofmpgxKcMgh5SWkHsSyf1F+3bx7Ya+UscsD23B6NK1IHnrPyCZcaL0Yoh\n" \
"F/xJl0xvZI8zJY8MtRCHdv7RzqlT9U0zMUOWhTudNN6PTILtKI/zQXiM47GkPI6Y\n" \
"CDNg4zewSP+vsPBmzJIyTYt08nwlubsCsh56t1GFFFCeRIxllfyz4dZEQWBtm6q/\n" \
"SEe1o2EOgy87YgzqBCZhvrRJyFQJXpCwo+pDg6L3GzpqvRNjlq9i/zjOxp9y5TeG\n" \
"voHISP950kaxaEanBcXu4ljJf3PA/ykvgT8yLjg1D1pTkQbnypoYAru+P5L6W/6G\n" \
"nr7w1ytu3ZnqlXwCiRgqZtDmQ0s=\n" \
"-----END CERTIFICATE-----\n";

char* privateKeyBuff = \
"-----BEGIN PRIVATE KEY-----\n" \
"MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCGmuGXj0Jg+nQf\n" \
"nDumzIVmeMkv9lEc0CEcCLA15N3GA6wAfZsqVJ6VsZCH3qM2yofViMpbVtuRiors\n" \
"fPjE9ScVRahSHGasbQ7G2VCKKEYH4RGYCz8FSQ5WeXEsN03OxxdAr6GhGZi4jhXw\n" \
"uQY7xQfbNEE50pMem8LejwluvbeYyXYWHjkW3vsscCipNwxHc87PMXm7c4gMkjWE\n" \
"pNJKXqq+3jLQXqEgNobR9NYTsadKZiw0+PswmEM+Io+jJOV9zUbIR/3xti/Eko9G\n" \
"yRkIt3NJz5BbDw1TiX6WL6NlTX6BaxnVI7qDVvezGHcuZYRIlf3M1gVvHojlqe0L\n" \
"g6SmyY99AgMBAAECggEAITZ2qwGlgI3T2NfhLSFCWjw78ipuLbSaMtNe/VFVOm5+\n" \
"Apn7jxiVj7yiO7A+5/5VwWXQ/nQiVdlQOZWFgX23EMvhAi+xUuLO9lg5Q/m3e+q4\n" \
"P2KpAGocEFDDUMRtnbOoxGSBtmE9Om6OsdjJlbZiX59XZ8fSVsdasDUtf+ZudpOF\n" \
"gOa9DBClsJHAX0QKeZJ8G8pIDKqOdoLn4gJFsDU2VmCtT2Y1rsp84mUmTGZAgScA\n" \
"wZF75WxPJA14QP6N3g32nX23JHt+Oiav96zBE2JOUUB+WbbR3RDxkPs0Ojn9SaS2\n" \
"f+t0aVuJMA8iIrwbaM0Wn1uTzxZAnms4Y9bQn8KpiQKBgQC9FVOuC+K07xMFc/18\n" \
"12Si94dz+e5o/KolLPLXDiaM60xRGt3Pq2iW+6SVAbdjjJ6RXG7wfX6t377Wg9EA\n" \
"IagwsAUGTc0C9cxE+dCU8RcdNXOeNsgRcyU0PuDe9YQKHz/R8w8KVUdZ7KC3gTm7\n" \
"3b97kb/UQIPVJw07fvET1DdZhQKBgQC2PeYwXaHhBTtW0yUaT1VJwDhYn9mBuF3n\n" \
"aag147ywYYja4XIoRxfxuxdp5tVN+UHBOB2LUw27oGtLHvFhqOFzDCpXDASFFVW9\n" \
"z2c8AeJdSCAwn6NYUNvOCWwrkrf7GoeS1Uk8897vYwUZ40kfmrcLs1siOyExC25M\n" \
"umVulS6DmQKBgCrb1a7ixM1sT2xyASg2DnqeDtbr9O1ZMBvjF1xFudlBUHgl1ddH\n" \
"rBplCcY4sF/hNOQQBpl+aKNyzugC0vHrrxryGwF1yx17p4SO94d7KlkVj5JyKmFC\n" \
"L1GrWv9OuVIuCSJGrMcT02t/pmJR6Us7FTcmL0wmH3vGMUqmGHD9LlqdAoGAC6YH\n" \
"OZ4yg6yO72zNmVHq1Kz5rQiCoZ0EO49wVgl5fRiu47a3UkXBDQ11YKqFhddh0ZTh\n" \
"po8neI+3a/TNXv2pc24aore87ji/40MHiTlhm67Jh/IcQb/hXNkTVfGp2t4GPmdt\n" \
"p/y6ijidEduKR//epVvQfm6jH3xoj3T2mMvmTjkCgYBsT+T4sdM2vOvovuCe9rDx\n" \
"Im/PnJ2Dw54shVYzJWGGwLHOfGLv03iXst9Arh0XPT64ZO5XmEcm/9N0lFX1I4ho\n" \
"crU1Ph5s3RiSkPE9pGUVhRCk3LYYY8oMzJa3fLJXRnXvwrlS/eJ7bAGkoiLvnTTk\n" \
"XivZ+hQpZE8pJDB6FeRdEg==\n" \
"-----END PRIVATE KEY-----\n";
#endif

#endif // SUPPORT_NCU_CLOUD

/* Water Level Sensor definitions */
#if SUPPORT_WATER_LEVEL_SENSOR
#include <Wire.h>

#define WL_GPIO_PIN			20

#define	MAX_LOW_DATA		8
#define MAX_HIGH_DATA		12

unsigned char low_data[MAX_LOW_DATA] = {0};
unsigned char high_data[MAX_HIGH_DATA] = {0};

#define ATTINY1_HIGH_ADDR   0x78
#define ATTINY2_LOW_ADDR    0x77

#define MIN_LEVEL_VALUE		230
#define MAX_LEVEL_VALUE		255
#endif // SUPPORT_WATER_LEVEL_SENSOR

#define MAX_CONNECT_TIME			3
#define MAX_RETRY_FAILED_TIME		3
#define MAX_PARA_NAME_LEN     		31
#define MAX_PARA_VALUE_LEN    		64
#define MAX_PARA_NUMBER       		12
#define MAX_SSID_LEN         		32
#define MAX_KEY_LEN           		64
#define MAX_MAC_LEN					12
#define MAX_IP_LEN					15
#define MAX_SCHEDULE_NUM      		5
#define MAX_SCHEDULE_NAME			20
#define MAX_SEED_NAME				50
#define MAX_LOG_NUM           		300
#define MAX_LOG_TIME_LEN      		19
#define MAX_LOG_MSG_LEN       		128
#define MAX_RECONN_NETWORK_TIME		60000	// 60 seconds
#define MAX_SCAN_NETWORK_TIME		60000	// 60 seconds

#define MAX_CHECK_INTERNET_STATE_INTERVAL	30000	// 30 seconds

#define MIN_CHECK_REMAINED_WATER_INTERVAL	60000	// 60 seconds

#if SUPPORT_WATERING_MOISTURE_TYPE
#define MAX_CHECK_MOISTURE_TIME		60000
#define MAX_CHECK_MOISTURE_COUNT	3

int check_moisture_counter = 0;
unsigned long last_check_moisture_time = 0;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

/* html and CGI definition */
#define SET_WIFI_HTM				"set_wifi.htm"
#define SET_SCHEDULE_HTM			"set_schedule.htm"
#define SET_WATERING_HTM			"set_watering.htm"
#define SYSTEM_SETTINGS_HTM			"system_settings.htm"
#define SYSTEM_LOGS_HTM				"system_logs.htm"

#define SET_WIFI_CGI				"set_wifi.cgi"
#define SET_SCHEDULE_CGI			"set_schedule.cgi"
#define SET_WATERING_CGI			"set_watering.cgi"
#define SET_SYS_CGI					"set_sys.cgi"
#define RESET_DAYS_CGI				"reset_days.cgi"
#define CHANGE_SCHEDULE_CGI			"change_schedule.cgi"
#define FACTORY_RESET_CGI			"factory_reset.cgi"
#define RESET_SESSIOIN_CGI			"reset_session.cgi"
#define PUMP_CTRL_CGI				"pump_ctrl.cgi"
#define SV_CTRL_CGI					"sv_ctrl.cgi"
#define SET_OTA_SERVER_CGI			"set_ota_server.cgi"
#define START_OTA_CGI				"start_ota.cgi"

#if SUPPORT_SOIL_MOISTURE_SENSOR
#define SET_SENSOR_OFFSET_CGI		"set_sensor_offset.cgi"
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

/* parameters for CGI */
// wifi Settings
#define SSID_PARA					"ssid"
#define KEY_PARA					"key"

// Schedule Settings
#define TMP_SCH_INDEX_PARA			"tmp_sch_index"

#if SUPPORT_FLOW_SENSOR
#define WATERING_VOLUME_PARA		"watering_volume"
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
#define WATERING_LEVEL_PARA			"watering_level"
#endif // SUPPORT_WATER_LEVEL_SENSOR

#define WATERING_INTERVAL_PARA		"watering_interval"
#define WATERING_TIME_PARA			"watering_time"
#define WAITING_TIME_PARA			"waiting_time"
#define DRAINING_TIME_PARA			"draining_time"

#if SUPPORT_WATERING_MOISTURE_TYPE
#define MOISTURE_CONTENT_PARA		"moisture_content"
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_LED
#define SUPPORT_LED_PARA			"support_led"
#define LED_START_TIME_PARA			"led_start_time"
#define LED_END_TIME_PARA			"led_end_time"
#endif // SUPPORT_LED

// Watering settings
#define SEED_NAME_PARA					"seed_name"
#define WATERING_IN_FIRSTDAY_PARA		"watering_in_firstday"
#define WORKING_DAY_PARA            	"working_day"
#define WATERING_DAY_PARA           	"watering_day"
#define MAX_CHECK_WL_TIME_PARA			"max_check_wl_time"
#define WATERING_START_TIME_PARA		"watering_start_time"
#define WATERING_END_TIME_PARA			"watering_end_time"
#define ENABLE_SCH_PARA					"enable_sch"
#define SCH_INDEX_PARA              	"sch_index"

#if SUPPORT_WATERING_MOISTURE_TYPE
#define WATERING_TYPE_PARA			"watering_type"
#endif // SUPPORT_WATERING_MOISTURE_TYPE

// System
#define ACTION_PARA					"action"

#if SUPPORT_SOIL_MOISTURE_SENSOR
#define SET_SENSOR_OFFSET_PARA		"sensor_offset"
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

/* incoming parameter structure definition */
typedef struct incoming_para {
	char name[MAX_PARA_NAME_LEN+1];
	char value[MAX_PARA_VALUE_LEN+1];
} incoming_para_t;

/* storage structure definition */
#define WATERING_BY_TIME_TYPE		0
#define WATERING_BY_MOISTURE_TYPE	1

typedef struct schedule_info {
	char schedule_name[MAX_SCHEDULE_NAME + 1];

#if SUPPORT_FLOW_SENSOR
	int watering_volume;      // the water volume of water flow sensor
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
	int watering_level;    // the target level of water level sensor
#endif // SUPPORT_WATER_LEVEL_SENSOR

	int watering_interval; // watering day interval
	int moisture_content;
	int watering_time;     // start watering time
	int waiting_time;      // the duration of soaking water
	int draining_time;     // the duration of draining water

	bool support_led;
	int led_start_time;		// turn on LED time
	int led_end_time;		// turn off LED time
} schedule_info_t;

typedef struct system_log {
	char log_time[MAX_LOG_TIME_LEN + 1];
	char log_msg[MAX_LOG_MSG_LEN + 1];
} system_log_t;

typedef struct system_setting {
	char ssid[MAX_SSID_LEN+1];
	char key[MAX_KEY_LEN+1];

	char seed_name[MAX_SEED_NAME + 1];
	bool watering_in_firstday;
	int start_day;			// the start day of year
	int max_day_of_year;	// the maximum day of year
	int current_day;		// the current day of year
	int working_day;
	int watering_day;
	int sensor_offset;
	int max_check_wl_time;	// Timeout for water-level check; stop pump if no reading
	int watering_start_time; // watering start time
	int watering_end_time; // watering end time

	bool sch_executed;
	bool isExecSch;
	bool isPumpProcCompleted;
	bool isWaitingProcCompleted;

	uint32_t startWaitingTime;

	bool enable_sch;
	int sch_index;
	int watering_type;	// 0: watering by time, 1: watering by soil moisture
	schedule_info_t schedule[MAX_SCHEDULE_NUM];

	int log_num;
	int log_index;
	system_log_t log[MAX_LOG_NUM];
    // Watering schedule from VGNMS
  char vgnms_start_date[12];   // "2026-05-31\0"
  int  vgnms_schedule[14];     // 最多14天
  int  vgnms_schedule_count;
  int  vgnms_day_count;
	char ota_server[MAX_IP_LEN+1];

#if SUPPORT_CAMERA
	// 0 (default): 0 degree
	// 1: 90 degree (Rotate Right)
	// 2: 90 degree (Rotate Left)
	// 3: 180 degree
	int rotate;

	bool save_image_flag1;
	bool save_image_flag2;

	int save_image_time1;
	int save_image_time2;

#if SUPPORT_NCU_CLOUD
	bool send_pic_flag1;
	bool send_pic_flag2;
#endif // SUPPORT_NCU_CLOUD
#endif // SUPPORT_CAMERA
} system_setting_t;

/* default values */
#define MAX_NTP_SERVER_NUM		4
char *ntp_server[] = {"watch.stdtime.gov.tw", "time.stdtime.gov.tw", "clock.stdtime.gov.tw", "tick.stdtime.gov.tw", NULL};
int ntp_server_index = 0;

const char default_seed_name[] = "Seed";

#if SUPPORT_FLOW_SENSOR
int default_watering_volume = 20; // L
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
int default_watering_level = 8; // level
#endif // SUPPORT_WATER_LEVEL_SENSOR

bool  default_watering_in_firstday = true;
int default_working_day = 0;
int default_watering_day = 0;

int default_max_check_wl_time = 60;		// 60 seconds
int default_watering_start_time = 420;	// 7:00
int default_watering_end_time = 900;		// 15:00

bool default_enable_sch = false;
int default_sch_index = 0;

int default_watering_interval = 1;
int default_watering_time = 1439;	// 23:59
int default_waiting_time = 180;
int default_draining_time = 300;

bool default_support_led = false;
int default_led_start_time = 420;	// 7:00
int default_led_end_time = 1380;	// 23:00

#if SUPPORT_WATERING_MOISTURE_TYPE
int default_moisture_content = 0;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

/* global variable definition */
incoming_para_t para[MAX_PARA_NUMBER];

system_setting_t current_system_setting;

bool isAPMode = false;
char ap_ssid[] = "seedCamAP";       // Set the AP SSID
char ap_pwd[] = "12345678";			// Set the AP password
char channel[] = "6";				// Set the AP channel

char mac_addr_str[MAX_MAC_LEN + 1] = {0};

int tmp_sch_index = default_sch_index;

int wifi_status = WL_IDLE_STATUS;        // Indicator of Wifi status
int ssid_status = 0;                // Set SSID status, 1 hidden, 0 not hidden
int para_counter = 0;

bool isPowerUp = false;
bool isExecuted = false;
bool isShiftSchedule = false;
bool isWatering = false;
bool isWaterRemained = false;
bool isOpenSV = false;
bool isCloseSV = false;
bool reexecSch = false;
bool reconnWiFi = false;
bool triggerWDT = false;

bool internetState = false;
unsigned long last_check_internet_state = 0;

bool pending_save_config = false;
unsigned long last_save_request_ms = 0;

#define CHECK_SCHEDULE_TIMEOUT		1000	// 1 second
#define MAX_CHECK_WL_INTERVAL		500		// 0.5 second

unsigned long start_pumping_time = 0;
unsigned long open_sv_time = 0;
unsigned long close_sv_time = 0;
unsigned long last_check_remained_water = 0;
unsigned long start_scan_time = 0;
unsigned long last_check_sch = 0;
unsigned long last_check_wl_rise = 0;

int last_water_level = 0;

uint32_t lastcheckwltime = 0;

#if SUPPORT_FLOW_SENSOR
volatile unsigned int pulse;
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_ENVIRONMENT_SENSOR
uint32_t last_reading_time = 0;

#if SUPPORT_TEMP_HUMIDITY_SENSOR

#if SUPPORT_DHT11 || SUPPORT_DHT22
/* Initialize DHT sensor */
DHT dht(DHT_DATA_PIN, DHT_TYPE);
#endif // SUPPORT_DHT11 || SUPPORT_DHT22

#if SUPPORT_SHT2X
uFire_SHT20 sht20;
#endif // SUPPORT_SHT2X

#define MAX_READ_TH_ERR_NUM		3

int read_th_err_count = 0;
#endif // SUPPORT_TEMP_HUMIDITY_SENSOR

/* Light Sensor definitions */
#if SUPPORT_LIGHT_SENSOR
#define CHECK_LIGHT_INTERVAL	900	// 15 minutes
#define MIN_LUX_VALUE			50
#define DAY_MODE_OFFSET			30

BH1750 lightMeter;
bool isNightMode = false;
uint16_t light_offset = 150;
unsigned long last_check_light = 0;
#endif // SUPPORT_LIGHT_SENSOR

#endif // SUPPORT_ENVIRONMENT_SENSOR

char current_time_str[MAX_LOG_TIME_LEN+1] = {0};
char start_day_str[MAX_LOG_TIME_LEN+1] = {0};

#if SUPPORT_WATCH_DOG
/* Watchdog */
#define AON_WDT_Enable (0)
WDT wdt(AON_WDT_Enable);

uint32_t lastupdatewdt = 0;
#endif // SUPPORT_WATCH_DOG

/* Web Server */
WiFiServer server(80);

/* Create NTPClient */
#define MAX_CHECK_NTP_INTERVAL		10000	// 10 seconds
unsigned long last_ntp_update = 0;

WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "tock.stdtime.gov.tw", 28800, 3600000);	// the update interval is one hour

OTA ota;

uint32_t lastOTAtime = 0;

/* Configure file */
static char conf_name[] = "config.bin";

#if SUPPORT_SAVE_ENV_DATA
static char env_data_name[] = "env_data.txt";
#endif // SUPPORT_SAVE_ENV_DATA

#if SUPPORT_CAMERA
#include "StreamIO.h"
#include "VideoStream.h"
#include "RTSP.h"
#include "Base64.h"

/* Video settings */
#define CHANNEL  				0

/* SD card settings */
static char image_dirname[] = "image";

#define MAX_CHECK_SAVE_IMAGE_INTERVAL	1000 // one second
unsigned long last_check_save_image = 0;

#define MAX_IMAGE_ID_LEN	12
#define MAX_IMAGE_NAME_LEN	16
#define MAX_IMAGE_PATH_LEN	128

char report_image_id[MAX_IMAGE_ID_LEN + 1] = {0};
char image_name[MAX_IMAGE_NAME_LEN + 1] = {0};
char image_path[MAX_IMAGE_PATH_LEN + 1] = {0};


#if SUPPORT_NCU_CLOUD

#define IMAGE_CHUNK_SIZE 			512
#define RESEND_IMAGE_INTERVAL		60

const static char reportImageAPI[] = "report-image";
char reportImageTopic[MAX_SUBSCRIBE_TOPIC_LEN + 1] = {0};
char reportImageRespTopic[MAX_SUBSCRIBE_TOPIC_LEN + 1] = {0};

uint32_t image_pid;
uint32_t image_stack_size = 4096; // must be greater than or equal to 4096, otherwise the stack will overflow
uint32_t send_image_time = 0;

int default_save_image_time1 = 720;	// 12:00
int default_save_image_time2 = 960; 	// 16:00
bool isSubscribeReportImage = false;
bool isSubscribeReportImageResp = false;
bool resend_image = false;
bool send_pic1 = false;
bool send_pic2 = false;
#endif // SUPPORT_NCU_CLOUD

/* html and CGI definition */
#define VIDEO_HTM					"video.htm"

#define REFRESH_VIDEO_CGI			"refresh_video.cgi"
#define ROTATE_CAMERA_CGI			"rotate_camera.cgi"
#define PICTURE_TIME_CGI			"picture_time.cgi"

/* parameters for CGI */
// Video
#define ROTATE_PARA					"rotate"

#if SUPPORT_NCU_CLOUD
#define SAVE_IMAGE_TIME1_PARA			"save_image_time1"
#define SAVE_IMAGE_TIME2_PARA			"save_image_time2"
#endif // SUPPORT_NCU_CLOUD

#define OTA_SERVER_PARA				"ota_server"

/* RTSP Settings */
/*
VideoSetting rtsp_config(CHANNEL);
RTSP rtsp;
StreamIO videoStreamer(1, 1);    // 1 Input Video -> 1 Output RTSP
*/

/* Image Settings */
// Use a pre-defined resolution, or choose to configure your own resolution
// Unable to upload Full HD image(VIDEO_FHD) to the broker
VideoSetting img_config(VIDEO_HD, CAM_FPS, VIDEO_JPEG, 1);

CameraSetting configCam;

uint32_t img_addr = 0;
uint32_t img_len = 0;

int encodedLen;
char *encodedData;
#endif // SUPPORT_CAMERA

/* File System */
AmebaFatFS fs;

// Summary:
//	convert timestamp to the date string
void getCurrentTimeStr() {
	long long seconds = timeClient.getEpochTime();
	struct tm *timeinfo = localtime(&seconds);

	snprintf(current_time_str, sizeof(current_time_str), "%04d/%02d/%02d %02d:%02d:%02d", (timeinfo->tm_year + 1900), (timeinfo->tm_mon + 1), timeinfo->tm_mday
				, timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec);
}

// Summary:
//	print the current time
void printCurrentTime() {
	getCurrentTimeStr();
	printf("[%s] : ", current_time_str);
}

// Summary:
//	get the current day of year
int getCurrentDayOfYear() {
	long long seconds = timeClient.getEpochTime();
	struct tm *timeinfo = localtime(&seconds);

	return timeinfo->tm_yday+1;
}

// Summary:
//	retrieve the start day
void getStartDayStr() {
	long long seconds = timeClient.getEpochTime();
	struct tm *timeinfo = localtime(&seconds);
	int current_day = timeinfo->tm_yday + 1;
	int minus_day = 0;

	// if the current day is less than the start day
	// that means we have crossed one year and we must added the maximum day of last year
	if (current_day < current_system_setting.start_day) {
		current_day += current_system_setting.max_day_of_year;
	}

	minus_day = current_day - current_system_setting.start_day;
	seconds -= (minus_day * 86400);
	timeinfo = localtime(&seconds);
	snprintf(start_day_str, sizeof(start_day_str), "%04d/%02d/%02d", (timeinfo->tm_year + 1900), (timeinfo->tm_mon + 1), timeinfo->tm_mday);
}

// Summary:
//	check the status of the internet connection
bool checkInternetStatus() {
    WiFiClient client;

    client.setTimeout(500);

	// try to connect Google's DNS server for testing the internet connection
    if (client.connect(IPAddress(8, 8, 8, 8), 53)) {
        client.stop();
        return true;
    }

    return false;
}

// Summary:
//	print the current network information
void printWifiData() {
	// print your WiFi IP address:
	IPAddress ip = WiFi.localIP();
	printf("IP Address: %s\r\n", ip.get_address());

	// print your subnet mask:
	IPAddress subnet = WiFi.subnetMask();
	printf("NetMask: %s\r\n", subnet.get_address());

	// print your gateway address:
	IPAddress gateway = WiFi.gatewayIP();
	printf("Gateway: %s\r\n", gateway.get_address());
}

// Summary:
//	print the current wifi information
void printCurrentNet() {
	byte bssid[6];
	char mac_addr[MAX_MAC_LEN + 6] = {0};

	// print the SSID of the AP:
	printf("SSID: %s\r\n", WiFi.SSID());

	// print the MAC address of AP:
	WiFi.BSSID(bssid);
	snprintf(mac_addr, sizeof(mac_addr), "%02X:%02X:%02X:%02X:%02X:%02X",
			bssid[0], bssid[1], bssid[2], bssid[3], bssid[4], bssid[5]);
	printf("BSSID: %s\r\n", mac_addr);

	// print the encryption type:
	printf("Encryption Type: %x\r\n", WiFi.encryptionType());
}

#if SUPPORT_WATER_LEVEL_SENSOR
void reboot_wl_sensor() {
	// initialize the state of flow sensor
	digitalWrite(WL_GPIO_PIN, LOW);
	delay(1000);
	// initialize the state of flow sensor
	digitalWrite(WL_GPIO_PIN, HIGH);
	delay(1000);
}
#endif // SUPPORT_WATER_LEVEL_SENSOR

#if SUPPORT_CAMERA
// Summary:
//	Encode the image content to base64
void encoding_jpg() {
    // Encode the file data as Base64
    encodedLen = base64_enc_len(img_len);
    encodedData = (char *)malloc(encodedLen);
    base64_encode(encodedData, (char *)img_addr, img_len);
}

// Summary:
//	Create an image folder to save all image files
void create_image_folder() {
	static char path[128];
	bool isExisted = false;

	fs.begin();

	memset(path, 0, sizeof(path));
	snprintf(path, sizeof(path), "%s%s", fs.getRootPath(), image_dirname);

	isExisted = fs.exists(path);

	if (!isExisted) {
		fs.mkdir(path);
	}

	fs.end();
}

// Summary:
//	save an image file to sd card
bool save_image_to_sd_card() {
	File img_file;
	uint16_t year = (uint16_t)timeClient.getYear();
    uint16_t month = (uint16_t)timeClient.getMonth();
    uint16_t date = (uint16_t)timeClient.getMonthDay();
    uint16_t hour = (uint16_t)timeClient.getHours();
    uint16_t minute = (uint16_t)timeClient.getMinutes();
	uint16_t second = (uint16_t)timeClient.getSeconds();
	uint32_t addr = 0;
	uint32_t len = 0;
	bool result = false;

	// capture a picture
	Camera.getImage(CHANNEL, &addr, &len);

	if (len > 0) {

		fs.begin();
		img_file = fs.open(image_path);

		delay(1000);
		img_file.write((uint8_t *)addr, len);
		img_file.close();

		// modify the image file’s date to the current time
		fs.setLastModTime(image_path, year, month, date, hour, minute, second);

		fs.end();

		result = true;
	}

	return result;
}

// Summary:
//	Generate the image file path on the SD card
void generate_image_path() {
	uint16_t year = (uint16_t)timeClient.getYear();
    uint16_t month = (uint16_t)timeClient.getMonth();
    uint16_t date = (uint16_t)timeClient.getMonthDay();
    uint16_t hour = (uint16_t)timeClient.getHours();
    uint16_t minute = (uint16_t)timeClient.getMinutes();

	memset(report_image_id, 0, sizeof(report_image_id));
	snprintf(report_image_id, sizeof(report_image_id), "%04d%02d%02d%02d%02d", year, month, date, hour, minute);

	memset(image_name, 0, sizeof(image_name));
	snprintf(image_name, sizeof(image_name), "%s.jpg", report_image_id);

	fs.begin();
	memset(image_path, 0, sizeof(image_path));
	snprintf(image_path, sizeof(image_path), "%s%s/%s", fs.getRootPath(), image_dirname, image_name);
	fs.end();
}

#if SUPPORT_NCU_CLOUD
// Summary:
//	read the image file from sd card and send it to the broker
bool send_image_to_broker() {
	File img_file;
	static char tmp_topic[MAX_SUBSCRIBE_TOPIC_LEN + 1];
	static char msg[128];
	uint8_t buffer[IMAGE_CHUNK_SIZE];
	int offset = 0;
	int chunk_id = 0;
	bool result = false;

	if (mqttClient.connected()) {

		fs.begin();

		if (strlen(image_path) > 0) {
			img_file = fs.open(image_path);
		}

		memset(msg, 0, sizeof(msg));

		if (img_file) {

			while (img_file.available()) {
				memset(tmp_topic, 0, sizeof(tmp_topic));
				snprintf(tmp_topic, sizeof(tmp_topic), "%s/%s/chunk/%d", reportImageTopic, report_image_id, chunk_id);

				offset = img_file.read(buffer, IMAGE_CHUNK_SIZE);

				result = mqttClient.publish(tmp_topic, buffer, offset);

				if (!result) {
					//printf("Failed to publish chunk %d\r\n", chunk_id);
					// when DUT is still connecting to the broker
					if (mqttClient.connected()) {
						// move the file pointer back to the previous position
						// and send the data again
						img_file.seek(chunk_id * IMAGE_CHUNK_SIZE);
						continue;
					} else {
						// when DUT is disconnected from the broker
						break;
					}
				}

				chunk_id++;
			}

			img_file.close();

			memset(tmp_topic, 0, sizeof(tmp_topic));
			snprintf(tmp_topic, sizeof(tmp_topic), "%s/%s/done/%d", reportImageTopic, report_image_id, chunk_id);
			// send the done topic to indicate all image data has been sent
			result = mqttClient.publish(tmp_topic, "");

			if (result) {
				snprintf(msg, sizeof(msg), "Send %s successfully", image_name);
			} else {
				snprintf(msg, sizeof(msg), "Fail to send %s", image_name);
			}
		} else {
			snprintf(msg, sizeof(msg), "Fail to open %s", image_name);
		}

		printCurrentTime();
		printf("%s\r\n", msg);

		save_log_msg(msg);

		fs.end();
	}

	return result;
}

// Summary:
//	Send an image to the broker
void publishImage() {
	bool isExisted = false;
	bool result = false;

	// when DUT re-sends an image to the broker
	// check whether the image already exists
	if (resend_image) {
		if (strlen(image_path) > 0) {
			isExisted = fs.exists(image_path);
		}
	}

	// generate an image if it doesn't exist
	if (!isExisted) {
		generate_image_path();
		isExisted = save_image_to_sd_card();
	}

	// if the image already exists
	if (isExisted) {

		result = send_image_to_broker();

		resend_image = true;

		// when sending image to the broker successfully
		if (result) {
			// keep the current time for re-sending image
			// if DUT doesn't receive the response from the broker
			send_image_time = timeClient.getEpochTime();
		} else {
			// re-sending image immediately if fail to send image to the broker
			send_image_time = 0;
		}
	} else {
		save_log_msg("The image is not existed");
		send_image_time = 0;
	}
}
#endif // SUPPORT_NCU_CLOUD

// Summary:
//	Save an image file to the local location
void save_image_to_local() {
	long long seconds = timeClient.getEpochTime();
	struct tm *timeinfo = localtime(&seconds);
	int32_t current_mins = timeinfo->tm_hour * 60 + timeinfo->tm_min;
	int image_time1 = current_system_setting.save_image_time1;
	int image_time2 = current_system_setting.save_image_time2;
	bool isTime1Up = false;
	bool isTime2Up = false;
	bool need_updated = false;

	if ((current_mins >= image_time1) && (current_mins < image_time2) && (!current_system_setting.save_image_flag1)) {
		isTime1Up = true;
	} else if ((current_mins >= image_time2) && (!current_system_setting.save_image_flag2)) {
		isTime2Up = true;
	}

	if (isTime1Up || isTime2Up) {
		generate_image_path();

		if (save_image_to_sd_card()) {

			need_updated = true;

			if (isTime1Up) {
				current_system_setting.save_image_flag1 = true;
			} else if (isTime2Up) {
				current_system_setting.save_image_flag2 = true;
			}
		}
	}

	// reset the save flags
	if ((current_system_setting.save_image_flag1 == true) && (current_mins < image_time1)) {
		current_system_setting.save_image_flag1 = false;
		need_updated = true;
	}

	if ((current_system_setting.save_image_flag2 == true) && (current_mins < image_time2)) {
		current_system_setting.save_image_flag2 = false;
		need_updated = true;
	}

#if SUPPORT_NCU_CLOUD
	if (safeMQTTconnected()) {
		if (isSubscribeReportImage && isSubscribeReportImageResp) {

			if ((current_mins >= image_time1) && (current_mins < image_time2) && (!current_system_setting.send_pic_flag1)) {
				send_pic1 = true;
				resend_image = false;
			} else if ((current_mins >= image_time2) && (!current_system_setting.send_pic_flag2)) {
				send_pic2 = true;
				resend_image = false;
			}

			if (send_pic1 || send_pic2) {
				if ((seconds - send_image_time) >= RESEND_IMAGE_INTERVAL) {
					publishImage();
				}
			}
		}

		// reset the send flags
		if ((current_system_setting.send_pic_flag1 == true) && (current_mins < image_time1)) {
			current_system_setting.send_pic_flag1 = false;
			need_updated = true;
		}

		if ((current_system_setting.send_pic_flag2 == true) && (current_mins < image_time2)) {
			current_system_setting.send_pic_flag2 = false;
			need_updated = true;
		}
	}
#endif // SUPPORT_NCU_CLOUD

	if (need_updated) {
		//update_config();
		pending_save_config = true;
		last_save_request_ms = millis();
	}
}

// Summary:
//	Initialize the Camera settings
void initialize_camera() {
	img_config.setRotation(current_system_setting.rotate);

	// initialize Camera
	Camera.configVideoChannel(CHANNEL, img_config);
	Camera.videoInit();

	// Start data stream from video channel
	Camera.channelBegin(CHANNEL);
}
#endif // SUPPORT_CAMERA

#if SUPPORT_NCU_CLOUD
#if SUPPORT_CAMERA
void update_send_flags() {
	bool need_updated = false;

	if (send_pic1) {
		send_pic1 = false;
		need_updated = true;
		current_system_setting.send_pic_flag1 = true;
	}

	if (send_pic2) {
		send_pic2 = false;
		need_updated = true;
		current_system_setting.send_pic_flag2 = true;
	}

	if (need_updated) {
		//update_config();
		pending_save_config = true;
		last_save_request_ms = millis();
	}
}
#endif // SUPPORT_CAMERA

// Summary:
//	The callback function to handle the messages from cloud
void cloud_callback(char* topic, byte* payload, unsigned int length) {
	cJSON *root = NULL;
	cJSON *uuid_item = NULL;
	cJSON *report_at_item = NULL;
	cJSON *server_status_item = NULL;
	String topicStr = String(topic);
	static char msg[MAX_PAYLOAD_LEN + 1];
	static char report_at[MAX_CHECK_RESP_ID_LEN + 1];
	int server_status = 0;

	printCurrentTime();
	printf("Message arrived [%s]\r\n", topic);
	printf("length=%d\r\n", length);

	if (length > MAX_PAYLOAD_LEN) {
		length = MAX_PAYLOAD_LEN;
	}

	memset(msg, 0, sizeof(msg));
	memcpy(msg, payload, length);
	msg[length] = '\0';

	printf("message=%s\r\n", msg);
	printf("============\r\n");

	if (topicStr.indexOf(uuidAPI) >= 0) {
		root = cJSON_Parse(msg);

		if (root != NULL){

			uuid_item = cJSON_GetObjectItem(root, DEVICE_UUID_ITEM);

			if ((uuid_item != NULL) && (uuid_item->valuestring != NULL)) {
				memset(device_uuid, 0, sizeof(device_uuid));
				memcpy(device_uuid, uuid_item->valuestring, strlen(uuid_item->valuestring));

				memset(reportTopic, 0, sizeof(reportTopic));
				snprintf(reportTopic, sizeof(reportTopic), uplinkTopic, reportAPI, device_uuid);

				isSubscribeReportResp = false;
				memset(reportRespTopic, 0, sizeof(reportRespTopic));
				snprintf(reportRespTopic, sizeof(reportRespTopic), downlinkTopic, reportAPI, device_uuid);
        snprintf(commandTopic, sizeof(commandTopic), downlinkTopic, "command", device_uuid);

#if SUPPORT_CAMERA
				isSubscribeReportImage = false;
				memset(reportImageTopic, 0, sizeof(reportImageTopic));
				snprintf(reportImageTopic, sizeof(reportImageTopic), uplinkTopic, reportImageAPI, device_uuid);

				isSubscribeReportImageResp = false;
				memset(reportImageRespTopic, 0, sizeof(reportImageRespTopic));
				snprintf(reportImageRespTopic, sizeof(reportImageRespTopic), downlinkTopic, reportImageAPI, device_uuid);
#endif // SUPPORT_CAMERA
			}

			cJSON_Delete(root);
		}

		retrieveUUID = false;
		last_retrieve_uuid = 0;
	} else if ((topicStr.indexOf(reportRespTopic) >= 0)
#if SUPPORT_CAMERA
			|| (topicStr.indexOf(reportImageRespTopic) >= 0)
#endif // SUPPORT_CAMERA
			) {

		root = cJSON_Parse(msg);

		if (root != NULL){

			report_at_item = cJSON_GetObjectItem(root, REPORT_AT_ITEM);

			memset(report_at, 0, sizeof(report_at));

			if (report_at_item != NULL) {
				snprintf(report_at, sizeof(report_at), "%s", cJSON_Print(report_at_item));
			}

			server_status_item = cJSON_GetObjectItem(root, SERVER_STATUS_ITEM);
			if (server_status_item != NULL) {
				server_status = server_status_item->valueint;
			}

			if (topicStr.indexOf(reportRespTopic) >= 0) {
				if (memcmp(report_hb_id, report_at, MAX_CHECK_RESP_ID_LEN) == 0) {
					if (server_status == SUCCESS_RESP_CODE) {
						printf("receive the heartbeat response for %s\r\n", (strlen(report_hb_id) > 0)?report_hb_id:"NULL");
						receive_hb_resp = true;
					}
				}
			}
#if SUPPORT_CAMERA
			else if (topicStr.indexOf(reportImageRespTopic) >= 0) {
				if (memcmp(report_image_id, report_at, MAX_CHECK_RESP_ID_LEN) == 0) {
					if (server_status == SUCCESS_RESP_CODE) {
						memset(report_image_id, 0, sizeof(report_image_id));
						memset(image_name, 0, sizeof(image_name));
						memset(image_path, 0, sizeof(image_path));
						resend_image = false;
						send_image_time = 0;
						update_send_flags();
					}
				}
			}
#endif // SUPPORT_CAMERA

			cJSON_Delete(root);
		}
	}
  else if (topicStr.indexOf("downlink/command") >= 0) {
  root = cJSON_Parse(msg);
    if (root != NULL) {
      cJSON *cmd = cJSON_GetObjectItem(root, "command_name");
      if (cmd != NULL && strcmp(cmd->valuestring, "set_watering_schedule") == 0) {
        cJSON *start = cJSON_GetObjectItem(root, "start_date");
        cJSON *sched = cJSON_GetObjectItem(root, "schedule");
        cJSON *days  = cJSON_GetObjectItem(root, "day_count");

        if (start && start->valuestring) {
          strncpy(current_system_setting.vgnms_start_date, start->valuestring, 11);
        }
        if (days) {
          current_system_setting.vgnms_day_count = days->valueint;
        }
        if (sched && cJSON_IsArray(sched)) {
          int n = cJSON_GetArraySize(sched);
          if (n > 14) n = 14;
          current_system_setting.vgnms_schedule_count = n;
          for (int i = 0; i < n; i++) {
            current_system_setting.vgnms_schedule[i] = cJSON_GetArrayItem(sched, i)->valueint;
          }
        }
        pending_save_config = true;
        last_save_request_ms = millis();
        printCurrentTime();
        printf("Watering schedule received from VGNMS\r\n");
      }
      cJSON_Delete(root);
    }
  }    
}

// Summary:
//	create a connection to the mqtt broker
uint8_t cloud_reconnect() {
	static char random_client_id[MAX_RANDOM_CLIENTID_LEN + 1];
	static char msg[80];
	int32_t seconds = timeClient.getEpochTime();
	uint8_t isConnected = 0;
	uint8_t result = 0;

#if SUPPORT_WATCH_DOG
	wdt.refresh();
#endif // SUPPORT_WATCH_DOG

	// we must re-subscribe all topics when the mqtt connection is broken
	// otherwise we won't receive the mqtt response from the broker
	isSubscribeReportResp = false;

#if SUPPORT_CAMERA
	isSubscribeReportImage = false;
	isSubscribeReportImageResp = false;
#endif // SUPPORT_CAMERA

	memset(random_client_id, 0, sizeof(random_client_id));
	snprintf(random_client_id, sizeof(random_client_id), "%s_%ld", clientId, seconds);

	printCurrentTime();
	printf("Attempting MQTT connection...\r\n");

	memset(msg, 0, sizeof(msg));

	result = mqttClient.connect(random_client_id, "test", "test");

	// Attempt to connect
	if (result) {

		snprintf(msg, sizeof(msg), "connected to the broker successfully");

		isConnected = 1;
		last_reconn_attempt = 0;
	} else {

		if (internetState) {
			internetState = false;
		}

		snprintf(msg, sizeof(msg), "connect to the broker failed,error=%d", mqttClient.state());
	}

	printCurrentTime();
	printf("%s\r\n", msg);

	if (result) {
		if ((prevMqttConnState == 0) || (prevMqttConnState == 2)) {
			prevMqttConnState = 1;
			save_log_msg(msg);
		}
	} else {
		if ((prevMqttConnState == 0) || (prevMqttConnState == 1)) {
			prevMqttConnState = 2;
			save_log_msg(msg);
		}
	}

	return isConnected;
}

// Summary:
//	Check the connection of mqtt client in every 0.5 second
bool safeMQTTconnected() {
    static unsigned long lastCheck = 0;

    if (millis() - lastCheck < 500) {
		return lastConnState;
	}

    lastCheck = millis();
	lastConnState = mqttClient.connected();

    return lastConnState;
}

// Summary:
//	verify the connection to the mqtt broker
bool verify_broker_connection() {
	uint8_t isConnected = safeMQTTconnected();

	if (!isConnected) {

		if (needCleanup) {
            mqttClient.disconnect(); // release the previous connection resource
            sslClient.stop();
            needCleanup = false;
            printf("[%s]: MQTT Disconnected. SSL resources released.\n", current_time_str);
        }

		if ((millis() - last_reconn_attempt) >= MQTT_RECONNECTION_INTERVAL) {
			printCurrentTime();
			printf("verify_broker_connection: isConnected=%d\r\n", isConnected);

			last_reconn_attempt = millis();
			isConnected = cloud_reconnect();

			if (isConnected) {
                needCleanup = true;
            }
		}
	}

	return isConnected;
}

// Summary:
//	subscribe the topic to the mqtt broker
bool subscribeTopic(char *topic) {
	bool result = false;

	//if (mqttClient.connected()) {
	if (safeMQTTconnected()) {

		printCurrentTime();
		printf("Subscribe Topic : %s\r\n", (strlen(topic) > 0)?topic:"NULL");

		result = mqttClient.subscribe(topic);

		printCurrentTime();
		printf("Subscribe Result : %s\r\n", result?"succeed":"failed");

		if (!result) {
			printCurrentTime();
			printf("Call the disconnect function\r\n");
			mqttClient.disconnect();
		}
	} else {
		printCurrentTime();
		printf("The MQTT is disconnected when subscribing %s\r\n", (strlen(topic) > 0)?topic:"NULL");
	}

	return result;
}

// Summary:
//	publish the message to the mqtt broker
bool publishMsg(char *topic, char *msg) {
	bool result = false;

	//if (mqttClient.connected()) {
	if (safeMQTTconnected()) {

		printCurrentTime();
		printf("Publish Topic : %s\r\n", (strlen(topic) > 0)?topic:"NULL");
		printf("Publish Message : %s\r\n", (strlen(msg) > 0)?msg:"NULL");

		result = mqttClient.publish(topic, msg);

		printCurrentTime();
		printf("Publish Result : %s\r\n", result?"succeed":"failed");

		if (result) {
			waitforresp = true;
		} else {
			printCurrentTime();
			printf("Call the disconnect function\r\n");
			mqttClient.disconnect();
		}
	} else {
		printCurrentTime();
		printf("The MQTT is disconnected when publishing %s\r\n", (strlen(topic) > 0)?topic:"NULL");
		printf("Publish Message: %s\r\n", (strlen(msg) > 0)?msg:"NULL");
	}

	return result;
}

// Summary:
//	 subscribe and publish the uuid-lookup topic to retrieve UUID
void getUUID() {
	static char uuidTopic[MAX_UUID_TOPIC_LEN + 1];
	bool result = false;

	retrieveUUID = true;
	last_retrieve_uuid = millis();

	memset(uuidTopic, 0, sizeof(uuidTopic));
	snprintf(uuidTopic, sizeof(uuidTopic), downlinkTopic, uuidAPI, mac_addr_str);

	if (subscribeTopic(uuidTopic)) {
		memset(uuidTopic, 0, sizeof(uuidTopic));
		snprintf(uuidTopic, sizeof(uuidTopic), uplinkTopic, uuidAPI, mac_addr_str);
		result = publishMsg(uuidTopic, "");
	}

	/*
	if (!result) {
		last_retrieve_uuid = 0;
	}
	*/
}

// Summary:
// 	Limit the maximum execution time of mqtt.loop() to prevent it from getting stuck indefinitely
void safeMQTTLoop() {
    uint32_t start = millis();

    while (millis() - start < 20) {
        if (!mqttClient.loop()) {
            break;
        }
    }
}

// Summary:
//	Check whether or not DUT needs to publish messages or images
void check_ncu_tasks() {
	static char hb_msg[MAX_HEARTBEAT_LEN + 1];
	static char hb_payload[MAX_PAYLOAD_LEN + 1];
	long long seconds = timeClient.getEpochTime();
	int sch_index = current_system_setting.sch_index;

	if (verify_broker_connection()) {

		if (strlen(reportTopic) > 0) {

#if SUPPORT_CAMERA
			if ((!isSubscribeReportImage) && (strlen(reportImageTopic) > 0)) {
				isSubscribeReportImage = subscribeTopic(reportImageTopic);
			}

			if ((!isSubscribeReportImageResp) && (strlen(reportImageRespTopic) > 0)) {
				isSubscribeReportImageResp = subscribeTopic(reportImageRespTopic);
			}
#endif // SUPPORT_CAMERA

			if ((!isSubscribeReportResp) && (strlen(reportRespTopic) > 0)) {
				isSubscribeReportResp = subscribeTopic(reportRespTopic);
			}
      if (strlen(commandTopic) > 0) {
      subscribeTopic(commandTopic);
      }

			if (strlen(reportTopic) > 0) {
				if (isSubscribeReportResp) {
					if (strlen(start_pumping_payload) > 0) {
						if (publishMsg(reportTopic, start_pumping_payload)) {
							memset(start_pumping_payload, 0, sizeof(start_pumping_payload));
						}
					}

					if (strlen(stop_pumping_payload) > 0) {
						if (publishMsg(reportTopic, stop_pumping_payload)) {
							memset(stop_pumping_payload, 0, sizeof(stop_pumping_payload));
						}
					}

					if (strlen(water_level_payload) > 0) {
						if (publishMsg(reportTopic, water_level_payload)) {
							memset(water_level_payload, 0, sizeof(water_level_payload));
						}
					}

					if (strlen(open_sv_payload) > 0) {
						if (publishMsg(reportTopic, open_sv_payload)) {
							memset(open_sv_payload, 0, sizeof(open_sv_payload));
						}
					}

					if (strlen(close_sv_payload) > 0) {
						if (publishMsg(reportTopic, close_sv_payload)) {
							memset(close_sv_payload, 0, sizeof(close_sv_payload));
						}
					}

					if (strlen(led_payload) > 0) {
						if (publishMsg(reportTopic, led_payload)) {
							memset(led_payload, 0, sizeof(led_payload));
						}
					}

					if (strlen(warning_msg) > 0) {
						if (publishMsg(reportTopic, warning_msg)) {
							memset(warning_msg, 0, sizeof(warning_msg));
						}
					}

					if (strlen(powerup_payload) > 0) {
						if (publishMsg(reportTopic, powerup_payload)) {
							memset(powerup_payload, 0, sizeof(powerup_payload));
						}
					}

					if (strlen(shift_sch_payload) > 0) {
						if (publishMsg(reportTopic, shift_sch_payload)) {
							memset(shift_sch_payload, 0, sizeof(shift_sch_payload));
						}
					}

#if SUPPORT_ENVIRONMENT_SENSOR
					if (strlen(env_payload) > 0) {
						if (publishMsg(reportTopic, env_payload)) {
							memset(env_payload, 0, sizeof(env_payload));
						}
					}
#endif // SUPPORT_ENVIRONMENT_SENSOR

					if ((seconds - last_heart_beat) >= HEARTBEAT_INTERVAL) {
						last_heart_beat = seconds;

						memset(hb_msg, 0, sizeof(hb_msg));
						snprintf(hb_msg, sizeof(hb_msg), heartbeatPayload, current_system_setting.seed_name, current_system_setting.working_day, current_system_setting.watering_day, (sch_index+1), WiFi.RSSI());

						memset(hb_payload, 0, sizeof(hb_payload));
						snprintf(hb_payload, sizeof(hb_payload), reportPayload, REPORT_TYPE, hb_msg, (timeClient.getEpochTime() - tw_offset));
						publishMsg(reportTopic, hb_payload);
					}
				}
			}
		} else {
			if (!retrieveUUID) {
				printCurrentTime();
				printf("Report Topic is NULL\r\n");
				getUUID();
			} else if ((millis() - last_retrieve_uuid) >= RETRIEVE_UUID_INTERVAL) {
				/*
				if (retrieve_uuid_error >= MAX_RETRIEVE_UUID_TIME) {
					sys_reset();
					return;
				}
				*/

				printCurrentTime();
				printf("Retrieve UUID again\r\n");
				getUUID();
			}
		}

		if ((waitforresp == true) || ((millis() - last_check_mqtt_loop) >= MAX_CHECK_MQTT_LOOP_INTERVAL)) {

			waitforresp = false;
			last_check_mqtt_loop = millis();
			// check the incoming messages
			// we must keep mqtt.loop() here to receive the response message from the broker
			// and check the the connection between the client and the broker
			safeMQTTLoop();
		}
	}
}
#endif // SUPPORT_NCU_CLOUD

// Summary:
//	Reset the wifi settings to default values
void restore_default_wifi() {
	strcpy(current_system_setting.ssid, "");
	strcpy(current_system_setting.key, "");
}

// Summary:
//	Reset all schedules to default values
void restore_default_schedules() {

	printf("Restore the schedules to factory default\r\n");

	snprintf(current_system_setting.seed_name, (MAX_SEED_NAME + 1), "%s", default_seed_name);
	current_system_setting.watering_in_firstday = default_watering_in_firstday;
	current_system_setting.start_day = 0;
	current_system_setting.max_day_of_year = 0;
	current_system_setting.current_day = 0;
	current_system_setting.working_day = default_working_day;
	current_system_setting.watering_day = default_watering_day;
	current_system_setting.sensor_offset = 0;
	current_system_setting.max_check_wl_time = default_max_check_wl_time;
	current_system_setting.watering_start_time = default_watering_start_time;
	current_system_setting.watering_end_time = default_watering_end_time;

	current_system_setting.sch_executed = false;
	current_system_setting.isExecSch = false;
	current_system_setting.isPumpProcCompleted = false;
	current_system_setting.isWaitingProcCompleted = false;

	current_system_setting.startWaitingTime = 0;

	current_system_setting.enable_sch = default_enable_sch;
	current_system_setting.sch_index = default_sch_index;
	current_system_setting.watering_type = WATERING_BY_TIME_TYPE;

	for (int i = 0; i < MAX_SCHEDULE_NUM; i++) {
#if SUPPORT_FLOW_SENSOR
		current_system_setting.schedule[i].watering_volume = default_watering_volume;
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
		current_system_setting.schedule[i].watering_level = default_watering_level;
#endif // SUPPORT_WATER_LEVEL_SENSOR

#if SUPPORT_WATERING_MOISTURE_TYPE
		current_system_setting.schedule[i].moisture_content = default_moisture_content;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

		current_system_setting.schedule[i].watering_interval = default_watering_interval;
		current_system_setting.schedule[i].watering_time = default_watering_time;
		current_system_setting.schedule[i].waiting_time = default_waiting_time;
		current_system_setting.schedule[i].draining_time = default_draining_time;

#if SUPPORT_LED
		current_system_setting.schedule[i].support_led = default_support_led;
		current_system_setting.schedule[i].led_start_time = default_led_start_time;
		current_system_setting.schedule[i].led_end_time = default_led_end_time;
#endif // SUPPORT_LED
	}

	current_system_setting.log_num = 0;
	current_system_setting.log_index = 0;
	memset(current_system_setting.log, 0, sizeof(system_log_t) * MAX_LOG_NUM);

#if SUPPORT_CAMERA
	current_system_setting.rotate = 0;
	current_system_setting.save_image_flag1 = false;
	current_system_setting.save_image_flag2 = false;

	current_system_setting.save_image_time1 = default_save_image_time1;
	current_system_setting.save_image_time2 = default_save_image_time2;

#if SUPPORT_NCU_CLOUD
	current_system_setting.send_pic_flag1 = false;
	current_system_setting.send_pic_flag2 = false;
#endif // SUPPORT_NCU_CLOUD
#endif // SUPPORT_CAMERA

	memset(current_system_setting.ota_server, 0, sizeof(current_system_setting.ota_server));
	strcpy(current_system_setting.ota_server, "0.0.0.0");
}

#if SUPPORT_SAVE_ENV_DATA
// Summary:
//	save the environment data to SD card
void save_env_data(char *data) {
	File data_file = NULL;
	static char path[128];
	uint32_t seek_size = 0;

	fs.begin();

	memset(path, 0, sizeof(path));
	snprintf(path, sizeof(path), "%s%s", fs.getRootPath(), env_data_name);

	data_file = fs.open(path);
	seek_size = data_file.size();

	if (data_file.seek(seek_size)) {
		getCurrentTimeStr();
		data_file.print(current_time_str);
		data_file.print("::");
		data_file.println(data);
	}

	data_file.close();

	fs.end();
}
#endif // SUPPORT_SAVE_ENV_DATA

// Summary:
//	update the config file to SD card
void update_config() {
	File conf_file = NULL;
	static char path[128];

	fs.begin();

	memset(path, 0, sizeof(path));
	snprintf(path, sizeof(path), "%s%s", fs.getRootPath(), conf_name);

	// if config.bin is not existed, restore the default values
	if (!fs.exists(path)) {
		//printf("%s is not existed\r\n", path);
		memset(&current_system_setting, 0, sizeof(system_setting_t));
		restore_default_wifi();
		restore_default_schedules();
	} else {
		//printf("%s is existed\r\n", path);
	}

	conf_file = fs.open(path);
	conf_file.write((uint8_t *)&current_system_setting, sizeof(system_setting_t));
	conf_file.close();

	fs.end();
}

// Summary:
//	load the config file from SD card
void load_config() {
	File conf_file = NULL;
	static char path[128];
	bool isExisted = false;

	fs.begin();

	memset(path, 0, sizeof(path));
	snprintf(path, sizeof(path), "%s%s", fs.getRootPath(), conf_name);
	isExisted = fs.exists(path);

	memset(&current_system_setting, 0, sizeof(system_setting_t));

	// if config.bin is not existed, restore the default values
	if (!isExisted) {
		restore_default_wifi();
		restore_default_schedules();
	}

	conf_file = fs.open(path);

	if (!isExisted) {
		//printf("save %s to %s\r\n", conf_name, path);
		conf_file.write((uint8_t *)&current_system_setting, sizeof(system_setting_t));
	} else {
		//printf("read %s\r\n", path);
		conf_file.read((uint8_t *)&current_system_setting, sizeof(system_setting_t));
	}

	conf_file.close();

	fs.end();
}

// Summary:
//	Save the messages to flash
void save_log_msg(char *msg) {
	int index = current_system_setting.log_index;

	if (index >= MAX_LOG_NUM){
		index = 0;
		current_system_setting.log_index = 0;
	}

	getCurrentTimeStr();

	memset(current_system_setting.log[index].log_time, 0, MAX_LOG_TIME_LEN + 1);
	memcpy(current_system_setting.log[index].log_time, current_time_str, MAX_LOG_TIME_LEN);

	memset(current_system_setting.log[index].log_msg, 0, MAX_LOG_MSG_LEN + 1);
	snprintf(current_system_setting.log[index].log_msg, (MAX_LOG_MSG_LEN+1), "%s", msg);

	if (current_system_setting.log_num < MAX_LOG_NUM) {
		current_system_setting.log_num++;
	}

	current_system_setting.log_index++;
	//update_config();

	pending_save_config = true;
	last_save_request_ms = millis();
}

//Summary:
//	looking for an available NTP server
void change_ntp_server() {

	/*
	for (int i = 0; ntp_server[i] != NULL; i++){
		timeClient.setPoolServerName(ntp_server[i]);
		printCurrentTime();
		printf("Change ntp server to %s\r\n", ntp_server[i]);
		if (timeClient.forceUpdate()){
			printf("Connect to %s successfully\r\n", ntp_server[i]);
			break;
		}

		delay(1000);
	}
	*/

	if (ntp_server_index >= MAX_NTP_SERVER_NUM) {
		ntp_server_index = 0;
	}

	timeClient.setPoolServerName(ntp_server[ntp_server_index]);
	printCurrentTime();
	printf("Change ntp server[%d] to %s\r\n", ntp_server_index, ntp_server[ntp_server_index]);

	if (timeClient.forceUpdate()){
		printf("Connect to %s successfully\r\n", ntp_server[ntp_server_index]);
	} else {
		printCurrentTime();
		printf("ntp server[%d]:: %s, failed\r\n", ntp_server_index, ntp_server[ntp_server_index]);

		ntp_server_index++;
	}
}

#if SUPPORT_WATCH_DOG
void watchdog_irq_handler(uint32_t id) {
	static char watchdog_msg[MAX_WATCHDOG_MSG_LEN + 1];

	wdt.stop();

	memset(watchdog_msg, 0, sizeof(watchdog_msg));
	snprintf(watchdog_msg, sizeof(watchdog_msg), "trigger the watchdog timer");

    printf("%s\r\n", watchdog_msg);

	save_log_msg(watchdog_msg);

	triggerWDT = true;
}
#endif // SUPPORT_WATCH_DOG

void setup() {
	byte mac_addr[6];
	char new_ssid[strlen(ap_ssid) + 7] = {0};
	int connect_counter = 0;

	//Initialize serial and wait for port to open:
	Serial.begin(115200);
	while (!Serial) {
		; // wait for serial port to connect. Needed for native USB port only
	}

	// initialize digital pin for relay control
	pinMode(PUMP_GPIO_PIN, OUTPUT);
	pinMode(SV_GPIO_PIN, OUTPUT);

	// initialize the state of relay control
	digitalWrite(PUMP_GPIO_PIN, RELAY_OFF);
	digitalWrite(SV_GPIO_PIN, RELAY_OFF);

#if SUPPORT_LED
	pinMode(LED_GPIO_PIN, OUTPUT);

	// initialize the state of LED
	digitalWrite(LED_GPIO_PIN, LED_OFF);
#endif // SUPPORT_LED

#if SUPPORT_FLOW_SENSOR
	// initialize digital pin for flow sensor
	pinMode(FLOW_GPIO_PIN, INPUT_IRQ_FALL);

	// initialize the state of flow sensor
	digitalWrite(FLOW_GPIO_PIN, LOW);
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
	// initialize digital pin for relay control
	pinMode(WL_GPIO_PIN, OUTPUT);

	reboot_wl_sensor();
#endif // SUPPORT_WATER_LEVEL_SENSOR

	WiFi.status();  // must be called before retrieving the mac address of WiFi
	WiFi.macAddress(mac_addr);
	snprintf(mac_addr_str, sizeof(mac_addr_str), "%02X%02X%02X%02X%02X%02X",  mac_addr[0], mac_addr[1], mac_addr[2], mac_addr[3], mac_addr[4], mac_addr[5]);
	printf("MAC Address: %s\r\n", mac_addr_str);

	WiFi.disablePowerSave();
	sslClient.setTimeout(max_stream_timeout);

	// load the system settings from config.bin
	load_config();

	// get the current schedule index to display the schedule index on the Schedule Settings page
	tmp_sch_index = current_system_setting.sch_index;

	if ((strlen(current_system_setting.ssid) > 0) && (strlen(current_system_setting.key) > 0)) {
		while (wifi_status != WL_CONNECTED) {
			printf("Connect to AP %s\r\n", current_system_setting.ssid);
			wifi_status = WiFi.begin(current_system_setting.ssid, current_system_setting.key);

			printf("\r\n");
			if (++connect_counter >= MAX_CONNECT_TIME) {
				printf("Fail to connect to AP %s\r\n", current_system_setting.ssid);
				break;
			}

			if (wifi_status == WL_CONNECTED) {
				printf("Connect to AP %s successfully\r\n", current_system_setting.ssid);
				break;
			}

			// wait 10 seconds for connection:
			delay(10000);
		}
	}

	// attempt to start AP:
	while (wifi_status != WL_CONNECTED) {
		snprintf(new_ssid, sizeof(new_ssid), "%s-%02x%02x", ap_ssid, mac_addr[4], mac_addr[5]);
		printf("Attempting to start AP with SSID: %s\r\n", new_ssid);

		wifi_status = WiFi.apbegin(new_ssid, ap_pwd, channel, ssid_status);
		isAPMode = true;
		delay(1000);
	}

	isPowerUp = true;
	isExecuted = false;
	isShiftSchedule = false;
	isWatering = false;
	isWaterRemained = false;
	reconnWiFi = false;

#if SUPPORT_NCU_CLOUD
	disablePublish = false;
#endif // SUPPORT_NCU_CLOUD

	// We must start the NTP client before starting the web server;
	// otherwise, the web server may be inaccessible, and the MQTT connection
	// status will behave erratically.
	timeClient.begin();

	// we must set the web server to the none-blocking mode for Amb82 mini
	server.setNonBlockingMode();
	// start the web server
	server.begin();

	if (!isAPMode) {

		internetState = checkInternetStatus();

		// we must call update() here to correct the system time
		if (internetState) {
			timeClient.update();
		}
	}

#if SUPPORT_WATER_LEVEL_SENSOR
	Wire.begin();
#endif // SUPPORT_WATER_LEVEL_SENSOR

#if SUPPORT_NCU_CLOUD
	snprintf(clientId, sizeof(clientId), "%s%02x%02x", DEFAULT_CLIENT_ID, mac_addr[4], mac_addr[5]);
	printf("Client ID: %s\r\n", clientId);

#if MQTT_TLS_SERVER_AUTH
	sslClient.setRootCA((unsigned char*)rootCABuff);
	sslClient.setClientCertificate((unsigned char*)certificateBuff, (unsigned char*)privateKeyBuff);
	mqttClient.setServer(mqttServer, 8884);
#else
	//sslClient.setRootCA((unsigned char*)rootCABuff);
	mqttClient.setServer(mqttServer, 8883);
#endif

	mqttClient.setCallback(cloud_callback);

	// For publish qos1 that server will send ack
    mqttClient.setPublishQos(MQTTQOS1);

	mqttClient.setBufferSize(max_mqtt_buf_size);
	mqttClient.setKeepAlive(mqtt_keep_alive);
	mqttClient.setSocketTimeout(max_socket_timeout);
    // we must set waitForAck to true to prevent the broker is unable
	// to handle multiple messages at same time
	mqttClient.waitForAck(true);

	if (!isAPMode) {

		if (internetState) {
			cloud_reconnect();
		}
	}
#endif // SUPPORT_NCU_CLOUD

#if SUPPORT_CAMERA

#if SUPPORT_NCU_CLOUD
	send_pic1 = false;
	send_pic2 = false;
	send_image_time = 0;

	create_image_folder();
#endif // SUPPORT_NCU_CLOUD

	initialize_camera();

	// set the camera to day mode by default
	configCam.setDayNightMode(0);

	/*
	// Configure camera video channel with video format information
    // Adjust the bitrate based on your WiFi network quality
    // config.setBitrate(2 * 1024 * 1024);     // Recommend to use 2Mbps for RTSP streaming to prevent network congestion
    Camera.configVideoChannel(CHANNEL, rtsp_config);
    Camera.videoInit();

    // Configure RTSP with identical video format information
    rtsp.configVideo(rtsp_config);
    rtsp.begin();

    // Configure StreamIO object to stream data from video channel to RTSP
    videoStreamer.registerInput(Camera.getStream(CHANNEL));
    videoStreamer.registerOutput(rtsp);
    if (videoStreamer.begin() != 0) {
        printf("StreamIO link start failed\r\n");
    }

    // Start data stream from video channel
    Camera.channelBegin(CHANNEL);
	*/
#endif // SUPPORT_CAMERA

	printWifiData();
	printCurrentNet();

#if SUPPORT_ENVIRONMENT_SENSOR
	last_reading_time = 0;

#if SUPPORT_SHT2X || SUPPORT_LIGHT_SENSOR
	Wire1.begin();
#endif // SUPPORT_SHT2X || SUPPORT_LIGHT_SENSOR

#if SUPPORT_LIGHT_SENSOR
	lightMeter.begin(BH1750::CONTINUOUS_HIGH_RES_MODE, 0x23, &Wire1);
	isNightMode = false;
	delay(3000);
#endif // SUPPORT_LIGHT_SENSOR

#if SUPPORT_TEMP_HUMIDITY_SENSOR

#if SUPPORT_DHT11 || SUPPORT_DHT22
	dht.begin();
#endif // SUPPORT_DHT11 || SUPPORT_DHT22

#if SUPPORT_SHT2X
	sht20.begin(SHT20_RESOLUTION_12BITS, SHT20_I2C, Wire1);
#endif // SUPPORT_SHT2X
#endif // SUPPORT_TEMP_HUMIDITY_SENSOR

#endif // SUPPORT_ENVIRONMENT_SENSOR

#if SUPPORT_WATCH_DOG
	printf("Set Watchdog timeout to %d seconds\r\n", (WATCHDOG_TIMEOUT/1000));
	// setup 120s watchdog
    wdt.init(WATCHDOG_TIMEOUT);

	wdt.init_irq((wdt_irq_handler)watchdog_irq_handler, 0);

	// enable watchdog timer before cloud_reconnect
    wdt.start();
#endif // SUPPORT_WATCH_DOG

    save_log_msg("the device is power-up");

	//reexecSch = current_system_setting.isExecSch;

	// wait for the peripheral interfaces to get ready
	delay(3000);
}

// Summary:
//	Print the menu
void printMenuRow(WiFiClient& client) {
	client.print("<tr rowspan='2'><td colspan='2'><a href='set_wifi.htm'>WiFi Settings</a>&nbsp;&nbsp;&nbsp;<a href='set_schedule.htm'>Schedule Settings</a>&nbsp;&nbsp;&nbsp;<a href='set_watering.htm'>Watering Settings</a>");
	client.print("&nbsp;&nbsp;&nbsp;<a href='system_settings.htm'>System Settings</a>&nbsp;&nbsp;&nbsp;<a href='system_logs.htm'>System Logs</a>");
#if SUPPORT_CAMERA
	client.print("&nbsp;&nbsp;&nbsp;<a href='video.htm'>Video</a>");
#endif // SUPPORT_CAMERA
	client.println("</td></tr>");
	client.print("<tr><td width='15%' align='right' nowrap>Firmware :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;");
	client.print("v" + String(FW_VER) + ", " + String(FW_DATE));
	client.println("</td></tr>");
	client.print("<tr><td width='15%' align='right' nowrap>Current Time :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;");
	getCurrentTimeStr();
	client.print(current_time_str);
	client.println("</td></tr>");
}

// Summary:
//	Print the log messages
void printLogMsg(WiFiClient& client) {
	int start_index = 0;
	int cnt = 1;
	int i;

	start_index = current_system_setting.log_index;

	for (i = (start_index - 1); i >= 0; i--) {
		client.println("<tr><td width='5%' align='center'>" + String(cnt++) + "</td>");
		client.println("<td width='30%' align='center'>" + String(current_system_setting.log[i].log_time) + "</td>");
		client.println("<td align='center'>" + String(current_system_setting.log[i].log_msg) + "</td></tr>");
	}

	if (current_system_setting.log_num == MAX_LOG_NUM){
		for (i = (MAX_LOG_NUM - 1); i >= start_index; i--) {
			client.println("<tr><td width='5%' align='center'>" + String(cnt++) + "</td>");
			client.println("<td width='30%' align='center'>" + String(current_system_setting.log[i].log_time) + "</td>");
			client.println("<td align='center'>" + String(current_system_setting.log[i].log_msg) + "</td></tr>");
		}
	}
}

#if SUPPORT_LIGHT_SENSOR
// Summary:
//	Retrieve the light level from the light sensor
uint16_t getLightLevel() {
	float lux = 0;

	// since opening/closing the solenoid valve will affect reading the light sensor
	// we must reset the I2C interface before reading values
	Wire1.end();

	if (lightMeter.measurementReady(true)) {
		lux = lightMeter.readLightLevel();

		/*
		if (lux > 0) {
			if (lux > 40000.0) {
				// reduce measurement time - needed in direct sun light
				if (lightMeter.setMTreg(32)) {
				  printf("Setting MTReg to low value for high light environment\r\n");
				} else {
				  printf("Error setting MTReg to low value for high light environment\r\n");
				}
			} else {
				if (lux > 10.0) {
					// typical light environment
					if (lightMeter.setMTreg(69)) {
						printf("Setting MTReg to default value for normal light environment\r\n");
					} else {
						printf("Error setting MTReg to default value for normal light environment\r\n");
					}
				} else if (lux <= 10.0) {
					// very low light environment
					if (lightMeter.setMTreg(138)) {
						printf("Setting MTReg to high value for low light environment\r\n");
					} else {
						printf("Error setting MTReg to high value for low light environment\r\n");
					}
				}
			}
		}
		*/
	}

	//return lux + light_offset;
	return lux;
}
#endif // SUPPORT_LIGHT_SENSOR

// Summary:
//	Run the uncompleted schedule
void run_uncompleted_schedule() {
	unsigned long seconds = timeClient.getEpochTime();
	int sch_index = current_system_setting.sch_index;
	int waiting_time = current_system_setting.schedule[sch_index].waiting_time;
	int draining_time = current_system_setting.schedule[sch_index].draining_time;

	printCurrentTime();
	printf("Restart schedule\r\n");

	if (!current_system_setting.isPumpProcCompleted) {
		if (digitalRead(PUMP_GPIO_PIN) == 0) {
#if SUPPORT_WATER_LEVEL_SENSOR
			// save the water level before pumping
			detect_water_level(true);
#endif // SUPPORT_WATER_LEVEL_SENSOR

			start_pumping();
		}

		// check the water level
		checkWaterLevel();
		return;
	} else if (!current_system_setting.isWaitingProcCompleted) {
		printCurrentTime();
		printf("Wait for watering\r\n");

		// if DUT didn't wait for watering in the previous schedule
		if (isOpenSV) {
			if ((seconds - open_sv_time) >= waiting_time){
				open_solenoid_valve();
			}
		} else if ((seconds - current_system_setting.startWaitingTime) >= waiting_time){
			open_solenoid_valve();
		}
	} else {
		printCurrentTime();
		printf("Wait for closing the solenoid valve\r\n");

		if (digitalRead(SV_GPIO_PIN) == 0) {
			open_solenoid_valve();
		} else if (!isCloseSV) {
			// if the solenoid valve has already been opened
			// we should restart to count the draining time
			isCloseSV = true;
			close_sv_time = seconds;
		}

		// if DUT didn't wait for draining in the previous schedule
		if (isCloseSV) {
			if ((seconds - close_sv_time) >= draining_time){
				complete_watering_process();
			}
		}
	}

	if (isPowerUp) {
		isPowerUp = false;

		printCurrentTime();
		printf("Power Up\r\n");

#if SUPPORT_NCU_CLOUD
		memset(powerup_payload, 0, sizeof(powerup_payload));
		snprintf(powerup_payload, sizeof(powerup_payload), reportPayload, NOTIFY_TYPE, "The device is power-up", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
	}

#if SUPPORT_NCU_CLOUD
	if ((millis() - last_check_ncu_task) >= MAX_CHECK_NUC_TASK_INTERVAL) {
		last_check_ncu_task = millis();
		check_ncu_tasks();
	}
#endif // SUPPORT_NCU_CLOUD
}

// Summary:
//	Scan the wireless network to check the remote AP has been readied for connection
//	if we find the AP, DUT needs to reboot itself to restart the wireless client
bool check_wifi_network() {
	char scan_ssid[MAX_SSID_LEN + 1];
	int num = 0;
	bool result = false;

	if ((strlen(current_system_setting.ssid) > 0) && (strlen(current_system_setting.key) > 0)) {
		num = WiFi.scanNetworks();

		printf("Scan Network...\r\n");

		if (num > 0) {
			for (int i = 0; i < num; i++) {
				memset(scan_ssid, 0, sizeof(scan_ssid));
				snprintf(scan_ssid, sizeof(scan_ssid), "%s", WiFi.SSID(i));

				if (strlen(scan_ssid) > 0) {
					if (memcmp(current_system_setting.ssid, scan_ssid, strlen(scan_ssid)) == 0) {
						result = true;
						printf("Find AP(%s)...\r\n", scan_ssid);
					}
				}
			}
		}
	}

	return result;
}

// Summary:
//   Convert a URL encoded time string (hh%3Amm) to an integer number (total minutes)
//   Example: "07%3A35" -> 455 (7*60 + 35)
int convert_time_str_to_int(String src) {
	int hour = src.substring(0, 2).toInt();
	int minute = src.substring(5, 7).toInt();

	return (60 * hour) + minute;
}

// Summary:
//   Convert an integer number (total minutes) to a time string (hh:mm)
//   Example: 455 -> "07:35"
String convert_int_to_time_str(int time_int) {
	int hour = time_int / 60;
	int minute = time_int % 60;
    String hourStr = (hour < 10 ? "0" : "") + String(hour);
    String minuteStr = (minute < 10 ? "0" : "") + String(minute);

    return hourStr + ":" + minuteStr;
}

void loop() {
	WiFiClient client = server.available();     // listen for incoming clients
	String currentLine = "";                // make a String to hold incoming data from the client
	String dataLine = "";
	String contentLength = "";
	bool isGet = false;
	bool isPost = false;
	bool isDataLine = false;
	bool isFirstLine = true;
	bool isSetWiFi = false;
	bool isSetSchedule = false;
	bool isChangeSchedule = false;
	bool isSetWatering = false;
	bool isResetDays = false;
	bool isFactoryDefault = false;
	bool isResetSession = false;
	bool isPumpCtrl = false;
	bool isSVCtrl = false;
	bool isSettings = false;
	bool isSetOTAServer = false;
	bool isStartOTA = false;

#if SUPPORT_SOIL_MOISTURE_SENSOR
	bool isSetSensorOffset = false;
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

	bool showSetWiFi = false;
	bool showSetSchedule = false;
	bool showSetWatering = false;
	bool showSetSys = false;
	bool showSysLogs = false;
	unsigned int data_length = 0;
	long long seconds = timeClient.getEpochTime();

	bool enable_sch = current_system_setting.enable_sch;
	bool watering_in_firstday = current_system_setting.watering_in_firstday;
	int working_day = current_system_setting.working_day;
	int watering_day = current_system_setting.watering_day;
	int max_check_wl_time = current_system_setting.max_check_wl_time;
	int watering_start_time = current_system_setting.watering_start_time;
	int watering_end_time = current_system_setting.watering_end_time;
	int sch_index = current_system_setting.sch_index;
	int watering_interval = current_system_setting.schedule[sch_index].watering_interval;
	int watering_time = current_system_setting.schedule[sch_index].watering_time;
	int waiting_time = current_system_setting.schedule[sch_index].waiting_time;
	int draining_time = current_system_setting.schedule[sch_index].draining_time;

	int watering_type = current_system_setting.watering_type;

#if SUPPORT_WATERING_MOISTURE_TYPE
	int moisture_content = current_system_setting.schedule[sch_index].moisture_content;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_FLOW_SENSOR
	int watering_volume = current_system_setting.schedule[sch_index].watering_volume;
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
	int watering_level = current_system_setting.schedule[sch_index].watering_level;
#endif // SUPPORT_WATER_LEVEL_SENSOR

#if SUPPORT_LED
	bool support_led = false;
	int led_start_time = 0;
	int led_end_time = 0;
#endif // SUPPORT_LED

#if SUPPORT_SOIL_MOISTURE_SENSOR
	int sensor_offset = current_system_setting.sensor_offset;
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

#if SUPPORT_CAMERA
	bool isRotateCamera = false;
	bool showVideo = false;

#if SUPPORT_NCU_CLOUD
	bool isSetPicTime = false;
#endif // SUPPORT_NCU_CLOUD
#endif // SUPPORT_CAMERA

#if SUPPORT_ENVIRONMENT_SENSOR
#if SUPPORT_LIGHT_SENSOR
	uint16_t lux = 0;
#endif // SUPPORT_LIGHT_SENSOR
#endif // SUPPORT_ENVIRONMENT_SENSOR

#if SUPPORT_WATCH_DOG
	if ((millis() - lastupdatewdt) >= MAX_UPDATE_WDT_INTERVAL) {
		printCurrentTime();
		printf("Refresh the watchdog timer\r\n");
		lastupdatewdt = millis();
		wdt.refresh();
	}
#endif // SUPPORT_WATCH_DOG

	if (isWatering) {
		if ((millis() - lastcheckwltime) >= MAX_CHECK_WL_INTERVAL) {
			lastcheckwltime = millis();
			checkWaterLevel();
		}
	} else {

#if SUPPORT_ENVIRONMENT_SENSOR
		if ((millis() - last_reading_time) >= MINIMUM_READING_TIME) {
			last_reading_time = millis();
			read_environment_sensor();
		}
#endif // SUPPORT_ENVIRONMENT_SENSOR

#if SUPPORT_CAMERA
		if ((millis() - last_check_save_image) >= MAX_CHECK_SAVE_IMAGE_INTERVAL) {
			last_check_save_image = millis();
			save_image_to_local();
		}
#endif // SUPPORT_CAMERA

#if SUPPORT_NCU_CLOUD
		if (!isAPMode) {

			if (!internetState) {
				if ((millis() - last_check_internet_state) >= MAX_CHECK_INTERNET_STATE_INTERVAL) {
					last_check_internet_state = millis();
					internetState = checkInternetStatus();
				}
			}

			if (internetState) {
				// when DUT is doing the watering process, DO NOT send any mqtt message to broker
				// in case of the mqtt connection stuck when the internet is unstable
				if (!disablePublish) {
					if ((millis() - last_check_ncu_task) >= MAX_CHECK_NUC_TASK_INTERVAL) {
						last_check_ncu_task = millis();
						check_ncu_tasks();
					}
				}
			}
		}
#endif // SUPPORT_NCU_CLOUD

#if SUPPORT_ENVIRONMENT_SENSOR
#if SUPPORT_LIGHT_SENSOR
		if ((seconds - last_check_light) >= CHECK_LIGHT_INTERVAL) {
			last_check_light = seconds;
			lux = getLightLevel();

			printCurrentTime();
			printf("the current lux is %d\r\n", lux);

#if SUPPORT_CAMERA
			if ((!isNightMode) && (lux <= MIN_LUX_VALUE)) {
				printCurrentTime();
				printf("the current lux is %d, night mode is enabled\r\n", lux);
				isNightMode = true;
				configCam.setDayNightMode(1);
			} else if ((isNightMode) && (lux >= (MIN_LUX_VALUE + DAY_MODE_OFFSET))) {
				printCurrentTime();
				printf("the current lux is %d, day mode is enabled\r\n", lux);
				isNightMode = false;
				configCam.setDayNightMode(0);
			}
#endif // SUPPORT_CAMERA
		}
#endif // SUPPORT_LIGHT_SENSOR
#endif // SUPPORT_ENVIRONMENT_SENSOR

		/*
		// if DUT needs to execute the schedule one more time
		if (reexecSch && !isAPMode) {
			run_uncompleted_schedule();
			return;
		}
		*/

		if (isOpenSV) {
			if ((seconds - open_sv_time) >= waiting_time){
				open_solenoid_valve();
			}
		}

		if (isCloseSV) {
			if ((seconds - close_sv_time) >= draining_time){
				complete_watering_process();
			}
		}

		if (client) {
			while (client.connected()) {          // loop while the client's connected
				if (client.available()) {           // if there's bytes to read from the client,
					char c = client.read();           // read a byte, then
					//Serial.write(c);                // print it out the serial monitor
					if (c == '\n') {                  // if the byte is a newline character
						// if the current line is blank, you got two newline characters in a row.
						// that's the end of the client HTTP request, so send a response:
						if (currentLine.length() == 0) {
							if (isPost) {
								isDataLine = true;
							} else {
								// break out of the while loop
								break;
							}
						} else {    // if you got a newline, then clear currentLine:
							if (isFirstLine) {
								if (currentLine.startsWith("GET")) {
									isGet = true;

									// retrieve the current schedule index
									// when accessing the WEB page is not the Schedule Settings page
									if (currentLine.indexOf(SET_SCHEDULE_HTM) == -1) {
										tmp_sch_index = sch_index;
									}

									if (currentLine.indexOf(SET_WIFI_HTM) != -1) {
										showSetWiFi = true;
									} else if (currentLine.indexOf(SET_SCHEDULE_HTM) != -1) {
										showSetSchedule = true;
#if SUPPORT_FLOW_SENSOR
										watering_volume = current_system_setting.schedule[tmp_sch_index].watering_volume;
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
										watering_level = current_system_setting.schedule[tmp_sch_index].watering_level;
#endif // SUPPORT_WATER_LEVEL_SENSOR

										watering_interval = current_system_setting.schedule[tmp_sch_index].watering_interval;
										watering_time = current_system_setting.schedule[tmp_sch_index].watering_time;
										waiting_time = current_system_setting.schedule[tmp_sch_index].waiting_time;
										draining_time = current_system_setting.schedule[tmp_sch_index].draining_time;

#if SUPPORT_WATERING_MOISTURE_TYPE
										moisture_content = current_system_setting.schedule[tmp_sch_index].moisture_content;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_LED
										support_led = current_system_setting.schedule[tmp_sch_index].support_led;
										led_start_time = current_system_setting.schedule[tmp_sch_index].led_start_time;
										led_end_time = current_system_setting.schedule[tmp_sch_index].led_end_time;
#endif // SUPPORT_LED
									} else if (currentLine.indexOf(SET_WATERING_HTM) != -1) {
										showSetWatering = true;
									} else if (currentLine.indexOf(SYSTEM_SETTINGS_HTM) != -1){
										showSetSys = true;
									} else if (currentLine.indexOf(SYSTEM_LOGS_HTM) != -1){
										showSysLogs = true;
#if SUPPORT_CAMERA
									} else if (currentLine.indexOf(VIDEO_HTM) != -1){
										showVideo = true;
#endif // SUPPORT_CAMERA
									} else {
										showSetWiFi = true;
									}
								} else if (currentLine.startsWith("POST")) {
									isPost = true;
									isSettings = true;

									if (currentLine.indexOf(SET_WIFI_CGI) != -1) {
										isSetWiFi = true;
										showSetWiFi = true;
									} else if (currentLine.indexOf(SET_SCHEDULE_CGI) != -1) {
										isSetSchedule = true;
										showSetSchedule = true;
									} else if (currentLine.indexOf(CHANGE_SCHEDULE_CGI) != -1) {
										isChangeSchedule = true;
										showSetSchedule = true;
										isSettings = false;
									} else if (currentLine.indexOf(SET_WATERING_CGI) != -1) {
										isSetWatering = true;
										showSetWatering = true;
									} else if (currentLine.indexOf(RESET_DAYS_CGI) != -1) {
										working_day = 0;
										watering_day = 0;
										isResetDays = true;
										showSetWatering = true;
										// break out of the while loop
										break;
									} else if (currentLine.indexOf(FACTORY_RESET_CGI) != -1) {
										isFactoryDefault = true;
										showSetSys = true;
										// break out of the while loop
										break;
									} else if (currentLine.indexOf(RESET_SESSIOIN_CGI) != -1) {
										isResetSession = true;
										showSetSys = true;
										// break out of the while loop
										break;
									} else if (currentLine.indexOf(PUMP_CTRL_CGI) != -1) {
										isPumpCtrl = true;
										showSetSys = true;
										isSettings = false;
									} else if (currentLine.indexOf(SV_CTRL_CGI) != -1) {
										isSVCtrl = true;
										showSetSys = true;
										isSettings = false;
									} else if (currentLine.indexOf(SET_OTA_SERVER_CGI) != -1) {
										isSetOTAServer = true;
										showSetSys = true;
									} else if (currentLine.indexOf(START_OTA_CGI) != -1) {
										isStartOTA = true;
										showSetSys = true;
										// break out of the while loop
										break;
									}
#if SUPPORT_SOIL_MOISTURE_SENSOR
									else if (currentLine.indexOf(SET_SENSOR_OFFSET_CGI) != -1) {
										isSetSensorOffset = true;
										showSetSys = true;
									}
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

#if SUPPORT_CAMERA
									else if (currentLine.indexOf(ROTATE_CAMERA_CGI) != -1) {
										isRotateCamera = true;
										showVideo = true;
									}
#if SUPPORT_NCU_CLOUD
									else if (currentLine.indexOf(PICTURE_TIME_CGI) != -1) {
										isSetPicTime = true;
										showSetSys = true;
									}
#endif // SUPPORT_NCU_CLOUD

#endif // SUPPORT_CAMERA
								}

								isFirstLine = false;
							}

							if (currentLine.startsWith("Content-Length")) {
								contentLength = currentLine.substring(currentLine.lastIndexOf(' ') + 1);
								data_length = contentLength.toInt();
							}

							currentLine = "";
						}
					} else if (c != '\r') {  // if you got anything else but a carriage return character,
						if (isPost && isDataLine) {
							dataLine += c;
							if ((data_length > 0) && (data_length == dataLine.length())) {
								break;
							}
						} else {
							currentLine += c;      // add it to the end of the currentLine
						}
					}
				}
			}

			// if we have the POST data
			if (dataLine.length() > 0) {
				para_counter = 0;
				for (unsigned int i = 0; i < dataLine.length(); ) {
					int index1 = dataLine.indexOf('=', i);
					int index2 = dataLine.indexOf('&', i+1);
					unsigned int name_len = 0;
					unsigned int val_len = 0;

					if (index1 != -1) {
						name_len = (index1 - i) + 1;
						dataLine.toCharArray(&para[para_counter].name[0], name_len, i);

						if (index2 != -1) {
							val_len = index2 - index1;
							i = index2 + 1;
						} else {
							val_len = dataLine.length() - index1;
							i = dataLine.length();
						}

						dataLine.toCharArray(&para[para_counter].value[0], val_len, (index1 + 1));
						para_counter++;
					}
				}

				for (int i = 0; i < para_counter; i++) {

					if (isSetWiFi) {
						if (memcmp(para[i].name, SSID_PARA, strlen(SSID_PARA)) == 0) {
							snprintf(current_system_setting.ssid, MAX_SSID_LEN+1, "%s", para[i].value);
						} else if (memcmp(para[i].name, KEY_PARA, strlen(KEY_PARA)) == 0) {
							snprintf(current_system_setting.key, MAX_KEY_LEN+1, "%s", para[i].value);
						}
					} else if (isSetSchedule) {
						if (memcmp(para[i].name, TMP_SCH_INDEX_PARA, strlen(TMP_SCH_INDEX_PARA)) == 0) {
							tmp_sch_index = atoi(para[i].value);
#if SUPPORT_FLOW_SENSOR
						} else if (memcmp(para[i].name, WATERING_VOLUME_PARA, strlen(WATERING_VOLUME_PARA)) == 0) {
							watering_volume = atoi(para[i].value);
#endif // SUPPORT_FLOW_SENSOR
#if SUPPORT_WATER_LEVEL_SENSOR
						} else if (memcmp(para[i].name, WATERING_LEVEL_PARA, strlen(WATERING_LEVEL_PARA)) == 0) {
							watering_level = atoi(para[i].value);
#endif // SUPPORT_WATER_LEVEL_SENSOR
						} else if (memcmp(para[i].name, WATERING_INTERVAL_PARA, strlen(WATERING_INTERVAL_PARA)) == 0) {
							watering_interval = atoi(para[i].value);
						} else if (memcmp(para[i].name, WATERING_TIME_PARA, strlen(WATERING_TIME_PARA)) == 0) {
							watering_time = convert_time_str_to_int(para[i].value);
						} else if (memcmp(para[i].name, WAITING_TIME_PARA, strlen(WAITING_TIME_PARA)) == 0) {
							waiting_time = atoi(para[i].value);
						} else if (memcmp(para[i].name, DRAINING_TIME_PARA, strlen(DRAINING_TIME_PARA)) == 0) {
							draining_time = atoi(para[i].value);
						}
#if SUPPORT_WATERING_MOISTURE_TYPE
						else if (memcmp(para[i].name, MOISTURE_CONTENT_PARA, strlen(MOISTURE_CONTENT_PARA)) == 0) {
							moisture_content = atoi(para[i].value);
						}
#endif // SUPPORT_WATERING_MOISTURE_TYPE
#if SUPPORT_LED
						else if (memcmp(para[i].name, SUPPORT_LED_PARA, strlen(SUPPORT_LED_PARA)) == 0) {
							support_led = (atoi(para[i].value) == 1) ? true:false;
						} else if (memcmp(para[i].name, LED_START_TIME_PARA, strlen(LED_START_TIME_PARA)) == 0) {
							led_start_time = convert_time_str_to_int(para[i].value);
						} else if (memcmp(para[i].name, LED_END_TIME_PARA, strlen(LED_END_TIME_PARA)) == 0) {
							led_end_time = convert_time_str_to_int(para[i].value);
						}
#endif // SUPPORT_LED
					} else if (isChangeSchedule) {
						if (memcmp(para[i].name, TMP_SCH_INDEX_PARA, strlen(TMP_SCH_INDEX_PARA)) == 0) {
							tmp_sch_index = atoi(para[i].value);

#if SUPPORT_FLOW_SENSOR
							watering_volume = current_system_setting.schedule[tmp_sch_index].watering_volume;
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
							watering_level = current_system_setting.schedule[tmp_sch_index].watering_level;
#endif // SUPPORT_WATER_LEVEL_SENSOR

							watering_interval = current_system_setting.schedule[tmp_sch_index].watering_interval;
							watering_time = current_system_setting.schedule[tmp_sch_index].watering_time;
							waiting_time = current_system_setting.schedule[tmp_sch_index].waiting_time;
							draining_time = current_system_setting.schedule[tmp_sch_index].draining_time;

#if SUPPORT_WATERING_MOISTURE_TYPE
							moisture_content = current_system_setting.schedule[tmp_sch_index].moisture_content;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_LED
							support_led = current_system_setting.schedule[tmp_sch_index].support_led;
							led_start_time = current_system_setting.schedule[tmp_sch_index].led_start_time;
							led_end_time = current_system_setting.schedule[tmp_sch_index].led_end_time;
#endif // SUPPORT_LED
							break;
						}
					} else if (isSetWatering) {
						if (memcmp(para[i].name, SEED_NAME_PARA, strlen(SEED_NAME_PARA)) == 0) {
							strcpy(current_system_setting.seed_name, para[i].value);
						} else if (memcmp(para[i].name, WATERING_IN_FIRSTDAY_PARA, strlen(WATERING_IN_FIRSTDAY_PARA)) == 0) {
							watering_in_firstday = (atoi(para[i].value) == 1) ? true:false;
						} else if (memcmp(para[i].name, WORKING_DAY_PARA, strlen(WORKING_DAY_PARA)) == 0) {
							working_day = atoi(para[i].value);
						} else if (memcmp(para[i].name, WATERING_DAY_PARA, strlen(WATERING_DAY_PARA)) == 0) {
							watering_day = atoi(para[i].value);
						} else if (memcmp(para[i].name, MAX_CHECK_WL_TIME_PARA, strlen(MAX_CHECK_WL_TIME_PARA)) == 0) {
							max_check_wl_time = atoi(para[i].value);
						} else if (memcmp(para[i].name, WATERING_START_TIME_PARA, strlen(WATERING_START_TIME_PARA)) == 0) {
							watering_start_time = convert_time_str_to_int(String(para[i].value));
						} else if (memcmp(para[i].name, WATERING_END_TIME_PARA, strlen(WATERING_END_TIME_PARA)) == 0) {
							watering_end_time = convert_time_str_to_int(String(para[i].value));
						} else if (memcmp(para[i].name, ENABLE_SCH_PARA, strlen(ENABLE_SCH_PARA)) == 0) {
							enable_sch = (atoi(para[i].value) == 1) ? true:false;
						} else if (memcmp(para[i].name, SCH_INDEX_PARA, strlen(SCH_INDEX_PARA)) == 0) {
							sch_index = atoi(para[i].value);
						}
#if SUPPORT_WATERING_MOISTURE_TYPE
						else if (memcmp(para[i].name, WATERING_TYPE_PARA, strlen(WATERING_TYPE_PARA)) == 0) {
							watering_type = atoi(para[i].value);
						}
#endif // SUPPORT_WATERING_MOISTURE_TYPE
					} else if (isPumpCtrl || isSVCtrl) {
						if (memcmp(para[i].name, ACTION_PARA, strlen(ACTION_PARA)) == 0) {

							if (atoi(para[i].value)) {
								if (isPumpCtrl) {
									digitalWrite(PUMP_GPIO_PIN, RELAY_ON);    // turn on the pump
									printf("Turn on the pump\r\n");
								} else {
									digitalWrite(SV_GPIO_PIN, RELAY_ON); // open the solenoid valve
									printf("Open the solenoid valve\r\n");
								}
							} else {
								if (isPumpCtrl) {
									digitalWrite(PUMP_GPIO_PIN, RELAY_OFF);    // turn off the pump
									printf("Turn off the pump\r\n");
								} else {
									digitalWrite(SV_GPIO_PIN, RELAY_OFF); // close the solenoid valve
									printf("Close the solenoid valve\r\n");
								}
							}
						}
					}
#if SUPPORT_SOIL_MOISTURE_SENSOR
					else if (isSetSensorOffset) {
						if (memcmp(para[i].name, SET_SENSOR_OFFSET_PARA, strlen(SET_SENSOR_OFFSET_PARA)) == 0) {
							sensor_offset = atoi(para[i].value);
						}
					}
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

#if SUPPORT_CAMERA
					else if (isRotateCamera) {
						current_system_setting.rotate = atoi(para[i].value);
						Camera.channelEnd(CHANNEL);
						initialize_camera();
					} else if (isSetPicTime) {
						if (memcmp(para[i].name, SAVE_IMAGE_TIME1_PARA, strlen(SAVE_IMAGE_TIME1_PARA)) == 0) {
							current_system_setting.save_image_time1 = convert_time_str_to_int(para[i].value);
						} else if (memcmp(para[i].name, SAVE_IMAGE_TIME2_PARA, strlen(SAVE_IMAGE_TIME2_PARA)) == 0) {
							current_system_setting.save_image_time2 = convert_time_str_to_int(para[i].value);
						}

						current_system_setting.save_image_flag1 = false;
						current_system_setting.save_image_flag2 = false;

#if SUPPORT_NCU_CLOUD
						current_system_setting.send_pic_flag1 = false;
						current_system_setting.send_pic_flag2 = false;
#endif // SUPPORT_NCU_CLOUD
					}
#endif // SUPPORT_CAMERA
					else if (isSetOTAServer) {
						if (memcmp(para[i].name, OTA_SERVER_PARA, strlen(OTA_SERVER_PARA)) == 0) {
							snprintf(current_system_setting.ota_server, MAX_IP_LEN+1, "%s", para[i].value);
						}
					}
				}
			}

			// HTTP headers always start with a response code (e.g. HTTP/1.1 200 OK)
			// and a content-type so the client knows what's coming, then a blank line:
			client.println("HTTP/1.1 200 OK");
			client.println("Content-type:text/html");
			client.println();

			// the content of the HTTP response follows the header:
			client.println("<!DOCTYPE html>");
			client.println("<html>");
			client.println("<head>");
			client.println("<meta charset='utf-8'>");
			client.println("<script type='text/javascript'>");

			if (showSetSchedule || showSetWatering || showSetSys) {
				client.println("        ");
				client.println("  function limitRange(el, min, max) {");
				client.println("  	el.value = el.value.replace(/[^0-9]/g, \"\");");
				client.println("        ");
				client.println("  	if (el.value == \"\") return;");
				client.println("        ");
				client.println("  	let v = Number(el.value);");
				client.println("        ");
				client.println("  	if (v > max) el.value = max;");
				client.println("  	if (v < min) el.value = min;");
				client.println("  }");
				client.println("        ");
				client.println(" function timeToMinutes(t) {");
				client.println("   const [h, m] = t.split(':').map(Number);");
				client.println("   return h * 60 + m;");
				client.println(" }");
			}

			if (showSetSchedule) {
				client.println("  function LoadScheduleSettings() {");
				client.println("    var tmp_sch_index = document.getElementById('tmp_sch_index');");

#if SUPPORT_FLOW_SENSOR
				client.println("    var watering_volume = document.getElementById('watering_volume');");
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
				client.println("    var watering_level = document.getElementById('watering_level');");
#endif // SUPPORT_WATER_LEVEL_SENSOR

				client.println("    var waiting_time = document.getElementById('waiting_time');");
				client.println("    var draining_time = document.getElementById('draining_time');");

				if (watering_type == WATERING_BY_TIME_TYPE) {
					client.println(" var watering_interval = document.getElementById('watering_interval');");
					client.println(" var watering_time = document.getElementById('watering_time');");
				}
#if SUPPORT_WATERING_MOISTURE_TYPE
				else {
					client.println(" var moisture_content = document.getElementById('moisture_content');");
				}
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_LED
				client.println("    var tmp_support_led = document.getElementById('tmp_support_led');");
				client.println("    var support_led = document.getElementById('support_led');");
				client.println("    var led_start_time = document.getElementById('led_start_time');");
				client.println("    var led_end_time = document.getElementById('led_end_time');");
#endif // SUPPORT_LED

				client.println("        ");
				client.println("    tmp_sch_index.selectedIndex = " + String(tmp_sch_index) + ";");

#if SUPPORT_FLOW_SENSOR
				client.println("    watering_volume.value='" + String(watering_volume) + "';");
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
				client.println("    watering_level.value='" + String(watering_level) + "';");
#endif // SUPPORT_WATER_LEVEL_SENSOR

				if (watering_type == WATERING_BY_TIME_TYPE) {
					client.println("watering_interval.value='" + String(watering_interval) + "';");
					client.println("watering_time.value='" + convert_int_to_time_str(watering_time) + "';");
				}
#if SUPPORT_WATERING_MOISTURE_TYPE
				else {
					client.println("moisture_content.value='" + String(moisture_content) + "';");
				}
#endif // SUPPORT_WATERING_MOISTURE_TYPE

				client.println("    waiting_time.value='" + String(waiting_time) + "';");
				client.println("    draining_time.value='" + String(draining_time) + "';");
				client.println("        ");
#if SUPPORT_LED
				client.print("      tmp_support_led.checked = ");
				client.print(support_led?"true":"false");
				client.println(";");
				client.println("    led_start_time.value='" + convert_int_to_time_str(led_start_time) + "';");
				client.println("    led_end_time.value='" + convert_int_to_time_str(led_end_time) + "';");
#endif // SUPPORT_LED

				client.println("  }");
				client.println("        ");
				client.println("  function ChangeSchedule() {");
				client.println("	var form1 = document.getElementById('form1');");
				client.println("        ");
				client.println("    form1.action = '" + String(CHANGE_SCHEDULE_CGI) + "';");
				client.println("    form1.submit();");
				client.println("        ");
				client.println("  }");
				client.println("        ");
				client.println("  function SetSchedule() {");
				client.println("    var form1 = document.getElementById('form1');");
#if SUPPORT_FLOW_SENSOR
				client.println("    var watering_volume = document.getElementById('watering_volume');");
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
				client.println("    var watering_level = document.getElementById('watering_level');");
#endif // SUPPORT_WATER_LEVEL_SENSOR

				if (watering_type == WATERING_BY_TIME_TYPE) {
					client.println("var watering_time = document.getElementById('watering_time');");
					client.println("var watering_interval = document.getElementById('watering_interval');");
					client.println("var watering_minutes = 0;");
				}
#if SUPPORT_WATERING_MOISTURE_TYPE
				else {
					client.println("var moisture_content = document.getElementById('moisture_content');");
				}
#endif // SUPPORT_WATERING_MOISTURE_TYPE

				client.println("    var waiting_time = document.getElementById('waiting_time');");
				client.println("    var draining_time = document.getElementById('draining_time');");

#if SUPPORT_LED
				client.println("    var tmp_support_led = document.getElementById('tmp_support_led');");
				client.println("    var support_led = document.getElementById('support_led');");
				client.println("    var led_start_time = document.getElementById('led_start_time');");
				client.println("    var led_end_time = document.getElementById('led_end_time');");
#endif // SUPPORT_LED

				client.println("        ");

#if SUPPORT_FLOW_SENSOR
				client.println("    if (watering_volume.value == ''){");
				client.println("        alert('The Watering Volume field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
				client.println("    if (watering_level.value == ''){");
				client.println("        alert('The Watering Level field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
#endif // SUPPORT_WATER_LEVEL_SENSOR

				client.println("        ");

				if (watering_type == WATERING_BY_TIME_TYPE) {
					client.println("	if (watering_interval.value == ''){");
					client.println("		alert('The Watering Interval field cannot be empty!');");
					client.println("		return;");
					client.println("	}");
					client.println("	");
					client.println("	if (watering_time.value == ''){");
					client.println("		alert('The Watering Time field cannot be empty!');");
					client.println("		return;");
					client.println("	}");
					client.println("	");
					client.println("	watering_minutes = timeToMinutes(watering_time.value);");
					client.print("		if ((watering_minutes < " + String(current_system_setting.watering_start_time) + ")");
					client.println("	 	|| (watering_minutes >= " + String(current_system_setting.watering_end_time) + ")) {");
					client.println("		alert('Watering time is out of range');");
					client.println("		return;");
					client.println("	}");
				}
#if SUPPORT_WATERING_MOISTURE_TYPE
				else {
					client.println("if (moisture_content.value == ''){");
					client.println("	alert('The Moisture Content field cannot be empty!');");
					client.println("	return;");
					client.println("}");
				}
#endif // SUPPORT_WATERING_MOISTURE_TYPE

				client.println("        ");
				client.println("    if (waiting_time.value == ''){");
				client.println("        alert('The Waiting Time field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
				client.println("        ");
				client.println("    if (draining_time.value == ''){");
				client.println("        alert('The Draining Time field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
				client.println("        ");
#if SUPPORT_LED
				client.println("    if (tmp_support_led.checked){");
				client.println("		support_led.value='1';");
				client.println("    } else {");
				client.println("		support_led.value='0';");
				client.println("    }");
				client.println("        ");
				client.println("	if (led_start_time.value == ''){");
				client.println("		alert('The LED Start Time field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("        ");
				client.println("	if (led_end_time.value == ''){");
				client.println("		alert('The LED End Time field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("        ");
#endif // SUPPORT_LED

				client.println("    form1.submit();");
				client.println("        ");
				client.println("  }");
				client.println("        ");
			}

			if (showSetWatering) {
				client.println("  function ResetDays() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    if (confirm('Would you like to reset Working Day and Watering Day?')) {");
				client.println("        form1.action = '" + String(RESET_DAYS_CGI) + "';");
				client.println("        form1.submit();");
				client.println("    }");
				client.println("  }");
				client.println("        ");
				client.println("  function LoadWateringSettings() {");
				client.println("    var tmp_firstday = document.getElementById('tmp_firstday');");
				client.println("    var seed_name = document.getElementById('seed_name');");
				client.println("    var watering_in_firstday = document.getElementById('watering_in_firstday');");
				client.println("    var working_day = document.getElementById('working_day');");
				client.println("    var watering_day = document.getElementById('watering_day');");
				client.println("    var max_check_wl_time = document.getElementById('max_check_wl_time');");
				client.println("    var watering_start_time = document.getElementById('watering_start_time');");
				client.println("    var watering_end_time = document.getElementById('watering_end_time');");

				client.println("    var tmp_enable_sch = document.getElementById('tmp_enable_sch');");
				client.println("    var enable_sch = document.getElementById('enable_sch');");
				client.println("    var sch_index = document.getElementById('sch_index');");
#if SUPPORT_WATERING_MOISTURE_TYPE
				client.println("    var watering_type = document.getElementsByName('watering_type');");
#endif // SUPPORT_WATERING_MOISTURE_TYPE
				client.println("        ");
				client.println("	seed_name.value ='" + String(current_system_setting.seed_name) + "';");
				client.print("      tmp_firstday.checked = ");
				client.print(watering_in_firstday?"true":"false");
				client.println(";");
				client.println("    working_day.value='" + String(working_day) + "';");
				client.println("    watering_day.value='" + String(watering_day) + "';");
				client.println("    max_check_wl_time.value='" + String(max_check_wl_time) + "';");
				client.println("    watering_start_time.value='" + convert_int_to_time_str(watering_start_time) + "';");
				client.println("    watering_end_time.value='" + convert_int_to_time_str(watering_end_time) + "';");
				client.print("      tmp_enable_sch.checked = ");
				client.print(enable_sch?"true":"false");
				client.println(";");
				client.println("    sch_index.selectedIndex = " + String(sch_index) + ";");
				client.println("        ");
#if SUPPORT_WATERING_MOISTURE_TYPE
				if (watering_type == WATERING_BY_TIME_TYPE) {
					client.println(" watering_type[0].checked = true;");
				} else {
					client.println(" watering_type[1].checked = true;");
				}
#endif // SUPPORT_WATERING_MOISTURE_TYPE
				client.println("        ");
				client.println("  }");
				client.println("        ");
				client.println("  function SetWatering() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var seed_name = document.getElementById('seed_name');");
				client.println("    var tmp_firstday = document.getElementById('tmp_firstday');");
				client.println("    var watering_in_firstday = document.getElementById('watering_in_firstday');");
				client.println("    var working_day = document.getElementById('working_day');");
				client.println("    var watering_day = document.getElementById('watering_day');");
				client.println("    var max_check_wl_time = document.getElementById('max_check_wl_time');");
				client.println("    var watering_start_time = document.getElementById('watering_start_time');");
				client.println("    var watering_end_time = document.getElementById('watering_end_time');");
				client.println("    var tmp_enable_sch = document.getElementById('tmp_enable_sch');");
				client.println("    var enable_sch = document.getElementById('enable_sch');");
				client.println("        ");
				client.println("    if (seed_name.value == ''){");
				client.println("        alert('The Seed Name field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
				client.println("        ");
				client.println("    if (tmp_firstday.checked){");
				client.println("      watering_in_firstday.value='1';");
				client.println("    } else {");
				client.println("      watering_in_firstday.value='0';");
				client.println("    }");
				client.println("        ");
				client.println("    if (working_day.value == ''){");
				client.println("        alert('The Working Day field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
				client.println("        ");
				client.println("    if (watering_day.value == ''){");
				client.println("        alert('The Watering Day field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
				client.println("        ");
				client.println("    if (max_check_wl_time.value == ''){");
				client.println("        alert('The Timeout field cannot be empty!');");
				client.println("        return;");
				client.println("    }");
				client.println("        ");
				client.println("	if (watering_start_time.value == ''){");
				client.println("		alert('The Watering Start Time field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("        ");
				client.println("	if (watering_end_time.value == ''){");
				client.println("		alert('The Watering End Time field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("        ");
				client.println("    if (tmp_enable_sch.checked){");
				client.println("    	enable_sch.value='1';");
				client.println("    } else {");
				client.println("    	enable_sch.value='0';");
				client.println("    }");
				client.println("        ");
				client.println("    form1.submit();");
				client.println("  }");
			}

			if (showSetSys){
				client.println("  function FactoryDefault() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    if (confirm('Would you like to reset to factory default?')) {");
				client.println("        form1.action = '" + String(FACTORY_RESET_CGI) + "';");
				client.println("        form1.submit();");
				client.println("    }");
				client.println("  }");
				client.println("        ");
				client.println("  function ResetSession() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    if (confirm('Would you like to reset the session?')) {");
				client.println("        form1.action = '" + String(RESET_SESSIOIN_CGI) + "';");
				client.println("        form1.submit();");
				client.println("    }");
				client.println("  }");
				client.println("        ");
				client.println("  function PumpControl(which_action) {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var action = document.getElementById('action');");
				client.println("		");
				client.println("        action.value = which_action;");
				client.println("        form1.action = '" + String(PUMP_CTRL_CGI) + "';");
				client.println("        form1.submit();");
				client.println("    	");
				client.println("  }");
				client.println("        ");
				client.println("  function SVControl(which_action) {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var action = document.getElementById('action');");
				client.println("		");
				client.println("        action.value = which_action;");
				client.println("        form1.action = '" + String(SV_CTRL_CGI) + "';");
				client.println("        form1.submit();");
				client.println("    	");
				client.println("  }");
				client.println("        ");
#if SUPPORT_SOIL_MOISTURE_SENSOR
				client.println("  function SetSensorOffset() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var sensor_offset = document.getElementById('sensor_offset');");
				client.println("		");
				client.println("	if (sensor_offset.value == ''){");
				client.println("		alert('The Sensor Offset field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("		");
				client.println("	form1.action = '" + String(SET_SENSOR_OFFSET_CGI) + "';");
				client.println("	form1.submit();");
				client.println("    	");
				client.println("  }");
#endif // SUPPORT_SOIL_MOISTURE_SENSOR
#if SUPPORT_CAMERA && SUPPORT_NCU_CLOUD
				client.println("  function SetPicTime() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var save_image_time1 = document.getElementById('save_image_time1');");
				client.println("    var save_image_time2 = document.getElementById('save_image_time2');");
				client.println("    	");
				client.println("	if (save_image_time1.value == ''){");
				client.println("		alert('The Picture Time 1 field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("	if (save_image_time2.value == ''){");
				client.println("		alert('The Picture Time 2 field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("    form1.action = '" + String(PICTURE_TIME_CGI) + "';");
				client.println("    form1.submit();");
				client.println("    	");
				client.println("  }");
#endif // SUPPORT_CAMERA && SUPPORT_NCU_CLOUD


				client.println("  function SetOTAServer() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var ota_server = document.getElementById('ota_server');");
				client.println("    ");
				client.println("    if (ota_server == '') {");
				client.println("		alert('The OTA Server field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("    ");
				client.println("    form1.action = '" + String(SET_OTA_SERVER_CGI) + "';");
				client.println("    form1.submit();");
				client.println("  	");
				client.println("  }");
				client.println("  	");
				client.println("  function StartOTAProcess() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("    var ota_server = document.getElementById('ota_server');");
				client.println("    ");
				client.println("    if (ota_server == '') {");
				client.println("		alert('The OTA Server field cannot be empty!');");
				client.println("		return;");
				client.println("	}");
				client.println("    ");
				client.println("    if (confirm('Would you like to start the OTA process?')) {");
				client.println("        form1.action = '" + String(START_OTA_CGI) + "';");
				client.println("        form1.submit();");
				client.println("    }");
				client.println("  }");
			}

#if SUPPORT_CAMERA
			if (showVideo) {
				client.println("  var time_id = -1");
				client.println("    	");
				client.println("  function LoadCameraSettings() {");
				client.println("    var rotate_index = document.getElementById('rotate');");
				client.println("        ");
				client.println("    rotate_index.selectedIndex = " + String(current_system_setting.rotate) + ";");
				client.println("  }");
				client.println("    	");
				client.println("  function ReloadPage() {");
				client.println("  	location.href='video.htm';");
				client.println("  }");
				client.println("    	");
				client.println("  function RotateCamera() {");
				client.println("    var form1 = document.getElementById('form1');");
				client.println("		");
				client.println("	if (time_id != -1) {");
				client.println("		clearInterval(time_id);");
				client.println("	}");
				client.println("    	");
				client.println("    form1.action = '" + String(ROTATE_CAMERA_CGI) + "';");
				client.println("    form1.submit();");
				client.println("    	");
				client.println("  }");
			}
#endif // SUPPORT_CAMERA

			client.println("</script>");
			client.println("</head>");

			// Body Section
			if (showSetSchedule) {
				client.println("<body onload='LoadScheduleSettings()'>");
			} else if (showSetWatering) {
				client.println("<body onload='LoadWateringSettings()'>");
#if SUPPORT_CAMERA
			} else if (showVideo) {
				client.println("<body onload='LoadCameraSettings()'>");
#endif // SUPPORT_CAMERA
			} else {
				client.println("<body>");
			}

			client.print("<form id='form1' name='form1' method='post' action='");
			if (showSetWiFi) {
				client.print(SET_WIFI_CGI);
			} else if (showSetSchedule) {
				client.print(SET_SCHEDULE_CGI);
			} else if (showSetWatering) {
				client.print(SET_WATERING_CGI);
			}
			client.println("'>");

			client.println("<table height='20%' border='0'>");
			printMenuRow(client);

			if (showSetWiFi) {
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>SSID :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='text' name='ssid' maxlength='32' value='" + String(current_system_setting.ssid) + "'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Password :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='password' name='key' maxlength='64' value='" + String(current_system_setting.key) + "'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%'>&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='submit' value='Apply' /></td>");
				client.println("</tr>");
			}

			if (showSetSchedule) {
				client.println("<tr>");
				client.print("<td width='15%' align='right' nowrap>Schedule :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<select id='tmp_sch_index' name='tmp_sch_index' onchange='ChangeSchedule()'>");
				for(int i = 0; i < MAX_SCHEDULE_NUM; i++){
					client.print("<option value='" + String(i) + "'>Schedule" + String(i+1) + "</option>");
				}
				client.print("</select>");
				client.println("</td>");
				client.println("</tr>");
				client.println("<tr>");
#if SUPPORT_FLOW_SENSOR
				client.println("<td width='15%' align='right' nowrap>Watering Volume :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='watering_volume' name='watering_volume' placeholder='0-99999' min='0' max='99999' oninput='limitRange(this, 0, 99999)' size='5' value='" + String(watering_volume) + "'>&nbsp;L</td>");
#endif // SUPPORT_FLOW_SENSOR
#if SUPPORT_WATER_LEVEL_SENSOR
				client.println("<td width='15%' align='right' nowrap>Watering Level :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='watering_level' name='watering_level' placeholder='1-20' min='1' max='20' oninput='limitRange(this, 1, 20)' size='3' size='3' value='" + String(watering_level) + "'>&nbsp;</td>");
#endif // SUPPORT_WATER_LEVEL_SENSOR

				if (watering_type == WATERING_BY_TIME_TYPE) {
					client.println("<tr>");
					client.println("<td width='15%' align='right' nowrap>Watering Interval :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='watering_interval' name='watering_interval' placeholder='1-99' min='1' max='99' oninput='limitRange(this, 1, 99)' size='3' value='" + String(watering_interval) + "'>&nbsp;Days</td>");
					client.println("</tr>");
					client.println("<tr>");
					client.println("<td width='15%' align='right' nowrap>Watering Time :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='time' id='watering_time' name='watering_time'></td>");
					client.println("</tr>");
				}
#if SUPPORT_WATERING_MOISTURE_TYPE
				else {
					client.println("</tr>");
					client.println("<td width='15%' align='right' nowrap>Moisture Content :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='moisture_content' name='moisture_content' placeholder='0-999' min='0' max='999' oninput='limitRange(this, 0, 999)' size='4' value='" + String(moisture_content) + "'></td>");
					client.println("</tr>");
				}
#endif // SUPPORT_WATERING_MOISTURE_TYPE

				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Waiting Time :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='waiting_time' name='waiting_time' placeholder='0-9999' min='0' max='9999' oninput='limitRange(this, 0, 9999)' size='5' value='" + String(waiting_time) + "'>&nbsp;Seconds</td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Draining Time :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='draining_time' name='draining_time' placeholder='0-9999' min='0' max='9999' oninput='limitRange(this, 0, 9999)' size='5' value='" + String(draining_time) + "'>&nbsp;Seconds</td>");
				client.println("</tr>");

#if SUPPORT_LED
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Support LED :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='checkbox' id='tmp_support_led' onclick='display_led_section()'><input type='hidden' id='support_led' name='support_led' value='1'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>LED Time Range:&nbsp;</td>");
				client.println("<td>&nbsp;&nbsp;&nbsp;Start:&nbsp;<input type='time' id='led_start_time' name='led_start_time'>");
				client.println("&nbsp;End:&nbsp;<input type='time' id='led_end_time' name='led_end_time'>");
				client.println("</td>");
				client.println("</tr>");
#endif // SUPPORT_LED

				client.println("<tr>");
				client.println("<td width='15%'>&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='button' value='Apply' onclick='SetSchedule()' /></td>");
				client.println("</tr>");
			}

			if (showSetWatering) {
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Seeds Name :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='text' id='seed_name' name='seed_name' maxlength='50' size='10' value=''></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.print("<td width='15%' align='right' nowrap>Start Day :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;");
				getStartDayStr();
				client.print(start_day_str);
				client.println("</td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Watering in First Day :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='checkbox' id='tmp_firstday'><input type='hidden' id='watering_in_firstday' name='watering_in_firstday' value='1'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.print("<td width='15%' align='right' nowrap>Working Day :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='working_day' name='working_day' placeholder='0-99' min='0' max='99' oninput='limitRange(this, 0, 99)' size='3' value=''>&nbsp;");
				client.print((working_day > 1) ? "Days":"Day");
				client.println("</td>");
				client.println("</tr>");
				client.println("<tr>");
				client.print("<td width='15%' align='right' nowrap>Watering Day :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='watering_day' name='watering_day' placeholder='0-99' min='0' max='99' oninput='limitRange(this, 0, 99)' size='3' value=''>&nbsp;");
				client.print((watering_day > 1) ? "Days":"Day");
				client.print("&nbsp;&nbsp;&nbsp;<input type='button' value='Reset Days' onClick='ResetDays()'>");
				client.println("</td>");
				client.println("</tr>");
				client.println("<tr>");
				client.print("<td width='15%' align='right' nowrap>No-rise Timeout :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='number' id='max_check_wl_time' name='max_check_wl_time' placeholder='0-3600' min='0' max='3600' oninput='limitRange(this, 0, 3600)' size='5'>&nbsp;seconds&nbsp;");
				client.print("<span class='hint'>(Stop pumping if water level doesn’t rise)</span>");
				client.println("</td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Watering Time Range:&nbsp;</td>");
				client.println("<td>&nbsp;&nbsp;&nbsp;Start:&nbsp;<input type='time' id='watering_start_time' name='watering_start_time'>");
				client.println("&nbsp;End:&nbsp;<input type='time' id='watering_end_time' name='watering_end_time'>");
				client.println("</td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Enable Schedule :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='checkbox' id='tmp_enable_sch'><input type='hidden' id='enable_sch' name='enable_sch' value='1'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.print("<td width='15%' align='right' nowrap>Schedule :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<select id='sch_index' name='sch_index'>");
				for(int i = 0; i < MAX_SCHEDULE_NUM; i++){
					client.print("<option value='" + String(i) + "'>Schedule" + String(i+1) + "</option>");
				}
				client.print("</select>");
				client.println("</td>");
				client.println("</tr>");
#if SUPPORT_WATERING_MOISTURE_TYPE
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Watering Type :&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='radio' name='watering_type' value='0'>&nbsp;Time<input type='radio' name='watering_type' value='1'>&nbsp;Moisture</td>");
				client.println("</tr>");
#endif // SUPPORT_WATERING_MOISTURE_TYPE
				client.println("<tr>");
				client.println("<td width='15%'>&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='button' value='Apply' onclick='SetWatering()' /></td>");
				client.println("</tr>");
			}

			if (showSetSys) {
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Factory Default :&nbsp;</td><td><input type='button' value='Reset' onClick='FactoryDefault()'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Reset Session :&nbsp;</td><td><input type='button' value='Reset' onClick='ResetSession()'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Pump Control :&nbsp;</td><td><input type='button' value='ON' onClick='PumpControl(1)'>&nbsp;&nbsp;&nbsp;<input type='button' value='OFF' onClick='PumpControl(0)'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Solenoid Valve Control :&nbsp;</td><td><input type='button' value='ON' onClick='SVControl(1)'>&nbsp;&nbsp;&nbsp;<input type='button' value='OFF' onClick='SVControl(0)'></td>");
				client.println("</tr>");
				client.println("<input type='hidden' id='action' name='action'>");
#if SUPPORT_SOIL_MOISTURE_SENSOR
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Sensor Offset :&nbsp;&nbsp;&nbsp;</td>");
				client.println("<td>");
				client.println("<input type='number' id='sensor_offset' name='sensor_offset' placeholder='0-999' min='0' max='999' oninput='limitRange(this, 0, 999)' size='4' value='" + String(sensor_offset) + "'>&nbsp;&nbsp;");
				client.println("<input type='button' value='Apply' onClick='SetSensorOffset()'></td>");
				client.println("</tr>");
#endif // SUPPORT_SOIL_MOISTURE_SENSOR
#if SUPPORT_CAMERA && SUPPORT_NCU_CLOUD
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Picture Time 1:&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='time' id='save_image_time1' name='save_image_time1' value='" + convert_int_to_time_str(current_system_setting.save_image_time1) + "'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Picture Time 2:&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='time' id='save_image_time2' name='save_image_time2' value='" + convert_int_to_time_str(current_system_setting.save_image_time2) + "'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%'>&nbsp;</td><td>&nbsp;&nbsp;&nbsp;<input type='button' value='Apply' onclick='SetPicTime()' /></td>");
				client.println("</tr>");
#endif // SUPPORT_CAMERA && SUPPORT_NCU_CLOUD
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>OTA Server :&nbsp;&nbsp;&nbsp;</td>");
				client.println("<td>");
				client.println("<input type='text' id='ota_server' name='ota_server' maxlength='15' value='" + String(current_system_setting.ota_server) + "'>&nbsp;&nbsp;");
				client.println("<input type='button' value='Apply' onClick='SetOTAServer()'></td>");
				client.println("</tr>");
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>&nbsp;&nbsp;&nbsp;</td>");
				client.println("<td><input type='button' value='Start OTA' onClick='StartOTAProcess()'></td>");
				client.println("</tr>");
			}

			if (showSysLogs) {
				client.println("<tr><td colspan='2'>");
				client.println("<table width='100%' border='1'>");
				client.println("<tr><td width='5%' align='center'>&nbsp;</td><td width='30%' align='center'>Time</td><td align='center'>Description</td></tr>");
				printLogMsg(client);
				client.println("</table>");
				client.println("</td></tr>");
			}

#if SUPPORT_CAMERA
			if (showVideo) {
				client.println("<tr>");
				client.println("<td width='15%' align='right' nowrap>Rotate Camera :&nbsp;</td>");
				client.println("<td>&nbsp;&nbsp;&nbsp;<select id='rotate' name='rotate' onchange='RotateCamera()'>");
				client.println("<option value='0'>0</option>");
				client.println("<option value='1'>90</option>");
				client.println("<option value='2'>-90</option>");
				client.println("<option value='3'>180</option>");
				client.print("</select>");
				client.println("</td></tr>");
				client.println("<tr><td colspan='2'>");
				Camera.getImage(CHANNEL, &img_addr, &img_len);
				encoding_jpg();

				if (encodedData !=  NULL) {
					client.println("<img width='800' height='480' src='data:image/png;base64," + String(encodedData) + "' />");
					free(encodedData);
				}

				client.println("<script>time_id = setInterval('ReloadPage()', 8000);</script>");
				client.println("</td></tr>");
			}
#endif // SUPPORT_CAMERA

			client.println("</table>");
			client.println("</form>");

			if (isSetWiFi) {
				client.println("<script>location.href='set_wifi.htm';</script>");
			} else if (isSetSchedule || isChangeSchedule) {
				client.println("<script>location.href='set_schedule.htm';</script>");
			} else if (isResetDays || isSetWatering) {
				client.println("<script>location.href='set_watering.htm';</script>");
			} else if (isFactoryDefault || isResetSession || isPumpCtrl || isSVCtrl || isStartOTA) {
				client.println("<script>location.href='system_settings.htm';</script>");
			}
#if SUPPORT_SOIL_MOISTURE_SENSOR
			else if (isSetSensorOffset) {
				client.println("<script>location.href='system_settings.htm';</script>");
			}
#endif // SUPPORT_SOIL_MOISTURE_SENSOR
#if SUPPORT_CAMERA
			else if (isRotateCamera) {
				client.println("<script>location.href='video.htm';</script>");
			}
#if SUPPORT_NCU_CLOUD
			else if (isSetPicTime) {
				client.println("<script>location.href='system_settings.htm';</script>");
			}
#endif // SUPPORT_NCU_CLOUD
#endif // SUPPORT_CAMERA

			client.println("</body>");
			client.println("</html>");
			// The HTTP response ends with another blank line
			client.println();

			// we must clear the remained data which hasn't been read in the POST action
			// otherwise, the response page won't display
			client.flush();

			// close the connection
			client.stop();

			if (isSetSchedule || isSetWatering || isResetDays || isResetSession) {

				isExecuted = false;
				isWatering = false;

				if (isSetSchedule) {
#if DEBUG
					printf("tmp_sch_index: %d\r\n", tmp_sch_index);
					printf("watering_level: %d\r\n", watering_level);
					printf("watering_interval: %d\r\n", watering_interval);
					printf("watering_time: %d\r\n", watering_time);

#if SUPPORT_WATERING_MOISTURE_TYPE
					printf("moisture_content: %d\r\n", moisture_content);
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_LED
					printf("support_led: %s\r\n", support_led?"Yes":"No");
					printf("led_start_time: %d\r\n", led_start_time);
					printf("led_end_time: %d\r\n", led_end_time);
#endif // SUPPORT_LED
#endif // DEBUG

#if SUPPORT_FLOW_SENSOR
					current_system_setting.schedule[tmp_sch_index].watering_volume = watering_volume;
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
					current_system_setting.schedule[tmp_sch_index].watering_level = watering_level;
#endif // SUPPORT_FLOW_SENSOR

					current_system_setting.schedule[tmp_sch_index].watering_interval = watering_interval;
					current_system_setting.schedule[tmp_sch_index].watering_time = watering_time;
					current_system_setting.schedule[tmp_sch_index].waiting_time = waiting_time;
					current_system_setting.schedule[tmp_sch_index].draining_time = draining_time;

#if SUPPORT_WATERING_MOISTURE_TYPE
					current_system_setting.schedule[tmp_sch_index].moisture_content = moisture_content;
#endif // SUPPORT_WATERING_MOISTURE_TYPE

#if SUPPORT_LED
					current_system_setting.schedule[tmp_sch_index].support_led = support_led;
					current_system_setting.schedule[tmp_sch_index].led_start_time = led_start_time;
					current_system_setting.schedule[tmp_sch_index].led_end_time = led_end_time;
#endif // SUPPORT_LED
				} else if (isSetWatering) {
					current_system_setting.watering_in_firstday = watering_in_firstday;
					current_system_setting.working_day = working_day;
					current_system_setting.watering_day = watering_day;
					current_system_setting.max_check_wl_time = max_check_wl_time;
					current_system_setting.watering_start_time = watering_start_time;
					current_system_setting.watering_end_time = watering_end_time;

					current_system_setting.enable_sch = enable_sch;
					current_system_setting.sch_index = sch_index;
					current_system_setting.sch_executed = false;
					tmp_sch_index = sch_index;

#if SUPPORT_WATERING_MOISTURE_TYPE
					current_system_setting.watering_type = watering_type;
#endif // SUPPORT_WATERING_MOISTURE_TYPE
				} else if (isResetDays) {
					current_system_setting.start_day = 0;
					current_system_setting.current_day = 0;
					current_system_setting.max_day_of_year = 0;
					current_system_setting.working_day = 0;
					current_system_setting.watering_day = 0;
					current_system_setting.sch_index = 0;
					current_system_setting.sch_executed = false;
					tmp_sch_index = 0;
#if SUPPORT_WATERING_MOISTURE_TYPE
					current_system_setting.watering_type = WATERING_BY_TIME_TYPE;
#endif // SUPPORT_WATERING_MOISTURE_TYPE
				} else if (isResetSession) {
					restore_default_schedules();
					tmp_sch_index = 0;
				}
			} else if (isFactoryDefault) {
				restore_default_wifi();
				restore_default_schedules();
			} else if (isStartOTA) {
				ota.start_OTA_threads(3000, current_system_setting.ota_server);
			}
#if SUPPORT_SOIL_MOISTURE_SENSOR
			else if (isSetSensorOffset) {
				current_system_setting.sensor_offset = sensor_offset;
			}
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

			if (isSettings) {
				//update_config();
				//delay(1000);

				if (isSetWiFi || isFactoryDefault) {
					update_config();
					delay(1000);
					sys_reset();
					return;
				}

				pending_save_config = true;
				last_save_request_ms = millis();
			}
		}

		if (!isAPMode) {

			wifi_status = WiFi.status();

			if (wifi_status == WL_CONNECTED) {

				if (internetState) {

					if ((millis() - last_ntp_update) >= MAX_CHECK_NTP_INTERVAL) {

						last_ntp_update = millis();

						if (timeClient.isUpdated()) {
							// NTPClient will run the forceUpdate function when the update interval has been reached
							timeClient.update();
						} else {
							change_ntp_server();
						}
					}
				}
			} else {
				if ((millis() - start_scan_time) >= MAX_SCAN_NETWORK_TIME) {
					start_scan_time = millis();
					if (check_wifi_network()) {
						sys_reset();
						return;
					}
				}
			}

			if (enable_sch) {
				if (timeClient.isUpdated()) {
					if (watering_type == WATERING_BY_TIME_TYPE) {
						if ((millis() - last_check_sch) >= CHECK_SCHEDULE_TIMEOUT) {
							last_check_sch = millis();
							checkSchedule();
						}
					}
#if SUPPORT_WATERING_MOISTURE_TYPE
					else {
						// check soil moisture when the system time reaches the maximum checking time
						if ((millis() - last_check_moisture_time) >= MAX_CHECK_MOISTURE_TIME) {
							last_check_moisture_time = millis();
							checkSchedule();
						}
					}
#endif // SUPPORT_WATERING_MOISTURE_TYPE
				}
			}
		} else {
			// While DUT is in AP mode, scan available Wi-Fi networks to find the SSID to connect to
			if ((millis() - start_scan_time) >= MAX_SCAN_NETWORK_TIME) {
				start_scan_time = millis();
				if (check_wifi_network()) {
					sys_reset();
					return;
				}
			}
		}

		if (isPowerUp || isWaterRemained) {

			if (isPowerUp) {
				isPowerUp = false;

				printCurrentTime();
				printf("Power Up\r\n");

#if SUPPORT_NCU_CLOUD
				memset(powerup_payload, 0, sizeof(powerup_payload));
				snprintf(powerup_payload, sizeof(powerup_payload), reportPayload, NOTIFY_TYPE, "The device is power-up", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
			}

			if ((millis() - last_check_remained_water) >= MIN_CHECK_REMAINED_WATER_INTERVAL) {
				last_check_remained_water = millis();

#if SUPPORT_WATER_LEVEL_SENSOR
				check_remained_water();
#endif // SUPPORT_WATER_LEVEL_SENSOR
			}
		}

		if (pending_save_config) {
			if (millis() - last_save_request_ms > 500) {
				update_config();
                pending_save_config = false;
			}
		}
	}

	// when the watchdog timer has been triggered
	if (triggerWDT) {
		// save the log message of watchdog to the SD card
		update_config();
		delay(1000);
		sys_reset();
	}
}

// Summary:
//	check the water level is reached the required level or not
void checkWaterLevel() {
#if SUPPORT_FLOW_SENSOR
	int watering_volume = current_system_setting.schedule[sch_index].watering_volume;
	int calculate = 0;

	// register the interrupt handler of flow sensor
	digitalSetIrqHandler(FLOW_GPIO_PIN, flow_sensor_handler);
	delay(1000);
	digitalClearIrqHandler(FLOW_GPIO_PIN);
	calculate = (pulse / 45) * 150; //one second around 150 ml; (Pulse frequency x 60) / 5.5Q, = flow rate in L/Min
	printf("pulse=%d, calculate=%d, watering_volume=%d\r\n", pulse, calculate, watering_volume);

	if (calculate >= (watering_volume * 1000)) {
		printf("Enough water!!!\r\n");
		digitalWrite(PUMP_GPIO_PIN, RELAY_OFF);
		isWatering = false;
	}
#endif // SUPPORT_FLOW_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
   detect_water_level(false);
#endif // SUPPORT_WATER_LEVEL_SENSOR
}

// Summary:
//	 Shift to the next schedule
void shiftSchedule(int sch_index) {
	char msg[30] = {0};

	printCurrentTime();
	printf("Shift Schedule to %d\r\n", (sch_index+1));
	snprintf(msg, sizeof(msg), "Shift Schedule Index to %d", (sch_index+1));
	save_log_msg(msg);

	isShiftSchedule = true;
	current_system_setting.sch_index = sch_index;

#if SUPPORT_NCU_CLOUD
	memset(shift_sch_payload, 0, sizeof(shift_sch_payload));
	snprintf(shift_sch_payload, sizeof(shift_sch_payload), reportPayload, REPORT_TYPE, msg, (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
}

// Summary:
//	 Check whether or not the current time is the time for watering
void checkSchedule() {
	long long seconds = timeClient.getEpochTime();
	struct tm *timeinfo = localtime(&seconds);
	int32_t current_mins = timeinfo->tm_hour * 60 + timeinfo->tm_min;
	int current_day = timeinfo->tm_yday + 1;
	int shift_sch2_day = current_system_setting.schedule[0].watering_interval + 1;
	int shift_sch3_day = shift_sch2_day + current_system_setting.schedule[1].watering_interval;
	int shift_sch4_day = shift_sch3_day + current_system_setting.schedule[2].watering_interval;
	int shift_sch5_day = shift_sch4_day + current_system_setting.schedule[3].watering_interval;
	int sch_index = current_system_setting.sch_index;
	int prev_working_day = 0;
	int watering_interval = 0;
	int watering_mins = 0;
	int expected_sch_idx = 0;
	bool update_system_setting = false;
	bool cancelWatering = false;
	static char log[128];

#if SUPPORT_NCU_CLOUD
	static char msg[128];
#endif // SUPPORT_NCU_CLOUD

	// save the start day of year to flash
	if (current_system_setting.start_day == 0) {
		update_system_setting = true;
		current_system_setting.start_day = current_day;
		current_system_setting.current_day = current_day;
		current_system_setting.max_day_of_year = current_day;

		current_system_setting.working_day++;

		memset(log, 0, sizeof(log));
		snprintf(log, sizeof(log), "initialize start day to %04d/%02d/%02d", (timeinfo->tm_year + 1900), (timeinfo->tm_mon + 1), timeinfo->tm_mday);
		save_log_msg(log);

		printCurrentTime();
		printf("%s\r\n", log);
	}

	// only save the maximum day of year to flash
	if (current_day > current_system_setting.max_day_of_year) {
		memset(log, 0, sizeof(log));
		snprintf(log, sizeof(log), "update max_day_of_year from %d to %d", current_system_setting.max_day_of_year, current_day);
		save_log_msg(log);

		printCurrentTime();
		printf("%s\r\n", log);

		update_system_setting = true;
		current_system_setting.max_day_of_year = current_day;
	}

	// if the current day is different than the previous current day
	if (current_day != current_system_setting.current_day) {

		update_system_setting = true;
		current_system_setting.current_day = current_day;

		// if the current day is less than the start day
		// that means we have crossed one year and we must added the maximum day of last year
		if (current_day < current_system_setting.start_day) {
			current_day += current_system_setting.max_day_of_year;
		}

		prev_working_day = current_system_setting.working_day;
		current_system_setting.working_day = (current_day - current_system_setting.start_day) + 1;

		memset(log, 0, sizeof(log));
		snprintf(log, sizeof(log), "update working_day from %d to %d", prev_working_day, current_system_setting.working_day);
		save_log_msg(log);

		printCurrentTime();
		printf("%s\r\n", log);
	}

	if (current_system_setting.watering_type == WATERING_BY_TIME_TYPE) {
		if ((current_system_setting.working_day >= shift_sch2_day) && (current_system_setting.working_day < shift_sch3_day)) {
			expected_sch_idx = 1;
		} else if ((current_system_setting.working_day >= shift_sch3_day) && (current_system_setting.working_day < shift_sch4_day)) {
			expected_sch_idx = 2;
		} else if ((current_system_setting.working_day >= shift_sch4_day) && (current_system_setting.working_day < shift_sch5_day)) {
			expected_sch_idx = 3;
		} else if (current_system_setting.working_day >= shift_sch5_day) {
			expected_sch_idx = 4;
		}

		// when the current schedule index is different than the expected schedule index
		// shifting to the expected schedule
		if (sch_index != expected_sch_idx) {
			sch_index = expected_sch_idx;
			shiftSchedule(sch_index);
		}

		watering_interval = current_system_setting.schedule[sch_index].watering_interval;

		// get the watering time in minute
		watering_mins = current_system_setting.schedule[sch_index].watering_time;

		//printCurrentTime();
		//printf("current_mins: %d, watering_mins: %d\r\n", current_mins, watering_mins);

		if ((current_mins < watering_mins) && (current_system_setting.sch_executed == true)) {
			current_system_setting.sch_executed = false;
			update_system_setting = true;
		}

		if (((current_mins == watering_mins) || ((current_mins > watering_mins) && (current_system_setting.sch_executed == false)))
				&& (!isExecuted)) {

			printCurrentTime();
			printf("Working Day: %d\r\n", current_system_setting.working_day);
			printf("Watering Day: %d\r\n", current_system_setting.watering_day);

			printf("Watering in First Day: %s\r\n", current_system_setting.watering_in_firstday?"Yes":"No");

			printf("Schedule Index: %d\r\n", sch_index);
			printf("Watering Interval: %d\r\n", watering_interval);
			printf("Schedule Executed: %s\r\n", current_system_setting.sch_executed?"Yes":"No");

			update_system_setting = true;
			current_system_setting.sch_executed = true;
			isWatering = false;

			// no need to water in the first day, but we need to increase the watering day
			
			if ((!current_system_setting.watering_in_firstday) && (current_system_setting.watering_day == 0)) {
				current_system_setting.watering_day = 1;
			}else{
                // 如果有 VGNMS 排程，優先使用
        bool useVGNMSSchedule = (current_system_setting.vgnms_schedule_count > 0);
        if (useVGNMSSchedule) {
          int today = current_system_setting.working_day - 1; // 0-based
          for (int i = 0; i < current_system_setting.vgnms_schedule_count; i++) {
            if (current_system_setting.vgnms_schedule[i] == today) {
              isWatering = true;
              current_system_setting.watering_day = current_system_setting.working_day;
              break;
            }
        }
			} else if (((current_system_setting.watering_day == 0) && (current_system_setting.working_day == 1))
					|| ((current_system_setting.working_day - current_system_setting.watering_day) >= watering_interval)
					|| (isShiftSchedule)) {

				isWatering = true;
				current_system_setting.watering_day = current_system_setting.working_day;

				// when the current working day is the first day of shifting schedule
				if (isShiftSchedule) {
					isShiftSchedule = false;
				}
			}
		}

			printf("Current Time: %d\r\n", current_mins);
			printf("Watering Start Time: %d\r\n", current_system_setting.watering_start_time);
			printf("Watering End Time: %d\r\n", current_system_setting.watering_end_time);

			if ((current_mins >= watering_mins) && (isWatering)){
				// Allow watering only between watering_start_time and watering_end_time
				if ((current_mins < current_system_setting.watering_start_time)
						|| (current_mins >= current_system_setting.watering_end_time)) {
					printf("Watering stopped: current time is outside the watering time range\r\n");
					cancelWatering = true;
					isWatering = false;
				}
			}

			// Set isExecuted to true to prevent the watering process from executing multiple times within one minute.
			isExecuted = true;

#if SUPPORT_NCU_CLOUD
			if (!isWatering) {

				memset(msg, 0, sizeof(msg));

				if (cancelWatering) {
					snprintf(msg, sizeof(msg), "Watering stopped: current time is outside the watering time range");
				} else {
					snprintf(msg, sizeof(msg), "Execute Schedule%d, no watering needed", (sch_index+1));
				}

				save_log_msg(msg);

				memset(shift_sch_payload, 0, sizeof(shift_sch_payload));
				snprintf(shift_sch_payload, sizeof(shift_sch_payload), reportPayload, REPORT_TYPE, msg, (timeClient.getEpochTime() - tw_offset));
			}
#endif // SUPPORT_NCU_CLOUD
		}
	}
#if SUPPORT_WATERING_MOISTURE_TYPE
	else {

		printCurrentTime();
		printf("Schedule Executed:  %s\r\n", current_system_setting.sch_executed?"Yes":"No");

		if ((current_system_setting.sch_executed == false) && (!isExecuted)) {
			int soil = analogRead(SOIL_DATA_PIN);

			soil -= current_system_setting.sensor_offset;

			printf("Schedule Index: %d\r\n", sch_index);
			printf("Moisture Content: %d\r\n", current_system_setting.schedule[sch_index].moisture_content);
			printf("Soil Moisture: %d\r\n", soil);

			if (soil >= current_system_setting.schedule[sch_index].moisture_content) {
				check_moisture_counter++;
			} else {
				if (check_moisture_counter > 0) {
					check_moisture_counter--;
				}
			}

			printf("Soil Moisture Counter: %d\r\n", check_moisture_counter);

			if (check_moisture_counter >= MAX_CHECK_MOISTURE_COUNT) {

				if (current_system_setting.sch_index < (MAX_SCHEDULE_NUM - 1)) {

					if (current_system_setting.watering_day != 0) {
						sch_index++;
						shiftSchedule(sch_index);
						isShiftSchedule = false;
					}
				}

				update_system_setting = true;
				current_system_setting.watering_day = current_system_setting.working_day;
				current_system_setting.sch_executed = true;

				isWatering = true;

				// set isExecuted to true to avoid the watering process is being executed many times in one minute
				isExecuted = true;

				check_moisture_counter = 0;
			}
		} else if ((current_mins == 0) && (current_system_setting.sch_executed == true)) {
			update_system_setting = true;
			current_system_setting.sch_executed = false;
		}
	}
#endif // SUPPORT_WATERING_MOISTURE_TYPE

	if (isWatering) {

		printCurrentTime();
		printf("Start watering\r\n");

#if SUPPORT_WATER_LEVEL_SENSOR
		reboot_wl_sensor();

		// save the water level before pumping
		detect_water_level(true);
#endif // SUPPORT_WATER_LEVEL_SENSOR

#if SUPPORT_NCU_CLOUD
		// disabled the mqtt publish function
		// while performing the watering processes
		disablePublish = true;
#endif // SUPPORT_NCU_CLOUD

		isOpenSV = false;
		open_sv_time = 0;

		isCloseSV = false;
		close_sv_time = 0;

		isWaterRemained = false;
		last_check_remained_water = 0;

		last_water_level = 0;
		last_check_wl_rise = 0;
		lastcheckwltime = 0;

		// initialize the status of watering processes
		current_system_setting.isExecSch = true;
		current_system_setting.isPumpProcCompleted = false;
		current_system_setting.isWaitingProcCompleted = false;

		current_system_setting.startWaitingTime = 0;

		start_pumping();
	}

	if (update_system_setting) {
		update_system_setting = false;
		//update_config();
		pending_save_config = true;
		last_save_request_ms = millis();
	}

	if ((isExecuted) && (current_mins > watering_mins)) {
		isExecuted = false;
	}

#if SUPPORT_LED
	check_led_time();
#endif // SUPPORT_LED
}

#if SUPPORT_FLOW_SENSOR
// summary:
//  calculate the pulse of flow sensor
void flow_sensor_handler(uint32_t id, uint32_t event) {
   pulse++;
}
#endif // SUPPORT_FLOW_SENSOR

void start_pumping() {
	printCurrentTime();
	printf("Start pumping\r\n");

#if SUPPORT_FLOW_SENSOR
	pulse = 0;
#endif // SUPPORT_FLOW_SENSOR

	digitalWrite(SV_GPIO_PIN, RELAY_OFF); // close the solenoid valve
	digitalWrite(PUMP_GPIO_PIN, RELAY_ON);    // turn on the pump

	save_log_msg("Start pumping");

	// Store the pump start time
	start_pumping_time = timeClient.getEpochTime();

	// Time of the last water level rise check
	last_check_wl_rise = start_pumping_time;

#if SUPPORT_NCU_CLOUD
	memset(start_pumping_payload, 0, sizeof(start_pumping_payload));
	snprintf(start_pumping_payload, sizeof(start_pumping_payload), reportPayload, REPORT_TYPE, "Start pumping", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
}

// Summary:
//  Stop pumping and trigger a timer to open the solenoid valve
void stop_pumping(void) {
	printCurrentTime();
	printf("Stop pumping\r\n");

	isWatering = false;
	digitalWrite(PUMP_GPIO_PIN, RELAY_OFF); // stop pumping

	isOpenSV = true;
	open_sv_time = timeClient.getEpochTime();

	save_log_msg("Stop pumping");

#if SUPPORT_NCU_CLOUD
	memset(stop_pumping_payload, 0, sizeof(stop_pumping_payload));
	snprintf(stop_pumping_payload, sizeof(stop_pumping_payload), reportPayload, REPORT_TYPE, "Stop pumping", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD

	if (current_system_setting.isExecSch) {
		// the pump process is completed
		current_system_setting.isPumpProcCompleted = true;

		// keep the start time of the waiting process
		current_system_setting.startWaitingTime = timeClient.getEpochTime();

		//update_config();
		pending_save_config = true;
		last_save_request_ms = millis();
	}
}

// Summary:
//  Close the solenoid valve
void close_solenoid_valve() {

	printCurrentTime();
	printf("Close the solenoid valve\r\n");

	digitalWrite(SV_GPIO_PIN, RELAY_OFF); // close the solenoid valve

	save_log_msg("Close the solenoid valve");

#if SUPPORT_NCU_CLOUD
	memset(close_sv_payload, 0, sizeof(close_sv_payload));
	snprintf(close_sv_payload, sizeof(close_sv_payload), reportPayload, REPORT_TYPE, "Close the solenoid valve", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
}

// Summary:
//  complete the watering process
void complete_watering_process() {

	// disable checking the time interval for closing the solenoid valve
	isCloseSV = false;

#if SUPPORT_NCU_CLOUD
	// we need to wait for the watering processes were completed and enabled the mqtt publish function here
	// otherwise the system may be blocked for one minutes when sending the messages to the broker
	disablePublish = false;
#endif // SUPPORT_NCU_CLOUD

#if SUPPORT_WATER_LEVEL_SENSOR
	check_remained_water();
#endif // SUPPORT_WATER_LEVEL_SENSOR

	if (!isWaterRemained) {
		// if the watering processes are all completed
		if (current_system_setting.isExecSch) {
			reexecSch = false;
			current_system_setting.isExecSch = false;
			//update_config();
			pending_save_config = true;
			last_save_request_ms = millis();
		}
	}
}

// Summary:
//  Open the solenoid valve and trigger a timer to close it
void open_solenoid_valve() {
	printCurrentTime();
	printf("Open the solenoid valve\r\n");

	digitalWrite(SV_GPIO_PIN, RELAY_ON); // open the solenoid valve

	save_log_msg("Open the solenoid valve");

	isOpenSV = false;
	isCloseSV = true;
	close_sv_time = timeClient.getEpochTime();

#if SUPPORT_NCU_CLOUD
	memset(open_sv_payload, 0, sizeof(open_sv_payload));
	snprintf(open_sv_payload, sizeof(open_sv_payload), reportPayload, REPORT_TYPE, "Open the solenoid valve", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD

	if (current_system_setting.isExecSch) {
		current_system_setting.isWaitingProcCompleted = true;
		//update_config();
		pending_save_config = true;
		last_save_request_ms = millis();
	}
}

#if SUPPORT_LED
// Summary:
//  Turn on the LEDs if the current time is before the end time
void check_led_time() {
	long long seconds = timeClient.getEpochTime();
	struct tm *current_time = localtime(&seconds);
	int current_mins = current_time->tm_hour * 60 + current_time->tm_min;
	int sch_index = current_system_setting.sch_index;
	int start_mins = current_system_setting.schedule[sch_index].led_start_time;
	int end_mins = current_system_setting.schedule[sch_index].led_end_time;
	bool isOn = false;
	bool isOff = false;

	if (current_system_setting.schedule[sch_index].support_led) {
		// when turn on the LEDs 24 hours
		if ((start_mins == 0) && (end_mins == 0)) {
			isOn = true;
		} else {
			if ((current_mins >= start_mins) && ((current_mins < end_mins) || (end_mins == 0))){
				isOn = true;
			} else if ((current_mins < start_mins) || (current_mins >= end_mins)) {
				isOff = true;
			}
		}
	} else {
		// turn off the LEDs if the LEDs is ON
		if (digitalRead(LED_GPIO_PIN)) {
			isOff = true;
		}
	}

	if (isOn) {
		if (digitalRead(LED_GPIO_PIN) == 0) {
			digitalWrite(LED_GPIO_PIN, LED_ON); // turn on the LED

			printCurrentTime();
			printf("Turn on LED\r\n");
			save_log_msg("Turn on LED");

#if SUPPORT_NCU_CLOUD
			memset(led_payload, 0, sizeof(led_payload));
			snprintf(led_payload, sizeof(led_payload), reportPayload, REPORT_TYPE, "Turn on LED", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
		}
	}

	if (isOff) {
		if (digitalRead(LED_GPIO_PIN)) {
			digitalWrite(LED_GPIO_PIN, LED_OFF); // turn off the LED

			printCurrentTime();
			printf("Turn off LED\r\n");
			save_log_msg("Turn off LED");

#if SUPPORT_NCU_CLOUD
			memset(led_payload, 0, sizeof(led_payload));
			snprintf(led_payload, sizeof(led_payload), reportPayload, REPORT_TYPE, "Turn off LED", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
		}
	}
}
#endif // SUPPORT_LED

#if SUPPORT_ENVIRONMENT_SENSOR
#if SUPPORT_TEMP_HUMIDITY_SENSOR
// Summary:
//	Check the error count has reached the maximum threshold or not
void check_read_th_err_count() {
	read_th_err_count++;

	if (read_th_err_count >= MAX_READ_TH_ERR_NUM) {
		save_log_msg("Reboot DUT due to the error count has been reached the maximum threshold");
		delay(1000);
		sys_reset();
	}
}
#endif // SUPPORT_TEMP_HUMIDITY_SENSOR

void read_environment_sensor() {
#if SUPPORT_NCU_CLOUD
	static char reading_msg[150];
#endif // SUPPORT_NCU_CLOUD

#if SUPPORT_LIGHT_SENSOR
	// since calling getLightLevel() in loop
	// we must wait for one second to make sure the I2C interface is free
	//delay(1000);
	uint16_t lux = getLightLevel();

	printCurrentTime();
	printf("Lux : %d\r\n", lux);
#endif // SUPPORT_LIGHT_SENSOR

#if SUPPORT_TEMP_HUMIDITY_SENSOR
	float h = 0;
	float t = 0;

	// since calling getLightLevel() in loop or above
	// we must wait for one second to make sure the I2C interface is free
	// delay(1000);

#if SUPPORT_DHT11 || SUPPORT_DHT22
	// Reading temperature or humidity takes about 250 milliseconds!
	// Sensor readings may also be up to 2 seconds 'old' (its a very slow sensor)
	h = dht.readHumidity();
	// Read temperature as Celsius (the default)
	t = dht.readTemperature();
#endif // SUPPORT_DHT11 || SUPPORT_DHT22

#if SUPPORT_SHT2X
	h = sht20.humidity();
	t = sht20.temperature();
#endif // SUPPORT_SHT2X

	// Check if any reads failed and exit early (to try again).
	if (isnan(h) || isnan(t)) {
#if SUPPORT_DHT11 || SUPPORT_DHT22
		printf("Failed to read from DHT sensor!\r\n");
#endif // SUPPORT_DHT11 || SUPPORT_DHT22

#if SUPPORT_SHT2X
		printf("Failed to read from SHT2x sensor!\r\n");
#endif // SUPPORT_SHT2X

		check_read_th_err_count();

		return;
	}

	if ((h <= 0) || (t <= 0)) {
		check_read_th_err_count();
	} else {
		// reset the error counter when the reading values are correct
		if (read_th_err_count > 0) {
			read_th_err_count = 0;
		}
	}

	printCurrentTime();
	printf("Temperature : %.2f°C  Humidity: %.2f%\r\n", t, h);
#endif // SUPPORT_TEMP_HUMIDITY_SENSOR

#if SUPPORT_SOIL_MOISTURE_SENSOR
	int soil = analogRead(SOIL_DATA_PIN);

	soil -= current_system_setting.sensor_offset;

	// print out the value
	printf("Soil Humidity: %d\r\n", soil);
#endif // SUPPORT_SOIL_MOISTURE_SENSOR

#if SUPPORT_NCU_CLOUD

	memset(reading_msg, 0, sizeof(reading_msg));

#if SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_LIGHT_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

	snprintf(reading_msg, sizeof(reading_msg), readingPayload, t, h, lux, soil);

#elif SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_LIGHT_SENSOR

	snprintf(reading_msg, sizeof(reading_msg), readingPayload, t, h, lux);

#elif SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

	snprintf(reading_msg, sizeof(reading_msg), readingPayload, t, h, soil);

#elif SUPPORT_LIGHT_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

	snprintf(reading_msg, sizeof(reading_msg), readingPayload, lux, soil);

#elif SUPPORT_TEMP_HUMIDITY_SENSOR
	snprintf(reading_msg, sizeof(reading_msg), readingPayload, t, h);

#elif SUPPORT_LIGHT_SENSOR
	snprintf(reading_msg, sizeof(reading_msg), readingPayload, lux);

#elif SUPPORT_SOIL_MOISTURE_SENSOR

	snprintf(reading_msg, sizeof(reading_msg), readingPayload, soil);

#endif // SUPPORT_TEMP_HUMIDITY_SENSOR && SUPPORT_LIGHT_SENSOR && SUPPORT_SOIL_MOISTURE_SENSOR

	memset(env_payload, 0, sizeof(env_payload));
	snprintf(env_payload, sizeof(env_payload), reportPayload, STATS_TYPE, reading_msg, (timeClient.getEpochTime() - tw_offset));

#if SUPPORT_SAVE_ENV_DATA
	save_env_data(reading_msg);
#endif // SUPPORT_SAVE_ENV_DATA

#endif // SUPPORT_NCU_CLOUD
}
#endif // SUPPORT_ENVIRONMENT_SENSOR

#if SUPPORT_WATER_LEVEL_SENSOR
// Summary:
//	retrieve the values from the high 12 sections
void getHigh12SectionValue(void) {
	int cnt = 0;

	// since opening/closing the solenoid valve will affect reading the water level sensor
	// we must reset the I2C interface before reading values
	Wire.end();

	Wire.requestFrom(ATTINY1_HIGH_ADDR, 12);
	delay(1);
	//printf("ATTINY1_HIGH_ADDR : %d\r\n", Wire.available());
	while (12 != Wire.available()) {
		if (++cnt == 3) {
			save_log_msg("the high 12 section is unavailable");
			return;
		}
	}

	// we must add a delay here, otherwise the data may not be ready
	delay(10);

	for (int i = 0; i < MAX_HIGH_DATA; i++) {
		high_data[i] = Wire.read();
	}

	delay(10);
}

// Summary:
//	retrieve the values from the low 8 sections
void getLow8SectionValue(void) {
	int cnt = 0;

	// since opening/closing the solenoid valve will affect reading the water level sensor
	// we must reset the I2C interface before reading values
	Wire.end();
	Wire.requestFrom(ATTINY2_LOW_ADDR, 8);
	delay(1);
	//printf("ATTINY2_LOW_ADDR : %d\r\n", Wire.available());
	while (8 != Wire.available()) {
		if (++cnt == 3){
			save_log_msg("the low 8 section is unavailable");
			return;
		}
	}

	// we must add a delay here, otherwise the low sections data will become the high sections data
	delay(10);

	for (int i = 0; i < MAX_LOW_DATA; i++) {
		low_data[i] = Wire.read(); // receive a byte as character
	}

	delay(10);
}

// Summary:
//	 Detect the water level reaches the watering_level variable or not
//	 stop motor if the water level has been reached
void detect_water_level(bool beforePumping) {
	static char level_msg[100];
	unsigned long seconds = timeClient.getEpochTime();
	int sch_index = current_system_setting.sch_index;
	int detect_level = 0;
	int low_count = 0;
	int high_count = 0;
	int total_count = 0;
	bool isReached = false;

	memset(level_msg, 0, sizeof(level_msg));

	if (!enableTestMode) {

		memset(low_data, 0, sizeof(low_data));
		memset(high_data, 0, sizeof(high_data));

		getLow8SectionValue();

		printCurrentTime();
		printf("low 8 sections value = \r\n");

		for (int i = 0; i < MAX_LOW_DATA; i++) {
			printf("%d", low_data[i]);

			if (i < (MAX_LOW_DATA - 1)) {
				printf(".");
			}

			if ((low_data[i] >= MIN_LEVEL_VALUE) && (low_data[i] <= MAX_LEVEL_VALUE)) {
				low_count = i + 1;
			}
		}

		printf("\r\n");
		printf("low_count = %d\r\n", low_count);

		if (low_count == MAX_LOW_DATA) {
			getHigh12SectionValue();
			printf("\r\n");
			printf("high 12 sections value = \r\n");

			for (int i = 0; i < MAX_HIGH_DATA; i++){
				printf("%d", high_data[i]);

				if (i < (MAX_HIGH_DATA - 1)) {
					printf(".");
				}

				if ((high_data[i] >= MIN_LEVEL_VALUE) && (high_data[i] <= MAX_LEVEL_VALUE)) {
					high_count = i + 1;
				}
			}

			printf("\r\n");
			printf("high_count = %d\r\n", high_count);
		}

		if (current_system_setting.schedule[sch_index].watering_level > 0) {
			detect_level = current_system_setting.schedule[sch_index].watering_level - 1;
		}

		if (detect_level < MAX_LOW_DATA) {
			if ((low_data[detect_level] >= MIN_LEVEL_VALUE) && (low_data[detect_level] <= MAX_LEVEL_VALUE)) {
				isReached = true;
			} else if (low_count > detect_level) {
				isReached = true;
			}
		} else {
			detect_level -= MAX_LOW_DATA;

			if ((high_data[detect_level] >= MIN_LEVEL_VALUE) && (high_data[detect_level] <= MAX_LEVEL_VALUE)) {
				isReached = true;
			} else if (high_count > detect_level) {
				isReached = true;
			}
		}

		if (beforePumping){
			snprintf(level_msg, sizeof(level_msg), "before pumping: [%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]", low_data[0],
				low_data[1], low_data[2], low_data[3], low_data[4], low_data[5], low_data[6], low_data[7],
				high_data[0], high_data[1], high_data[2], high_data[3], high_data[4], high_data[5], high_data[6], high_data[7], high_data[8],
				high_data[9], high_data[10], high_data[11]);

			save_log_msg(level_msg);
		} else {

			total_count = low_count + high_count;

			// when the current level reaches the water level we wanted
			if (isReached) {
				snprintf(level_msg, sizeof(level_msg), "[%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]", low_data[0],
					low_data[1], low_data[2], low_data[3], low_data[4], low_data[5], low_data[6], low_data[7],
					high_data[0], high_data[1], high_data[2], high_data[3], high_data[4], high_data[5], high_data[6], high_data[7], high_data[8],
					high_data[9], high_data[10], high_data[11]);
				stop_pumping();

				save_log_msg(level_msg);

				if (total_count > current_system_setting.schedule[sch_index].watering_level) {

					save_log_msg("The water level is overflow");

#if SUPPORT_NCU_CLOUD
					memset(warning_msg, 0, sizeof(warning_msg));
					snprintf(warning_msg, sizeof(warning_msg), reportPayload, NOTIFY_TYPE, "The water level is overflow", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD
				}
			} else {

				if ((total_count == last_water_level) && ((seconds - last_check_wl_rise) >= current_system_setting.max_check_wl_time)){
					snprintf(level_msg, sizeof(level_msg), "%s", "Something went wrong during watering");
					stop_pumping();

					save_log_msg(level_msg);
				}

				if (total_count != last_water_level) {
					last_water_level = total_count;
					last_check_wl_rise = seconds;
				}
			}
		}
	} else {
		if (beforePumping){
			snprintf(level_msg, sizeof(level_msg), "before pumping: [%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]", low_data[0],
				low_data[1], low_data[2], low_data[3], low_data[4], low_data[5], low_data[6], low_data[7],
				high_data[0], high_data[1], high_data[2], high_data[3], high_data[4], high_data[5], high_data[6], high_data[7], high_data[8],
				high_data[9], high_data[10], high_data[11]);
		} else {
			snprintf(level_msg, sizeof(level_msg), "[%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]", low_data[0],
				low_data[1], low_data[2], low_data[3], low_data[4], low_data[5], low_data[6], low_data[7],
				high_data[0], high_data[1], high_data[2], high_data[3], high_data[4], high_data[5], high_data[6], high_data[7], high_data[8],
				high_data[9], high_data[10], high_data[11]);
			stop_pumping();
		}

		save_log_msg(level_msg);
	}

#if SUPPORT_NCU_CLOUD
	if (strlen(level_msg) > 0) {
		memset(water_level_payload, 0, sizeof(water_level_payload));
		snprintf(water_level_payload, sizeof(water_level_payload), reportPayload, NOTIFY_TYPE, level_msg, (timeClient.getEpochTime() - tw_offset));
	}
#endif // SUPPORT_NCU_CLOUD
}

// Summary:
//	Check the water is still remained in the box or not
void check_remained_water() {
	int low_count = 0;
	int high_count = 0;

	if (!enableTestMode) {

		memset(low_data, 0, sizeof(low_data));
		memset(high_data, 0, sizeof(high_data));

		getLow8SectionValue();

		printCurrentTime();
		printf("low 8 sections value = \r\n");

		for (int i = 0; i < MAX_LOW_DATA; i++) {
			printf("%d", low_data[i]);

			if (i < (MAX_LOW_DATA - 1)) {
				printf(".");
			}

			if ((low_data[i] >= MIN_LEVEL_VALUE) && (low_data[i] <= MAX_LEVEL_VALUE)) {
				low_count = i + 1;
			}
		}

		printf("\r\n");
		printf("low_count = %d\r\n", low_count);

		if (low_count == MAX_LOW_DATA) {
			getHigh12SectionValue();
			printf("\r\n");
			printf("high 12 sections value = \r\n");
			for (int i = 0; i < MAX_HIGH_DATA; i++){
				printf("%d", high_data[i]);

				if (i < (MAX_HIGH_DATA - 1)) {
					printf(".");
				}

				if ((high_data[i] >= MIN_LEVEL_VALUE) && (high_data[i] <= MAX_LEVEL_VALUE)) {
					high_count = i + 1;
				}
			}

			printf("\r\n");
			printf("high_count = %d\r\n", high_count);
		}

		if (low_count > 0) {

			if (!isWaterRemained) {
				isWaterRemained = true;

#if SUPPORT_NCU_CLOUD
				memset(warning_msg, 0, sizeof(warning_msg));
				snprintf(warning_msg, sizeof(warning_msg), reportPayload, NOTIFY_TYPE, "water is still remained", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD

				save_log_msg("water is still remained");

				// if the gpio of solenoid valve is off
				// we must turn on the solenoid valve
				if (digitalRead(SV_GPIO_PIN) == 0) {
					digitalWrite(SV_GPIO_PIN, RELAY_ON);
				}
			}
		} else {

			isWaterRemained = false;

#if SUPPORT_NCU_CLOUD
			memset(warning_msg, 0, sizeof(warning_msg));
			snprintf(warning_msg, sizeof(warning_msg), reportPayload, REPORT_TYPE, "no water remained", (timeClient.getEpochTime() - tw_offset));
#endif // SUPPORT_NCU_CLOUD

			save_log_msg("no water remained");

			// if the gpio of solenoid valve is on
			// we must turn off the solenoid valve
			if (digitalRead(SV_GPIO_PIN)) {
				close_solenoid_valve();
			}
		}
	}
}
#endif // SUPPORT_WATER_LEVEL_SENSOR
