package mqtt

import (
	//"bytes"
	"encoding/json"
	"fmt"
	"sync"
	//"image"
	"image/jpeg"
	"os"
	"sort"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	cfg "v-gnms/config"

	. "v-gnms/pkg/core/database"
	"v-gnms/pkg/core/sns"
	"v-gnms/pkg/core/socketio"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)

type ImageBuffer struct {
	chunks map[int][]byte
	mutex  sync.Mutex
}

var img_list = make(map[string]*ImageBuffer)

var MAX_REPORT_IMAGE_SIZE_BYTES = 20971520

var MQTT_DEVICE_REPORT_RESPONSE_DOWNLINK_PREFIX = "downlink/report"
var MQTT_DEVICE_REPORT_IMAGE_RESPONSE_DOWNLINK_PREFIX = "downlink/report-image"

var MQTT_DEVICE_REPORT_STATUS_OK = 200
var MQTT_DEVICE_REPORT_STATUS_BAD_PAYLOAD = 400
var MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR = 500
var MQTT_DEVICE_REPORT_STATUS_BAD_IMAGE = 422
var MQTT_DEVICE_REPORT_STATUS_IMAGE_TOO_LARGE = 413

// Default publish handler
// This function is called whenever a MQTT message is received
func messageHandler(client mqtt.Client, msg mqtt.Message) {

	// if cfg.GetConfigValue("GO_ENV") == "development" {
	// 	log.Info().Msgf("Message received from %s:\n%s", msg.Topic(), msg.Payload())
	// } else {
	// 	log.Info().Msgf("Message received from %s", msg.Topic())
	// }

	log.Info().Msgf("Message received from %s", msg.Topic())

	topicParts := strings.Split(msg.Topic(), "/")

	deviceID := topicParts[2]

	if topicParts[0] != "uplink" {
		return
	}

	// Override the topic "report-image"
	// since it is not a JSON object but a raw byte array
	if topicParts[1] == "report-image" {
		if len(topicParts) == 6 {
			reportedAt, err := strconv.Atoi(topicParts[3])

			if err != nil {
				log.Info().Msgf("Invalid reportedAt:", topicParts[3])
				return
			}

			imageAction := topicParts[4]

			if imageAction != "chunk" && imageAction != "done" {
				log.Info().Msgf("Invalid Image Action:", topicParts[4])
				return
			}

			// chunk id
			chunkID, err := strconv.Atoi(topicParts[5])
			if err != nil {
				log.Info().Msgf("Invalid chunk id: %s", topicParts[5])
				return
			}

			HandleDeviceReportImageUplink(deviceID, reportedAt, imageAction, chunkID, msg.Payload())
		}

		return
	}

	var payload map[string]interface{}
	json.Unmarshal(msg.Payload(), &payload)
	log.Info().Any("", payload).Msg("")

	switch topicParts[1] {
	case "polling":
		HandleDevicePollingResultUplink(deviceID, payload)
	case "report":
		HandleDeviceReportUplink(deviceID, payload)
	case "query":
		DirectMQTTQueryUplinkToSocketIO(deviceID, string(msg.Payload()))
	case "command":
		DirectMQTTCommandUplinkToSocketIO(deviceID, string(msg.Payload()))
	case "uuid-lookup":
		HandleGetDeviceUUIDByCustomDeviceID(deviceID)
	}
}

func HandleDevicePollingResultUplink(deviceId string, payload map[string]interface{}) {
	_recordId, ok1 := payload["record_id"]
	_status, ok2 := payload["status"]
	_respondedAt, ok3 := payload["responded_at"]

	if !ok1 || !ok2 || !ok3 {
		log.Error().Msgf("Malformed payload detected when handling device polling uplink.")
		return
	}

	recordId, ok := _recordId.(string)
	if !ok {
		recordId = "(V-GNMS: Malformed Data Received)"
	}

	var status int
	if tryConvertValue, ok := _status.(float64); ok {
		status = int(tryConvertValue)
	} else {
		status = 0
	}

	var respondedAt int
	if tryConvertValue, ok := _respondedAt.(float64); ok {
		respondedAt = int(tryConvertValue)
	} else {
		respondedAt = 0
	}

	pollingResult, err := FindSingleByID[DevicePollingResult](DevicePollingResultsCollection, recordId, nil)
	if err != nil {
		log.Error().Msg("Polling result record not found when handling device polling uplink.")
		return
	}

	if pollingResult.DeviceID != deviceId {
		log.Error().Msg("Device ID mismatch.")
		return
	}

	pollingResult.Status = &status
	pollingResult.RespondedAt = respondedAt

	pollingResult, err = Update[DevicePollingResult](DevicePollingResultsCollection, recordId, pollingResult)
	if err != nil {
		log.Error().Msgf("Polling result update failed. Record ID: %s", recordId)
		return
	}

	device, _ := FindSingleByID[Device](DevicesCollection, deviceId, nil)
	device.LatestPollingStatus = &status
	device, _ = Update[Device](DevicesCollection, deviceId, device)

	// Emit data through socket.io
	log.Info().Msg("Sending socket.io")
	socketioMessage := ConstructGetSingleResponsePayloadRaw[DevicePollingResult](pollingResult)
	socketio.EmitDevicePollingUplinkMessage(deviceId, socketioMessage)

	log.Info().Msgf("Device polling result updated. Record ID: %s", recordId)
}

func HandleDeviceReportUplink(deviceId string, payload map[string]interface{}) {

	responseTopic := fmt.Sprintf("%s/%s", MQTT_DEVICE_REPORT_RESPONSE_DOWNLINK_PREFIX, deviceId)

	serverReceivedAt := int(time.Now().Unix())

	_reportType, ok1 := payload["type"]
	_level, ok2 := payload["level"]
	_info, ok3 := payload["info"]
	_reportedAt, ok4 := payload["reported_at"]

	if !ok1 || !ok2 || !ok3 || !ok4 {
		SendMQTTReportResponse(responseTopic, deviceId, -1, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_BAD_PAYLOAD)
		log.Error().Msgf("Malformed payload detected when handling device report uplink.")
		return
	}

	var reportedAt int
	if tryConvertValue, ok := _reportedAt.(float64); ok {
		reportedAt = int(tryConvertValue)
	} else {
		reportedAt = -1
	}

	var reportType int
	if tryConvertValue, ok := _reportType.(float64); ok {
		reportType = int(tryConvertValue)
	} else {
		reportType = -1
	}

	var level int
	if tryConvertValue, ok := _level.(float64); ok {
		level = int(tryConvertValue)
	} else {
		level = 0
	}

	info, ok := _info.(string)
	if !ok {
		SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_BAD_PAYLOAD)
		info = "(V-GNMS: Malformed Data Received)"
	}

	var deviceReport DeviceReport

	deviceReport.ID = GenerateUUIDv7()
	deviceReport.DeviceID = deviceId
	deviceReport.Type = &reportType
	deviceReport.Level = &level
	deviceReport.Info = info
	deviceReport.ReportedAt = reportedAt

	deviceReport, err := Create[DeviceReport](DeviceReportsCollection, deviceReport)
	if err != nil {
		SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
		log.Error().Msgf("Failed to save device report. Device ID: %s", deviceId)
		return
	}

	// Emit data through socket.io
	log.Info().Msg("Sending socket.io")
	socketioMessage := ConstructGetSingleResponsePayloadRaw[DeviceReport](deviceReport)
	socketio.EmitDeviceReportUplinkMessage(deviceId, socketioMessage)

	device, err := FindSingleByID[Device](DevicesCollection, deviceId, nil)
	if err != nil {
		SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
		log.Error().Msgf("Cannot find device for the provided device ID: %s", deviceId)
		return
	}

	SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_OK)

	// Emit data through socket.io
	if reportType == 500 {
		log.Info().Msg("Alarm detected. Publishing notification via AWS SNS...")

		reportTimeUTC := time.Unix(int64(deviceReport.ReportedAt), 0).UTC()

		// Create a location for UTC+8.
		// For example, using "Asia/Taipei":
		loc, err := time.LoadLocation("Asia/Taipei")
		if err != nil {
			panic(err)
		}

		// Convert the UTC time to the location's time zone.
		localTime := reportTimeUTC.In(loc)
		reportTimeLocal := localTime.Format("2006-01-02 15:04:05")

		snsPayload := fmt.Sprintf(`
The server has received an alarm report from one of the registered devices.

==============================

【Report Record UUID】
 • %s

【Device UUID】
 • %s

【Custom Device ID】
%s

【Custom Device Name】
%s

【Report Time】
 • %s

【Report Type】
 • %d

==============================

【Report Info】

 %s

==============================

Visit https://seedling.mwnl.ce.ncu.edu.tw for more details.
`, deviceReport.ID, deviceReport.DeviceID, device.DataFields["device_id"], device.DataFields["device_name"], reportTimeLocal, *deviceReport.Type, deviceReport.Info)

		snsSubject := fmt.Sprintf("【育苗系統 Alarm】%s (%s)", device.DataFields["device_name"], device.DataFields["device_id"])

		sns.PublishSnsMessage(snsPayload, snsSubject, "", "", "", "")
	}

	log.Info().Msgf("Device report has been saved. Report ID: %s", deviceReport.ID)
}

func HandleDeviceReportImageUplink(deviceId string, reportedAt int, imageAction string, chunkID int, payload []byte) {

	if imageAction == "chunk" {

		if _, exists := img_list[deviceId]; !exists {
			img_list[deviceId] = &ImageBuffer{chunks: make(map[int][]byte)}
		}

		buf := img_list[deviceId]
		buf.mutex.Lock()
		buf.chunks[chunkID] = payload
		buf.mutex.Unlock()

	} else if imageAction == "done" {

		responseTopic := fmt.Sprintf("%s/%s", MQTT_DEVICE_REPORT_IMAGE_RESPONSE_DOWNLINK_PREFIX, deviceId)
		serverReceivedAt := int(time.Now().Unix())

		buf, exists := img_list[deviceId]
		if !exists {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Info().Msgf("No image found for %s", deviceId)
			return
		}
		
		device, err := FindSingleByID[Device](DevicesCollection, deviceId, nil)
		if err != nil {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Error().Msgf("Cannot find device for the provided device ID: %s", deviceId)
			return
		}

		// Define the file path
		var device_name = fmt.Sprintf("%s", device.DataFields["device_name"])
		var filename = fmt.Sprintf("%d.jpg", reportedAt)
		folderPath := filepath.Join("media/images", device_name)
		filePath := filepath.Join(folderPath, filename)

		// Ensure the file path exists. Create folders if not.
		err = os.MkdirAll(folderPath, os.ModePerm)
		if err != nil {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Error().Msg(err.Error())
			return
		}

		// Reassemble image
		buf.mutex.Lock()
		keys := make([]int, 0, len(buf.chunks))
		for k := range buf.chunks {
			keys = append(keys, k)
		}
		sort.Ints(keys)

		// create the target file
		targetFile, err := os.Create(filePath)
		if err != nil {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Error().Msg(err.Error())
			buf.mutex.Unlock()
			return
		}
		defer targetFile.Close()

		// Write the image data to the file
		for _, k := range keys {
			targetFile.Write(buf.chunks[k])
		}
		buf.mutex.Unlock()

		// change the file permissions
		err = os.Chmod(filePath, 0644)
		if err != nil {
			log.Error().Msg(err.Error())
		}

		if !isValidJpegImage(filePath) {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_BAD_IMAGE)
			log.Error().Msgf("Invalid image received.")
			return
		}

		SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_OK)

		log.Info().Msgf("Image has been saved. File path: %s", filePath)

		delete(img_list, deviceId)

		/*
		// File size limit assertion (discard if too large)
		size := len(payload)
		if size > MAX_REPORT_IMAGE_SIZE_BYTES {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_IMAGE_TOO_LARGE)
			log.Error().Msg(fmt.Sprintf("Image size too large (%d) and will be discarded. (Max: %d MB)", size, MAX_REPORT_IMAGE_SIZE_BYTES/1024/1024))
			return
		}

		if !isValidJpegImageBytes(payload) {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_BAD_IMAGE)
			//log.Error().Msg("Invalid image bytes received.")
			log.Error().Msgf("Invalid image bytes(%d) received.", size)
			return
		}

		device, err := FindSingleByID[Device](DevicesCollection, deviceId, nil)
		if err != nil {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Error().Msgf("Cannot find device for the provided device ID: %s", deviceId)
			return
		}

		// Define the file path
		var device_name = fmt.Sprintf("%s", device.DataFields["device_name"])
		var filename = fmt.Sprintf("%d.jpg", reportedAt)
		//folderPath := filepath.Join("media/images", deviceId)
		folderPath := filepath.Join("media/images", device_name)
		filePath := filepath.Join(folderPath, filename)

		// Ensure the file path exists. Create folders if not.
		err = os.MkdirAll(folderPath, os.ModePerm)
		if err != nil {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Error().Msg(err.Error())
			return
		}

		// Write the image data to the file
		err = os.WriteFile(filePath, payload, 0644)
		if err != nil {
			SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_SERVER_ERROR)
			log.Error().Msg(err.Error())
			return
		}

		SendMQTTReportResponse(responseTopic, deviceId, reportedAt, serverReceivedAt, MQTT_DEVICE_REPORT_STATUS_OK)

		log.Info().Msgf("Image has been saved. File path: %s", filePath)
		*/
	}
}

func DirectMQTTQueryUplinkToSocketIO(deviceID string, message string) {
	socketio.EmitDeviceQueryUplinkMessage(deviceID, message)
}

func DirectMQTTCommandUplinkToSocketIO(deviceID string, message string) {
	socketio.EmitDeviceCommandUplinkMessage(deviceID, message)
}

func HandleGetDeviceUUIDByCustomDeviceID(customDeviceID string) {
	responseTopic := fmt.Sprintf("downlink/uuid-lookup/%s", customDeviceID)
	qos := byte(cfg.GetConfigValueAsInt("MQTT_QOS"))

	var responsePayload = make(map[string]interface{})

	device, err := FindSingleByField[Device](DevicesCollection, "data_fields.device_id", customDeviceID, nil)
	if err != nil {
		log.Err(err).Msg("")

		responsePayload["device_uuid"] = nil
		finalResponsePayload, _ := json.Marshal(responsePayload)

		GetMqttClient().Publish(responseTopic, qos, false, finalResponsePayload)
		return
	}

	responsePayload["device_uuid"] = device.ID
	finalResponsePayload, _ := json.Marshal(responsePayload)

	GetMqttClient().Publish(responseTopic, qos, false, finalResponsePayload)
}

func SendMQTTReportResponse(responseTopic string, deviceID string, reportedAt int, serverReceivedAt int, serverStatus int) {
	response := make(map[string]interface{})
	response["reported_at"] = reportedAt
	response["server_received_at"] = serverReceivedAt
	response["server_status"] = serverStatus

	jsonPayload, err := json.Marshal(response)
	if err != nil {
		return
	}

	PublishMessageToTopic(responseTopic, string(jsonPayload))
}

// isValidImageBytes checks whether the provided image bytes (in JPEG format)
// represent a valid image.
/*
func isValidJpegImageBytes(imgBytes []byte) bool {
	// image.Decode will try to decode the image. If it fails, it's not a valid image.

	_, err1 := jpeg.DecodeConfig(bytes.NewReader(imgBytes))
	_, _, err2 := image.Decode(bytes.NewReader(imgBytes))

	log.Err(err1).Msg("")
	log.Err(err2).Msg("")

	return err1 == nil && err2 == nil
}
*/

func isValidJpegImage(filepath string) bool {

	imageFile, err := os.Open(filepath)
	if err != nil {
		log.Info().Msgf("Failed to open %s to verify the image", filepath)
		return false
	}
	defer imageFile.Close()

	// check the header
	if _, err := jpeg.DecodeConfig(imageFile); err != nil {
		log.Info().Msgf("invalid JPEG header for %s: %v", filepath, err)
		return false
	}

	// must move the file pointer back to beginning for decoding
	 _, err = imageFile.Seek(0, 0)
    if err != nil {
        log.Info().Msgf("Failed to reset file pointer for %s: %v", filepath, err)
        return false
    }

	// verify the image file is valid or not
	if _, err := jpeg.Decode(imageFile); err != nil {
		log.Info().Msgf("invalid JPEG data for %s: %v", filepath, err)
		return false
	}

	return true
}
