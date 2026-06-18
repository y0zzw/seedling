package socketio

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io/v2/socket"
)

var SOCKETIO_DEVICE_POLLING_UPLINK_EMIT_TOPIC_PREFIX = "uplink/polling"
var SOCKETIO_DEVICE_REPORT_UPLINK_EMIT_TOPIC_PREFIX = "uplink/report"
var SOCKETIO_DEVICE_QUERY_UPLINK_EMIT_TOPIC_PREFIX = "uplink/query"
var SOCKETIO_DEVICE_COMMAND_UPLINK_EMIT_TOPIC_PREFIX = "uplink/command"

var socketioServer *socket.Server
var socketioLocalClient *socket.Socket
var once sync.Once

func GetSocketIOEngine() *socket.Server {
	once.Do(func() {
		// https://github.com/zishang520/socket.io/discussions/34
		socketioServer = socket.NewServer(nil, nil)
		socketioServer.Of(
			regexp.MustCompile(`/\w+`),
			nil,
		).Use(func(client *socket.Socket, next func(*socket.ExtendedError)) {
			utils.Log().Success("MId:%v", client.Connected())
			next(nil)
		}).On("connection", func(clients ...interface{}) {
			socketioLocalClient = clients[0].(*socket.Socket)
			socketioLocalClient.On("event", func(clients ...interface{}) {
				// utils.Log().Success("/ test eventeventeventeventevent%v", clients)
			})
			socketioLocalClient.On("disconnect", func(...interface{}) {
				// utils.Log().Success("/ test disconnect")
			})
		})
	})

	return socketioServer
}

func EmitDevicePollingUplinkMessage(deviceID string, message string) {
	topic := SOCKETIO_DEVICE_POLLING_UPLINK_EMIT_TOPIC_PREFIX
	if socketioLocalClient != nil {
		socketioLocalClient.Emit(topic, message)
	}
}

func EmitDeviceReportUplinkMessage(deviceID string, message string) {
	topic := SOCKETIO_DEVICE_REPORT_UPLINK_EMIT_TOPIC_PREFIX
	if socketioLocalClient != nil {
		socketioLocalClient.Emit(topic, message)
	}
}

func EmitDeviceQueryUplinkMessage(deviceID string, message string) {
	topic := fmt.Sprintf("%s/%s", SOCKETIO_DEVICE_QUERY_UPLINK_EMIT_TOPIC_PREFIX, deviceID)
	if socketioLocalClient != nil {
		socketioLocalClient.Emit(topic, message)
	}
}

func EmitDeviceCommandUplinkMessage(deviceID string, message string) {
	topic := fmt.Sprintf("%s/%s", SOCKETIO_DEVICE_COMMAND_UPLINK_EMIT_TOPIC_PREFIX, deviceID)
	if socketioLocalClient != nil {
		socketioLocalClient.Emit(topic, message)
	}
}

// func broadcast(topic string, message string) {
// 	socketioServer.Engine().Clients().(func(sockets []*socket.RemoteSocket, _ error) {
// 		for _, socket := range sockets {
// 			socket.Emit(topic, message)
// 		}
// 	})
// }
