package mqtt

import (
	"time"
	"sync"
	cfg "v-gnms/config"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)

var mqttClient mqtt.Client
var once sync.Once

// Singleton MQTT instance getter
// This initializes the MQTT client for the first time
func GetMqttClient() mqtt.Client {
	once.Do(func() {
		opts := mqtt.NewClientOptions()
		
		opts.AddBroker(cfg.GetConfigValue("MQTT_BROKER_URI"))
		opts.SetUsername(cfg.GetConfigValue("MQTT_USERNAME"))
		opts.SetPassword(cfg.GetConfigValue("MQTT_PASSWORD"))
		opts.SetClientID(cfg.GetConfigValue("MQTT_SELF_CLIENT_ID"))

		opts.OnConnect = handleMqttOnConnect
		opts.OnConnectionLost = handleMqttOnConnectionLost

		opts.SetDefaultPublishHandler(messageHandler)

		mqttClient = mqtt.NewClient(opts)
		
		for {
			if token := mqttClient.Connect(); token.Wait() && token.Error() == nil {				
				log.Info().Msg("connect to broker successfully...")
				return
			} else {				
				if err := token.Error(); err != nil {
					log.Info().Msg(err.Error())
				}
				time.Sleep(3 * time.Second)
			}
		}
		
		/*
		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			log.Fatal().Err(token.Error())
		}
		*/			
	})

	return mqttClient
}

// Default handler for mqtt connect events
func handleMqttOnConnect(client mqtt.Client) {
	client.Subscribe("uplink/#", byte(cfg.GetConfigValueAsInt("MQTT_QOS")), nil)	
}

// Default handler for mqtt connection lost events
func handleMqttOnConnectionLost(client mqtt.Client, err error) {
	log.Info().Msg("MQTT connection lost. Reconnecting...")
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal().Err(token.Error())
	}
}

func PublishMessageToTopic(topic string, message string) {
	token := mqttClient.Publish(topic, byte(cfg.GetConfigValueAsInt("MQTT_QOS")), false, message)
	token.Wait()
}
