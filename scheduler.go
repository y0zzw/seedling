package main

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"

	"v-gnms/pkg/core/database"
	"v-gnms/pkg/core/mqtt"
	"v-gnms/pkg/models"
)

func startWateringScheduler() {
	for {
		now := time.Now()
		// 計算到今天（或明天）06:00 的等待時間
		next := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, now.Location())
		if !next.After(now) {
			next = next.Add(24 * time.Hour)
		}
		log.Info().Msgf("[Scheduler] 下次澆水檢查時間：%s", next.Format("2006-01-02 15:04:05"))
		time.Sleep(time.Until(next))
		executeWateringIfScheduled()
	}
}

func executeWateringIfScheduled() {
	log.Info().Msg("[Scheduler] 開始檢查今日澆水排程")

	schedules, _, err := database.FindMultiple[models.WateringSchedule](
		database.WateringSchedulesCollection,
		bson.M{},
		nil,
	)
	if err != nil {
		log.Error().Err(err).Msg("[Scheduler] 讀取澆水排程失敗")
		return
	}

	for _, s := range schedules {
		startDate, err := time.Parse("2006-01-02", s.StartDate)
		if err != nil {
			log.Error().Err(err).Msgf("[Scheduler] 日期解析失敗：%s", s.StartDate)
			continue
		}

		dayIndex := int(time.Since(startDate).Hours() / 24)

		if dayIndex < 0 || dayIndex >= s.DayCount {
			log.Info().Msgf("[Scheduler] 設備 %s 排程已過期或未開始 (day=%d)", s.DeviceID, dayIndex)
			continue
		}

		// 檢查 dayIndex 是否在澆水列表中
		shouldWater := false
		for _, d := range s.Schedule {
			if d == dayIndex {
				shouldWater = true
				break
			}
		}

		if shouldWater {
			topic := fmt.Sprintf("devices/%s/command/water", s.DeviceID)
			mqtt.PublishMessageToTopic(topic, `{"action":"water"}`)
			log.Info().Msgf("[Scheduler] ✅ 澆水指令已發送 device=%s day=%d", s.DeviceID, dayIndex)
		} else {
			log.Info().Msgf("[Scheduler] ⏭ 今日不澆水 device=%s day=%d", s.DeviceID, dayIndex)
		}
	}
}
