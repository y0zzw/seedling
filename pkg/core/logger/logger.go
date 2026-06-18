package logger

import (
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/rs/zerolog"
)

var infoColor = color.New(color.BgCyan, color.FgBlack)
var errColor = color.New(color.BgRed, color.FgBlack)
var fatalColor = color.New(color.BgMagenta, color.FgWhite)
var debugColor = color.New(color.BgBlack, color.FgWhite)

func GetLogger() zerolog.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	logWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.TimeOnly}
	// logWriter.FormatLevel = func(i interface{}) string {
	// 	txt := strings.ToUpper(fmt.Sprintf("| %-6s|", i))

	// 	switch i {
	// 	case zerolog.FatalLevel:
	// 		return fatalColor.Sprintf("%s", txt)
	// 	case zerolog.ErrorLevel:
	// 		return errColor.Sprintf("%s", txt)
	// 	case zerolog.InfoLevel:
	// 		return infoColor.Sprintf("%s", txt)
	// 	default:
	// 		return debugColor.Sprintf("%s", txt)
	// 	}
	// }

	return zerolog.New(logWriter).With().Timestamp().Caller().Logger()
}
