// Package logger provides compact colored logging for SubStream
package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
)

// Level represents log severity
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	currentLevel = LevelInfo
	useColors    = true
)

func init() {
	// Check if running in non-TTY environment
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		useColors = false
	}
	// Set up default logger flags (remove date, keep time)
	log.SetFlags(0)
}

// SetLevel sets the minimum log level
func SetLevel(l Level) {
	currentLevel = l
}

// DisableColors disables ANSI color output
func DisableColors() {
	useColors = false
}

// formatTime returns compact timestamp: MM/DD HH:MM:SS
func formatTime(t time.Time) string {
	return fmt.Sprintf("%02d/%02d %02d:%02d:%02d",
		t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
}

// colorize returns text with ANSI color codes
func colorize(text string, color string) string {
	if !useColors {
		return text
	}
	return color + text + colorReset
}

// levelColor returns the appropriate color for a log level
func levelColor(level Level) string {
	switch level {
	case LevelDebug:
		return colorGray
	case LevelInfo:
		return colorCyan
	case LevelWarn:
		return colorYellow
	case LevelError:
		return colorRed
	default:
		return colorReset
	}
}

// levelString returns the level prefix
func levelString(level Level) string {
	switch level {
	case LevelDebug:
		return "DBG"
	case LevelInfo:
		return "INF"
	case LevelWarn:
		return "WRN"
	case LevelError:
		return "ERR"
	default:
		return "???"
	}
}

// log prints a log message with the compact format
func logf(level Level, format string, v ...interface{}) {
	if level < currentLevel {
		return
	}

	timestamp := formatTime(time.Now())
	levelStr := levelString(level)
	coloredLevel := colorize(levelStr, levelColor(level))

	msg := fmt.Sprintf(format, v...)
	
	// Preserve existing bracket prefixes like [SUBS], [MIRROR], etc.
	// by detecting them at the start of the message
	if strings.HasPrefix(msg, "[") {
		// Message already has prefix, insert level after timestamp
		fmt.Printf("%s %s %s\n", timestamp, coloredLevel, msg)
	} else {
		fmt.Printf("%s %s %s\n", timestamp, coloredLevel, msg)
	}
}

// Debug logs a debug message
func Debug(format string, v ...interface{}) {
	logf(LevelDebug, format, v...)
}

// Info logs an info message
func Info(format string, v ...interface{}) {
	logf(LevelInfo, format, v...)
}

// Warn logs a warning message
func Warn(format string, v ...interface{}) {
	logf(LevelWarn, format, v...)
}

// Error logs an error message
func Error(format string, v ...interface{}) {
	logf(LevelError, format, v...)
}

// Printf wraps standard log.Printf with compact format (backward compatible)
func Printf(format string, v ...interface{}) {
	logf(LevelInfo, format, v...)
}

// Println wraps standard log.Println with compact format
func Println(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logf(LevelInfo, "%s", msg)
}
