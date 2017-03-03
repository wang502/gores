package gores

import (
    "os"
    "log"
)

const (
    Debug = 1
    Trace = 2
)

var logLevel int = 0
var logger *log.Logger

func init() {
    logger = log.New(os.Stdout, "[Gores]", log.Lmicroseconds)
}

func LogLevel() int {
    return logLevel
}

func SetLogLevel(level int) {
    logLevel = level
}

// Debugging

func debug(v ...interface{}) {
    if logLevel >= Debug {
        logger.Print(v...)
    }
}

func debugf(format string, v ...interface{}) {
    if logLevel >= Debug {
        logger.Printf(format, v...)
    }
}

func debugln(v ...interface{}){
    if logLevel >= Debug {
        logger.Println(v...)
    }
}

// Trace Level Debugging

func trace(v ...interface{}){
    if logLevel >= Trace {
        logger.Print(v...)
    }
}

func tracef(format string, v ...interface{}) {
    if logLevel >= Trace {
        logger.Printf(format, v...)
    }
}

func traceln(v ...interface{}) {
    if logLevel >= Trace {
        logger.Println(v...)
    }
}
