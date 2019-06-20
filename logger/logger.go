package logger

import "go.uber.org/zap"

var log *zap.SugaredLogger

func Debugf(fmt string, args ...interface{}) {
	log.Debugf(fmt, args...)
}

func Fatalf(fmt string, args ...interface{}) {
	log.Fatalf(fmt, args...)
}

func Infof(fmt string, args ...interface{}) {
	log.Infof(fmt, args...)
}

func Init() {
	//TODO: map from env config
	zlog, _ := zap.NewProduction()
	log = zlog.Sugar()
}
