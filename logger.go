// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

type Logger interface {
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

type nullLogger struct{}

func (l *nullLogger) Infof(f string, a ...interface{})  {}
func (l *nullLogger) Debugf(f string, a ...interface{}) {}

var logger Logger = &nullLogger{}
