package main

import (
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type logrusTraceHook struct{}

func (t logrusTraceHook) Levels() []logrus.Level { return logrus.AllLevels }

func (t logrusTraceHook) Fire(entry *logrus.Entry) error {
	loggerCtx := entry.Context
	if loggerCtx == nil {
		return nil
	}
	span := trace.SpanFromContext(loggerCtx)
	if !span.IsRecording() {
		return nil
	}

	spanContext := span.SpanContext()
	if spanContext.HasTraceID() {
		entry.Data["traceid"] = spanContext.TraceID().String()
	}
	if spanContext.HasSpanID() {
		entry.Data["spanid"] = spanContext.SpanID().String()
	}

	// code from: https://github.com/uptrace/opentelemetry-go-extra/tree/main/otellogrus
	// whose license(BSD 2-Clause) can be found at: https://github.com/uptrace/opentelemetry-go-extra/blob/v0.1.18/LICENSE
	attrs := make([]attribute.KeyValue, 0)
	logSeverityKey := attribute.Key("log.severity")
	logMessageKey := attribute.Key("log.message")
	attrs = append(attrs, logSeverityKey.String(entry.Level.String()))
	attrs = append(attrs, logMessageKey.String(entry.Message))

	span.AddEvent("log", trace.WithAttributes(attrs...))
	if entry.Level <= logrus.ErrorLevel {
		span.SetStatus(codes.Error, entry.Message)
	}

	return nil
}

//
