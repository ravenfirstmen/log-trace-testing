package messaging

import (
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type RequestProvider interface {
	get() map[string]string
}

type FieldsToProvider interface {
	get() log.Fields
}

type RequestIdProvider struct {
}

func NewRequestIdProvider() *RequestIdProvider {
	return &RequestIdProvider{}
}

func (r RequestIdProvider) get() map[string]string {
	return map[string]string{"requestId": uuid.NewString()}
}

type SubjectNameProvider struct {
	msg *nats.Msg
}

func NewSubjectNameProvider(msg *nats.Msg) *SubjectNameProvider {
	return &SubjectNameProvider{msg: msg}
}

func (s SubjectNameProvider) get() map[string]string {
	return map[string]string{"subject": s.msg.Subject}
}

type TraceIdProvider struct {
	logger *log.Entry
	msg    *nats.Msg
}

func NewTraceIdProvider(logger *log.Entry, msg *nats.Msg) *TraceIdProvider {
	return &TraceIdProvider{
		logger: logger,
		msg:    msg,
	}
}

func (t TraceIdProvider) get() map[string]string {
	var trace map[string]string = nil

	if t.msg.Header != nil {
		traceId := t.msg.Header.Get("trace-id")
		if traceId != "" {
			t.logger.WithField("trace_id", traceId).Info("Found a trace id on message header, reusing it")
			trace = map[string]string{"trace_id": traceId}
		}
	}

	if trace == nil {
		newTraceId := uuid.NewString()
		t.logger.WithField("trace_id", newTraceId).Info("No trace id found on message, generating new one")
		trace = map[string]string{"trace_id": newTraceId}
	}

	return trace
}

type KarateTestIdProvider struct {
	logger *log.Entry
	msg    *nats.Msg
}

func NewKarateTestIdProvider(logger *log.Entry, msg *nats.Msg) *KarateTestIdProvider {
	return &KarateTestIdProvider{
		logger: logger,
		msg:    msg,
	}
}

func (t KarateTestIdProvider) get() map[string]string {
	var trace map[string]string = nil

	if t.msg.Header != nil {
		testId := t.msg.Header.Get("karate-test-id")
		if testId != "" {
			trace = map[string]string{
				"karate-test-id":      testId,
				"is-integration-test": strconv.FormatBool(true),
			}
		}
	}
	if trace == nil {
		trace = map[string]string{"is-integration-test": strconv.FormatBool(false)}
	}

	return trace
}

type RequestFieldsToLogProvider struct {
	logger *log.Entry
	msg    *nats.Msg
}

func NewRequestFieldsToLogProvider(logger *log.Entry, msg *nats.Msg) *RequestFieldsToLogProvider {
	return &RequestFieldsToLogProvider{
		logger: logger,
		msg:    msg,
	}
}

func (r RequestFieldsToLogProvider) get() log.Fields {

	providers := []RequestProvider{
		NewSubjectNameProvider(r.msg),
		NewRequestIdProvider(),
		//NewTraceIdProvider(r.logger, r.msg),
		NewKarateTestIdProvider(r.logger, r.msg),
	}

	fields := make(log.Fields)

	for _, provider := range providers {
		for a, field := range provider.get() {
			fields[a] = field
		}
	}

	return fields
}
