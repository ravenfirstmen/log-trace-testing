package messaging

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"log-trace-testing/pkg/db"
)

const (
	// subjects
	createSubject = "create"
	listSubject   = "list"
	deleteSubject = "delete"
	//
	tableName = "my-table"
)

var natsSubjects = []string{createSubject, listSubject, deleteSubject}

type MessageProcessor interface {
	Init() bool
	Shutdown() bool
	Subscribe() bool
}

type NatsMessageProcessor struct {
	logger     *log.Entry
	connection *nats.Conn
	tracer     trace.Tracer
	// public
	URL string
}

func NewNatsMessageProcessor(logger *log.Entry, tracer trace.Tracer, url string) *NatsMessageProcessor {
	return &NatsMessageProcessor{
		logger: logger.WithFields(log.Fields{
			"cluster": url,
		}),
		URL:    url,
		tracer: tracer,
	}
}

func (n *NatsMessageProcessor) Init() bool {
	logger := n.logger

	logger.Info("Connecting to NATS server...")
	con, err := nats.Connect(n.URL, nats.Token("s3cr3t"))
	if err != nil {
		logger.WithError(err).Error("Failed to connect to NATS server")
		return false
	}
	logger.Info("Successful connected to NATS server...")
	n.connection = con
	return true
}

func (n *NatsMessageProcessor) Shutdown() bool {
	logger := n.logger

	if n.connection != nil {
		logger.Info("Shutting down NATS connection...")
		n.connection.Close()
		logger.Info("Successful NATS connection shut down...")
		n.connection = nil
		return true
	}
	logger.Warn("No active connection to server! No shutdown done...")
	return false
}

func (n *NatsMessageProcessor) Subscribe() bool {
	logger := n.logger

	for _, subject := range natsSubjects {
		_, err := n.connection.Subscribe(subject, n.messageHandler)
		if err != nil {
			logger.WithError(err).WithFields(log.Fields{
				"subject": subject,
			}).Error("Could not subscribe to subject")
			return false
		}
	}
	logger.WithFields(log.Fields{
		"subjects": natsSubjects,
	}).Info("Successfully subscribed to subject(s)")
	return true
}

func (n *NatsMessageProcessor) getOrCreateSpanForMessageProcessing(logger *log.Entry, context context.Context, msg *nats.Msg, name string) (context.Context, trace.Span) {
	ctx := otel.GetTextMapPropagator().Extract(context, NatsHeaderCarrier(msg.Header))
	testSpan := trace.SpanFromContext(ctx)
	if !testSpan.SpanContext().IsValid() {
		logger.WithContext(context).Info("Trace not found, generating new one.")
		return n.tracer.Start(context, name, trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindConsumer))
	}
	ctx, span := n.tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindConsumer))
	logger.WithContext(context).Info(fmt.Sprintf("Trace found with value: %s. reusing it", span.SpanContext().TraceID().String()))

	return ctx, span
}

func (n *NatsMessageProcessor) messageHandler(msg *nats.Msg) {
	logger := n.logger.WithFields(NewRequestFieldsToLogProvider(n.logger, msg).get())
	ctx, span := n.getOrCreateSpanForMessageProcessing(logger, context.Background(), msg, "Process message")
	defer span.End()

	requestLogger := logger.WithContext(ctx)

	debugFields := log.Fields{ // apenas para debug
		"headers": msg.Header,
		"data":    string(msg.Data),
	}
	requestLogger.WithFields(debugFields).Info("Starting processing message")
	defer requestLogger.WithFields(debugFields).Info("Ending processing message")

	repository, err := db.NewDynamoDbRepository(ctx, n.tracer, requestLogger, tableName)
	if err != nil {
		requestLogger.WithError(err).Error("Failed to create repository")
		return
	}

	switch msg.Subject {
	case createSubject:
		processCreateMessage(requestLogger, repository, msg)
		break
	case listSubject:
		processListMessage(requestLogger, repository, msg)
		break
	case deleteSubject:
		processDeleteMessage(requestLogger, repository, msg)
		break
	default:
		requestLogger.WithFields(debugFields).Error("Unknown message subject. THIS SHOULD NEVER HAPPEN!")
		break
	}

	//otel.GetTextMapPropagator().Inject(ctx, DebuggerCarrier{})
}

func processCreateMessage(logger *log.Entry, repository db.Repository, msg *nats.Msg) {

	logger.Info("Starting processing create record message")
	defer logger.Info("End processing create record message")

	message, err := deserializeCreateMessage(msg)
	if err != nil {
		logger.WithError(err).Error("Failed to deserialize message")
		return
	}

	err = repository.Create(message.Key, message.Info)
	if err != nil {
		logger.WithError(err).Error("Failed to create record")
		return
	}
}

func processListMessage(logger *log.Entry, repository db.Repository, msg *nats.Msg) {

	logger.Info("Starting processing list records message")
	defer logger.Info("End processing list records message")

	message, err := deserializeListMessage(msg)
	if err != nil {
		logger.WithError(err).Error("Failed to deserialize message")
		return
	}

	err = repository.List(message.Key)
	if err != nil {
		logger.WithError(err).Error("Failed to list records ")
		return
	}
}

func processDeleteMessage(logger *log.Entry, repository db.Repository, msg *nats.Msg) {

	logger.Info("Starting processing delete message")
	defer logger.Info("End processing delete message")

	message, err := deserializeDeleteMessage(msg)
	if err != nil {
		logger.WithError(err).Error("Failed to deserialize message")
		return
	}

	err = repository.Delete(message.Key)
	if err != nil {
		logger.WithError(err).Error("Failed to delete record")
		return
	}
}
