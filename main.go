package main

import (
	"context"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/yukitsune/lokirus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"log-trace-testing/pkg/messaging"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const appName = "my-test-application"

func prepareForSendingLogsToLoki(logger *log.Logger) {

	tracingLabels := func(entry *log.Entry) lokirus.Labels {
		loggerCtx := entry.Context
		if loggerCtx == nil {
			return nil
		}
		span := trace.SpanFromContext(loggerCtx)
		if !span.IsRecording() {
			return nil
		}

		var labels = lokirus.Labels{}
		spanContext := span.SpanContext()
		if spanContext.HasTraceID() {
			labels["traceid"] = spanContext.TraceID().String()
		}
		if spanContext.HasSpanID() {
			labels["spanid"] = spanContext.SpanID().String()
		}

		return labels
	}

	opts := lokirus.NewLokiHookOptions().
		// Grafana doesn't have a "panic" level, but it does have a "critical" level
		// https://grafana.com/docs/grafana/latest/explore/logs-integration/
		WithLevelMap(lokirus.LevelMap{log.PanicLevel: "critical"}).
		WithFormatter(&log.JSONFormatter{}).
		WithDynamicLabelProvider(tracingLabels).
		WithStaticLabels(lokirus.Labels{
			"app": appName,
		})

	hook := lokirus.NewLokiHookWithOpts(
		"http://localhost:3100",
		opts,
		log.DebugLevel,
		log.InfoLevel,
		log.WarnLevel,
		log.ErrorLevel,
		log.FatalLevel)

	// Configure the logger
	logger.AddHook(hook)
}

func initLogger() *log.Logger {
	logger := log.New()
	logger.SetReportCaller(true)
	logger.SetFormatter(&log.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(log.DebugLevel)

	logger.AddHook(logrusTraceHook{})

	// remover em caso nao ter loki
	prepareForSendingLogsToLoki(logger)

	return logger
}

func initOtelProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpoint("localhost:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v2/traces"),
		otlptracehttp.WithTimeout(5*time.Second),
	)

	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, err
	}

	serviceResources, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(appName),
		),
	)

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(serviceResources),
		sdktrace.WithBatcher(exporter),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.Baggage{},
			propagation.TraceContext{},
		),
	)

	return provider, nil
}

func execute() int {
	ctx := context.Background()

	logger := initLogger().WithFields(log.Fields{
		"application":  appName,
		"execution-id": uuid.NewString(),
	})
	logger.Info("Starting up...")
	defer logger.Info("Ending up...")

	tracerProvider, err := initOtelProvider(ctx)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	defer func() {
		err := tracerProvider.Shutdown(ctx)
		if err != nil {
			logger.WithError(err).Error("Error shutting down tracer...")
		}
	}()

	tracer := otel.Tracer(appName)

	processor := messaging.NewNatsMessageProcessor(logger, tracer, "localhost:4222")
	if !processor.Init() {
		logger.Error("Failed to initialize nats message. Existing!")
		return 1
	}

	if !processor.Subscribe() {
		logger.Error("Failed to subscribe to nats messages. Existing!")
		return 1
	}

	waiForCtrlC := make(chan os.Signal, 1)
	signal.Notify(waiForCtrlC, os.Interrupt, syscall.SIGTERM)
	<-waiForCtrlC
	success := processor.Shutdown()
	if !success {
		logger.Error("Failed to shutdown nats message. Exiting...")
		return 1
	}
	return 0
}

func main() {

	os.Exit(execute())
}

// https://www.elastic.co/observability-labs/blog/manual-instrumentation-apps-opentelemetry
// https://www.komu.engineer/blogs/11/opentelemetry-and-go
