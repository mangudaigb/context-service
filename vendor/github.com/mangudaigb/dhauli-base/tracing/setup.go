package tracing

import (
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func InitTracerProvider(cfg *config.Config, log *logger.Logger) *sdktrace.TracerProvider {

	if !cfg.Tracing.Enabled {
		log.Info("Tracing is disabled")
		ntp := sdktrace.NewTracerProvider(
			sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(tracetest.NewNoopExporter())),
			sdktrace.WithResource(resource.Empty()),
		)
		otel.SetTracerProvider(ntp)

		return ntp
	}

	serviceName := cfg.Server.Name
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal("failed to initialize stdout exporter:", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{}) // W3C Trace Context

	return tp
}
