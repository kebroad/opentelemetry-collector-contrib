package metricsaggregationprocessor

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.robot.car/cruise/cruise-otel-collector/processor/metricsaggregationprocessor/internal/metadata"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

var ErrInvalidConfig = errors.New("invalid config")

// NewFactory creates a new factory for the metrics aggregation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment))
}

// createDefaultConfig creates the default configuration for the processor.
func createDefaultConfig() component.Config {
	return &Config{}
}

// createProcessor creates a metrics aggregation processor from the configuration.
func createMetricsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	rCfg, ok := cfg.(*Config)
	if !ok {
		return nil, ErrInvalidConfig
	}
	if err := validateConfiguration(rCfg); err != nil {
		return nil, err
	}

	metricsProcessor, err := newMetricsAggregationProcessor(rCfg, set.Logger, &realClock{})
	if err != nil {
		return nil, err
	}

	return processorhelper.NewMetricsProcessor(
		ctx,
		set,
		cfg,
		nextConsumer,
		metricsProcessor.processMetrics,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(metricsProcessor.Start),
		processorhelper.WithShutdown(metricsProcessor.Shutdown),
	)
}
