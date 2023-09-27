package metricsaggregationprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

const (
	typeStr = "metricsaggregationprocessor"
)

// NewFactory creates a new factory for the metrics aggregation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),)
}

// createDefaultConfig creates the default configuration for the processor.
func createDefaultConfig() component.Config {
	return &Config{
	}
}

// createProcessor creates a metrics aggregation processor from the configuration.
func createMetricsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	rCfg := cfg.(*Config)
	// TODO: Validate config
	if err := validateConfiguration(rCfg); err != nil {
		return nil, err
	}

	metricsProcessor, err := newMetricsAggregationProcessor(rCfg, set.Logger)
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
	)
	
}

func validateConfiguration(config *Config) error {
	return nil
}