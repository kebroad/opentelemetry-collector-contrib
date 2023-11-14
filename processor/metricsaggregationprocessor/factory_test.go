package metricsaggregationprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor"
)

func TestType(t *testing.T) {
	factory := NewFactory()
	pType := factory.Type()
	assert.Equal(t, pType, factory.Type(), component.Type("metricsaggregation"))
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig()
	assert.Equal(t, config, factory.CreateDefaultConfig(), &Config{})
	assert.NoError(t, componenttest.CheckConfigStruct(config))
}

func TestCreateMetricsProcessor(t *testing.T) {
	cfg := createDefaultConfig()
	processor, err := createMetricsProcessor(
		context.Background(),
		processor.CreateSettings{
			component.NewID(component.DataTypeMetrics),
			componenttest.NewNopTelemetrySettings(),
			component.NewDefaultBuildInfo(),
		}, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, processor)
}
