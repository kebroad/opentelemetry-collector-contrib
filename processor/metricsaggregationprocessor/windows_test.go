package metricsaggregationprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestGenerateWindowKey(t *testing.T) {
	metric := pmetric.NewMetric()
	metric.SetName("test")
	metric.SetEmptyGauge()
	attributes := pcommon.NewMap()
	attributes.FromRaw(map[string]any{
		"atest": "test",
		"btest": "test2",
	})
	startTime := pcommon.NewTimestampFromTime(time.Now())
	windowKey := generateWindowKey(metric, attributes, startTime)
	require.Equal(t, windowKey.Name, "test")
	require.Equal(t, windowKey.Attributes, "atest=test,btest=test2")
	require.Equal(t, windowKey.Type, pmetric.MetricTypeGauge)
	require.Equal(t, windowKey.StartTime, startTime)
}

func TestGetRoundedStartTime(t *testing.T){
	startTime := pcommon.NewTimestampFromTime(time.Date(2023, time.September, 18, 14, 4, 21, 39, time.UTC))
	roundedStartTime := getRoundedStartTime(startTime, 10 * time.Second)
	expectedTime := pcommon.NewTimestampFromTime(time.Date(2023, time.September, 18, 14, 4, 20, 00, time.UTC))
	require.Equal(t, roundedStartTime.AsTime(), expectedTime.AsTime())
}