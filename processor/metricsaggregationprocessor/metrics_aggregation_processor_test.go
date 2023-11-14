package metricsaggregationprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.robot.car/cruise/cruise-otel-collector/processor/metricsaggregationprocessor/internal/testutil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

var testBaseTime = time.Date(2023, time.September, 18, 14, 0, 0, 0, time.UTC)

type gaugeTestMetric struct {
	name       string
	attributes map[string]any
	value      float64
	timestamp  time.Time
}

type histogramTestMetric struct {
	name       string
	attributes map[string]any
	bounds     []float64
	counts     []uint64
	sum        float64
	count      uint64
	min        float64
	max        float64
	timestamp  time.Time
}

type metricEvent struct {
	metricIn           pmetric.Metrics
	processTime        time.Time
	validateOutputFunc func(t *testing.T, metrics pmetric.Metrics)
}

func generateGaugeMetrics(tm []gaugeTestMetric) pmetric.Metrics {
	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for _, tm := range tm {
		m := ms.AppendEmpty()
		m.SetName(tm.name)
		dps := m.SetEmptyGauge().DataPoints()
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(tm.timestamp))
		dp.SetDoubleValue(tm.value)
		dp.Attributes().FromRaw(tm.attributes)
	}
	return md
}

func generateHistogramMetric(tm histogramTestMetric) pmetric.Metrics {
	pmetric.NewGauge()
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(tm.name)
	dps := m.SetEmptyHistogram().DataPoints()
	dp := dps.AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(tm.timestamp))
	dp.SetCount(tm.count)
	dp.SetSum(tm.sum)
	dp.BucketCounts().FromRaw(tm.counts)
	dp.ExplicitBounds().FromRaw(tm.bounds)
	dp.Attributes().FromRaw(tm.attributes)
	dp.SetMin(tm.min)
	dp.SetMax(tm.max)
	return md
}

type metricsAggregationTest struct {
	name              string
	config            *Config
	metricEvents      []metricEvent
	endValidationFunc func(metrics pmetric.Metrics)
}

func TestMetricsAggregationProcessor(t *testing.T) {
	testCases := []metricsAggregationTest{
		{
			name: "Test Bucketize Aggregation",
			config: &Config{
				AggregationPeriod: 1 * time.Minute,
				MaxStaleness:      3 * time.Minute,
				Aggregations: []MetricAggregationConfig{
					{
						MetricName:          "system.cpu.utilization",
						MatchType:           Strict,
						AggregationType:     Bucketize,
						NewName:             "system.cpu.utilization.bucket",
						DataPointAttributes: []string{"type"},
						LowerBound:          0,
						UpperBound:          100,
						BucketCount:         10,
						KeepOriginal:        false,
					},
				},
			},
			metricEvents: []metricEvent{
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":     "a",
								"location": "west",
								"uid":      1234893,
							},
							timestamp: testBaseTime.Add(-10 * time.Second),
							value:     12,
						},
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":     "a",
								"location": "west",
								"uid":      3298434,
							},
							timestamp: testBaseTime.Add(-12 * time.Second),
							value:     34,
						},
					}),
					processTime: testBaseTime,
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":     "a",
								"location": "west",
								"uid":      43597239,
							},
							timestamp: testBaseTime.Add(-7 * time.Second),
							value:     40,
						},
					}),
					processTime: testBaseTime.Add(5 * time.Second),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":            "a",
								"randomattribute": 2,
							},
							value:     50,
							timestamp: testBaseTime.Add(-7 * time.Second),
						},
					}),
					processTime: testBaseTime.Add(5 * time.Minute),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						desiredMetrics := generateHistogramMetric(
							histogramTestMetric{
								name: "system.cpu.utilization.bucket",
								attributes: map[string]any{
									"type": "a",
								},
								timestamp: time.Unix(0, 1695045570000000000),
								sum:       86,
								count:     3,
								min:       12,
								max:       40,
								bounds:    []float64{10, 20, 30, 40, 50, 60, 70, 80, 90},
								counts:    []uint64{0, 1, 0, 2, 0, 0, 0, 0, 0, 0},
							},
						)
						require.Equal(t, desiredMetrics.MetricCount(), metrics.MetricCount())
						desiredMetric := desiredMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						actualMetric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						require.Equal(t, desiredMetric.Name(), actualMetric.Name())
						require.Equal(t, desiredMetric.Type(), actualMetric.Type())
						require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).Timestamp(), actualMetric.Histogram().DataPoints().At(0).Timestamp())
						require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).Count(), actualMetric.Histogram().DataPoints().At(0).Count())
						require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).Sum(), actualMetric.Histogram().DataPoints().At(0).Sum())
						require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).Min(), actualMetric.Histogram().DataPoints().At(0).Min())
						require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).Max(), actualMetric.Histogram().DataPoints().At(0).Max())
						require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).BucketCounts().Len(), actualMetric.Histogram().DataPoints().At(0).BucketCounts().Len())
						for i := 0; i < desiredMetric.Histogram().DataPoints().At(0).BucketCounts().Len(); i++ {
							require.Equal(t, desiredMetric.Histogram().DataPoints().At(0).BucketCounts().At(i), actualMetric.Histogram().DataPoints().At(0).BucketCounts().At(i))
						}
					},
				},
			},
		},
		{
			name: "Test Average Aggregation",
			config: &Config{
				AggregationPeriod: 1 * time.Minute,
				MaxStaleness:      3 * time.Minute,
				Aggregations: []MetricAggregationConfig{
					{
						MetricName:          "system.cpu.utilization",
						MatchType:           Strict,
						AggregationType:     Average,
						NewName:             "system.cpu.utilization.average",
						DataPointAttributes: []string{"type"},
						KeepOriginal:        false,
					},
				},
			},
			metricEvents: []metricEvent{
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":     "a",
								"location": "west",
								"uid":      1234893,
							},
							timestamp: testBaseTime.Add(-10 * time.Second),
							value:     12.5,
						},
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":     "a",
								"location": "west",
								"uid":      3298434,
							},
							timestamp: testBaseTime.Add(-12 * time.Second),
							value:     34.5,
						},
					}),
					processTime: testBaseTime,
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":     "a",
								"location": "west",
								"uid":      43597239,
							},
							timestamp: testBaseTime.Add(-7 * time.Second),
							value:     13,
						},
					}),
					processTime: testBaseTime.Add(7 * time.Second),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.cpu.utilization",
							attributes: map[string]any{
								"type":            "a",
								"randomattribute": 2,
							},
							value:     50,
							timestamp: testBaseTime.Add(-7 * time.Second),
						},
					}),
					processTime: testBaseTime.Add(5 * time.Minute),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						desiredMetrics := generateGaugeMetrics([]gaugeTestMetric{
							{
								name: "system.cpu.utilization.average",
								attributes: map[string]any{
									"type": "a",
								},
								timestamp: time.Unix(0, 1695045570000000000),
								value:     20,
							},
						})
						require.Equal(t, desiredMetrics.MetricCount(), metrics.MetricCount())
						desiredMetric := desiredMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						actualMetric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						require.Equal(t, desiredMetric.Name(), actualMetric.Name())
						require.Equal(t, desiredMetric.Type(), actualMetric.Type())
						require.Equal(t, desiredMetric.Gauge().DataPoints().At(0).Timestamp(), actualMetric.Gauge().DataPoints().At(0).Timestamp())
						require.Equal(t, desiredMetric.Gauge().DataPoints().At(0).DoubleValue(), actualMetric.Gauge().DataPoints().At(0).DoubleValue())
					},
				},
			},
		},
		{
			name: "Test Max Aggregation",
			config: &Config{
				AggregationPeriod: 2 * time.Minute,
				MaxStaleness:      6 * time.Minute,
				Aggregations: []MetricAggregationConfig{
					{
						MetricName:          `system\.memory\.(.*)`,
						MatchType:           Regexp,
						AggregationType:     Max,
						NewName:             "system.memory.${1}.max",
						DataPointAttributes: []string{"state"},
						KeepOriginal:        false,
					},
				},
			},
			metricEvents: []metricEvent{
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "dsgeaw",
							},
							timestamp: testBaseTime.Add(-20 * time.Second),
							value:     12.5,
						},
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "sdfaer",
							},
							timestamp: testBaseTime.Add(-15 * time.Second),
							value:     34.5,
						},
					}),
					processTime: testBaseTime,
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "jgdfre",
							},
							timestamp: testBaseTime.Add(-4 * time.Second),
							value:     13,
						},
					}),
					processTime: testBaseTime.Add(40 * time.Second),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "jgdfre",
							},
							value:     50,
							timestamp: testBaseTime.Add(6 * time.Minute),
						},
					}),
					processTime: testBaseTime.Add(8 * time.Minute),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						desiredMetrics := generateGaugeMetrics([]gaugeTestMetric{
							{
								name: "system.memory.utilization.max",
								attributes: map[string]any{
									"state": "buffered",
								},
								timestamp: time.Unix(0, 1695045540000000000),
								value:     34.5,
							},
						})
						require.Equal(t, desiredMetrics.MetricCount(), metrics.MetricCount())
						desiredMetric := desiredMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						actualMetric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						require.Equal(t, desiredMetric.Name(), actualMetric.Name())
						require.Equal(t, desiredMetric.Type(), actualMetric.Type())
						require.Equal(t, desiredMetric.Gauge().DataPoints().At(0).Timestamp(), actualMetric.Gauge().DataPoints().At(0).Timestamp())
						require.Equal(t, desiredMetric.Gauge().DataPoints().At(0).DoubleValue(), actualMetric.Gauge().DataPoints().At(0).DoubleValue())
					},
				},
			},
		},
		{
			name: "Test Min Aggregation",
			config: &Config{
				AggregationPeriod: 2 * time.Minute,
				MaxStaleness:      6 * time.Minute,
				Aggregations: []MetricAggregationConfig{
					{
						MetricName:          `system\..*\.utilization`,
						MatchType:           Regexp,
						AggregationType:     Min,
						DataPointAttributes: []string{"state"},
						KeepOriginal:        false,
					},
				},
			},
			metricEvents: []metricEvent{
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "dsgeaw",
							},
							timestamp: testBaseTime.Add(-20 * time.Second),
							value:     12.5,
						},
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "sdfaer",
							},
							timestamp: testBaseTime.Add(-15 * time.Second),
							value:     34.5,
						},
					}),
					processTime: testBaseTime,
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "jgdfre",
							},
							timestamp: testBaseTime.Add(-4 * time.Second),
							value:     13,
						},
					}),
					processTime: testBaseTime.Add(40 * time.Second),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						require.Equal(t, 0, metrics.MetricCount())
					},
				},
				{
					metricIn: generateGaugeMetrics([]gaugeTestMetric{
						{
							name: "system.memory.utilization",
							attributes: map[string]any{
								"state":    "buffered",
								"instance": "jgdfre",
							},
							value:     50,
							timestamp: testBaseTime.Add(6 * time.Minute),
						},
					}),
					processTime: testBaseTime.Add(8 * time.Minute),
					validateOutputFunc: func(t *testing.T, metrics pmetric.Metrics) {
						desiredMetrics := generateGaugeMetrics([]gaugeTestMetric{
							{
								name: "system.memory.utilization",
								attributes: map[string]any{
									"state": "buffered",
								},
								timestamp: time.Unix(0, 1695045540000000000),
								value:     12.5,
							},
						})
						require.Equal(t, desiredMetrics.MetricCount(), metrics.MetricCount())
						desiredMetric := desiredMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						actualMetric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
						require.Equal(t, desiredMetric.Name(), actualMetric.Name())
						require.Equal(t, desiredMetric.Type(), actualMetric.Type())
						require.Equal(t, desiredMetric.Gauge().DataPoints().At(0).Timestamp(), actualMetric.Gauge().DataPoints().At(0).Timestamp())
						require.Equal(t, desiredMetric.Gauge().DataPoints().At(0).DoubleValue(), actualMetric.Gauge().DataPoints().At(0).DoubleValue())
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clock := &testutil.MockClock{CurrentTime: testBaseTime, AdvanceCh: make(chan time.Duration)}
			logger := zap.NewNop()
			processor, err := newMetricsAggregationProcessor(tc.config, logger, clock)
			go processor.startFlushInterval()
			if err != nil {
				t.Fatalf("Failed to create metrics aggregation processor: %v", err)
			}
			for _, me := range tc.metricEvents {
				clock.Advance(me.processTime.Sub(clock.Now()))

				time.Sleep(100 * time.Millisecond)
				outMetrics, err := processor.processMetrics(context.Background(), me.metricIn)
				if err != nil {
					t.Fatalf("Failed to process metrics: %v", err)
				}
				me.validateOutputFunc(t, outMetrics)
			}
		})
	}
}
