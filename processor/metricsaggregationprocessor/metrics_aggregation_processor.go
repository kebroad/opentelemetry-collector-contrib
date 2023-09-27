package metricsaggregationprocessor

import (
	"context"
	"regexp"
	"slices"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type ContextKey string

const (
	CurrentTimeContextKey ContextKey = "currentTime"
)

type metricsAggregationProcessor struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	next                consumer.Metrics
	clock               Clock
	compiledPatterns    map[string]*regexp.Regexp
	logger              *zap.Logger
	config              *Config
	flushedMetrics      pmetric.MetricSlice
	flushedMetricsMutex sync.RWMutex
	windows             map[windowKey]*aggregatedWindow
	windowsMutex        sync.RWMutex
}

func newMetricsAggregationProcessor(cfg *Config, logger *zap.Logger) (*metricsAggregationProcessor, error) {
	compiledPatterns := make(map[string]*regexp.Regexp)
	for _, aggregationConfig := range cfg.Aggregations {
		if aggregationConfig.MatchType == Regexp {
			pattern, err := regexp.Compile(aggregationConfig.MetricName)
			if err != nil {
				logger.Error("Failed to compile regex pattern for metric name", zap.String("metric_name", aggregationConfig.MetricName), zap.Error(err))
				return nil, err
			}
			compiledPatterns[aggregationConfig.MetricName] = pattern
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &metricsAggregationProcessor{
		ctx:              ctx,
		cancel:           cancel,
		config:           cfg,
		logger:           logger,
		windows:          make(map[windowKey]*aggregatedWindow),
		clock:            &realClock{},
		flushedMetrics:   pmetric.NewMetricSlice(),
		compiledPatterns: compiledPatterns,
	}, nil
}

func (m *metricsAggregationProcessor) getAggregationConfigForMetric(metric pmetric.Metric) *MetricAggregationConfig {
	for _, aggregationConfig := range m.config.Aggregations {
		matchesMetricName := false
		switch aggregationConfig.MatchType {
		case Strict:
			if aggregationConfig.MetricName == metric.Name() {
				matchesMetricName = true
			}
		case Regexp:
			pattern, exists := m.compiledPatterns[aggregationConfig.MetricName]
			if exists && pattern.MatchString(metric.Name()) {
				matchesMetricName = true
			}
		}
		if matchesMetricName {
			return &aggregationConfig
		}
	}
	return nil
}

func getMatchingAttributes(aggregationConfig *MetricAggregationConfig, attributes pcommon.Map) pcommon.Map {
	// Get matching keys from the attributes that are in the aggregationConfig.DataPointAttributes
	matchingAttributes := pcommon.NewMap()
	attributes.CopyTo(matchingAttributes)
	matchingAttributes.RemoveIf(func(k string, v pcommon.Value) bool {
		return !slices.Contains(aggregationConfig.DataPointAttributes, k)
	})
	return matchingAttributes
}

func (m *metricsAggregationProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	currentTime := m.clock.Now()
	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			metrics := sm.Metrics()
			metrics.RemoveIf(func(metric pmetric.Metric) bool {
				aggregationConfig := m.getAggregationConfigForMetric(metric)
				if aggregationConfig != nil {
					switch metric.Type() {
					case pmetric.MetricTypeGauge:
						m.aggregateGaugeMetric(metric, aggregationConfig, currentTime)
						// case pmetric.MetricTypeSum:
						// 	m.aggregateSumMetric(metric)
						// case pmetric.MetricTypeHistogram:
						// 	m.aggregateHistogramMetric(metric)
						// }
					}
					if !aggregationConfig.KeepOriginal {
						return true
					}
				}
				return false
			})
			return metrics.Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	m.flushedMetricsMutex.Lock()
	if m.flushedMetrics.Len() > 0 {
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		for i := 0; i < m.flushedMetrics.Len(); i++ {
			m.flushedMetrics.At(i).CopyTo(sm.Metrics().AppendEmpty())
		}
		// Clear the flushed metrics
		m.flushedMetrics = pmetric.NewMetricSlice()

	}
	m.flushedMetricsMutex.Unlock()

	return md, nil
}

func (m *metricsAggregationProcessor) Start(ctx context.Context, host component.Host) error {
	go m.flushExpiredWindows()
	return nil
}

func (m *metricsAggregationProcessor) Shutdown(ctx context.Context) error {
	m.cancel()
	return nil
}
