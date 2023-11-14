package metricsaggregationprocessor

import (
	"context"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type metricsAggregationProcessor struct {
	clock               Clock
	compiledPatterns    map[string]*regexp.Regexp
	logger              *zap.Logger
	config              *Config
	flushedMetrics      pmetric.MetricSlice
	flushedMetricsMutex sync.RWMutex
	windows             map[windowKey]*aggregatedWindow
	windowsMutex        sync.RWMutex
}

func newMetricsAggregationProcessor(cfg *Config, logger *zap.Logger, clock Clock) (*metricsAggregationProcessor, error) {
	logger.Info(
		"Metric aggregation processor configured",
		zap.String("max_staleness", cfg.MaxStaleness.String()),
		zap.String("aggregation_period", cfg.AggregationPeriod.String()),
		zap.Int("number_of_aggregations", len(cfg.Aggregations)),
	)

	for _, aggregationConfig := range cfg.Aggregations {
		logger.Info(
			"Metric aggregation configured",
			zap.String("metric_name", aggregationConfig.MetricName),
			zap.String("match_type", string(aggregationConfig.MatchType)),
			zap.String("aggregation_type", string(aggregationConfig.AggregationType)),
			zap.String("new_name", aggregationConfig.NewName),
			zap.Bool("keep_original", aggregationConfig.KeepOriginal),
			zap.String("data_point_attributes", strings.Join(aggregationConfig.DataPointAttributes, ",")),
		)
	}

	compiledPatterns := make(map[string]*regexp.Regexp)
	for _, aggregationConfig := range cfg.Aggregations {
		if aggregationConfig.MatchType == Regexp {
			pattern, err := regexp.Compile(aggregationConfig.MetricName)
			if err != nil {
				logger.Error("Failed to compile regex pattern for metric name",
					zap.String("metric_name", aggregationConfig.MetricName),
					zap.Error(err))
				return nil, err
			}
			compiledPatterns[aggregationConfig.MetricName] = pattern
		}
	}

	return &metricsAggregationProcessor{
		config:           cfg,
		logger:           logger,
		windows:          make(map[windowKey]*aggregatedWindow),
		clock:            clock,
		flushedMetrics:   pmetric.NewMetricSlice(),
		compiledPatterns: compiledPatterns,
	}, nil
}

func (m *metricsAggregationProcessor) getAggregationConfigForMetric(metric pmetric.Metric) *MetricAggregationConfig {
	for _, aggregationConfig := range m.config.Aggregations {
		switch aggregationConfig.MatchType {
		case Strict:
			if aggregationConfig.MetricName == metric.Name() {
				return &aggregationConfig
			}
		case Regexp:
			pattern, exists := m.compiledPatterns[aggregationConfig.MetricName]
			if !exists {
				m.logger.Warn("Compiled regex pattern not found for metric name",
					zap.String("metric_name", aggregationConfig.MetricName))
				return nil
			}
			matches := pattern.FindStringSubmatch(metric.Name())
			if matches != nil {
				newConfig := aggregationConfig
				for i, match := range matches[1:] {
					placeholder := "${" + strconv.Itoa(i+1) + "}"
					newConfig.NewName = strings.ReplaceAll(newConfig.NewName, placeholder, match)
				}
				newConfig.MetricName = metric.Name()
				newConfig.MatchType = Strict
				return &newConfig
			}
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

func (m *metricsAggregationProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	currentTime := m.clock.Now()
	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			metrics := sm.Metrics()
			metrics.RemoveIf(func(metric pmetric.Metric) bool {
				aggregationConfig := m.getAggregationConfigForMetric(metric)
				if aggregationConfig != nil {
					m.logger.Debug("Aggregating metric", zap.String("metric_name", metric.Name()))
					if metric.Type() == pmetric.MetricTypeGauge {
						m.aggregateGaugeMetric(metric, aggregationConfig, currentTime)
					} else {
						m.logger.Warn(
							"Unsupported metric type matches aggregation config, not aggregating and sending original metric",
							zap.String("metric_type", string(metric.Type())),
							zap.String("metric_name", metric.Name()),
						)
						return false
					}
					if !aggregationConfig.KeepOriginal {
						return true
					}
				}
				m.logger.Debug("Not aggregating metric", zap.String("metric_name", metric.Name()))
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

func (m *metricsAggregationProcessor) Start(_ context.Context, _ component.Host) error {
	go m.startFlushInterval()
	return nil
}

func (m *metricsAggregationProcessor) Shutdown(_ context.Context) error {
	m.flushWindows(true)
	return nil
}
