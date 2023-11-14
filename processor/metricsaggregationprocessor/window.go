package metricsaggregationprocessor

import (
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type aggregatedWindow struct {
	sync.RWMutex
	startTime       time.Time
	metric          pmetric.Metric
	aggregationType AggregationType
	count           int64
}

type windowKey struct {
	Name       string
	Attributes string // Serialized representation of attributes
	Type       pmetric.MetricType
	StartTime  pcommon.Timestamp
}

func generateWindowKey(metric pmetric.Metric, attributes pcommon.Map, startTime pcommon.Timestamp) windowKey {
	// Serialize attributes in a consistent manner
	var keys []string
	attrMap := make(map[string]string)
	attributes.Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, k)
		attrMap[k] = v.AsString()
		return true
	})
	sort.Strings(keys) // Sort by keys

	serializedAttributes := make([]string, 0, len(keys))
	for _, k := range keys {
		serializedAttributes = append(serializedAttributes, k+"="+attrMap[k])
	}

	return windowKey{
		Name:       metric.Name(),
		Attributes: strings.Join(serializedAttributes, ","),
		Type:       metric.Type(),
		StartTime:  startTime,
	}
}

func (m *metricsAggregationProcessor) createNewWindow(
	metric pmetric.Metric,
	attributes pcommon.Map,
	startTime pcommon.Timestamp,
	aggregationConfig *MetricAggregationConfig,
) *aggregatedWindow {
	m.logger.Debug("Creating new window",
		zap.String("metric_name", metric.Name()),
		zap.String("aggregation_type", string(aggregationConfig.AggregationType)),
		zap.String("data_point_attributes", strings.Join(aggregationConfig.DataPointAttributes, ",")),
		zap.Time("window_start_time", startTime.AsTime()),
	)
	//nolint:gomnd
	windowMetricTimestamp := startTime.AsTime().Add(m.config.AggregationPeriod / 2)
	// create a metric copy from the original metric
	windowMetric := pmetric.NewMetric()
	var windowMetricType pmetric.MetricType
	if aggregationConfig.AggregationType == Bucketize {
		windowMetricType = pmetric.MetricTypeHistogram
	} else {
		windowMetricType = metric.Type()
	}
	switch windowMetricType {
	case pmetric.MetricTypeGauge:
		dp := windowMetric.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(windowMetricTimestamp))
		attributes.CopyTo(dp.Attributes())
	case pmetric.MetricTypeHistogram:
		windowMetric.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		dp := windowMetric.Histogram().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(windowMetricTimestamp))
		attributes.CopyTo(dp.Attributes())
		// If its to bucketize, initialize the scale and bounds
		if aggregationConfig.AggregationType == Bucketize {
			dp.BucketCounts().FromRaw(make([]uint64, aggregationConfig.BucketCount))
			scale := (aggregationConfig.UpperBound - aggregationConfig.LowerBound) / float64(aggregationConfig.BucketCount)
			for i := 1; i < aggregationConfig.BucketCount; i++ {
				dp.ExplicitBounds().Append(aggregationConfig.LowerBound + float64(i)*scale)
			}
		}
	}
	if aggregationConfig.NewName != "" {
		windowMetric.SetName(aggregationConfig.NewName)
	} else {
		windowMetric.SetName(metric.Name())
	}
	windowMetric.SetDescription(metric.Description())
	windowMetric.SetUnit(metric.Unit())
	window := &aggregatedWindow{
		startTime:       startTime.AsTime(),
		metric:          windowMetric,
		aggregationType: aggregationConfig.AggregationType,
	}
	return window
}

func getRoundedStartTime(timestamp pcommon.Timestamp, aggregationPeriod time.Duration) pcommon.Timestamp {
	// Convert the pcommon.Timestamp to time.Time
	timestampTime := timestamp.AsTime()

	// Calculate the number of aggregation periods since the Unix epoch
	periodsSinceEpoch := timestampTime.UnixNano() / int64(aggregationPeriod)

	// Calculate the rounded start time in nanoseconds
	roundedStartTimeNano := periodsSinceEpoch * int64(aggregationPeriod)

	// Convert the rounded start time in nanoseconds back to pcommon.Timestamp
	roundedStartTime := pcommon.NewTimestampFromTime(time.Unix(0, roundedStartTimeNano))

	return roundedStartTime
}

func (m *metricsAggregationProcessor) getWindowForMetric(
	metric pmetric.Metric,
	attributes pcommon.Map,
	timestamp pcommon.Timestamp,
	aggregationConfig *MetricAggregationConfig,
) *aggregatedWindow {
	roundedStartTime := getRoundedStartTime(timestamp, m.config.AggregationPeriod)

	windowKey := generateWindowKey(metric, attributes, roundedStartTime)
	m.windowsMutex.Lock()
	defer m.windowsMutex.Unlock()

	// Check if a window for the metric key already exists
	if window, exists := m.windows[windowKey]; exists {
		m.logger.Debug("Window found for metric",
			zap.String("metric_name", metric.Name()),
			zap.String("aggregation_type", string(aggregationConfig.AggregationType)),
			zap.String("data_point_attributes", strings.Join(aggregationConfig.DataPointAttributes, ",")),
		)
		return window
	}

	newWindow := m.createNewWindow(metric, attributes, roundedStartTime, aggregationConfig)

	// Store the new window in the map and return it
	m.windows[windowKey] = newWindow
	return newWindow
}

func (m *metricsAggregationProcessor) flushWindows(flushAll bool) {
	m.logger.Info("Flushing windows")
	currentTime := m.clock.Now()
	m.windowsMutex.Lock()
	defer m.windowsMutex.Unlock()
	m.flushedMetricsMutex.Lock()
	defer m.flushedMetricsMutex.Unlock()
	for key, window := range m.windows {
		if window.startTime.Add(m.config.MaxStaleness).Before(currentTime) || flushAll {
			m.logger.Debug("flushing window",
				zap.String("metric_name", window.metric.Name()),
				zap.String("aggregation_type", string(window.aggregationType)),
				zap.Time("window_start_time", window.startTime),
			)
			window.complete()
			window.metric.CopyTo(m.flushedMetrics.AppendEmpty())
			delete(m.windows, key)
		}
	}
}

func (m *metricsAggregationProcessor) startFlushInterval() {
	m.logger.Info("Starting flush interval", zap.Duration("flush_interval", m.config.AggregationPeriod))
	for {
		<-m.clock.After(m.config.AggregationPeriod)
		m.flushWindows(false)
	}
}

func (w *aggregatedWindow) complete() {
	w.Lock()
	defer w.Unlock()
	if w.metric.Type() == pmetric.MetricTypeGauge {
		completeGaugeAggregation(w.metric, w.aggregationType, w.count)
	}
}
