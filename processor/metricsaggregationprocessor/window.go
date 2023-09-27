package metricsaggregationprocessor

import (
	"sort"
	"strings"
	"sync"
	"time"



	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var baseTime = time.Unix(0, 0) // Unix epoch

type aggregatedWindow struct {
	sync.RWMutex
	startTime time.Time
	metric    pmetric.Metric
	count     int64 // for average calculation
}

type windowKey struct {
	Name       string
	Attributes string // Serialized representation of attributes
	Type       pmetric.MetricType
	StartTime  pcommon.Timestamp
}

func generateWindowKey(metric pmetric.Metric, attributes pcommon.Map, startTime pcommon.Timestamp) windowKey {
	// Serialize attributes in a consistent manner
	// For simplicity, we'll just join them as key=value pairs, but in practice, you might want a more efficient representation.
	var serializedAttributes []string
	attributes.Range(func(k string, v pcommon.Value) bool {
		serializedAttributes = append(serializedAttributes, k+"="+v.AsString())
		return true
	})
	sort.Strings(serializedAttributes) // Ensure consistent order

	return windowKey{
		Name:       metric.Name(),
		Attributes: strings.Join(serializedAttributes, ","),
		Type:       metric.Type(),
		StartTime:  startTime,
	}
}

func (m *metricsAggregationProcessor) createNewWindow(metric pmetric.Metric, attributes pcommon.Map, timestamp pcommon.Timestamp, aggregationConfig *MetricAggregationConfig) *aggregatedWindow {
	windowMetricTimestamp := timestamp.AsTime().Add(m.config.AggregationPeriod / 2)
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
		dp := windowMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(windowMetricTimestamp))
		attributes.CopyTo(dp.Attributes())
	case pmetric.MetricTypeSum:
		dp := windowMetric.Sum().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(windowMetricTimestamp))
		attributes.CopyTo(dp.Attributes())
	case pmetric.MetricTypeHistogram:
		dp := windowMetric.Histogram().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(windowMetricTimestamp))
		attributes.CopyTo(dp.Attributes())
		// If its to bucketize, initialize the scale and bounds
		if aggregationConfig.AggregationType == Bucketize {
			dp.BucketCounts().FromRaw(make([]uint64, aggregationConfig.BucketCount))
			scale := float64(aggregationConfig.UpperBound-aggregationConfig.LowerBound) / float64(aggregationConfig.BucketCount)
			for i := 0; i < aggregationConfig.BucketCount; i++ {
				dp.ExplicitBounds().Append(aggregationConfig.LowerBound + float64(i)*scale)
			}
		}
	}
	if aggregationConfig.NewName != "" {
		windowMetric.SetName(aggregationConfig.NewName)
	}
	window := &aggregatedWindow{
		startTime: timestamp.AsTime(),
		metric:    windowMetric,
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

func (m *metricsAggregationProcessor) getWindowForMetric(metric pmetric.Metric, attributes pcommon.Map, timestamp pcommon.Timestamp, aggregationConfig *MetricAggregationConfig) *aggregatedWindow {
	roundedStartTime := getRoundedStartTime(timestamp, m.config.AggregationPeriod)

	windowKey := generateWindowKey(metric, attributes, roundedStartTime)
	m.windowsMutex.Lock()
	defer m.windowsMutex.Unlock()

	// Check if a window for the metric key already exists
	if window, exists := m.windows[windowKey]; exists {
		return window
	}

	newWindow := m.createNewWindow(metric, attributes, roundedStartTime, aggregationConfig)

	// Store the new window in the map and return it
	m.windows[windowKey] = newWindow
	return newWindow

}

func (m *metricsAggregationProcessor) flushExpiredWindows() {
	delay := m.config.AggregationPeriod / 4
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.clock.After(delay):
			currentTime := m.clock.Now()
			m.windowsMutex.Lock()
			m.flushedMetricsMutex.Lock()
			for key, window := range m.windows {
				if window.startTime.Add(m.config.MaxStaleness).Before(currentTime) {
					window.metric.CopyTo(m.flushedMetrics.AppendEmpty())
					delete(m.windows, key)
				}
			}
			m.windowsMutex.Unlock()
			m.flushedMetricsMutex.Unlock()
		}
	}
}
