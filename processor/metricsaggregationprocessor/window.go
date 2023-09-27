package metricsaggregationprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type aggregatedWindow struct {
	sync.RWMutex
	startTime       time.Time
	metric          pmetric.Metric
	count           int64   // for average calculation
}

func (m *metricsAggregationProcessor) createNewWindowIfExists(ctx context.Context, metric pmetric.Metric, attributes pcommon.Map, aggregationConfig *MetricAggregationConfig) *aggregatedWindow {
	currentTime := ctx.Value(CurrentTimeContextKey).(time.Time)
	windowMetricTimestamp := currentTime.Add(m.config.AggregationPeriod/2)
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
			scale := float64(aggregationConfig.UpperBound - aggregationConfig.LowerBound) / float64(aggregationConfig.BucketCount)
			for i := 0; i < aggregationConfig.BucketCount; i++ {
				dp.ExplicitBounds().Append(aggregationConfig.LowerBound + float64(i)*scale)
			}
		}
	}
	if aggregationConfig.NewName != "" {
		windowMetric.SetName(aggregationConfig.NewName)
	}
	window := &aggregatedWindow{
		startTime:       currentTime,
		metric:          windowMetric,
	}
	return window
}

func (m *metricsAggregationProcessor) getWindowForMetric(ctx context.Context, metric pmetric.Metric, attributes pcommon.Map, aggregationConfig *MetricAggregationConfig) *aggregatedWindow {
	metricKey := generateMetricKey(metric, attributes)
	var metricWindow *aggregatedWindow
	// check if this metricKey has an aggregation with the same type
	m.windowsMutex.RLock()
	windowsList, exists := m.windows[metricKey]
	m.windowsMutex.RUnlock()
	if !exists {
		metricWindow = m.createNewWindowIfExists(ctx, metric, attributes, aggregationConfig)
		if metricWindow == nil {
			return nil
		}
		m.windowsMutex.Lock()
		m.windows[metricKey] = []*aggregatedWindow{metricWindow}
		m.windowsMutex.Unlock()
	} else {
		found := false
		for _, window := range windowsList {
			if time.Now().After(window.startTime) && time.Now().Before(window.startTime.Add(m.config.AggregationPeriod)) {
				metricWindow = window
				found = true
				break
			}
		}
		if !found {
			// Create a new window and add to the list
			metricWindow = m.createNewWindowIfExists(ctx, metric, attributes, aggregationConfig)
			if metricWindow != nil {
				m.windowsMutex.Lock()
				m.windows[metricKey] = append(m.windows[metricKey], metricWindow)
				m.windowsMutex.Unlock()
			} else {
				return nil
			}
		}
	}

	return metricWindow
}

func (m *metricsAggregationProcessor) flushExpiredWindows() {
	delay := m.config.AggregationPeriod / 2
    for {
        <-time.After(delay)
        currentTime := m.clock.Now()
		m.windowsMutex.Lock()
        for key, windowsList := range m.windows {
            newWindowsList := windowsList[:0]
            for _, window := range windowsList {
                if window.startTime.Add(m.config.MaxStaleness).Before(currentTime) {
                    window.metric.CopyTo(m.flushedMetrics.AppendEmpty())
                } else {
                    newWindowsList = append(newWindowsList, window)
                }
            }

            if len(newWindowsList) == 0 {
                delete(m.windows, key)
            } else {
                m.windows[key] = newWindowsList
            }
        }
		m.windowsMutex.Unlock()
    }
}