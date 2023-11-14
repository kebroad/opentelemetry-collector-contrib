package metricsaggregationprocessor

import (
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// Helper function to get the value from a data point based on its type
func getValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	default:
		return 0
	}
}

// Helper function to set the value of a data point based on its type
func setValue(dp pmetric.NumberDataPoint, value float64, inType pmetric.NumberDataPointValueType) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		dp.SetDoubleValue(value)
	case pmetric.NumberDataPointValueTypeInt:
		dp.SetIntValue(int64(value))
	case pmetric.NumberDataPointValueTypeEmpty:
		switch inType {
		case pmetric.NumberDataPointValueTypeDouble:
			dp.SetDoubleValue(value)
		case pmetric.NumberDataPointValueTypeInt:
			dp.SetIntValue(int64(value))
		default:
			dp.SetDoubleValue(value)
		}
	}
}

func (m *metricsAggregationProcessor) aggregateGaugeMetric(
	metric pmetric.Metric,
	aggregationConfig *MetricAggregationConfig,
	currentTime time.Time,
) {
	// Get the data points from the metric
	dps := metric.Gauge().DataPoints()

	// Iterate over the data points
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		// If the timestamp on this datapoint is before the current time minus max_staleness, skip it
		if dp.Timestamp().AsTime().Before(currentTime.Add(-m.config.MaxStaleness)) {
			m.logger.Info("Skipping stale data point",
				zap.String("metric_name", metric.Name()),
				zap.String("aggregation_type", string(aggregationConfig.AggregationType)),
				zap.String("data_point_attributes", strings.Join(aggregationConfig.DataPointAttributes, ",")),
				zap.Time("data_point_timestamp", dp.Timestamp().AsTime()), zap.Time("current_time", currentTime))
			continue
		}
		matchingAttributes := getMatchingAttributes(aggregationConfig, dp.Attributes())
		if matchingAttributes.Len() == 0 {
			continue
		}
		relevantWindow := m.getWindowForMetric(metric, matchingAttributes, dp.Timestamp(), aggregationConfig)
		if relevantWindow == nil {
			continue
		}
		switch aggregationConfig.AggregationType {
		// Check the aggregation type and aggregate the data point accordingly
		case Min:
			m.aggregateGaugeMin(relevantWindow, dp)
		case Max:
			m.aggregateGaugeMax(relevantWindow, dp)
		case Count:
			m.aggregateGaugeCount(relevantWindow)
		case Average:
			m.aggregateGaugeAverage(relevantWindow, dp)
		case Bucketize:
			m.aggregateGaugeToHistogram(relevantWindow, dp)
		}
	}
}

func isEmpty(metric pmetric.Metric) bool {
	return metric.Gauge().DataPoints().At(0).ValueType() == pmetric.NumberDataPointValueTypeEmpty
}

func (m *metricsAggregationProcessor) aggregateGaugeMin(window *aggregatedWindow, dp pmetric.NumberDataPoint) {
	window.Lock()
	defer window.Unlock()

	if (getValue(dp) < getValue(window.metric.Gauge().DataPoints().At(0))) || isEmpty(window.metric) {
		setValue(window.metric.Gauge().DataPoints().At(0), getValue(dp), dp.ValueType())
	}
}

func (m *metricsAggregationProcessor) aggregateGaugeMax(window *aggregatedWindow, dp pmetric.NumberDataPoint) {
	window.Lock()
	defer window.Unlock()

	if (getValue(dp) > getValue(window.metric.Gauge().DataPoints().At(0))) || isEmpty(window.metric) {
		setValue(window.metric.Gauge().DataPoints().At(0), getValue(dp), dp.ValueType())
	}
}

func (m *metricsAggregationProcessor) aggregateGaugeCount(window *aggregatedWindow) {
	window.Lock()
	defer window.Unlock()

	window.count++
}

func (m *metricsAggregationProcessor) aggregateGaugeAverage(
	window *aggregatedWindow,
	dp pmetric.NumberDataPoint,
) {
	window.Lock()
	defer window.Unlock()

	sum := getValue(window.metric.Gauge().DataPoints().At(0)) + getValue(dp)
	setValue(window.metric.Gauge().DataPoints().At(0), sum, dp.ValueType())
	window.count++
}

func (m *metricsAggregationProcessor) aggregateGaugeToHistogram(
	window *aggregatedWindow,
	dp pmetric.NumberDataPoint,
) {
	window.Lock()
	defer window.Unlock()
	// Get the data points from the metric
	histDp := window.metric.Histogram().DataPoints().At(0)
	histDp.SetCount(histDp.Count() + 1)
	if histDp.HasSum() {
		histDp.SetSum(histDp.Sum() + getValue(dp))
	} else {
		histDp.SetSum(getValue(dp))
	}
	if histDp.Max() < getValue(dp) || !histDp.HasMax() {
		histDp.SetMax(getValue(dp))
	}
	if histDp.Min() > getValue(dp) || !histDp.HasMin() {
		histDp.SetMin(getValue(dp))
	}
	// Get the bucket index for the value
	bucketIndex := findBucketIndex(getValue(dp), histDp.ExplicitBounds().AsRaw())
	// Increment the bucket count
	histDp.BucketCounts().SetAt(bucketIndex, histDp.BucketCounts().At(bucketIndex)+1)
}

// Helper function to determine the bucket index for a value
func findBucketIndex(value float64, explicitBounds []float64) int {
	for i, bound := range explicitBounds {
		if value <= bound {
			return i
		}
	}
	return len(explicitBounds) // This will be the index of the last bucket
}

func completeGaugeAggregation(metric pmetric.Metric, aggregationType AggregationType, count int64) {
	switch aggregationType {
	case Count:
		metric.Gauge().DataPoints().At(0).SetIntValue(count)
	case Average:
		setValue(metric.Gauge().DataPoints().At(0),
			getValue(metric.Gauge().DataPoints().At(0))/float64(count),
			metric.Gauge().DataPoints().At(0).ValueType())
	}
}
