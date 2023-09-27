package metricsaggregationprocessor

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
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
func setValue(dp pmetric.NumberDataPoint, value float64) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		dp.SetDoubleValue(value)
	case pmetric.NumberDataPointValueTypeInt:
		dp.SetIntValue(int64(value))
	}
}



func (m *metricsAggregationProcessor) aggregateGaugeMetric(metric pmetric.Metric, aggregationConfig *MetricAggregationConfig, currentTime time.Time) {
	// Get the data points from the metric
	dps := metric.Gauge().DataPoints()

	// Iterate over the data points
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		// If the timestamp on this datapoint is before the current time minus max_staleness, skip it
		if dp.Timestamp().AsTime().Before(currentTime.Add(-m.config.MaxStaleness)) {
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
			m.aggregateGaugeMin(relevantWindow, metric, dp)
		case Max:
			m.aggregateGaugeMax(relevantWindow, metric, dp)
		case Count:
			m.aggregateGaugeCount(relevantWindow)
		case Average:
			m.aggregateGaugeAverage(relevantWindow, metric, dp)
		case Bucketize:
			m.aggregateGaugeToHistogram(relevantWindow, metric, dp)
		}
	}

}

func (m *metricsAggregationProcessor) aggregateGaugeMin(window *aggregatedWindow, metric pmetric.Metric, dp pmetric.NumberDataPoint) {
	window.Lock()
	defer window.Unlock()

	if getValue(dp) < getValue(window.metric.Gauge().DataPoints().At(0)) {
		setValue(window.metric.Gauge().DataPoints().At(0), getValue(dp))
	}
}

func (m *metricsAggregationProcessor) aggregateGaugeMax(window *aggregatedWindow, metric pmetric.Metric, dp pmetric.NumberDataPoint) {
	window.Lock()
	defer window.Unlock()

	if getValue(dp) > getValue(window.metric.Gauge().DataPoints().At(0)) {
		setValue(window.metric.Gauge().DataPoints().At(0), getValue(dp))
	}
}

func (m *metricsAggregationProcessor) aggregateGaugeCount(window *aggregatedWindow) {
	window.Lock()
	defer window.Unlock()

	window.count++
}

func (m *metricsAggregationProcessor) aggregateGaugeAverage(window *aggregatedWindow, metric pmetric.Metric, dp pmetric.NumberDataPoint) {
	window.Lock()
	defer window.Unlock()

	sum := getValue(window.metric.Gauge().DataPoints().At(0)) + getValue(dp)
	setValue(window.metric.Gauge().DataPoints().At(0), sum)
	window.count++
}

func (m *metricsAggregationProcessor) aggregateGaugeToHistogram(window *aggregatedWindow, metric pmetric.Metric, dp pmetric.NumberDataPoint) {
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
	histDp.BucketCounts().SetAt(bucketIndex, histDp.BucketCounts().At(bucketIndex) + 1)
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