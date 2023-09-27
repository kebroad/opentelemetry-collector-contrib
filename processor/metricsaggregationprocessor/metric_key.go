package metricsaggregationprocessor

import (
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type metricKey struct {
	Name       string
	Attributes string // Serialized representation of attributes
	Type 	 pmetric.MetricType
}

func generateMetricKey(metric pmetric.Metric, attributes pcommon.Map) metricKey {
	// Serialize attributes in a consistent manner
	// For simplicity, we'll just join them as key=value pairs, but in practice, you might want a more efficient representation.
	var serializedAttributes []string
	attributes.Range(func(k string, v pcommon.Value) bool {
		serializedAttributes = append(serializedAttributes, k+"="+v.AsString())
		return true
	})
	sort.Strings(serializedAttributes) // Ensure consistent order

	return metricKey{
		Name:       metric.Name(),
		Attributes: strings.Join(serializedAttributes, ","),
		Type: 		metric.Type(),
	}
}




