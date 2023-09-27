package metricsaggregationprocessor

import (
	"time"
)

type Config struct {
	AggregationPeriod time.Duration `mapstructure:"aggregation_period"`
	MaxStaleness      time.Duration `mapstructure:"max_staleness"`
	// Aggregations contains the metric aggregation settings.
	Aggregations []MetricAggregationConfig `mapstructure:"aggregations"`
}

type MetricAggregationConfig struct {
	// MetricName is the pattern to match metric names against.
	MetricName string `mapstructure:"metric_name"`

	// MatchType determines how the metric name pattern should be matched against metric names.
	MatchType MatchType `mapstructure:"match_type"`

	// NewName is the new name for the metric after aggregation.
	NewName string `mapstructure:"new_name"`

	// AggregationType defines the type of aggregation to be performed.
	AggregationType AggregationType `mapstructure:"aggregation_type"`

	// DataPointAttributes is a list of attributes to be aggregated over.
	DataPointAttributes []string `mapstructure:"data_point_attributes"`

	// KeepOriginal determines whether the original metric is also emitted. This is only applicable when new_name is set.
	KeepOriginal bool `mapstructure:"keep_original"`

	// LowerBound is the lower bound for the histogram buckets. Only applicable when AggregationType is Bucketize.
	LowerBound float64 `mapstructure:"lower_bound,omitempty"`

	// UpperBound is the upper bound for the histogram buckets. Only applicable when AggregationType is Bucketize.
	UpperBound float64 `mapstructure:"upper_bound,omitempty"`

	// BucketCount is the number of buckets between LowerBound and UpperBound. Only applicable when AggregationType is Bucketize.
	BucketCount int `mapstructure:"bucket_count,omitempty"`
}

type MatchType string

const (
	// MatchTypeStrict matches metric names exactly
	Strict MatchType = "strict"
	
	// MatchTypeRegexp matches metric names using a regular expression.
	Regexp  MatchType = "regex"
)

// AggregationType defines the type of aggregation to perform on matching metrics.
type AggregationType string

const (
	// AggregationTypeMin calculates the minimum value of matching metrics.
	Min     AggregationType = "min"
	// AggregationTypeMax calculates the maximum value of matching metrics.
	Max     AggregationType = "max"
	// AggregationTypeCount calculates the count of matching metrics.
	Count   AggregationType = "count"
	// AggregationTypeCount calculates the count of matching metrics.
	Average AggregationType = "average"
	// Bucketize calculates the distribution of matching metrics.
	Bucketize AggregationType = "bucketize"
)





