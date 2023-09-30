package metricsaggregationprocessor

import (
	"fmt"
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

var MatchTypes = []MatchType{Strict, Regexp}

func (mt MatchType) isValid() bool {
	for _, matchType := range MatchTypes {
		if matchType == mt {
			return true
		}
	}
	return false
}

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

var AggregationTypes = []AggregationType{Min, Max, Count, Average, Bucketize}

func (at AggregationType) isValid() bool {
	for _, aggregationType := range AggregationTypes {
		if aggregationType == at {
			return true
		}
	}
	return false
}

func validateConfiguration(config *Config) error {
	if config.AggregationPeriod <= 0 {
		return fmt.Errorf("aggregation_period must be greater than 0, got %v", config.AggregationPeriod)
	}
	if config.MaxStaleness <= 0 {
		return fmt.Errorf("max_staleness must be greater than 0, got %v", config.MaxStaleness)
	}
	if config.AggregationPeriod >= config.MaxStaleness {
		return fmt.Errorf("aggregation_period must be less than max_staleness, got %v and %v", config.AggregationPeriod, config.MaxStaleness)
	}
	for _, aggregationConfig := range config.Aggregations {
		if aggregationConfig.MetricName == "" {
			return fmt.Errorf("metric_name cannot be empty")
		}
		if !aggregationConfig.MatchType.isValid() {
			return fmt.Errorf("invalid match_type %v", aggregationConfig.MatchType)
		}
		if !aggregationConfig.AggregationType.isValid() {
			return fmt.Errorf("invalid aggregation_type %v", aggregationConfig.AggregationType)
		}
		if aggregationConfig.NewName == "" && !aggregationConfig.KeepOriginal {
			return fmt.Errorf("new_name cannot be empty if keep_original is false")
		}
		if aggregationConfig.AggregationType == Bucketize {
			if aggregationConfig.LowerBound >= aggregationConfig.UpperBound {
				return fmt.Errorf("lower_bound must be less than upper_bound, got %v and %v", aggregationConfig.LowerBound, aggregationConfig.UpperBound)
			}
			if aggregationConfig.BucketCount <= 0 {
				return fmt.Errorf("bucket_count must be greater than 0, got %v", aggregationConfig.BucketCount)
			}
		}
		if len(aggregationConfig.DataPointAttributes) == 0 {
			return fmt.Errorf("data_point_attributes cannot be empty")
		}
		

	}



}
