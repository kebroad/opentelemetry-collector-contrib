# Metrics Aggregation Processor

| Status        |           |
| ------------- |-----------|
| Stability     | [development]: metrics (gauges)  |

## Description

The metrics aggregation can be used to temporally aggregate metrics that match the same name and attributes over set time periods. It creates windows in memory with a resultant aggregated metric when a metric comes in, with a configurable aggregation period and max staleness, where after the latter has passed, the window is closed, and the aggregated metric result is sent to the next processor.

:information_source: This processor only is supported for gauges currently.


## Configuration

```yaml
processors:
  metricsaggregation:
    # Aggregation period is the time period in seconds that the processor will aggregate matching metrics for.
    aggregation_period: 10s
    # Max staleness is the time period in seconds that the processor will wait for matching metrics to come in and aggregate before closing the window and sending the aggregated metric to the next processor.
    max_staleness: 1m
    # Aggregations describe each aggregation that will be performed on matching metrics.
    aggregations:
      # Metric name is the name of the metric that will be aggregated. This can be a regex expression with match groups if match_type is regexp.
    - metric_name: hydra_workers_cpu_usage_percent
      # New name is the name of the aggregated metric that will be sent to the next processor.
      new_name: hydra_workers_cpu_usage_percent_exp_dist
      # Keep original is a boolean that determines whether the original metric will be sent to the next processor as well
      keep_original: false
      # Match type is the type of matching that will be performed on the metric name. The supported types are: strict and regexp.
      match_type: strict
      # Aggregation type is the type of aggregation that will be performed on the metric. The supported types are: sum, min, max, mean, count, bucketize, and percentile.
      aggregation_type: bucketize
      # Upper bound is the upper bound of explicit bounds for the aggregation histogram.
      upper_bound: 6400
      # Lower bound is the lower bound of  explicit bounds for the aggregation histogram.
      lower_bound: 0
      # Bucket count is the number of buckets for the aggregation histogram.
      bucket_count: 6400
      # Data point attributes are the keys of attributes that will be matched on the metric. If the metric does has only a subset of the attributes, it will still be aggregated, with each window being based on the unique combinations of the attributes matching these that are present.
      data_point_attributes:
      - worker_class
      - framework_id
      - type
      - mount_point

```
