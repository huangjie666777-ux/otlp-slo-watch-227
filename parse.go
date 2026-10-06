package main

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

type kind int

const (
	kindSum kind = iota
	kindHistogram
)

func (k kind) String() string {
	if k == kindSum {
		return "sum"
	}
	return "histogram"
}

type pointUpdate struct {
	metric   string
	instance string
	scope    string
	attrs    [][2]string
	kind     kind
	start    uint64
	end      uint64
	delta    float64
	bounds   []float64
	buckets  []uint64
	count    uint64
	hsum     float64
}

var nameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

var reservedLabels = map[string]bool{"instance": true, "scope": true, "le": true}

func parseRequest(body []byte) ([]pointUpdate, error) {
	var req collectormetrics.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid protobuf: %w", err)
	}
	var updates []pointUpdate
	for _, rm := range req.ResourceMetrics {
		instance, err := instanceID(rm.Resource.GetAttributes())
		if err != nil {
			return nil, err
		}
		for _, sm := range rm.ScopeMetrics {
			scope := sm.GetScope().GetName()
			for _, m := range sm.Metrics {
				if !nameRE.MatchString(m.Name) {
					return nil, fmt.Errorf("metric name %q violates Prometheus naming", m.Name)
				}
				us, err := parseMetric(instance, scope, m)
				if err != nil {
					return nil, err
				}
				updates = append(updates, us...)
			}
		}
	}
	return updates, nil
}

func instanceID(attrs []*commonpb.KeyValue) (string, error) {
	for _, kv := range attrs {
		if kv.Key == "service.instance.id" {
			if _, ok := kv.Value.Value.(*commonpb.AnyValue_StringValue); !ok {
				return "", errors.New("resource attribute service.instance.id must be a string")
			}
			return kv.Value.GetStringValue(), nil
		}
	}
	return "", nil
}

func pointAttrs(attrs []*commonpb.KeyValue) ([][2]string, error) {
	var out [][2]string
	for _, kv := range attrs {
		sv, ok := kv.Value.Value.(*commonpb.AnyValue_StringValue)
		if !ok {
			continue
		}
		if !nameRE.MatchString(kv.Key) {
			return nil, fmt.Errorf("attribute key %q violates Prometheus naming", kv.Key)
		}
		if reservedLabels[kv.Key] {
			return nil, fmt.Errorf("attribute key %q is reserved", kv.Key)
		}
		out = append(out, [2]string{kv.Key, sv.StringValue})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	for i := 1; i < len(out); i++ {
		if out[i][0] == out[i-1][0] {
			return nil, fmt.Errorf("duplicate attribute key %q", out[i][0])
		}
	}
	return out, nil
}

func parseMetric(instance, scope string, m *metricspb.Metric) ([]pointUpdate, error) {
	switch d := m.Data.(type) {
	case *metricspb.Metric_Sum:
		return parseSum(instance, scope, m.Name, d.Sum)
	case *metricspb.Metric_Histogram:
		return parseHistogram(instance, scope, m.Name, d.Histogram)
	default:
		return nil, fmt.Errorf("metric %q: unsupported data type (only delta monotonic Sum and delta explicit-bucket Histogram with sum)", m.Name)
	}
}

func checkWindow(mname string, start, end uint64) error {
	if start == 0 || end == 0 {
		return fmt.Errorf("metric %q: timestamps must be non-zero nanoseconds", mname)
	}
	if start >= end {
		return fmt.Errorf("metric %q: start time must be earlier than end time", mname)
	}
	return nil
}

func parseSum(instance, scope, name string, s *metricspb.Sum) ([]pointUpdate, error) {
	if s.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
		return nil, fmt.Errorf("metric %q: only DELTA sums are supported", name)
	}
	if !s.IsMonotonic {
		return nil, fmt.Errorf("metric %q: only monotonic sums are supported", name)
	}
	var out []pointUpdate
	for _, dp := range s.DataPoints {
		if err := checkWindow(name, dp.StartTimeUnixNano, dp.TimeUnixNano); err != nil {
			return nil, err
		}
		attrs, err := pointAttrs(dp.Attributes)
		if err != nil {
			return nil, err
		}
		var v float64
		switch x := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsInt:
			v = float64(x.AsInt)
		case *metricspb.NumberDataPoint_AsDouble:
			v = x.AsDouble
		default:
			return nil, fmt.Errorf("metric %q: data point has no value", name)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("metric %q: non-finite sum value", name)
		}
		if v < 0 {
			return nil, fmt.Errorf("metric %q: negative delta for monotonic sum", name)
		}
		out = append(out, pointUpdate{
			metric: name, instance: instance, scope: scope, attrs: attrs,
			kind: kindSum, start: dp.StartTimeUnixNano, end: dp.TimeUnixNano, delta: v,
		})
	}
	return out, nil
}

func parseHistogram(instance, scope, name string, h *metricspb.Histogram) ([]pointUpdate, error) {
	if h.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
		return nil, fmt.Errorf("metric %q: only DELTA histograms are supported", name)
	}
	var out []pointUpdate
	for _, dp := range h.DataPoints {
		if err := checkWindow(name, dp.StartTimeUnixNano, dp.TimeUnixNano); err != nil {
			return nil, err
		}
		attrs, err := pointAttrs(dp.Attributes)
		if err != nil {
			return nil, err
		}
		if len(dp.BucketCounts) != len(dp.ExplicitBounds)+1 {
			return nil, fmt.Errorf("metric %q: bucket count must equal bounds+1", name)
		}
		prev := math.Inf(-1)
		for _, b := range dp.ExplicitBounds {
			if math.IsNaN(b) || math.IsInf(b, 0) {
				return nil, fmt.Errorf("metric %q: bucket bounds must be finite", name)
			}
			if b <= prev {
				return nil, fmt.Errorf("metric %q: bucket bounds must be strictly increasing", name)
			}
			prev = b
		}
		var total uint64
		for _, c := range dp.BucketCounts {
			if total+c < total {
				return nil, fmt.Errorf("metric %q: bucket counts overflow", name)
			}
			total += c
		}
		if total != dp.Count {
			return nil, fmt.Errorf("metric %q: bucket counts do not add up to count", name)
		}
		if dp.Sum == nil {
			return nil, fmt.Errorf("metric %q: histogram sum is required", name)
		}
		if math.IsNaN(*dp.Sum) || math.IsInf(*dp.Sum, 0) {
			return nil, fmt.Errorf("metric %q: histogram sum must be finite", name)
		}
		if *dp.Sum < 0 {
			return nil, fmt.Errorf("metric %q: histogram sum must be non-negative", name)
		}
		out = append(out, pointUpdate{
			metric: name, instance: instance, scope: scope, attrs: attrs,
			kind: kindHistogram, start: dp.StartTimeUnixNano, end: dp.TimeUnixNano,
			bounds:  append([]float64(nil), dp.ExplicitBounds...),
			buckets: append([]uint64(nil), dp.BucketCounts...),
			count:   dp.Count, hsum: *dp.Sum,
		})
	}
	return out, nil
}
