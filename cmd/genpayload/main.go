// Command genpayload writes an OTLP/Protobuf ExportMetricsServiceRequest to
// stdout, for demoing the otlp_slo227 server with curl.
package main

import (
	"flag"
	"os"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func main() {
	kind := flag.String("kind", "sum", "sum or histogram")
	name := flag.String("name", "http_requests", "metric name")
	instance := flag.String("instance", "demo-1", "service.instance.id")
	scope := flag.String("scope", "demo-lib", "scope name")
	attr := flag.String("attr", "method=GET", "point attribute k=v")
	start := flag.Uint64("start", 1000, "start unix nanos")
	end := flag.Uint64("end", 2000, "end unix nanos")
	value := flag.Float64("value", 1, "sum delta or histogram sum")
	count := flag.Uint64("count", 3, "histogram count")
	flag.Parse()

	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.instance.id", *instance)}}
	sc := &commonpb.InstrumentationScope{Name: *scope}
	var ptAttrs []*commonpb.KeyValue
	if *attr != "" {
		for i := 0; i < len(*attr); i++ {
			if (*attr)[i] == '=' {
				ptAttrs = append(ptAttrs, strAttr((*attr)[:i], (*attr)[i+1:]))
				break
			}
		}
	}

	var m *metricspb.Metric
	if *kind == "histogram" {
		sum := *value
		m = &metricspb.Metric{Name: *name, Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			DataPoints: []*metricspb.HistogramDataPoint{{
				Attributes: ptAttrs, StartTimeUnixNano: *start, TimeUnixNano: *end,
				Count: *count, Sum: &sum,
				BucketCounts: []uint64{1, *count - 1}, ExplicitBounds: []float64{0.5},
			}},
		}}}
	} else {
		m = &metricspb.Metric{Name: *name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			IsMonotonic:            true,
			DataPoints: []*metricspb.NumberDataPoint{{
				Attributes: ptAttrs, StartTimeUnixNano: *start, TimeUnixNano: *end,
				Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: *value},
			}},
		}}}
	}

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: res,
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: sc, Metrics: []*metricspb.Metric{m},
			}},
		}},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	os.Stdout.Write(data)
}
