package main

import (
	"strings"
	"testing"
	"time"
)

func sumUpdate(start, end uint64, delta float64) pointUpdate {
	return pointUpdate{
		metric: "requests", instance: "i1", scope: "lib",
		attrs: [][2]string{{"method", "GET"}},
		kind:  kindSum, start: start, end: end, delta: delta,
	}
}

func histUpdate(start, end uint64, count uint64, sum float64, buckets ...uint64) pointUpdate {
	return pointUpdate{
		metric: "latency", instance: "i1", scope: "lib",
		kind: kindHistogram, start: start, end: end,
		bounds: []float64{0.5, 1}, buckets: buckets, count: count, hsum: sum,
	}
}

func TestAccumulateContiguous(t *testing.T) {
	st := NewStore(100, time.Minute)
	if err := st.Apply([]pointUpdate{sumUpdate(10, 20, 3)}); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply([]pointUpdate{sumUpdate(20, 30, 4)}); err != nil {
		t.Fatal(err)
	}
	out := Render(st)
	if !strings.Contains(out, `requests_total{instance="i1",scope="lib",method="GET"} 7`) {
		t.Fatalf("missing cumulative total:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE requests_total counter\n") {
		t.Fatalf("missing TYPE line:\n%s", out)
	}
}

func TestRetransmitSameSegment(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.now = func() time.Time { return time.Unix(1000, 0) }
	if err := st.Apply([]pointUpdate{sumUpdate(10, 20, 3)}); err != nil {
		t.Fatal(err)
	}
	st.now = func() time.Time { return time.Unix(2000, 0) }
	if err := st.Apply([]pointUpdate{sumUpdate(10, 20, 3)}); err != nil {
		t.Fatal(err)
	}
	s := st.series[seriesKey("i1", "lib", "requests", [][2]string{{"method", "GET"}})]
	if s.value != 3 {
		t.Fatalf("retransmit accumulated: %v", s.value)
	}
	if s.lastActive != time.Unix(1000, 0) {
		t.Fatalf("retransmit extended lifetime: %v", s.lastActive)
	}
}

func TestConflictingRetransmit(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{sumUpdate(10, 20, 3)})
	if err := st.Apply([]pointUpdate{sumUpdate(10, 20, 5)}); err == nil {
		t.Fatal("expected rejection of conflicting retransmission")
	}
}

func TestOverlapAndRegress(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{sumUpdate(10, 20, 3)})
	if err := st.Apply([]pointUpdate{sumUpdate(15, 25, 1)}); err == nil {
		t.Fatal("expected overlap rejection")
	}
	if err := st.Apply([]pointUpdate{sumUpdate(5, 15, 1)}); err == nil {
		t.Fatal("expected regress rejection")
	}
}

func TestGapResets(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{sumUpdate(10, 20, 3)})
	if err := st.Apply([]pointUpdate{sumUpdate(25, 30, 9)}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(Render(st), " 9\n") {
		t.Fatalf("gap should reset cumulative:\n%s", Render(st))
	}
}

func TestAtomicBatch(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{sumUpdate(10, 20, 3)})
	batch := []pointUpdate{sumUpdate(20, 30, 4), sumUpdate(15, 25, 1)}
	if err := st.Apply(batch); err == nil {
		t.Fatal("expected batch rejection")
	}
	if !strings.Contains(Render(st), " 3\n") {
		t.Fatalf("partial state leaked:\n%s", Render(st))
	}
}

func TestHistogramAccumulate(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{histUpdate(10, 20, 3, 1.5, 1, 1, 1)})
	st.Apply([]pointUpdate{histUpdate(20, 30, 2, 3, 0, 2, 0)})
	out := Render(st)
	for _, want := range []string{
		"# TYPE latency histogram",
		`latency_bucket{instance="i1",scope="lib",le="0.5"} 1`,
		`latency_bucket{instance="i1",scope="lib",le="1"} 4`,
		`latency_bucket{instance="i1",scope="lib",le="+Inf"} 5`,
		`latency_sum{instance="i1",scope="lib"} 4.5`,
		`latency_count{instance="i1",scope="lib"} 5`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestHistogramBoundsImmutable(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{histUpdate(10, 20, 1, 1, 1, 0, 0)})
	bad := histUpdate(20, 30, 1, 1, 1, 0, 0)
	bad.bounds = []float64{0.1, 2}
	if err := st.Apply([]pointUpdate{bad}); err == nil {
		t.Fatal("expected bounds-change rejection")
	}
}

func TestNameTypeConflict(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{sumUpdate(10, 20, 3)})
	h := histUpdate(10, 20, 1, 1, 1, 0, 0)
	h.metric = "requests"
	if err := st.Apply([]pointUpdate{h}); err == nil {
		t.Fatal("expected same-name different-type rejection")
	}
}

func TestSeriesLimitAndExpiry(t *testing.T) {
	st := NewStore(1, time.Second)
	st.now = func() time.Time { return time.Unix(1000, 0) }
	st.Apply([]pointUpdate{sumUpdate(10, 20, 3)})
	u2 := sumUpdate(10, 20, 1)
	u2.metric = "other"
	if err := st.Apply([]pointUpdate{u2}); err != errLimit {
		t.Fatalf("expected limit error, got %v", err)
	}
	st.now = func() time.Time { return time.Unix(1002, 0) }
	if n := st.ExpireIdle(); n != 1 {
		t.Fatalf("expected 1 expired, got %d", n)
	}
	if err := st.Apply([]pointUpdate{u2}); err != nil {
		t.Fatalf("capacity not released after expiry: %v", err)
	}
}

func TestOverflowRejected(t *testing.T) {
	st := NewStore(100, time.Minute)
	st.Apply([]pointUpdate{histUpdate(10, 20, ^uint64(0), 1, 1, 0, ^uint64(0)-1)})
	if err := st.Apply([]pointUpdate{histUpdate(20, 30, 1, 1, 1, 0, 0)}); err == nil {
		t.Fatal("expected count overflow rejection")
	}
}

func TestRenderEscapesLabels(t *testing.T) {
	st := NewStore(100, time.Minute)
	u := sumUpdate(10, 20, 1)
	u.instance = "a\"b\\c\n"
	u.attrs = [][2]string{{"path", "/x\"y"}}
	st.Apply([]pointUpdate{u})
	out := Render(st)
	if !strings.Contains(out, `instance="a\"b\\c\n"`) {
		t.Fatalf("labels not escaped:\n%s", out)
	}
	if !strings.Contains(out, `path="/x\"y"`) {
		t.Fatalf("attr not escaped:\n%s", out)
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	if _, err := parseRequest([]byte("not protobuf")); err == nil {
		t.Fatal("expected protobuf error")
	}
}
