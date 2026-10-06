package main

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type series struct {
	kind       kind
	metric     string
	instance   string
	scope      string
	attrs      [][2]string
	lastStart  uint64
	lastEnd    uint64
	lastFP     string
	value      float64
	bounds     []float64
	buckets    []uint64
	count      uint64
	hsum       float64
	lastActive time.Time
}

func (s *series) clone() *series {
	c := *s
	c.attrs = append([][2]string(nil), s.attrs...)
	c.bounds = append([]float64(nil), s.bounds...)
	c.buckets = append([]uint64(nil), s.buckets...)
	return &c
}

func seriesKey(instance, scope, metric string, attrs [][2]string) string {
	var b strings.Builder
	b.WriteString(instance)
	b.WriteByte(0)
	b.WriteString(scope)
	b.WriteByte(0)
	b.WriteString(metric)
	for _, a := range attrs {
		b.WriteByte(0)
		b.WriteString(a[0])
		b.WriteByte(0)
		b.WriteString(a[1])
	}
	return b.String()
}

func fingerprint(u pointUpdate) string {
	if u.kind == kindSum {
		return fmt.Sprintf("s:%g", u.delta)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "h:%d:%g", u.count, u.hsum)
	for _, x := range u.bounds {
		fmt.Fprintf(&b, ":%g", x)
	}
	for _, c := range u.buckets {
		fmt.Fprintf(&b, ":%d", c)
	}
	return b.String()
}

type Store struct {
	mu        sync.Mutex
	series    map[string]*series
	names     map[string]kind
	maxSeries int
	expiry    time.Duration
	now       func() time.Time
}

func NewStore(maxSeries int, expiry time.Duration) *Store {
	return &Store{
		series:    make(map[string]*series),
		names:     make(map[string]kind),
		maxSeries: maxSeries,
		expiry:    expiry,
		now:       time.Now,
	}
}

var errLimit = errors.New("series limit reached")

// Apply validates and atomically commits a batch of delta points.
func (st *Store) Apply(updates []pointUpdate) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.now()

	pending := make(map[string]*series)
	pendingNames := make(map[string]kind)
	get := func(u pointUpdate) (*series, bool) {
		key := seriesKey(u.instance, u.scope, u.metric, u.attrs)
		if s, ok := pending[key]; ok {
			return s, true
		}
		if s, ok := st.series[key]; ok {
			c := s.clone()
			pending[key] = c
			return c, true
		}
		return nil, false
	}

	for i, u := range updates {
		if k, ok := st.names[u.metric]; ok && k != u.kind {
			return fmt.Errorf("metric %q already registered as %s", u.metric, k)
		}
		if k, ok := pendingNames[u.metric]; ok && k != u.kind {
			return fmt.Errorf("metric %q has conflicting types in batch", u.metric)
		}
		pendingNames[u.metric] = u.kind

		s, exists := get(u)
		if !exists {
			ns := &series{
				kind: u.kind, metric: u.metric, instance: u.instance,
				scope: u.scope, attrs: u.attrs,
				lastActive: now,
			}
			if err := ns.init(u); err != nil {
				return fmt.Errorf("update %d: %w", i, err)
			}
			pending[seriesKey(u.instance, u.scope, u.metric, u.attrs)] = ns
			continue
		}
		if s.kind != u.kind {
			return fmt.Errorf("metric %q: type mismatch for existing series", u.metric)
		}
		if err := s.accumulate(u, now); err != nil {
			return fmt.Errorf("metric %q: %w", u.metric, err)
		}
	}

	newCount := 0
	for key := range pending {
		if _, ok := st.series[key]; !ok {
			newCount++
		}
	}
	if len(st.series)+newCount > st.maxSeries {
		return errLimit
	}

	for key, s := range pending {
		st.series[key] = s
	}
	for name, k := range pendingNames {
		st.names[name] = k
	}
	return nil
}

func (s *series) init(u pointUpdate) error {
	s.lastStart, s.lastEnd = u.start, u.end
	s.lastFP = fingerprint(u)
	if u.kind == kindSum {
		s.value = u.delta
		return nil
	}
	s.bounds = append([]float64(nil), u.bounds...)
	s.buckets = append([]uint64(nil), u.buckets...)
	s.count = u.count
	s.hsum = u.hsum
	return nil
}

func (s *series) accumulate(u pointUpdate, now time.Time) error {
	fp := fingerprint(u)
	if u.start == s.lastStart && u.end == s.lastEnd {
		if fp == s.lastFP {
			return nil
		}
		return errors.New("conflicting retransmission of the same interval")
	}
	if u.start < s.lastEnd {
		return errors.New("overlapping or regressive time interval")
	}
	if u.kind == kindHistogram && !boundsEqual(s.bounds, u.bounds) {
		return errors.New("histogram bucket bounds changed for an existing series")
	}
	if u.start > s.lastEnd {
		err := s.init(u)
		s.lastActive = now
		return err
	}
	if u.kind == kindSum {
		v := s.value + u.delta
		if math.IsInf(v, 0) || math.IsNaN(v) {
			return errors.New("cumulative sum overflow")
		}
		s.value = v
	} else {
		for i := range s.buckets {
			nb := s.buckets[i] + u.buckets[i]
			if nb < s.buckets[i] {
				return errors.New("cumulative bucket count overflow")
			}
			s.buckets[i] = nb
		}
		if s.count+u.count < s.count {
			return errors.New("cumulative count overflow")
		}
		s.count += u.count
		hs := s.hsum + u.hsum
		if math.IsInf(hs, 0) || math.IsNaN(hs) {
			return errors.New("cumulative histogram sum overflow")
		}
		s.hsum = hs
	}
	s.lastStart, s.lastEnd = u.start, u.end
	s.lastFP = fp
	s.lastActive = now
	return nil
}

func boundsEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ExpireIdle removes series idle longer than the configured expiry and
// releases their capacity. It never evicts active series.
func (st *Store) ExpireIdle() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	cutoff := st.now().Add(-st.expiry)
	removed := 0
	inUse := make(map[string]bool)
	for key, s := range st.series {
		if s.lastActive.Before(cutoff) {
			delete(st.series, key)
			removed++
		} else {
			inUse[s.metric] = true
		}
	}
	for name := range st.names {
		if !inUse[name] {
			delete(st.names, name)
		}
	}
	return removed
}

func (st *Store) snapshot() []*series {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make([]*series, 0, len(st.series))
	for _, s := range st.series {
		out = append(out, s.clone())
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.metric != b.metric {
			return a.metric < b.metric
		}
		if a.instance != b.instance {
			return a.instance < b.instance
		}
		if a.scope != b.scope {
			return a.scope < b.scope
		}
		return seriesKey("", "", "", a.attrs) < seriesKey("", "", "", b.attrs)
	})
	return out
}
