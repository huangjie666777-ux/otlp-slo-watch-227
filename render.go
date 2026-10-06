package main

import (
	"strconv"
	"strings"
)

func escapeLabelValue(v string) string {
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func labelSet(s *series, le string) string {
	var b strings.Builder
	b.WriteByte('{')
	b.WriteString(`instance="`)
	b.WriteString(escapeLabelValue(s.instance))
	b.WriteString(`",scope="`)
	b.WriteString(escapeLabelValue(s.scope))
	b.WriteByte('"')
	for _, a := range s.attrs {
		b.WriteByte(',')
		b.WriteString(a[0])
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(a[1]))
		b.WriteByte('"')
	}
	if le != "" {
		b.WriteString(`,le="`)
		b.WriteString(le)
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// Render produces a Prometheus 0.0.4 text exposition of a consistent snapshot.
func Render(st *Store) string {
	series := st.snapshot()
	var b strings.Builder
	seenType := make(map[string]bool)
	for _, s := range series {
		if s.kind == kindSum {
			name := s.metric + "_total"
			if !seenType[name] {
				b.WriteString("# TYPE ")
				b.WriteString(name)
				b.WriteString(" counter\n")
				seenType[name] = true
			}
			b.WriteString(name)
			b.WriteString(labelSet(s, ""))
			b.WriteByte(' ')
			b.WriteString(formatFloat(s.value))
			b.WriteByte('\n')
			continue
		}
		if !seenType[s.metric] {
			b.WriteString("# TYPE ")
			b.WriteString(s.metric)
			b.WriteString(" histogram\n")
			seenType[s.metric] = true
		}
		var cum uint64
		for i, bound := range s.bounds {
			cum += s.buckets[i]
			b.WriteString(s.metric)
			b.WriteString("_bucket")
			b.WriteString(labelSet(s, formatFloat(bound)))
			b.WriteByte(' ')
			b.WriteString(strconv.FormatUint(cum, 10))
			b.WriteByte('\n')
		}
		cum += s.buckets[len(s.buckets)-1]
		b.WriteString(s.metric)
		b.WriteString("_bucket")
		b.WriteString(labelSet(s, "+Inf"))
		b.WriteByte(' ')
		b.WriteString(strconv.FormatUint(cum, 10))
		b.WriteByte('\n')
		b.WriteString(s.metric)
		b.WriteString("_sum")
		b.WriteString(labelSet(s, ""))
		b.WriteByte(' ')
		b.WriteString(formatFloat(s.hsum))
		b.WriteByte('\n')
		b.WriteString(s.metric)
		b.WriteString("_count")
		b.WriteString(labelSet(s, ""))
		b.WriteByte(' ')
		b.WriteString(strconv.FormatUint(s.count, 10))
		b.WriteByte('\n')
	}
	return b.String()
}
