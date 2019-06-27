package reporter

import (
	"math"
	"sort"
)

type Latency struct {
	latencies []uint32
	maxN      int
	max       uint32
	min       uint32
}

func (l *Latency) Push(t interface{}) {
	lt := t.(uint32)
	length := l.Len()
	if length < l.maxN {
		l.latencies = append(l.latencies, lt)
	} else if lt > l.latencies[0] {
		l.latencies[0] = lt
	}
	sort.Slice(l.latencies, l.Less)
	l.trackMin(lt)
	l.trackMax(lt)
}

func (l *Latency) trackMin(val uint32) {
	if val < l.min {
		l.min = val
	}
}

func (l *Latency) trackMax(val uint32) {
	if val > l.max {
		l.max = val
	}
}

func (l *Latency) Pop() interface{} {
	length := l.Len()
	if length == 0 {
		return nil
	}
	top := l.latencies[length-1]
	l.latencies = l.latencies[:length-1]
	return top
}

func (l *Latency) Swap(i, j int) { l.latencies[i], l.latencies[j] = l.latencies[j], l.latencies[i] }

func (l *Latency) Less(i, j int) bool { return l.latencies[i] < l.latencies[j] }

func (l *Latency) Len() int    { return len(l.latencies) }
func (l *Latency) Min() uint32 { return l.min }
func (l *Latency) Max() uint32 { return l.max }

func NewLatencyReporter(topN int) *Latency {
	return &Latency{
		maxN:      topN,
		min:       math.MaxUint32,
		max:       0,
		latencies: make([]uint32, 0),
	}
}
