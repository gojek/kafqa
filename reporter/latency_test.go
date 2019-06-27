package reporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldInsertAllDataWhenItsBelowLimit(t *testing.T) {
	lr := NewLatencyReporter(5)
	latencies := []uint32{1, 2, 3, 4, 5}
	for _, l := range latencies {
		lr.Push(l)
	}

	assert.Equal(t, len(latencies), lr.Len())
	assert.Equal(t, latencies, lr.latencies)
}

func TestShouldInsertElementsInOrder(t *testing.T) {
	lr := NewLatencyReporter(5)
	for _, l := range []uint32{1, 3, 2, 5, 4} {
		lr.Push(l)
	}

	assert.Equal(t, 5, lr.Len())
	assert.Equal(t, []uint32{1, 2, 3, 4, 5}, lr.latencies)

}

func TestShouldInsertDataDroppingMin(t *testing.T) {
	lr := NewLatencyReporter(5)
	latencies := []uint32{1, 3, 2, 5, 4, 100}
	for _, l := range latencies {
		lr.Push(l)
	}

	assert.Equal(t, 5, lr.Len())
	assert.Equal(t, []uint32{2, 3, 4, 5, 100}, lr.latencies)
}

func TestShouldKeepTopForPercentiles(t *testing.T) {
	lr := NewLatencyReporter(3)
	latencies := []uint32{1, 10, 2, 50, 4, 300, 123, 200, 900, 1, 2, 10, 50, 1000, 12, 1, 4}
	for _, l := range latencies {
		lr.Push(l)
	}

	assert.Equal(t, 3, lr.Len())
	assert.Equal(t, []uint32{300, 900, 1000}, lr.latencies)
}

func TestShouldKeepTrackOfMinAndMax(t *testing.T) {
	maxN := 5
	lr := NewLatencyReporter(maxN)
	latencies := []uint32{12, 3, 1, 50, 70, 100, 20, 40, 90}
	for _, l := range latencies {
		lr.Push(l)
	}

	assert.Equal(t, maxN, lr.Len())
	assert.Equal(t, uint32(1), lr.Min(), "min doesn't match")
	assert.Equal(t, uint32(100), lr.Max(), "max doesn't match")
}
