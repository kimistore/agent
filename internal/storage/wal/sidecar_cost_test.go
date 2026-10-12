package wal

import "testing"

// The sidecar grows with the segment, and it is fetched in full on every read
// before any record data is returned. This test records the arithmetic so the
// relationship is visible next to the constant it constrains.
func TestMaxSegmentSize_SidecarStaysSmall(t *testing.T) {
	const entrySize = 8
	const indexEvery = 4096

	sidecarFor := func(segBytes int64) int64 {
		return (segBytes / indexEvery) * entrySize
	}

	got := sidecarFor(MaxSegmentSize)
	// The ceiling is the fetch budget, because that is what bounds the record
	// data a read returns. A sidecar at or under it cannot dominate the read it
	// is meant to accelerate.
	//
	// This was 1MB, which was too loose to have any effect: a 256MB segment
	// produces a 512KB sidecar and passed anyway. At 64KB -- an eighth of the
	// 512KB default fetch budget -- the 256MB value fails and the 16MB value
	// passes with room to spare.
	const budget = int64(64 * 1024)
	if got > budget {
		t.Errorf("a %d-byte segment needs a %d-byte sidecar, over the %d-byte ceiling; "+
			"every read fetches it in full before returning a record", MaxSegmentSize, got, budget)
	}
	t.Logf("  MaxSegmentSize=%d -> sidecar %d bytes (%.1f KB)", MaxSegmentSize, got, float64(got)/1024)

	// And the reduction against the segment size this workload produces.
	const observed = int64(45 * 1024)
	t.Logf("  reduction vs 45KB observed segments: %.0fx", float64(MaxSegmentSize)/float64(observed))
}
