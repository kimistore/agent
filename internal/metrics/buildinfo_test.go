package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestBuildInfoIsRegisteredOnce guards a real hazard. Init registers with the
// promauto package, which uses the default registerer and panics on a
// duplicate name. Every test in this package that calls Init therefore shares
// one registration, so this test calls Init exactly once and the others must
// not.
func TestBuildInfoIsRegisteredOnce(t *testing.T) {
	Init(
		func() float64 { return 1 },
		func() float64 { return 1 },
	)

	if BuildInfo == nil {
		t.Fatal("BuildInfo is nil after Init")
	}

	metric, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	var found bool
	for _, m := range metric {
		if m.GetName() == "kimistore_build_info" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("kimistore_build_info was not exposed by the default gatherer")
	}
}

// TestBuildInfoLabelsCarryTheStamp checks that the version reaches the scrape
// output. A build_info metric with empty labels is the failure this prevents:
// it looks healthy in a dashboard while telling an operator nothing.
func TestBuildInfoLabelsCarryTheStamp(t *testing.T) {
	// Registration is shared across the package, so only gather.
	metric, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	for _, m := range metric {
		if m.GetName() != "kimistore_build_info" {
			continue
		}

		mf := m.GetMetric()
		if len(mf) != 1 {
			t.Fatalf("got %d series for kimistore_build_info, want 1", len(mf))
		}

		if got := mf[0].GetGauge().GetValue(); got != 1 {
			t.Errorf("kimistore_build_info value = %v, want 1", got)
		}

		labels := map[string]string{}
		for _, lp := range mf[0].GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}

		for _, want := range []string{"version", "commit", "date"} {
			if _, ok := labels[want]; !ok {
				t.Errorf("kimistore_build_info is missing the %q label", want)
			}
		}

		// The version label must never be empty. A release build stamps it,
		// and an unstamped build reports "dev" rather than nothing, so an
		// empty value would mean the stamping path broke.
		if v := labels["version"]; strings.TrimSpace(v) == "" {
			t.Error("kimistore_build_info version label is empty, want \"dev\" or a semantic version")
		}
		return
	}

	// The other test in this package may not have run yet under -run filters.
	t.Skip("kimistore_build_info not registered yet; run the whole package")
}
