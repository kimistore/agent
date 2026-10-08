package version

import (
	"runtime"
	"testing"
)

func TestDefaultsReportDev(t *testing.T) {
	// The package-level variables are overwritten by -ldflags in a release
	// build. A test binary keeps the defaults, so the tests below restore the
	// originals after each case rather than assuming a value.
	origVersion, origCommit, origDate := Version, Commit, Date
	t.Cleanup(func() { Version, Commit, Date = origVersion, origCommit, origDate })

	if Version != "dev" {
		t.Fatalf("Version default = %q, want %q", Version, "dev")
	}
}

func TestString(t *testing.T) {
	origVersion, origCommit, origDate := Version, Commit, Date
	t.Cleanup(func() { Version, Commit, Date = origVersion, origCommit, origDate })

	cases := []struct {
		name    string
		version string
		commit  string
		date    string
		want    []string
		absent  []string
	}{
		{
			name:    "unstamped build",
			version: "dev",
			want:    []string{"kimistore dev"},
			absent:  []string{"built", "("},
		},
		{
			name:    "release build",
			version: "1.0.0",
			commit:  "0123456789abcdef0123456789abcdef01234567",
			date:    "2026-10-08T10:00:00Z",
			want:    []string{"kimistore 1.0.0", "0123456", "built"},
			absent:  nil,
		},
		{
			name:    "short commit is not truncated further",
			version: "1.0.0",
			commit:  "abc",
			want:    []string{"kimistore 1.0.0", "abc"},
			absent:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			Version, Commit, Date = tc.version, tc.commit, tc.date
			got := String()
			for _, w := range tc.want {
				if !contains(got, w) {
					t.Errorf("String() = %q, want it to contain %q", got, w)
				}
			}
			for _, a := range tc.absent {
				if contains(got, a) {
					t.Errorf("String() = %q, want it not to contain %q", got, a)
				}
			}
		})
	}
}

func TestLineIsPrefixed(t *testing.T) {
	origVersion := Version
	t.Cleanup(func() { Version = origVersion })

	Version = "1.2.3"
	got := Line()

	const want = "Version: kimistore 1.2.3"
	if len(got) < len(want) || got[:len(want)] != want {
		t.Fatalf("Line() = %q, want it to begin with %q", got, want)
	}
	// The line must also name the Go runtime, so a bug report from the field
	// carries enough information to reproduce the build.
	if !contains(got, runtime.Version()) {
		t.Errorf("Line() = %q, want it to contain the Go version %q", got, runtime.Version())
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
