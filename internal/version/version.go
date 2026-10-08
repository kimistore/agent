// Package version reports which build is running.
//
// The values are set at link time by the Makefile and by the release
// workflow. A build that does not set them reports "dev" for the version and
// an empty commit, which is deliberate: an unstamped build should look
// unstamped in the log rather than claim a release it is not.
package version

import (
	"fmt"
	"runtime"
)

// These are overridden with -ldflags -X. Do not rename them without updating
// the Makefile and the release workflow.
var (
	// Version is a semantic version such as 1.0.0, or dev.
	Version = "dev"

	// Commit is the full git commit SHA the build came from.
	Commit = ""

	// Date is the UTC build timestamp.
	Date = ""
)

// String returns a one-line description of the build.
func String() string {
	out := "kimistore " + Version
	if Commit != "" {
		out += " (" + short(Commit) + ")"
	}
	if Date != "" {
		out += " built " + Date
	}
	out += " " + runtime.Version()
	return out
}

// Line returns the same description for a startup log line.
func Line() string { return fmt.Sprintf("Version: %s", String()) }

// short trims a commit SHA to the seven characters Git shows by default.
func short(commit string) string {
	if len(commit) > 7 {
		return commit[:7]
	}
	return commit
}
