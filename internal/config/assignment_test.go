/*
 * Copyright 2026 by Andy Lo-A-Foe
 *
 * This file is part of kimistore-agent.
 *
 * Licensed under the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package config

import (
	"strings"
	"testing"
)

// Assignment is opt-in and validated up front, because a misconfigured fleet
// that discovers the problem at rebalance time has already started moving
// partitions around.
func TestAssignmentConfig(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		t.Setenv("KIMISTORE_ASSIGNMENT", "")
		cfg := FromEnv()
		if cfg.Assignment.Enabled {
			t.Error("assignment is on by default; it must be opt-in so an upgrade " +
				"does not start releasing partitions in an existing deployment")
		}
	})

	t.Run("needs per-partition ownership", func(t *testing.T) {
		t.Setenv("KIMISTORE_ASSIGNMENT", "rendezvous")
		t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "false")

		cfg := FromEnv()
		err := cfg.Validate()
		if err == nil {
			t.Fatal("assignment without per-partition ownership must be refused")
		}
		// The claim in the object store is what settles a disagreement between
		// two agents that computed different assignments, so ownership is not
		// optional here.
		if !strings.Contains(err.Error(), "PARTITION_OWNERSHIP") {
			t.Errorf("the error does not name what to set: %v", err)
		}
	})

	t.Run("accepted with ownership", func(t *testing.T) {
		t.Setenv("KIMISTORE_ASSIGNMENT", "rendezvous")
		t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "true")

		c := FromEnv()
		if err := c.Validate(); err != nil {
			t.Fatalf("assignment with ownership must be accepted: %v", err)
		}
		if !c.Assignment.Enabled {
			t.Error("assignment is not enabled")
		}
		// The settle window defaults to the ownership TTL, so the live set has to
		// hold still for as long as a claim survives.
		if c.Assignment.Settle != c.Ownership.TTL {
			t.Errorf("settle = %s, want the ownership TTL %s", c.Assignment.Settle, c.Ownership.TTL)
		}
	})

	t.Run("rejects a zero settle window", func(t *testing.T) {
		t.Setenv("KIMISTORE_ASSIGNMENT", "rendezvous")
		t.Setenv("KIMISTORE_ASSIGNMENT_SETTLE_MS", "0")
		cfg := FromEnv()
		if err := cfg.Validate(); err == nil {
			t.Error("a zero settle window must be refused: every tick would rebalance")
		}
	})

	t.Run("an unknown mode does not enable it", func(t *testing.T) {
		// Silently defaulting to "on" for a typo would start releasing partitions
		// in a fleet that never asked for it.
		t.Setenv("KIMISTORE_ASSIGNMENT", "rendezvousing")
		cfg := FromEnv()
		if cfg.Assignment.Enabled {
			t.Error("an unrecognised mode enabled assignment")
		}
	})
}
