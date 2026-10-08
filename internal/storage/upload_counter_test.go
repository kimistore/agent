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

package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
)

// TestReconciliationUploadBalancesThePendingCounter is a regression test for a
// bug found in production.
//
// The uploader worker decrements pendingUploads after every task it processes.
// The fast path increments before sending. The reconciliation path did not, so
// every reconciled segment drove the counter one below zero and never back.
//
// Two things followed from that. drainUploads waits for exactly zero, so a
// negative count meant it spun until its budget expired: every shutdown stalled
// for the full 30 seconds and then logged a negative number of segments still
// uploading. Observed on knode3 as "Shutdown: -143 segment(s) still uploading
// after 30s".
func TestReconciliationUploadBalancesThePendingCounter(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(50), 50, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Seal a segment and wait for the fast path to finish with it. The fast path
	// deletes the local file once the upload lands, so by this point the
	// reconciler would find nothing: the counter can only go negative for a
	// segment the fast path never touched, which is exactly the case that
	// produced the production log line.
	engine.walMgr.FlushPartition("orders", 0)
	waitForPendingUploads(t, engine, 0)
	waitForNoSealedSegments(t, engine)

	// Drop an orphaned sealed segment onto disk, standing in for one left behind
	// by a crash between seal and upload.
	epoch := engine.partitionEpoch("orders", 0)
	const orphanBase = 100
	writeOrphanSegment(t, engine.walDir, "orders", 0, orphanBase, epoch)

	engine.uploadSegments()

	// Wait for the worker to have taken and finished the task, rather than for
	// the counter to read zero. Reading zero is racy: the counter is still zero
	// in the window between the send and the decrement, so a test that waits for
	// zero returns immediately and asserts before the bug can happen.
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(orphanBase, epoch))
	waitForPendingUploads(t, engine, 0)

	if got := engine.pendingUploads.Load(); got != 0 {
		t.Errorf("pendingUploads = %d after a reconciliation upload, want 0; "+
			"a negative count makes every shutdown wait out its full budget", got)
	}
}

// TestDrainUploadsReturnsImmediatelyWhenIdle is the operational half of the same
// bug: a clean shutdown must not pay the drain budget when there is nothing to
// wait for.
func TestDrainUploadsReturnsImmediatelyWhenIdle(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	engine.walMgr.FlushPartition("orders", 0)
	waitForPendingUploads(t, engine, 0)

	start := time.Now()
	engine.drainUploads(30 * time.Second)
	elapsed := time.Since(start)

	// The function polls every 5ms, so an idle drain returns in well under a
	// second. The bug made this take the full budget.
	if elapsed > 2*time.Second {
		t.Errorf("drainUploads took %s with nothing pending, want an immediate return",
			elapsed.Truncate(time.Millisecond))
	}
}

// waitForNoSealedSegments waits until the fast path has removed the sealed
// segment it uploaded, so the test knows the reconciler starts from an empty
// local directory.
func waitForNoSealedSegments(t *testing.T, engine *StorageEngine) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if localSealedSegments(t, engine.walDir) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the sealed segment was never removed; the fast path did not complete")
}

// writeOrphanSegment writes a sealed segment file directly onto local disk,
// standing in for one a crash left behind between the seal and the upload.
func writeOrphanSegment(t *testing.T, walDir, topic string, partition int32, baseOffset, epoch int64) {
	t.Helper()

	dir := filepath.Join(walDir, topic, strconv.Itoa(int(partition)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	body := []byte("orphan segment body")
	if err := os.WriteFile(filepath.Join(dir, wal.SegmentName(baseOffset, epoch)), body, 0o644); err != nil {
		t.Fatalf("write orphan segment: %v", err)
	}
}

func waitForPendingUploads(t *testing.T, engine *StorageEngine, want int64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if engine.pendingUploads.Load() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pendingUploads = %d, want %d after 10s", engine.pendingUploads.Load(), want)
}
