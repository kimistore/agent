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

package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// A segment name has to carry the ownership epoch, or a superseded writer's
// upload lands on its successor's key and destroys it.
func TestSegmentName_CarriesTheEpoch(t *testing.T) {
	if got, want := SegmentName(42, 7), "00000000000000000042-e7.log"; got != want {
		t.Errorf("SegmentName(42, 7) = %q, want %q", got, want)
	}
	// No epoch means the pre-ownership name, unchanged, so a bucket written
	// before this change keeps reading and keeps its keys.
	if got, want := SegmentName(42, 0), "00000000000000000042.log"; got != want {
		t.Errorf("SegmentName(42, 0) = %q, want %q", got, want)
	}
	if got, want := SegmentName(42, -1), "00000000000000000042.log"; got != want {
		t.Errorf("a negative epoch should be treated as none, got %q want %q", got, want)
	}
}

// Both shapes have to parse, or segments written before ownership existed
// become unreadable on upgrade.
func TestParseSegmentName_ReadsBothShapes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		offset  int64
		epoch   int64
		wantOK  bool
		comment string
	}{
		{name: "00000000000000000042.log", offset: 42, epoch: 0, wantOK: true, comment: "pre-ownership"},
		{name: "00000000000000000042-e7.log", offset: 42, epoch: 7, wantOK: true},
		{name: "0-e1.log", offset: 0, epoch: 1, wantOK: true},
		{name: "active.log", wantOK: false, comment: "the active segment is not a sealed one"},
		{name: "00000000000000000042.index", wantOK: false},
		{name: "00000000000000000042.log.gz", wantOK: false},
		{name: "notanumber.log", wantOK: false},
		{name: "00000000000000000042-eseven.log", wantOK: false, comment: "a non-numeric epoch"},
		{name: "noextension", wantOK: false},
		{name: "-e7.log", wantOK: false, comment: "an epoch with no offset"},
	} {
		offset, epoch, ok := ParseSegmentName(tc.name)
		if ok != tc.wantOK {
			t.Errorf("ParseSegmentName(%q) ok = %v, want %v", tc.name, ok, tc.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if offset != tc.offset || epoch != tc.epoch {
			t.Errorf("ParseSegmentName(%q) = (%d, %d), want (%d, %d) %s",
				tc.name, offset, epoch, tc.offset, tc.epoch, tc.comment)
		}
	}
}

// The round trip has to hold for any offset and epoch: this is what every
// reader of a segment name depends on.
func TestSegmentName_RoundTrips(t *testing.T) {
	for _, offset := range []int64{0, 1, 999, 1 << 40} {
		for _, epoch := range []int64{1, 2, 99, 1 << 20} {
			name := SegmentName(offset, epoch)
			gotOffset, gotEpoch, ok := ParseSegmentName(name)
			if !ok {
				t.Fatalf("ParseSegmentName(%q) failed", name)
			}
			if gotOffset != offset || gotEpoch != epoch {
				t.Errorf("%q round-tripped to (%d, %d), want (%d, %d)", name, gotOffset, gotEpoch, offset, epoch)
			}
		}
	}
}

// A sealed segment must be named for the epoch in force when it was written, and
// a partition that is re-claimed afterwards must not rename what is already on
// disk.
func TestPartitionWAL_SealedNameFollowsTheEpoch(t *testing.T) {
	dir := t.TempDir()
	sealed := make(chan UploadTask, 4)

	pw, err := NewPartitionWAL(dir, "orders", 0, 0, 3, func(tk UploadTask) { sealed <- tk })
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := pw.Append(nRecordsV2(0, 1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := pw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	task := <-sealed
	if task.Epoch != 3 {
		t.Errorf("upload task epoch = %d, want 3", task.Epoch)
	}
	if got, want := filepath.Base(task.Path), SegmentName(0, 3); got != want {
		t.Errorf("sealed as %q, want %q", got, want)
	}

	// A later epoch applies to the next segment only; the sealed one keeps the
	// epoch it was written under, which is what makes a late upload of it safe.
	pw.SetEpoch(4)
	if _, err := pw.Append(nRecordsV2(0, 1), 1, true); err != nil {
		t.Fatalf("append after re-claim: %v", err)
	}
	if err := pw.Flush(); err != nil {
		t.Fatalf("flush after re-claim: %v", err)
	}
	next := <-sealed
	if next.Epoch != 4 {
		t.Errorf("second upload task epoch = %d, want 4", next.Epoch)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// The epoch must never move backwards. A segment named after a lower epoch than
// the partition is currently written under would sort below the real one and be
// picked as the newest, which is how a read ends up serving replaced data.
func TestPartitionWAL_SetEpochNeverGoesBackwards(t *testing.T) {
	pw, err := NewPartitionWAL(t.TempDir(), "orders", 0, 0, 5, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	pw.SetEpoch(2)
	if got := pw.Epoch(); got != 5 {
		t.Errorf("epoch = %d after a lower SetEpoch, want 5", got)
	}
	pw.SetEpoch(6)
	if got := pw.Epoch(); got != 6 {
		t.Errorf("epoch = %d after a higher SetEpoch, want 6", got)
	}
}

// A partition opened after its claim was taken must already know the epoch, so
// its first segment is named correctly rather than falling back to epoch 0.
func TestManager_OpeningAfterAClaimCarriesTheEpoch(t *testing.T) {
	mgr, err := NewManager(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()

	mgr.SetEpoch("orders", 2, 9)
	if got := mgr.Epoch("orders", 2); got != 9 {
		t.Fatalf("Epoch = %d, want 9", got)
	}

	if _, err := mgr.Append("orders", 2, nRecordsV2(0, 1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	mgr.FlushPartition("orders", 2)
	if got := mgr.Epoch("orders", 2); got != 9 {
		t.Errorf("the epoch must survive opening the partition WAL, got %d", got)
	}
}

// Recovery from local disk has to pick the newest segment, which at an equal base
// offset is the one with the highest epoch. Picking the superseded copy would
// restore a log position from data the current owner replaced.
func TestPartitionWAL_RecoveryPrefersTheHighestEpochAtAnOffset(t *testing.T) {
	dir := t.TempDir()
	pw, err := NewPartitionWAL(dir, "orders", 0, 0, 1, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := pw.Append(nRecordsV2(0, 1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A segment written later at the same base offset, by a newer owner.
	// Two segments at the same base offset: the one written at epoch 1 covers
	// offsets 0-1, the one written at epoch 2 covers 0-2. Recovery has to end
	// where the newer one does.
	writeSealedSegment(t, filepath.Join(dir, SegmentName(0, 1)), nRecordsV2(0, 1))
	writeSealedSegment(t, filepath.Join(dir, SegmentName(0, 2)), nRecordsV2(0, 2))

	recovered, err := NewPartitionWAL(dir, "orders", 0, 0, 2, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()

	if got := recovered.nextOffset; got != 2 {
		t.Errorf("recovered next offset = %d, want 2 (the end of the highest-epoch segment)", got)
	}
}

// writeSealedSegment writes one sealed segment file holding a single stored
// entry, in the same on-disk layout the WAL uses: [offset(8)][size(4)][body].
func writeSealedSegment(t *testing.T, path string, body []byte) {
	t.Helper()
	entry := make([]byte, 12+len(body))
	binary.BigEndian.PutUint64(entry[0:8], 0)
	binary.BigEndian.PutUint32(entry[8:12], uint32(len(body)))
	copy(entry[12:], body)
	if err := os.WriteFile(path, entry, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
