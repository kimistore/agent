package protocol

import (
	"os"
	"testing"

	"kimistore/internal/coordinator"
	"kimistore/internal/storage"
)

func TestGroupObservabilityHandlers(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "group_obs_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewStorageEngine(tmpDir, &MockObjectStore{}, "test-bucket", storage.RetentionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Seed Coordinator with a fake group
	GlobalCoordinator = coordinator.NewCoordinator()

	protocols := []coordinator.GroupProtocol{
		{Name: "range", Metadata: []byte{}},
	}
	_, _, _, _, err = GlobalCoordinator.JoinGroup("test-group-obs", "member-1", "consumer", protocols, 10000, 10000)
	if err != nil {
		t.Fatal(err)
	}

	// Test ListGroups
	t.Run("ListGroups", func(t *testing.T) {
		respBytes, err := handleListGroups(nil, NewEncoder(), engine, 0)
		if err != nil {
			t.Fatal(err)
		}

		dec := NewDecoder(respBytes)
		errCode, _ := dec.Int16()
		if errCode != 0 {
			t.Errorf("Expected 0 error, got %d", errCode)
		}

		cnt, _ := dec.Int32()
		if cnt != 1 {
			t.Errorf("Expected 1 group, got %d", cnt)
		}

		gid, _ := dec.String()
		if gid != "test-group-obs" {
			t.Errorf("Expected test-group-obs, got %s", gid)
		}
	})

	// Test DescribeGroups
	t.Run("DescribeGroups", func(t *testing.T) {
		reqEnc := NewEncoder()
		reqEnc.Int32(1)
		reqEnc.String("test-group-obs")
		dec := NewDecoder(reqEnc.Bytes())

		respBytes, err := handleDescribeGroups(dec, NewEncoder(), engine, 0)
		if err != nil {
			t.Fatal(err)
		}

		respDec := NewDecoder(respBytes)
		cnt, _ := respDec.Int32()
		if cnt != 1 {
			t.Errorf("Expected 1 group, got %d", cnt)
		}

		errCode, _ := respDec.Int16()
		if errCode != 0 {
			t.Errorf("Expected error 0, got %d", errCode)
		}

		gid, _ := respDec.String()
		if gid != "test-group-obs" {
			t.Errorf("Bad GID: %s", gid)
		}

		state, _ := respDec.String()
		// It might be "CompletingRebalance" because we just Joined but didn't Sync.
		if state == "" {
			t.Errorf("Empty state")
		}

		pType, _ := respDec.String()
		if pType != "consumer" {
			t.Errorf("Expected consumer, got %s", pType)
		}

		_, _ = respDec.String() // Protocol

		memCount, _ := respDec.Int32()
		if memCount != 1 {
			t.Errorf("Expected 1 member, got %d", memCount)
		}
	})
}
