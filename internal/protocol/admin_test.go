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

package protocol

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"kimistore/internal/storage"
)

type MockObjectStore struct{}

func (m *MockObjectStore) Put(ctx context.Context, key string, r io.Reader) error { return nil }
func (m *MockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not found")
}

func (m *MockObjectStore) List(ctx context.Context, prefix string) ([]storage.ObjectMetadata, error) {
	return nil, nil
}
func (m *MockObjectStore) Delete(ctx context.Context, key string) error { return nil }
func (m *MockObjectStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return nil, nil
}

func TestAdminHandlers(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "admin_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewStorageEngine(tmpDir, &MockObjectStore{}, "test-bucket", storage.RetentionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Test CreateTopics
	t.Run("CreateTopics", func(t *testing.T) {
		reqEnc := NewEncoder()
		reqEnc.Int32(1) // Count = 1

		// Topic 1
		reqEnc.String("test-admin-topic")
		reqEnc.Int32(2)    // Partitions = 2
		reqEnc.Int16(1)    // Replication = 1
		reqEnc.Int32(0)    // Assignments
		reqEnc.Int32(0)    // Configs
		reqEnc.Int32(5000) // Timeout

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleCreateTopics(dec, respEnc, engine, 0)
		if err != nil {
			t.Fatalf("Handle error: %v", err)
		}

		// Decode Response
		respDec := NewDecoder(respBytes)
		cnt, _ := respDec.Int32()
		if cnt != 1 {
			t.Errorf("Expected 1 result, got %d", cnt)
		}
		name, _ := respDec.String()
		if name != "test-admin-topic" {
			t.Errorf("Expected topic name, got %s", name)
		}
		errCode, _ := respDec.Int16()
		if errCode != 0 {
			t.Errorf("Expected error 0, got %d", errCode)
		}

		// Verify via Engine
		parts, err := engine.GetPartitions("test-admin-topic")
		if err != nil {
			t.Errorf("GetPartitions failed: %v", err)
		}
		if len(parts) != 2 {
			// Note: ListPartitions might scan. Since we just created invalid/empty WALs?
			// Wait, CreateTopic accesses them, which creates directories.
			// ListPartitions scans directories.
			// But ListPartitions scans "topic/" then "0", "1".
			// So it should find 0 and 1.
			t.Errorf("Expected 2 partitions, got %v", parts)
		}
	})

	// Test DeleteTopics
	t.Run("DeleteTopics", func(t *testing.T) {
		reqEnc := NewEncoder()
		reqEnc.Int32(1)
		reqEnc.String("test-admin-topic")
		reqEnc.Int32(5000)

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleDeleteTopics(dec, respEnc, engine, 0)
		if err != nil {
			t.Fatal(err)
		}

		respDec := NewDecoder(respBytes)
		cnt, _ := respDec.Int32()
		if cnt != 1 {
			t.Errorf("Expected 1 result")
		}
		name, _ := respDec.String()
		if name != "test-admin-topic" {
			t.Errorf("bad name: %s", name)
		}
		errCode, _ := respDec.Int16()
		if errCode != 0 {
			t.Errorf("bad error: %d", errCode)
		}

		// Verify gone
		parts, _ := engine.GetPartitions("test-admin-topic")
		if len(parts) != 0 {
			t.Errorf("Expected 0 partitions after delete, got %v", parts)
		}

		// Verify disk gone
		if _, err := os.Stat(filepath.Join(tmpDir, "test-admin-topic")); !os.IsNotExist(err) {
			t.Errorf("Directory should be gone")
		}
	})
}
