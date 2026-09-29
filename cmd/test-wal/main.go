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

package main

import (
	"fmt"
	"log"
	"os"

	"kimistore/internal/storage/wal"
)

func main() {
	tmpDir := "./tmp-wal-test"
	os.RemoveAll(tmpDir) // cleanup

	mgr, err := wal.NewManager(tmpDir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	topic := "my-topic"
	partition := int32(0)

	// Write
	msg := []byte("Hello Kimistore!")
	off, err := mgr.Append(topic, partition, msg, 1, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Appended at offset: %d\n", off)

	// Read
	readMsg, err := mgr.Read(topic, partition, off)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read message: %s\n", string(readMsg))

	if string(readMsg) != string(msg) {
		log.Fatal("Message mismatch")
	}

	// Write more
	off2, _ := mgr.Append(topic, partition, []byte("Msg 2"), 1, true)
	fmt.Printf("Appended at offset: %d\n", off2)

	readMsg2, _ := mgr.Read(topic, partition, off2)
	fmt.Printf("Read message 2: %s\n", string(readMsg2))
}
