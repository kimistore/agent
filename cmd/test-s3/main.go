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
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"kimistore/internal/storage/s3"
)

func main() {
	// NOTE: This test requires AWS credentials to be configured in env or ~/.aws/credentials
	// AND a valid bucket name.

	bucket := os.Getenv("TEST_BUCKET")
	if bucket == "" {
		fmt.Println("Skipping S3 test (TEST_BUCKET not set)")
		return
	}

	ctx := context.Background()
	s, err := s3.NewStore(ctx, bucket, "us-east-1") // Assumed region
	if err != nil {
		log.Fatal(err)
	}

	key := "test-kimistore/hello.txt"
	body := "Hello Object Storage!"

	fmt.Printf("Uploading to %s/%s...\n", bucket, key)
	if err := s.Put(ctx, key, strings.NewReader(body)); err != nil {
		log.Fatal("Put failed:", err)
	}

	fmt.Println("Downloading...")
	r, err := s.Get(ctx, key)
	if err != nil {
		log.Fatal("Get failed:", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		log.Fatal("Read failed:", err)
	}

	fmt.Printf("Content: %s\n", string(data))
}
