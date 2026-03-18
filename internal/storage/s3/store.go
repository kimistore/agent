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

package s3

import (
	"context"
	"fmt"
	"io"

	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"kimistore/internal/storage"
)

type Store struct {
	client *s3.Client
	bucket string
}

func NewStore(ctx context.Context, bucket string, region string) (*Store, error) {
	// For testing/local: if region is "local", we could use MinIO or just rely on default credentials
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		endpoint := os.Getenv("S3_ENDPOINT")
		if endpoint == "" {
			endpoint = os.Getenv("AWS_ENDPOINT_URL")
		}
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return &Store{
		client: client,
		bucket: bucket,
	}, nil
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	// We need to know Size for S3 PutObject optimally, or use a Seekable reader.
	// For now assume r provides content.

	// If r is a bytes.Buffer or strings.Reader, this works fine.
	// If it's a file, we should probably pass the file so SDK can seek.

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	return err
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]storage.ObjectMetadata, error) {
	var results []storage.ObjectMetadata
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			meta := storage.ObjectMetadata{
				Key:  *obj.Key,
				Size: *obj.Size,
			}
			if obj.LastModified != nil {
				meta.LastModified = obj.LastModified.Unix()
			}
			results = append(results, meta)
		}
	}
	return results, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *Store) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	rangeHeader := aws.String(fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  rangeHeader,
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}
