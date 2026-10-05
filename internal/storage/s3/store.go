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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"kimistore/internal/metrics"
	"kimistore/internal/storage"
)

// DefaultTimeout bounds a single object-store request.
//
// The SDK's dial and TLS handshakes have no deadline, and neither does a
// response that stops mid-body, so without this an agent can be pinned
// indefinitely by a storage endpoint that accepts a connection and then goes
// quiet. The engine applies its own bound on top; this is the backstop at the
// HTTP layer.
const DefaultTimeout = 30 * time.Second

// maxAttempts is how many times the SDK retries a throttled or transient
// failure before the engine sees an error.
const maxAttempts = 4

type Store struct {
	client *s3.Client
	bucket string
}

// The lease depends on the conditional half of this interface, so a Store that
// only satisfies ObjectStore would silently degrade the fence.
var _ storage.ConditionalObjectStore = (*Store)(nil)

func NewStore(ctx context.Context, bucket string, region string) (*Store, error) {
	return NewStoreWithTimeout(ctx, bucket, region, DefaultTimeout)
}

// NewStoreWithTimeout builds a store whose requests are bounded by timeout.
func NewStoreWithTimeout(ctx context.Context, bucket string, region string, timeout time.Duration) (*Store, error) {
	return newStore(ctx, bucket, region, timeout, maxAttempts)
}

// newStore is the shared constructor. attempts is a parameter so a test can
// isolate the cost of one bounded attempt from the SDK's retry budget.
func newStore(ctx context.Context, bucket string, region string, timeout time.Duration, attempts int) (*Store, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if attempts <= 0 {
		attempts = 1
	}

	// For testing/local: if region is "local", we could use MinIO or just rely on default credentials
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(attempts),
		config.WithRetryMode(aws.RetryModeStandard),
		config.WithHTTPClient(httpClient(timeout)),
	)
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

// httpClient returns an HTTP client with a whole-request timeout plus a
// per-response-header deadline, so neither a stalled connection nor a stalled
// first byte can hold a request open.
func httpClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = timeout
	transport.DialContext = (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	return &http.Client{Timeout: timeout, Transport: transport}
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	// Size the request when the caller can tell us. Without a length the SDK
	// has to work out or stream the size itself, which for a sealed segment
	// means copying a file we already know the length of.
	if size, ok := readerSize(r); ok {
		in.ContentLength = aws.Int64(size)
	}

	_, err := s.client.PutObject(ctx, in)
	if err != nil && ctx.Err() != nil {
		metrics.ObjStoreTimeouts.Inc()
	}
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

// GetRange reads length bytes from start. A length of zero or less means
// "to the end of the object", which S3 expresses as an open-ended range.
func (s *Store) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	var rangeHeader *string
	if length <= 0 {
		rangeHeader = aws.String(fmt.Sprintf("bytes=%d-", start))
	} else {
		rangeHeader = aws.String(fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	}
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

// GetVersion returns an object's bytes and the ETag a conditional write has to
// name. A missing key is reported as found=false rather than as an error,
// because "not there yet" is the normal state a lease starts from.
func (s *Store) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var missing *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &missing) || errors.As(err, &notFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	defer func() { _ = out.Body.Close() }()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, out.Body); err != nil {
		return nil, "", false, err
	}
	version := ""
	if out.ETag != nil {
		version = aws.ToString(out.ETag)
	}
	return buf.Bytes(), version, true, nil
}

// PutVersion writes data only when the object is still at version, or -- with
// an empty version -- only when it does not exist.
//
// This is what turns the writer lease into a fence. S3 enforces both
// preconditions on the server, so two agents racing to create the lease cannot
// both succeed, and neither can renew over a takeover that already happened.
func (s *Store) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}
	if version == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(version)
	}

	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", storage.ErrVersionMismatch
		}
		if ctx.Err() != nil {
			metrics.ObjStoreTimeouts.Inc()
		}
		return "", err
	}
	if out.ETag == nil {
		return "", nil
	}
	return aws.ToString(out.ETag), nil
}

// isPreconditionFailed recognises the S3 answer to a failed If-Match or
// If-None-Match. The SDK does not model these two codes as named error types,
// so they arrive as generic API errors, and a concurrent modification of the
// same key can report either code. To a compare-and-swap both mean "retry".
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "PreconditionFailed", "ConditionalRequestConflict", "412", "409":
		return true
	}
	return false
}

// readerSize reports the length of a body when it is cheaply knowable.
func readerSize(r io.Reader) (int64, bool) {
	switch v := r.(type) {
	case *os.File:
		if info, err := v.Stat(); err == nil {
			return info.Size(), true
		}
	case *bytes.Reader:
		return int64(v.Len()), true
	case *bytes.Buffer:
		return int64(v.Len()), true
	case *strings.Reader:
		return int64(v.Len()), true
	}
	return 0, false
}
