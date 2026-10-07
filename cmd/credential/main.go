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

// Command kimistore-credential manages the SCRAM credentials in a bucket.
//
// It is a separate binary from the agent on purpose. Writing a credential is an
// operator action that must not require starting a broker, and -- more
// importantly -- it must not be something the agent does to itself, so there is
// no path by which a running process can create the credential that authorises
// it.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"kimistore/internal/auth"
	"kimistore/internal/config"
	"kimistore/internal/storage/s3"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "kimistore-credential: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("no command given")
	}
	switch args[0] {
	case "create":
		return cmdCreate(args[1:])
	case "delete":
		return cmdDelete(args[1:])
	case "list":
		return cmdList(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `kimistore-credential manages SCRAM credentials in a bucket.

Usage:
  kimistore-credential create -user NAME [-stdin | -password-stdin]
                               [-mechanism SCRAM-SHA-256|SCRAM-SHA-512]
                               [-iterations N]
  kimistore-credential delete -user NAME
  kimistore-credential list

The bucket and region come from the same environment the agent uses
(S3_BUCKET, AWS_REGION), so credentials land in the same bucket the agent
reads.

The password is never accepted as a command-line argument. Arguments are
visible to every process on the host through /proc and to anything reading the
shell history; -stdin reads it from stdin instead.

After creating or deleting a credential, existing connections stay
authenticated. SCRAM is checked once per connection, so a rotation takes
effect for new connections, not for ones already open.
`)
}

// readPassword reads a password without ever putting it in argv.
func readPassword(fs *flag.FlagSet) (string, error) {
	if fs.Lookup("password-stdin") != nil {
		r := bufio.NewReader(os.Stdin)
		line, err := r.ReadString('\n')
		if err != nil && line == "" {
			return "", fmt.Errorf("read password from stdin: %w", err)
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
	// No terminal here, so prompting is not an option without pulling in a
	// dependency; say so rather than silently reading nothing.
	return "", errors.New("no password source: pass -password-stdin and pipe the password")
}

// objectAdapter presents a raw object store as the credential store's view of
// object storage.
//
// It exists so this tool never opens a StorageEngine. Opening one claims
// partitions, publishes liveness and writes a checkpoint, and running a second
// process against the same WAL directory as a live agent is a good way to have
// two writers disagree about who owns a partition. Credential management touches
// exactly one prefix of one bucket and has no business doing any of that.
type objectAdapter struct{ *s3.Store }

func (a objectAdapter) GetObject(ctx context.Context, key string) ([]byte, error) {
	r, err := a.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

func (a objectAdapter) PutObject(ctx context.Context, key string, data []byte) error {
	return a.Put(ctx, key, bytes.NewReader(data))
}

func (a objectAdapter) ListObjectKeys(ctx context.Context, prefix string) ([]string, error) {
	objs, err := a.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys, nil
}

func (a objectAdapter) DeleteObject(ctx context.Context, key string) error {
	return a.Delete(ctx, key)
}

// openStore builds the credential store from the agent's own configuration,
// touching the bucket but never the WAL.
func openStore(ctx context.Context) (*auth.Store, error) {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	objStore, err := s3.NewStoreWithTimeout(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Timeout)
	if err != nil {
		return nil, fmt.Errorf("init S3: %w", err)
	}
	return auth.NewStore(objectAdapter{objStore})
}

func cmdCreate(args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	user := fs.String("user", "", "username to create or replace")
	mechanism := fs.String("mechanism", auth.MechanismSCRAMSHA256,
		"SCRAM mechanism: SCRAM-SHA-256 or SCRAM-SHA-512")
	iterations := fs.Int("iterations", 0,
		"PBKDF2 iterations; 0 selects the package default")
	stdin := fs.Bool("password-stdin", false, "read the password from stdin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *user == "" {
		return errors.New("-user is required")
	}
	if !*stdin {
		return errors.New("-password-stdin is required; passwords are never taken as arguments")
	}
	password, err := readPassword(fs)
	if err != nil {
		return err
	}
	if password == "" {
		return errors.New("password is empty")
	}

	ctx := context.Background()
	store, err := openStore(ctx)
	if err != nil {
		return err
	}

	verifier, err := auth.NewVerifier(*mechanism, *user, password, *iterations)
	if err != nil {
		return err
	}

	// Merge with whatever is already stored, so adding a mechanism does not
	// silently drop the other one.
	cred, err := store.Get(ctx, *user)
	if err != nil && !errors.Is(err, auth.ErrNoSuchUser) {
		return err
	}
	if cred == nil {
		cred = &auth.Credential{Username: *user, Verifiers: map[string]*auth.Verifier{}}
	}
	creds := cred.Verifiers
	if creds == nil {
		creds = map[string]*auth.Verifier{}
	}
	creds[*mechanism] = verifier
	cred.Verifiers = creds
	cred.UpdatedAt = nowRFC3339()

	if err := store.Put(ctx, cred); err != nil {
		return err
	}
	names := make([]string, 0, len(cred.Verifiers))
	for m := range cred.Verifiers {
		names = append(names, m)
	}
	fmt.Printf("stored %s credential for %q in %s (mechanisms: %s)\n",
		*mechanism, *user, auth.Prefix, strings.Join(names, ", "))
	return nil
}

func cmdDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	user := fs.String("user", "", "username to remove")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *user == "" {
		return errors.New("-user is required")
	}

	ctx := context.Background()
	store, err := openStore(ctx)
	if err != nil {
		return err
	}

	if !store.Exists(ctx, *user) {
		return fmt.Errorf("no credential for %q", *user)
	}
	if err := store.Delete(ctx, *user); err != nil {
		return err
	}
	fmt.Printf("deleted credential for %q from %s\n", *user, auth.Prefix)
	return nil
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	store, err := openStore(ctx)
	if err != nil {
		return err
	}

	names, err := store.List(ctx)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Println("no credentials")
		return nil
	}
	for _, n := range names {
		cred, err := store.Get(ctx, n)
		if err != nil {
			fmt.Printf("  %-24s <unreadable: %v>\n", n, err)
			continue
		}
		mechs := make([]string, 0, len(cred.Verifiers))
		for m := range cred.Verifiers {
			mechs = append(mechs, m)
		}
		sortStrings(mechs)
		fmt.Printf("  %-24s %s\n", n, strings.Join(mechs, ","))
	}
	return nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// nowRFC3339 stamps a credential object so an operator can tell when it was
// last written without diffing the whole record.
func nowRFC3339() string {
	return timeNow().Format("2006-01-02T15:04:05Z07:00")
}

// timeNow is indirected so the timestamp is testable.
var timeNow = func() time.Time { return time.Now() }
