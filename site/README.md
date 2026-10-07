# Kimistore website

Static, dependency-free marketing site. No build step, no bundler, no framework.

## Files

| File | Purpose |
| :--- | :--- |
| `index.html` | Single-page site |
| `logo.svg` | Canonical brand mark (gradient tile, `KS` monogram) |
| `styles.css` | All styling (design tokens as CSS custom properties at the top) |
| `script.js` | Progressive enhancement: sticky nav, mobile menu, scroll reveal, code tabs |
| `favicon.svg` | Browser icon, same geometry as `logo.svg` |
| `og.svg` | Social preview card source — the mark plus the tagline |
| `og.png` | 1200×630 social card actually served in `og:image`. Social platforms do not render SVG, so this is what crawlers get |
| `robots.txt` | Crawler policy, points at the sitemap |
| `sitemap.xml` | Single-URL sitemap |
| `CNAME` | Custom domain for Cloudflare Pages |
| `_headers` | Cloudflare Pages response headers: revalidation for the un-fingerprinted assets, longer caching for the SVGs, and `nosniff` / `Referrer-Policy` / `Permissions-Policy` |

## Run locally

```bash
cd site
python3 -m http.server 8080
# → http://localhost:8080
```

Opening `index.html` directly from disk also works.

## Deploy

`site/` is pure static output. Any of these work as-is:

```bash
# Cloudflare Pages / Netlify / Vercel
npx serve site

# object storage
aws s3 sync site/ s3://your-bucket/ --delete
aws cloudfront create-invalidation --distribution-id XXXXX --paths "/*"
```

### Cloudflare Pages

Connect the repository and set:

| Setting | Value |
| :--- | :--- |
| Build command | *(leave empty)* |
| Build output directory | `site` |
| Production branch | `main` |

There is no build step, so the build command must stay empty — anything you
put there will be run and is expected to produce output in the directory above.
`_headers` and `CNAME` are picked up from the output directory automatically.

To regenerate `og.png` after editing `og.svg` (no SVG rasteriser is vendored,
so this uses whatever Chrome is already installed):

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless --disable-gpu --hide-scrollbars \
  --screenshot=og.png --window-size=1200,630 "file://$PWD/og.svg"
```

`og.svg` is already 1200×630, which is the size Open Graph expects. `qlmanage`
is not usable here: it pads to a square.

### Before you publish

1. ~~**Update the canonical URL.**~~ Done: `https://kimistore.eu` is set in
   `sitemap.xml`, `robots.txt`, the canonical `<link>`, and `CNAME`. It appears
   in four places — change all four if the domain changes.
2. ~~**Replace the OG image.**~~ Done: `og.png` is rendered at 1200×630 and
   referenced by absolute URL from both `og:image` and `twitter:image`.
3. **Verify the GitHub links.** They point at `https://github.com/kimistore/agent`,
   which matches the `origin` remote. Update the links if the repo moves.
4. **Refresh `sitemap.xml`'s `lastmod`.** It is a single-URL sitemap, so
   `lastmod` is the only signal a crawler gets that anything changed.
5. **Refresh `og.png` if `og.svg` changes**, per the command above. The PNG is
   a build artefact checked into the repo, so it will silently go stale.

### Deliberately absent

The page does not advertise lines of code or test counts. Those numbers drift
out of date within a few commits and say nothing about whether the broker is
correct; the claims that are on the page are protocol versions, durability
semantics and stated non-goals, all of which stay true or visibly break.

## Content sourcing

All technical claims come from the repo, not from imagination:

| Claim | Source |
| :--- | :--- |
| Supported API keys & versions | `internal/protocol/handler.go` (`handleApiVersions`) |
| SASL PLAIN | `internal/protocol/handler.go`, `internal/server/server.go` |
| 64 MB segment roll | `internal/storage/wal/partition.go` (`MaxSegmentSize`) |
| 8 uploader workers | `internal/storage/engine.go` (`NewStorageEngine`) |
| Env vars | `cmd/agent/main.go`, `internal/storage/s3/store.go` |
| Port 19092 / 9091 | `cmd/agent/main.go` |
| "Go 1.27" | `go.mod` (`go` directive) |
| Non-goals (replication, transactions) | `COMPATIBILITY.md` |
| Architecture diagram | `kimistore-impl-plan.md` |

If you add a feature, update the compatibility table, the status list, and the diagram in that order.

## Tone

The site deliberately leads with transparent architectural boundaries in the comparison table and the capabilities section. Kimistore is intentionally designed as a single-node streaming engine backed by S3 rather than a distributed KRaft cluster. Marketing it as a generic Kafka replacement would misalign expectations for workloads that require distributed replication or 2PC transactions. Keep the technical honesty — articulating transparent architectural scope and production-grade built-in guarantees is a competitive advantage against the "we built a better Kafka" crowd.
