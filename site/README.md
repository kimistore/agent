# Kimistore website

Static, dependency-free marketing site. No build step, no bundler, no framework.

## Files

| File | Purpose |
| :--- | :--- |
| `index.html` | Single-page site |
| `styles.css` | All styling (design tokens as CSS custom properties at the top) |
| `script.js` | Progressive enhancement: sticky nav, mobile menu, scroll reveal, code tabs |
| `favicon.svg` | Browser icon |
| `og.svg` | Social preview card — **convert to PNG/JPG before publishing**, most platforms won't render SVG |
| `robots.txt` | Crawler policy |
| `sitemap.xml` | Single-URL sitemap |

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
# GitHub Pages (push site/ to the gh-pages branch, or /docs)
# Cloudflare Pages / Netlify / Vercel
npx serve site

# object storage
aws s3 sync site/ s3://your-bucket/ --delete
aws cloudfront create-invalidation --distribution-id XXXXX --paths "/*"
```

### Before you publish

1. **Update the canonical URL.** `sitemap.xml` and `robots.txt` currently assume `https://kimistore.dev`. Change both if the real domain differs, and add a canonical `<link>` to `index.html`.
2. **Replace the OG image.** Convert `og.svg` to a 1200×630 PNG and reference that in the `og:image` meta tag.
3. **Verify the GitHub links.** They point at `https://github.com/kimistore/agent`. Change the `origin` remote or the links if the repo moves.
4. **Check the numbers.** "10.5k lines of Go", "68 tests", "27 MB binary" were read from the repo on 2026-09-29, and the binary was last rebuilt with Go 1.27. Refresh them as the code changes or they will quietly become wrong.

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
