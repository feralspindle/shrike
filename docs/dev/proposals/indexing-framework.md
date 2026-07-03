# duffle — an "index anything" framework (proposal)

Status: **proposal, pre-implementation**. Working name `duffle` (free on crates.io as of
2026-07-03; `riffle` is taken, `snaffle` is the runner-up). A duffle is one bag that
carries everything and travels — one SQLite file that holds an application's entire
derived search world and can be copied, synced, or replicated anywhere.

This is the third spin-off from Shrike, and the one that composes the first two:

| Project | What it extracted |
|---------|-------------------|
| [trifle](https://github.com/lathrys-at/trifle) | Typo-/partial-tolerant trigram fuzzy search over SQLite (the lexical channel). |
| [ruffle](https://github.com/lathrys-at/ruffle) | Weighted, adaptive, calibration-free RRF (the fusion step). |
| **duffle** | Everything between a caller's corpus and those two: extraction, embedding, the vector index, the derived stores, consistency, and search composition. |

Shrike keeps what is genuinely Shrike: the Anki coupling (the collection actor and
protobuf services), the MCP server and CLI, the concrete engines, and Anki-specific
policy (embed-text normalization, tag centroids). Duffle takes the machine those sit on.

## Problem statement

`shrike-kernel` is two things fused together: an Anki application core, and a general
machine for maintaining derived, rebuildable search state over a caller-owned source of
truth. The second thing — BYOM embedding and recognition slots, drift detection and
reconcile, the single-writer ingest actor, per-modality vector indexes, the derived-text
sidecar, rank fusion, staleness accounting — is not Anki-shaped at all, and building any
application *like* Shrike means rebuilding all of it.

Duffle is that machine as a library: a framework for indexing "documents" from any
caller-owned source, maintaining every derived artifact in **one SQLite database**, and
serving semantic + lexical search with fused results. It is the framework up to the point
where an application connects the API to routes.

**Non-goals.** Duffle is not a server (no transport, no MCP, no HTTP); not a datastore of
record (the caller owns the source of truth; every duffle artifact is derived and
rebuildable); not a relevance engine (ruffle's scope note applies — a domain reranker
composes on top); not a model runtime (BYOM: duffle ships contracts, never weights or
inference code). It never stores source documents or media bytes — only derived
artifacts and provenance.

## The model

```
Bag (one SQLite file)
 └── Corpus (namespaced; one per Source)
      ├── Source        ← caller adapter: revision(), scan(), fetch()   [pull]
      ├── Writer ops    ← upsert/delete/metadata_changed               [push]
      ├── MediaResolver ← caller adapter: read(ref), exists(ref)
      ├── Spaces        ← embedding spaces (BYOM embedders attach here)
      ├── Purposes      ← extraction lanes (BYOM extractors attach here)
      └── Search        ← channels → ruffle fusion → hits with provenance
```

- A **Document** is a `Key` (Integer/Text/Blob, trifle's key shapes) plus named text
  **segments** (`label → text`) plus zero or more **media refs** (`kind` — image, audio,
  … — plus an opaque `ref` string the caller's `MediaResolver` can resolve to bytes).
  For Shrike: a note; segments are its fields; media refs are its image/sound names.
- A **Bag** is one SQLite database. It hosts one or more **corpora**, each fully
  namespaced (`duffle_<corpus>_*` tables), each bound to one Source. The bag is the
  *only* persisted artifact — everything else (the ANN graphs, the id maps) is memory,
  reconstructed from the bag.
- A **Space** is an embedding vector space: `(key, fingerprint, dim, modalities)`.
  Fingerprint is the opaque compatibility key (Shrike's `model_id` discipline — a change
  means every vector lives in a different space and forces a rebuild of that space only).
  Spaces are first-class and plural from day one, promoting Shrike's
  `attach_embedder_space` seam.
- A **Purpose** is an extraction lane, and it is *data*, not an enum — Shrike's
  `RecognitionPurpose::{Ocr, Describe, Asr}` generalized to a caller-declared record:

  ```rust
  Purpose {
      source: &str,                 // provenance tag: "ocr", "asr", "vlm", …
      media_kind: MediaKind,        // what it consumes
      destination: Destination,     // LexicalAndVector | VectorOnly
      long_running: bool,           // semaphore-bounded (VLM/ASR) vs not (OCR)
      gate: ExtractionGate,         // min_confidence / min_chars_lexical / min_chars_vector
  }
  ```

  `Destination::VectorOnly` preserves Shrike's rule that VLM-describe prose is stored for
  provenance and embedding but hidden from lexical search (a literal hit on text the user
  never sees can't be explained).

### Push and pull

Both ingestion modes exist, as in Shrike:

- **Push** (the hot path): `upsert(docs, revision)`, `delete(keys, revision)`,
  `metadata_changed(revision)`. The caller calls these *inside its own write
  serialization, in commit order* — that contract is what makes duffle's queue order
  match the source's revision order (Shrike's enqueue-inside-the-collection-write-job
  invariant, restated as an API contract since duffle doesn't own the source's writer).
  The call returns as soon as the intent is queued; embedding and index maintenance
  drain asynchronously. `metadata_changed` advances watermarks without re-deriving
  (Shrike's metadata-only `col.mod` bump).
- **Pull** (the drift path): the `Source` trait —

  ```rust
  trait Source: Send + Sync {
      fn revision(&self) -> BoxFuture<'_, Result<i64>>;          // the drift watermark
      fn scan(&self) -> BoxStream<'_, Result<Vec<Key>>>;          // all live keys, chunked
      fn fetch(&self, keys: &[Key]) -> BoxFuture<'_, Result<Vec<Document>>>;
  }
  ```

  Reconcile and rebuild stream `scan → fetch` in bounded chunks (Shrike's
  `STREAM_CHUNK = 512` shape), so peak memory is O(chunk) regardless of corpus size.
  A corpus *may* run push-only (no Source bound), but then drift can only be healed by a
  caller-driven full reset — reconcile needs the pull side. Documented loudly.

## The bag: one universal SQLite database

The headline departure from Shrike's persistence: **no index files**. Shrike persists
USearch HNSW graphs to disk (`index.usearch` + `index.meta.json` + `index.hashes.json`)
next to a separate FTS5 sidecar (`shrike.db`) — four artifacts with independent failure
modes, fsync'd by a debounced saver, none of them mutually transactional. Duffle persists
**one SQLite file**; the ANN structure is an *accelerator* behind a provider seam —
in-memory USearch by default — over vectors whose durable home is a table.

Schema sketch (per corpus, all names namespaced; DDL interpolation follows trifle's
validated-`Namespace` rule so there is no injection surface):

```sql
-- identity & stamps
meta(key TEXT PRIMARY KEY, value)          -- schema_version, corpus config fingerprint,
                                           -- revision watermarks, per-purpose extractor
                                           -- fingerprints, poison floors
doc(doc INTEGER PRIMARY KEY,               -- monotonic, never reused (USearch needs u64
    key BLOB UNIQUE NOT NULL)              --   keys; trifle-style id allocation)
state(doc INTEGER PRIMARY KEY,             -- per-doc embed fingerprint (blake2b-8);
      fingerprint BLOB NOT NULL)           --   the reconcile baseline (was index.hashes.json)

-- embedding spaces & vectors
space(space INTEGER PRIMARY KEY, key TEXT UNIQUE, fingerprint TEXT, dim INTEGER,
      generation INTEGER, activation TEXT) -- registry + drift stamp + calibration record
vec(item INTEGER PRIMARY KEY,              -- monotonic; the USearch key
    space INTEGER, doc INTEGER, source TEXT, ref TEXT,   -- provenance
    v BLOB NOT NULL,                       -- f32 LE, post-normalization (canonical)
    UNIQUE(space, doc, source, ref))
snapshot(space INTEGER, seq INTEGER,       -- OPTIONAL warm-boot accelerator: serialized
         generation INTEGER, chunk BLOB,   --   provider image (USearch's in v1), chunked
         PRIMARY KEY(space, seq))          --   under the 1 GB blob cap; valid iff
                                           --   generation matches

-- derived text (the expensive tier: model outputs only)
text(doc INTEGER, source TEXT, ref TEXT, txt TEXT NOT NULL,
     PRIMARY KEY(doc, source, ref))
segments(doc INTEGER, source TEXT, ref TEXT, json TEXT NOT NULL,   -- locators (bbox/span)
         PRIMARY KEY(doc, source, ref))
gated(doc INTEGER, source TEXT, ref TEXT,  -- judged-once below-gate markers
      PRIMARY KEY(doc, source, ref))

-- fusion
fusion(key TEXT PRIMARY KEY, state TEXT)   -- serialized ruffle RuffleState (serde JSON)

-- lexical: trifle's own tables, co-located via Namespace::prefixed("duffle_<corpus>_lex")
```

`(doc, source, ref)` is Shrike's derived-store key seam, kept verbatim: `source` is
*where* text came from (`seg` for source segments, else a Purpose's tag), `ref` is the
segment label or media ref.

### Three tiers of rebuildability

The bag's tables sort by what a reset costs, and drift stamps are scoped per tier so a
cheap tier never triggers an expensive reset:

| Tier | Contents | Reset cost | Reset trigger |
|------|----------|-----------|---------------|
| Accelerator | `snapshot` | Free (re-add from `vec` rows) | Generation mismatch — any doubt drops it |
| Lexical | trifle's namespace | Cheap (re-tokenize from source + `text`) | trifle's own stamps (tokenizer/`data_version`/schema) |
| Model outputs | `vec`, `text`, `segments`, `gated`, `state` | Expensive (re-run models) | Space fingerprint (per space) / extractor fingerprint (per purpose) |

This tiering is why extraction outputs live in duffle's `text` table rather than only
inside trifle: trifle is a cache that drops itself on drift, and OCR/ASR text must
survive a lexical reset. Trifle indexes *over* `text` + the source's segments; duffle's
`text` is the durable home for what a model produced.

### Transactionality — the headline win over Shrike's layout

Shrike cannot commit a vector, its derived rows, and the watermark atomically (USearch
file + SQLite + JSON metas). A hard kill discards edits since the last debounced flush,
and the next boot pays a **re-embed**. In duffle, each drained ingest batch commits
`vec` + `state` + `text` + watermark advance in **one transaction**; the in-memory ANN
is patched after commit by the same single writer. A hard kill costs at most an HNSW
re-add from rows at next boot — **pure compute, never a re-embed**. The debounced saver
disappears (SQLite commits are durable; the only file-flush artifact left, the snapshot,
is optional and written at quiesce).

Trifle writes ride the same drained batch through trifle's writer lease, ordered
**before** the duffle transaction, with the watermark advance last: a crash between the
two commits leaves the watermark behind, and the next reconcile replays the batch —
trifle's `upsert` and duffle's `vec` rewrite are both idempotent, so replay converges.
(A future trifle extension — accepting an external connection so both stores share one
transaction — would collapse this to a single commit; the watermark-last protocol makes
it unnecessary for correctness. Filed as an open question below.)

Because the corpus's single ingest actor is the *only* writer of both stores, trifle's
`Error::Busy` never fires in steady state; the read pool runs concurrently under WAL.

### Co-location and replication

Two placements, both load-bearing:

- **Sidecar bag** (Shrike's case): the source of truth is a foreign schema
  (`collection.anki2` — Anki owns it, sync would ship rebuildable data). The bag is its
  own file in a cache dir.
- **Co-located bag** (the new-application case): the caller's own SQLite database *is*
  the bag — duffle's namespaced tables live beside the application's. "Index your whole
  application's world in one db." `SqlFilter` predicates then hit application tables
  with no `ATTACH`.

Replication is the file: WAL-checkpoint (`Bag::checkpoint()`), then copy /
`sqlite3_rsync` / Litestream / whatever the caller uses. A bag opened anywhere is safe by
construction — stamps gate every artifact, so a replica with a different model rebuilds
exactly the mismatched spaces and loads the rest. Ruffle's state model was built for this:
`RuffleState::merge` reconciles fusion baselines across replicas, and its per-channel
semantic tags refuse to blend a swapped model under a kept name.

## The vector accelerator: a provider seam

The `vec` rows are the canonical vector store; whatever answers nearest-neighbour
queries over them is an **accelerator** — tier-1, rebuildable, replaceable. Duffle
defines that as a seam rather than a hard dependency, because the ground is moving under
it: SQLite's experimental `vec1` branch appears to be growing an in-database HNSW index,
and Shrike's own decision record already names "SQLite-co-located vectors" as the
trigger for revisiting USearch. The seam is what makes that revisit a provider swap
instead of a redesign.

```rust
trait VectorAccelerator: Send + Sync {
    fn ensure(&self, space: SpaceId, dim: usize) -> Result<()>;
    fn add(&self, space: SpaceId, items: &[(u64, &[f32])]) -> Result<()>;
    fn remove(&self, space: SpaceId, items: &[u64]) -> Result<()>;
    fn search(&self, space: SpaceId, queries: &[Vec<f32>], k: usize,
              scope: Option<&ItemPredicate>) -> Result<Vec<Ranking>>;
    fn persist(&self, mode: PersistMode) -> Result<()>;  // snapshot or no-op — provider's business
    fn capabilities(&self) -> AcceleratorCaps;           // filtered_search, transactional_update, needs_warm_boot, …
    fn identity(&self) -> String;                        // the accelerator stamp
}
```

What stays **above** the seam, in duffle: max-sim-per-doc dedupe, over-fetch policy
(driven by `capabilities`), activation calibration, scope-set resolution, and the
`item → (doc, source, ref)` provenance map. What stays below: index structure, memory vs
in-database residency, snapshot format. Two rules keep the seam honest:

- **Provider identity is an accelerator stamp, never part of the space fingerprint.**
  Swapping providers — or upgrading one incompatibly — drops the accelerator and
  re-adds from rows: pure compute. It must never trigger a re-embed. The stamp lives
  beside the snapshot generation, in the accelerator tier.
- **IP over boundary-normalized unit vectors is the contract's only metric.** A provider
  that can't do exact inner product doesn't qualify; nothing else about scoring may leak
  below the seam.

| Provider | Residency | Status |
|----------|-----------|--------|
| `usearch` (default cargo feature) | in-memory HNSW; snapshot table for warm boot | the v1 default |
| brute-force scan | none — reads `vec` rows directly | built-in: tiny corpora, and the `building` window |
| SQLite `vec1` | in-database (index pages live in the bag) | watch — experimental branch; qualify when it lands |
| `sqlite-vec` | in-database, scan-based | qualifies at small scale only; no ANN today |

An in-database provider changes the trade-offs, not the architecture: index updates join
the batch transaction (the crash story gets even simpler), warm boot and the `snapshot`
table retire — while the bag grows by the index pages and replication ships them
(replicas skip the re-add in exchange for bytes). Both postures are legitimate; per-bag
config with a per-space override picks one, and the drop-and-re-add rule makes changing
your mind cheap.

### The default provider: in-memory USearch

Per space, one in-memory USearch index (`MetricKind::IP`, unit vectors enforced at the
boundary — Shrike's `NormalizingEmbedder` wrap, with the audited `assume_normalized`
opt-out — so IP ≡ cosine and no metric/normalization metadata needs storing).

- **Keys are `vec.item`** (per-item, monotonic), not per-doc `multi=true` keys as in
  Shrike. The single writer maintains the in-memory `item → (doc, source, ref)` map
  (a few MB at 100k items); search results resolve to docs through it, dedupe
  **max-sim per doc per modality** (ported), and carry *item-level* semantic provenance
  Shrike can't currently express ("matched via `ocr:diagram.png`", not just "matched").
  Removal walks the writer's `doc → items` map.
- **Boot**: load the snapshot if its generation matches the space's current generation,
  else stream `vec` rows and re-add on the compute pool. During a warm build the space
  reports `building` with progress; a config flag (`serve_exact_while_building`) lets the
  semantic channel serve brute-force scans over the rows meanwhile — degraded latency
  instead of degraded availability. Default off (Shrike-parity: actionable
  "building m/n" status).
- **Snapshot**: written at graceful shutdown, rebuild end, and quiesce; `generation`
  bumps on every committed `vec` write, so any doubt invalidates. Chunked rows stay under
  SQLite's blob cap.
- **Scoped search** rides USearch's filtered walk over a doc-id predicate (ported), so
  filters don't over-fetch-then-drop.
- **Aux vector namespaces**: `put_aux(space, key, vec)` / `dot_scores(space, keys, q)` —
  the storage/scoring primitive under Shrike's tag centroids, without the tag policy.
  Shrike rebuilds centroid maintenance on top and feeds the result in as a caller
  channel.

Everything else about vector policy ports intact: the media-aware per-doc fingerprint
(fold resolvable media refs by cheap `exists`, fold extracted text, byte-identical when
neither is present so upgrades never spuriously rebuild); replace-semantics adds with a
doc's text + gated-extraction vectors landing atomically; chunked embeds at the proven
batch size.

## BYOM: `duffle-engine-api`

`shrike-engine-api` lifts nearly wholesale — it is already corpus-agnostic. The crate
stays a leaf (the kernel-may-not-depend-on-engines layering rule comes with it).

- **Async traits the kernel consumes**: `Embedder` (text), `MediaEmbedder`
  (generalizing `ImageEmbedder` — `embed_media(Vec<MediaItem>)`, modality declared at
  attach), `Extractor` (generalizing `Recognizer` — `extract(Vec<MediaItem>) ->
  Vec<Extraction>`, where `Extraction` keeps Shrike's `Recognition` shape: text,
  confidence, segments with `Locator::{Bbox, Span}`). Runtime-agnostic `BoxFuture`s.
  The **bounded-time liveness invariant** on `embed` carries over verbatim (the
  single-flight drain has no per-call timeout; a never-resolving future wedges the sole
  writer).
- **Sync compute traits + the `Blocking` adapter + `BlockingDispatch`** — the two
  conformance routes (sync compute bridged onto the host pool; naturally-async direct),
  eager dispatch preserved.
- **Host-policy wrappers** (`WithPolicy` / `AsyncWithPolicy`): fingerprint, dim, and
  `safe_batch` are host-assembled, never trusted from the engine.
- **The batch-safety probe** (`probe_max_safe_batch`, magnitude-spiked set, serial vs
  batched within tolerance) ships in Rust so every host gets it, not just the Python
  harness.
- **`MediaResolver`** (generalizing `ImageResolver`): `read(ref) -> Option<Vec<u8>>`,
  `exists(ref) -> bool`. Caller-owned; **path safety is the resolver's problem** —
  duffle never touches the filesystem for media, so the CVE-class path-handling burden
  stays with the code that knows the media layout.
- **Text policy is caller-supplied and versioned**: `TextPolicy { render, normalize,
  version }` — how a document's segments become its embed text (default: `"label: text"`
  newline-join) and how raw segment text normalizes. The version folds into the space
  fingerprint exactly like `EMBED_TEXT_VERSION`; Shrike supplies its Anki-specific
  cloze/LaTeX/HTML pipeline here and keeps ownership of it.
- **Extraction sweeps**: `extract_pending(purpose, max_items)` ports
  `recognize_pending` — *pending* = resolvable media of the purpose's kind with no
  `text` row and no `gated` marker; a fingerprint change invalidates that purpose's rows
  and markers wholesale; gate-passing text lands as derived rows and (above the vector
  bar) mints text-space vectors under the doc — same encoder, no modality gap.
  Long-running purposes acquire the bounded semaphore.

Graceful absence is first-class everywhere, as in Shrike: no embedder → space
`unavailable`, lexical-only search; no extractor → sweep reports `Unavailable`; a failed
per-item unit is caught (`catch_unwind`), counted, and **poisons the watermark floors**
so drift can't be certified past a lossy batch until a reconcile heals it.

## Consistency machinery

Ported from Shrike with names generalized (`col.mod` → source `revision`):

- **One-directional consistency.** The source never lags duffle; duffle may lag the
  source. Every artifact is a rebuildable projection.
- **Drift = one comparison per artifact.** On open and on attach: space fingerprint
  differs → rebuild that space; extractor fingerprint differs → re-derive that purpose;
  revision differs → **reconcile**, not rebuild — stream `scan`/`fetch`, diff per-doc
  fingerprints against `state`, re-embed only the changed docs. Explicit rebuilds stay
  full. Trifle self-gates with its own stamps (drop, never migrate).
- **Status per artifact** — `ready | building{done,total} | unavailable | error`, a
  discriminated union in every binding (Shrike's schema house style).
- **`settle()` and the staleness advisory.** The ingest handle's outstanding-work gauge
  (`AtomicU64`, bumped before send, decremented on drain) backs both: `settle()` awaits
  quiescence for search-after-write; every search brackets its reads with
  `FreshnessStamp { revision, settled }` at both edges and returns
  `stale = start != end || !settled` — computed inside the read, conservative OR, serve
  immediately and let the caller decide.

## Search

### Channels

Each channel produces an independent candidate ranking; ruffle fuses. Base channels:

| Channel | Produces | Notes |
|---------|----------|-------|
| `sem:<space>:<modality>` | cosine-scored docs (max-sim per doc) | one channel per (space, modality) — the per-modality split that defeats the CLIP gap by comparing rank positions, generalized to N spaces |
| `lex:fuzzy` | trifle matches with `(source, ref, span, text)` provenance | the sole lexical read; `VectorOnly` sources excluded before ranking |
| `lex:exact` | literal-confirmed hits, ranked by length-normalized literal-TF | **recovered from the fuzzy hits**, never a second read: a `memchr::memmem` finder (built once per query) verifies each hit's matched-segment text — Shrike's `recover_exact`, which trifle's span-carrying `Match`/`Candidate` was shaped for |
| caller channels | scored or rank-only lists (recency, structured boosts, tag expansion) | registered per query; ruffle handles them natively (`ChannelInput::ranked`) |

### Fusion

`ruffle::Fuser`, one per corpus, state persisted in `fusion` and decayed/merged on the
caller's schedule. Channel identity is where drift meets fusion: `ChannelId`'s semantic
tag is the space fingerprint (semantic channels) or trifle's tokenizer
fingerprint + `data_version` (lexical), so a model swap makes `Fuser::resume` refuse the
old baseline and duffle resets that channel's statistics — the same event that rebuilds
the vectors, handled by the same stamp.

Two Shrike behaviours sit *above* ruffle, in duffle's thin fusion shim:

- **The exact priority tier**: literal hits float above the fused ranking, TF-ordered
  within (rank fusion deliberately discards the magnitude that makes an exact hit
  special).
- **Non-text modality admission**: the offline activation calibration (sample stored
  text vectors as pseudo-queries; record best non-self match mean/std per modality)
  ports over, stored per space in `space.activation`. Its floor becomes each semantic
  channel's `GoodScore` prior, letting ruffle's per-query discrimination do the
  down-weighting adaptively; a hard `FloorAdmit` gate remains as per-channel config for
  Shrike-parity. This replaces a hand-rolled gate with the mechanism ruffle was built to
  provide, without giving up the conservative floor.

Static-weight plain-RRF mode (ruffle's neutral configuration, coupling off) is the
compatibility posture for Shrike's frozen parity suite; adaptive weighting is opt-in.

### Read surface

Mirrors trifle's two front doors, corpus-wide:

- **Eager**: `search(query, opts) -> SearchResult { hits, stale }`; each hit carries the
  doc key, fused score, per-channel contributions `(channel, rank, score?)`, lexical
  provenance (`source`, `ref`, byte span, segment text), and semantic provenance
  (space, item `source`/`ref`). Collapse per doc by default, per segment on request.
- **Lazy**: per-channel candidate streams (trifle's `CandidateStream`; semantic ranked
  chunks) for callers composing their own rerank/pagination/fusion, plus `hydrate` for
  exactly what they keep.
- **Query sources**: text queries (semantic + lexical) and doc anchors by key
  (more-like-this: semantic only, stored vectors as queries, self excluded) — Shrike's
  `SearchSource` shape. Query-by-media (image → image space) is a seam: the contract
  admits it wherever a space embeds that modality.
- **Filtering**: `Filter::Keys(set)` is the native mode both channels honor (trifle
  `rarray`, USearch predicate). `Filter::Sql { fragment, params }` is sugar over the
  caller's live tables (directly in a co-located bag, via `ATTACH` for a sidecar): it
  materializes a key set per query, bounded, then proceeds as `Keys`. No filter columns
  are ever stored — they'd go stale (trifle's rule).

Sub-floor queries degrade gracefully: trifle's dual-order tokenizer covers 2-char Latin
and 1-char CJK natively (already better than the FTS5 trigram floor); below that the
lexical channels return empty and semantic still serves. Shrike's Anki-wildcard fallback
becomes a caller channel in shrike, not duffle machinery.

## Runtime and concurrency

Shrike's driven model, minus the collection thread (duffle doesn't own a source):

- A `current_thread` tokio runtime the library owns but **never spawns threads for**;
  the host donates **N + 1** threads: `drive_io` ×1 (drivers + executor, spawned first,
  probe-barriered), `drive_compute` ×N (work-stealing; embeds, extraction, HNSW adds,
  blocking-fs and blocking-SQLite leaves; N ≥ 2 preserves the search/batch overlap).
- **One ingest actor per corpus** — the single writer of trifle, the bag's tables, the
  in-memory indexes, and the watermarks. FIFO channel; push ops enqueue in caller commit
  order (the API contract above); bulk jobs (reconcile/rebuild/extraction stores) ride
  the same channel so `settle()` means settled. Drain batches embed on the compute pool,
  commit, then patch memory. The leaf invariant (a pool job never enqueues-and-awaits
  pool work) and the eager-dispatch rule carry over, tripwired in debug builds.
- **Reads**: pooled read-only connections (trifle's pool + duffle's own) run concurrently
  with the writer under WAL; searches share `RwLock` read guards on the in-memory state;
  snapshot writes clone `Arc` handles under the lock and write outside it (never hold a
  lock across a file write or compute).
- **The op edge**: `spawn_op` → oneshot-backed future, pollable from any host; dropping
  it detaches observation, never aborts (a half-applied write would be corruption).
  `submit_blocking` for threaded hosts.
- **Embedded mode**: `Duffle::open_with_runtime(handle, dispatch)` accepts an external
  tokio handle + `BlockingDispatch` instead of the driven pools — so Shrike hosts duffle
  *inside* its existing N + 2 provisioning rather than paying a second thread fleet. The
  driven mode is the standalone default; the seam is the same two functions either way.

Performance rules are inherited as design constraints, not aspirations: no per-item
source reads inside loops (discover ids, batch-fetch, assemble from the map); one
transaction per drained batch; `prepare_cached` in row loops; ceilings with
deterministic sampling on anything that scales with the corpus; hand out `Arc`s and
views, not clones.

## Crates, bindings, packaging

```
duffle/                    # workspace
├── duffle/                # the library: bag, corpus, ingest, search, runtime
├── duffle-engine-api/     # leaf: traits, adapters, probe, MediaItem, Extraction
└── bindings/
    ├── duffle-pyo3/       # Python: AsyncDuffle (asyncio bridge), capture escape hatch
    └── duffle-cabi/       # later: C ABI for Swift/Kotlin hosts (same driven entries)
```

- Dependencies: `trifle`, `ruffle`, `usearch`, `rusqlite` (re-exported for `SqlFilter`
  binding, trifle's rule), `croaring` transitively. MSRV/edition aligned with trifle
  (1.85 / 2024). Features: `usearch` **on by default** but a feature nonetheless — the
  accelerator seam keeps it swappable, and a future in-database provider (SQLite `vec1`)
  arrives as a sibling feature, not a fork; `tracing` off by default. `cargo-deny` + the
  `-D warnings` lint bar, per the sibling projects.
- License **MIT OR Apache-2.0**, matching trifle/ruffle. Shrike is AGPL-3.0: the
  extracted machinery must be relicensed by its copyright holder (single-author today,
  so a decision, not a negotiation — but it must be a conscious one, and third-party
  contributions to the extracted files, if any exist by then, need checking).
- **Python binding**: `AsyncDuffle` mirrors `AsyncKernel` — every op through one audited
  `spawn_op` + hand-rolled asyncio bridge (the one-wake, weakref-cycle-safe,
  finalize-gated bridge ports as-is); `PyEmbedder`/`PyExtractor` capture classes as the
  custom/test escape hatch with the same GIL discipline (compute under `py.detach`,
  Python re-entered only on `spawn_blocking` threads behind the finalization gate).
  Standalone Python users start on the capture path; a native-engine companion crate
  (`duffle-engines`) is deliberately out of scope for v1 — duffle ships no models, and
  Shrike composes its own native engines in its own pyo3 crate.
- **wasm**: out of scope (usearch + rusqlite are native); ruffle's wasm binding is
  unaffected.

## How Shrike adopts duffle

The adoption is a kernel refit, not a rewrite; the derived-cache contract makes data
migration **free** (delete the old artifacts, let drift rebuild the bag from the
collection).

| Shrike today | Becomes |
|--------------|---------|
| `MultiModalIndex` + `IndexOrchestrator` + `IndexSet` + `DebouncedSaver` (`index.usearch`, `index.meta.json`, `index.hashes.json`) | duffle spaces: in-memory USearch, `vec`/`state`/`space` tables, optional snapshot |
| `DerivedEngine` (FTS5 trigram + `trigram_*` roaring tables in `shrike.db`) | trifle in the bag's lexical namespace (the `trigram_*` machinery was trifle's embryo) |
| `fusion.rs` RRF + priority tier + activation gate | ruffle via duffle's fusion shim (parity mode first, adaptive later) |
| `recognize.rs` purposes/gate/sweeps + `segments`/`gated` tables | duffle Purposes + extraction sweeps + `text`/`segments`/`gated` |
| ingest actor + `watermark.rs` floors + settle/stale bracket | duffle's ingest actor (same invariants, revision-generalized) |
| `shrike-engine-api` | `duffle-engine-api` (re-export or thin alias during transition) |
| `embed_text.rs` (`EMBED_TEXT_VERSION`) | stays in Shrike, supplied as duffle's `TextPolicy` |
| tag centroids + `tag.text` space | stays in Shrike, rebuilt on duffle's aux-vector API, fed in as a caller channel |
| collection actor, Anki coupling, MCP server, CLI, engines, llama-server manager | stay in Shrike, untouched |

Shrike's `Collection` trait already *is* duffle's `Source` shape (`col_mod` →
`revision`, `find_notes`+`note_embed_inputs` → `scan`/`fetch`,
`derived_field_rows` → segments); the adapter is thin. The frozen search-parity suite is
the adoption gate: static-weight ruffle + priority tier + `FloorAdmit` must reproduce
today's rankings before any adaptive behaviour turns on.

## Invariants (the ones a reviewer should defend)

Carried from the parents, restated as duffle's own floor:

1. The source never lags duffle; every duffle artifact is a rebuildable projection
   (drift → reset per tier, never migrate).
2. One writer per corpus — a single FIFO ingest actor mutates trifle, the bag, the
   in-memory indexes, and the watermarks; push order is caller commit order by contract.
3. One transaction per drained batch; watermark advances last; replay is idempotent.
4. A hard kill never costs a re-embed — vectors are durable the moment their batch
   commits; only accelerator state (HNSW graphs, snapshots) is ever rebuilt from rows.
   Accelerators are replaceable providers behind the seam: swapping or upgrading one is
   a drop-and-re-add, never a re-embed.
5. Fingerprint is the single compatibility key, per artifact: space fingerprint gates
   vectors, extractor fingerprint gates derived text, trifle's stamps gate the lexical
   cache, and the same tags gate ruffle's fusion baselines.
6. Reconcile equals rebuild (identical end state), which is what the batch-safety probe
   protects; "proven safe" and "what we batch" are the same number.
7. Staleness is computed inside the read (bracketed stamps, conservative OR), never
   sampled before it; `settle()` means queue drained and no bulk job in flight.
8. Graceful absence everywhere: a missing engine degrades capability
   (`unavailable`, empty channel), never errors a search; a panicking unit poisons the
   watermark floor rather than certifying drift away.
9. Boundary normalization makes IP ≡ cosine; neither metric nor normalization is stored
   metadata.
10. No sleeps, no internal retries against the caller: transient store contention
    surfaces as trifle's retryable `Busy`; the caller owns backoff (moot in steady state
    — see invariant 2).

## Testing posture

Trifle's bar, not Shrike's Python-heavy pyramid: integration-style Rust test binaries
one concern each (`ingest`, `drift`, `search`, `fusion`, `extraction`, `lifecycle`,
`filter`, `stale`), a proptest **thrash oracle** driving randomized
op/crash/reopen sequences against a reference model (the reconcile-equals-rebuild and
replay-idempotence invariants are exactly what an oracle catches), and the Shrike parity
fixtures as a downstream consumer suite. The batch probe and normalization decorators
keep their exact-equality pins.

## Open questions

1. **Name.** `duffle` is the working title (bag metaphor fits the one-file story;
   crates.io free). Decide before the repo exists.
2. **Trifle shared-connection mode.** Same-file co-location works today
   (`Sidecar::open_with_namespace`); a mode accepting an external connection would fold
   the two-commit protocol into one transaction. Nice-to-have, not correctness-bearing —
   file as a trifle issue only if the watermark-last protocol proves annoying in
   practice.
3. **Vector row encoding.** Canonical f32 LE blobs first. A per-space scalar option
   (f16 rows, half the bag size; USearch quantizes internally regardless) is a
   schema-versioned addition later — measure before adding.
4. **`serve_exact_while_building` default.** Off for Shrike parity; revisit once the
   brute-force scan path has perf numbers at 50k/100k docs.
5. **Multi-corpus federation.** One `search()` spanning corpora (ruffle can fuse
   cross-corpus channels) — out of scope for v1; the API leaves room (channels are
   already namespaced).
6. **Extraction inputs beyond media.** Purposes today consume media refs; a purpose
   consuming *segments* (e.g. summarization → embedding-only text) fits the same
   `(source, ref)` seam and `VectorOnly` destination. Seam, not v1 feature.
7. **SQLite `vec1`.** Track the experimental branch. If an in-database HNSW lands, it
   becomes a candidate accelerator provider; the qualification bar is the seam's
   contract — exact IP over unit vectors, real ANN behaviour at ≥100k vectors, filtered
   search (or tolerable over-fetch), and updates that can join the batch transaction.
   Nothing waits on it; the seam is priced in, and adopting it retires the `snapshot`
   table for the spaces that use it.
