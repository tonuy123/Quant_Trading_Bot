# Phase 3 Execution Handoff — Historical Data and Data Quality

**Document type:** Living execution handoff and continuation record
**Canonical plan:** `docs/KPI_PLAN.md`, Phase 3 and governance sections
**Active checkpoint (read first):** [`PHASE-3-active-checkpoint.md`](PHASE-3-active-checkpoint.md)
**Last refreshed:** 2026-08-27 15:34 ICT
**Current phase:** Phase 3 — Historical Data and Data Quality
**Current KPI task:** DATA-005 — Convert raw archives to research format
**Current implementation slice:** C4B/5 — canonical bounded raw-manifest loader, ready for isolated assignment
**Current implementation owner:** Unassigned until the coordinator creates the C4B/5 worktree and exact prompt; C4A/5 is frozen and approved
**Next phase remains locked:** Phase 4 must not start before the Phase 3 exit gate is reviewed and accepted.

This file exists so a new AI session can continue without reconstructing weeks of conversation. It is an operational record, not permission to change the approved KPI baseline. Refresh live repository state before acting because source, tests, and agent availability can change after the timestamp above.

The active checkpoint is the single mutable source for the current slice, dirty paths, owner, and next action. This master handoff holds durable phase history, contracts, and review decisions; do not duplicate its full content into the checkpoint.

Use progressive reading to protect agent context:

| Agent job | Required sections |
|---|---|
| New phase coordinator or final reviewer | Entire document |
| Active-slice coder | Sections 1–5, the active DATA task section, and Sections 17–20 |
| Focused reviewer | Sections 1–5, the reviewed slice, Sections 16–20, plus actual source/tests |
| Paw status check | Sections 4, 5, 16, and 20 |

Do not paste all 900+ lines into every implementation prompt. Point the active coder to this file and inline only the active slice's exact write-set, acceptance criteria, and stop conditions.

---

## 1. Five-minute takeover protocol

Every agent taking over Phase 3 must read the active checkpoint first, follow its reading route above, and perform these steps before editing code:

- [ ] Read `docs/operations/PHASE-3-active-checkpoint.md` and refresh its live-state claims.
- [ ] Read `docs/KPI_PLAN.md` lines 211–233 and 521–588.
- [ ] Read the required sections for the assigned role; the phase coordinator reads the complete document.
- [ ] Run `git branch --show-current`, `git status --short`, and `git log --oneline -5`.
- [ ] Confirm the active slice in Section 5 against actual files and tests.
- [ ] Inspect every dirty/untracked file before touching it; preserve another agent's work.
- [ ] Run the narrowest relevant baseline tests before making changes when the current file is stable.
- [ ] Work on one approved micro-slice only, with an explicit write-set.
- [ ] Treat coder self-report as `IMPLEMENTED_UNREVIEWED`, never as `APPROVED`.
- [ ] Hand implementation to the independent reviewer for source inspection, adversarial probes, and gates.
- [ ] Update this handoff only after the review verdict is known.

Immediate continuation command sequence on Windows PowerShell 5.1:

```powershell
git branch --show-current
git status --short
git log --oneline -5
py -3.12 -m pytest -q --no-cov <targeted-test-files>
```

Do not use `&&` in PowerShell 5.1. Do not run live Binance calls, private endpoints, account operations, or order placement as part of Phase 3 verification.

---

## 2. Authority, ownership, and status language

### 2.1 Authority order

When instructions appear inconsistent, use this order:

1. Paw's latest explicit instruction.
2. Approved `docs/KPI_PLAN.md` baseline and recorded change requests.
3. Accepted module contracts and tests.
4. This living handoff.
5. A coder's report or an older chat summary.

No agent may silently change task IDs, phase order, acceptance criteria, schema direction, storage strategy, trading mode, or review ownership. Submit a change request using the template in `KPI_PLAN.md` if implementation requires one of those changes.

### 2.2 Operational role assignment

The KPI baseline names DeepSeek as the Phase 3 owner and ChatGPT as reviewer. Paw subsequently instructed Claude to write the remaining Phase 3 code and authorized GPT to continue the identical bounded prompt when Claude exhausts quota. C3B-2C exercised that fallback successfully on 2026-08-23. The KPI ownership row has not been edited. Therefore:

| Role | Current operational responsibility | Must not do |
|---|---|---|
| Paw | Approves scope, plan changes, task closure, and phase transition | Delegate live-money authority implicitly |
| Claude | Primary implementation owner for each approved subsequent Phase 3 coding slice | Self-approve a slice, widen task scope, or edit the reviewer-owned checkpoint/handoff |
| GPT | Same-prompt fallback when Claude exhausts quota and Paw transfers the active slice | Work concurrently with Claude, start an unapproved slice, self-approve work, or treat fallback as permanent ownership |
| ChatGPT/Codex | Architecture, prompt decomposition, read-only review, adversarial probes, full gate, active checkpoint, and this handoff | Treat test count or coder report as proof; write production code while Paw's model-specific implementation directive remains active |
| DeepSeek | Optional read-only design critique, research, fixture/test-matrix suggestions, or second opinion | Modify code unless Paw explicitly reassigns implementation |

If Claude becomes unavailable or out of quota, preserve the checkpoint and current dirty tree; Paw may transfer the identical active prompt to GPT. Only one coder may write a slice at a time, and every fallback result remains `IMPLEMENTED_UNREVIEWED` until ChatGPT/Codex reviews it.

Changing the canonical model ownership in `KPI_PLAN.md` still requires an approved change request; this file records the operational directive without mutating the baseline.

### 2.3 Status vocabulary

Use exactly these meanings:

| Status | Meaning |
|---|---|
| `NOT_STARTED` | No authorized implementation has begun. |
| `DESIGN_REVIEW` | Contract is being designed; production code must not start yet. |
| `IMPLEMENTING` | A coder owns an explicit write-set and is actively editing. |
| `IMPLEMENTED_UNREVIEWED` | Coder reports completion, but independent review has not approved it. |
| `REVIEW_FINDINGS` | Reviewer found defects; repair the same slice before moving on. |
| `APPROVED_LOCAL` | Source and tests passed independent local review; work may still be uncommitted. |
| `COMMITTED` | Approved work is present in a known Git commit. |
| `BLOCKED` | A concrete dependency or approval prevents progress. |

`pytest passed` is evidence, not a status transition. A full suite can stay green while a new module is unimported and untested.

---

## 3. Phase 3 objective, exit gate, and non-negotiable invariants

### 3.1 Objective

Build versioned, reproducible, validated historical datasets suitable for research and Phase 4 backtesting. The pipeline must preserve lineage from public Binance Spot raw archives to deterministic research artifacts and quality evidence.

### 3.2 Canonical exit gate

A reproducible dataset passes quality checks, carries source/symbol/interval/UTC range/version/integrity metadata, reports invalid records instead of silently discarding them, and can be regenerated from documented commands.

### 3.3 Phase-wide invariants

These rules apply to every DATA-001…010 implementation:

1. **Public-only boundary:** no API key, private endpoint, account, balance, order, strategy, risk decision, or live trading.
2. **Raw archive immutability:** conversion and quality analysis read raw data; they never rewrite it.
3. **Determinism:** identical inputs, versions, and configuration produce byte-identical outputs and checksums.
4. **Strict runtime types:** no blind `str()`/`int()` coercion; `bool` is rejected where an integer is required.
5. **No financial float:** prices, quantities, timestamps, thresholds, and serialized financial values never use binary float.
6. **UTC and half-open ranges:** use aware UTC and `[start, end)` semantics.
7. **`1m` is not `1M`:** `1M` is a UTC calendar month, not 30 days. Use `interval_boundary_after()`.
8. **Close boundary:** Binance raw kline close is inclusive; canonical research close is exclusive and must match the interval boundary.
9. **Invalid data is observable:** quarantine/report; never silently repair, drop, or reinterpret malformed input.
10. **Checksum domains stay distinct:** raw file bytes, canonical research bytes, dataset content payload, and metadata JSON are separate digest contracts.
11. **Sanitized errors:** public errors do not expose raw payloads, paths, numeric values, credentials, original OS exceptions, `__cause__`, or `__context__`.
12. **Filesystem fail-closed:** reject traversal, symlink/junction/reparse redirection, aliasing, hard-link identity conflicts, overwrite, and unsafe concurrent changes.
13. **No fake atomicity:** a pair of file renames is not a dataset commit. The final research manifest is the dataset-level commit marker.
14. **No lookahead claims before DATA-010:** transformations and statistics must record whether they use future observations.
15. **Network-free tests:** fake transports, temporary directories, frozen time, and deterministic fixtures only.

### 3.4 Dependency flow

```text
DATA-001 metadata/version
       ↓
DATA-002 raw download ──→ DATA-003 raw integrity
       ↓                         ↓
DATA-004 UTC/time semantics ─→ DATA-005 research conversion/publication
                                      ↓
                                DATA-006 quality detection
                                      ↓
                                DATA-007 quality report
                                      ↓
                       DATA-008 fixtures + DATA-009 documentation
                                      ↓
                                DATA-010 bias audit
                                      ↓
                               Phase 3 exit review
```

DATA-006 must not begin until DATA-005 publication is complete and independently approved. DATA-010 is the final review gate, not an early design substitute.

---

## 4. Repository snapshot and evidence caveats

Snapshot captured at 2026-08-23 02:13 ICT:

| Item | Snapshot |
|---|---|
| Branch | `main` |
| HEAD | `3a491b7` — initial repository commit |
| Dirty state at initial capture | Only `packages/market_data/datasets/_publication_conversion.py` was untracked |
| Graph project | `quant-trading-bot` |
| Graph generation | 2026-08-20 11:21:22Z |
| Graph limitation | Dataset files are metadata-changed or private publication files are not tracked; direct source inspection is authoritative |
| Last independent full pytest before current repair | `1356 passed` |
| Meaning of that full pytest | Existing repository stayed green, but no test referenced `_publication_conversion.py`; it did not validate C3B-2C |
| Static state at rejected C3B-2C draft | Compile/Mypy passed; Ruff had 13 errors and format check failed |

The active GPT repair can change the dirty state after this snapshot. At takeover, production state included a modified `_publication_staging.py` and an untracked `_publication_conversion.py`; both are inside the conditional write-set of the repair prompt. The next reviewer must rerun `git status`, verify whether `tests/unit/test_market_data_publication_conversion.py` exists, and rerun all C3B-2C gates before changing this record.

The repository currently has one visible commit. Do not infer that locally implemented Phase 3 slices are committed merely because they exist in the working tree.

---

## 5. Live task board

| Task/slice | State | Evidence/source | Next transition |
|---|---|---|---|
| DATA-001 | `APPROVED_LOCAL` | Metadata/version implementation and regression tests; KPI `[x]` | Preserve contract |
| DATA-002 | `APPROVED_LOCAL` | Historical downloader, CLI, fake transport, atomic/incomplete manifest tests; KPI `[x]` | Preserve raw contract |
| DATA-003 | `APPROVED_LOCAL` | Streaming SHA-256 verifier and CLI; read-failure false-pass fixed; KPI `[x]` | Reuse for published research verification |
| DATA-004 | `APPROVED_LOCAL` | Strict timestamp unit/UTC primitives and direct-constructor hardening; KPI `[x]` | Reuse everywhere |
| DATA-005 B-1 | `APPROVED_LOCAL` | `research_format.py` and focused tests | Preserve schema |
| DATA-TIME-001 | `APPROVED_LOCAL` | `1m`/`1M` distinction and calendar-month boundary | Preserve shared helper |
| DATA-005 B-2A | `APPROVED_LOCAL` | Pure one-line conversion kernel and preview security hardening | Preserve pure boundary |
| DATA-005 B-2B stream | `APPROVED_LOCAL` | Bounded stream conversion, exact checksums, short-write handling | Preserve caller-owned stream boundary |
| DATA-005 manifest contract | `APPROVED_LOCAL` | Immutable artifacts/manifests, strict JSON and hardening | Preserve pure metadata boundary |
| DATA-005 C1 | `APPROVED_LOCAL` | Lexical publication layout | Preserve no-I/O boundary |
| DATA-005 C2 | `APPROVED_LOCAL` | Read-only physical preflight and hardening | Preserve snapshot-only boundary |
| DATA-005 C3B-1 | `APPROVED_LOCAL` | Pre-conversion plan/path seam | Preserve centralized naming |
| DATA-005 C3B-2A | `APPROVED_LOCAL` | Shared physical-inspection seam | Preserve C2 behavior |
| DATA-005 C3B-2B | `APPROVED_LOCAL` | Exclusive zero-byte staging pair creation | Preserve no-write/no-promotion boundary |
| DATA-005 C3B-2C | `APPROVED_LOCAL` | Durable one-pass staged conversion; ownership, observable raw mutation, and error-constructor hardening independently verified | Preserve bounded one-pass/no-promotion contract |
| DATA-005 C3B-2D-A/2 | `APPROVED_LOCAL` | One-entry Windows no-clobber promotion; observational concurrency and temporal drift classification independently verified | Preserve exact primitive contract |
| DATA-005 C3B-2D-B/2 | `APPROVED_LOCAL` | Thin failure-first/research-second package-private orchestrator; sequential observations, detached partial-progress errors, and real Windows state independently verified | Preserve single-writer/observational contract |
| DATA-005 C4 | `IMPLEMENTING` | Five-slice lifecycle fixed; C4A/5 independently approved with final stateful-timezone regressions | Assign and review C4B/5 only |
| DATA-005 C5 | `NOT_STARTED` | Recovery, idempotent rerun, stale-state classification, final E2E/CLI | Design after C4 |
| DATA-006 | `NOT_STARTED` | Quality detection | Wait for DATA-005 |
| DATA-007 | `NOT_STARTED` | Quality report | Wait for DATA-006 |
| DATA-008 | `NOT_STARTED` | Reproducible dataset fixtures | Wait for DATA-005/006 report schema |
| DATA-009 | `NOT_STARTED` | Source/coverage/limitations/retention docs | Finalize after DATA-008 |
| DATA-010 | `NOT_STARTED` | Lookahead/survivorship audit | Final Phase 3 review |

---

## 6. DATA-001 — Dataset metadata and version format

### Goal

Provide immutable identity, version, coverage, schema, checksum, record count, and quality-status metadata for a dataset.

### Implemented surface

- `packages/market_data/datasets/metadata.py`
- Lazy exports in `packages/market_data/datasets/__init__.py`
- `tests/unit/test_market_data_datasets.py`

### Accepted decisions

- Dataset version uses deterministic `MAJOR.MINOR.PATCH`.
- Dataset identity is UUID5 over source, exchange, market type, canonical symbols/intervals, UTC coverage, and schema version.
- A correction keeps identity and bumps version; checksum/record count/quality do not enter identity.
- Symbols are canonical `BASE/QUOTE`, sorted, and deduplicated.
- Current exchange/market contract is exactly Binance Spot.
- Metadata checksum is canonical metadata JSON; dataset content checksum is a separate contract.

### Failure hotspots to preserve

- `bool` must not pass as `int` for schema version or record count.
- `from_record()` must not coerce strings/floats into valid fields.
- Direct construction must validate exchange/market membership, not rely on type hints.
- Naive timestamps must fail; aware offsets normalize to UTC.
- Do not add a quality status silently. Adding a public status may require a MINOR version decision.

### Verification expectations

- Round-trip record/JSON determinism.
- Dataset ID equivalence for canonicalized symbols and equivalent UTC instants.
- Strict invalid-type parametrization and Python `-O` validation where invariants matter.

### Trade-off, risk, scale

UUID5 gives stable identity but deliberately cannot identify content corruption; checksums do that. Metadata operations are O(number of symbols + intervals), negligible relative to archive I/O.

---

## 7. DATA-002 — Historical downloader

### Goal

Download fully closed Binance Spot public klines for selected symbols/intervals and a half-open UTC range into deterministic raw NDJSON plus `manifest.json`.

### Implemented surface

- `packages/market_data/datasets/downloader.py`
- `apps/data_download_cli/`
- `tests/fixtures/fake_http.py`
- `tests/unit/test_market_data_download.py`
- `tests/unit/test_data_download_cli.py`
- `data-download` entry point in `pyproject.toml`

### Accepted decisions

- Public REST only; base URL validation rejects auth/user-data/listen-key paths.
- Raw Binance 12-field payload is preserved unchanged inside a six-field envelope.
- Only candles with canonical exclusive close `<= requested end` and `<= server time` are retained.
- Atomic per-file write uses staging plus replacement; write failure returns an incomplete manifest where possible.
- Resume uses an interval boundary watermark and deduplication.
- HTTP 429/418 uses bounded cooldown/retry; other failures do not blind-retry.
- `1M` uses the next UTC calendar-month boundary.

### Failure hotspots to preserve

- Never compute `1M` as 30 days.
- Raw close field remains inclusive; do not rewrite it in the raw archive.
- A failed local file write must not list a nonexistent file as complete.
- If manifest write itself fails, propagate the original infrastructure failure after cleaning only the owned temporary file.
- No `.tmp` must remain after handled failure paths.
- A completed dataset rerun must be deterministic and no-op; resume must not duplicate records.

### Trade-off, risk, scale

Sequential pagination is predictable and rate-limit safe but slower than parallel download. It is appropriate for bounded research backfills; future parallelism requires a shared limiter and must not change deterministic ordering.

---

## 8. DATA-003 — Archive checksum verification

### Goal

Verify expected raw file integrity using streaming SHA-256 without mutating or loading entire archives.

### Implemented surface

- `packages/market_data/datasets/checksum.py`
- `apps/data_checksum_cli/`
- `tests/unit/test_market_data_checksum.py`
- `data-checksum` entry point in `pyproject.toml`

### Digest domains

| Digest | Bytes covered |
|---|---|
| Raw file checksum | Exact physical file bytes |
| Research artifact checksum | Exact canonical research NDJSON bytes |
| Dataset content checksum | Caller-defined canonical record payload bytes |
| Metadata checksum | Canonical metadata JSON |

Never compare digests from different domains as if they were interchangeable.

### Failure hotspots to preserve

- `read_failure` must never aggregate to overall `verified`.
- Overall priority remains documented and deterministic.
- Expected digest is lowercase 64-hex; malformed values fail before I/O.
- Reject traversal, nested names, directory entries, and symlink escapes.
- Missing or unreadable expected files fail verification; unrelated unexpected files follow the existing informational policy.
- Do not hash `manifest.json` as an unexpected data file.

### Trade-off, risk, scale

Streaming uses O(chunk size) memory and O(total bytes) time. It detects corruption, not semantic correctness; DATA-006 owns semantic quality.

---

## 9. DATA-004 and DATA-TIME-001 — UTC and interval semantics

### Goal

Normalize explicit timestamp units and aware datetimes into UTC without float arithmetic, and centralize fixed/calendar interval boundaries.

### Implemented surface

- `packages/market_data/datasets/timestamps.py`
- `packages/domain/enums/timeframe.py`
- `tests/unit/test_market_data_timestamps.py`
- Calendar-boundary regressions in downloader/research-format tests

### Accepted decisions

- Timestamp units are exactly `s`, `ms`, `us`, or `ns`; never infer from magnitude.
- Naive datetimes fail.
- Negative epochs/pre-1970 values are outside current dataset policy.
- Nanosecond values with sub-microsecond residue fail instead of truncating silently.
- Epoch-ms conversion uses integer arithmetic.
- `NormalizedTimestampRange` permits an empty range, while dataset metadata coverage remains a separate stricter contract.
- `1M` requires an open aligned to the first UTC day of a month at 00:00:00.

### Failure hotspots to preserve

- Never case-fold `1M` into `1m`.
- Never call `.seconds` for a calendar month.
- Never use `datetime.timestamp() * 1000` on dataset paths.
- Direct dataclass construction must enforce the same invariants as helper factories.

### Trade-off, risk, scale

Strict rejection costs caller convenience but prevents ambiguous timestamps and irreversible data drift. All operations are O(1).

---

## 10. DATA-005 — Raw-to-research conversion and publication

### 10.1 End goal

Convert each DATA-002 raw archive exactly once into canonical research NDJSON and sanitized failure sidecars, publish artifacts without clobbering existing data, then atomically expose a deterministic dataset manifest as the dataset-level commit marker.

DATA-005 does not own duplicate/gap/outlier quality scoring. It may quarantine malformed records and enforce objective candle invariants required to construct `ResearchCandle`; DATA-006 owns dataset-level quality analysis.

### 10.2 Layer map

```text
research_format.py
  strict ResearchCandle + Binance 12-field factory
        ↓
converter.py
  one raw bytes line → candle XOR sanitized failure
        ↓
conversion_stream.py
  bounded caller-owned streams → output bytes + report
        ↓
conversion_manifest.py
  immutable plan/artifact/dataset-manifest contracts
        ↓
publication_layout.py
  lexical paths only
        ↓
publication_preflight.py + _publication_fs.py
  read-only physical snapshot and shared inspection
        ↓
_publication_staging.py
  directory creation + exclusive zero-byte staging pair
        ↓
_publication_conversion.py                 ← current repair
  one-pass raw hash + conversion + flush/fsync/close + staged artifact
        ↓
future C3B-2D
  no-clobber promotion for one artifact pair
        ↓
future C4
  all artifacts + aggregate checksums + final manifest commit
        ↓
future C5
  stale-state recovery, rerun/idempotency, final CLI/E2E
```

### 10.3 Approved completed slices

#### B-1 — Research schema

Owns strict canonical decimals, timestamp consistency, OHLC bounds, nonnegative fields, source/schema identity, deterministic sorted JSON/NDJSON, and Binance inclusive-to-exclusive close validation.

Frequent bugs:

- Scientific notation amplification before `format(..., "f")`.
- Raw value retained in `DecimalInvalidError` or lower-level exception context.
- `assert`-based invariants disappearing under Python `-O`.
- Blind numeric/string coercion.
- Raw close mismatch silently corrected instead of rejected.

#### B-2A — Pure line kernel

Owns exact raw-line SHA-256, bounded/sanitized preview, strict UTF-8/JSON/envelope validation, factory delegation, half-open range filtering, and monotonic order checks. Duplicate timestamps remain accepted for DATA-006.

Frequent bugs:

- Hashing normalized text instead of exact LF/CRLF bytes.
- Secret marker bypass through JSON Unicode escapes or Unicode control/format characters.
- Unknown/missing envelope fields accepted.
- Raw exception text leaked into failure reason.

#### B-2B stream — Bounded orchestration

Owns bounded `readline(max + 1)`, oversized-line drain/hash, exact short-write handling, output checksums/counters/coverage, and caller-owned streams.

Frequent bugs:

- Unbounded `read()`/iteration.
- Updating report counters before a full record is written.
- Writer returning zero, bool, negative, impossible, or non-integer counts.
- Holding original OSError in `__context__`.
- Decimal exponent expanding into unbounded fixed-point output.

#### Manifest/layout/preflight/staging contracts

Own deterministic names, immutable artifact/manifest records, lexical layout, physical path inspection, and exclusive zero-byte staging creation.

Frequent bugs:

- Treating caller-supplied digest as proof of physical bytes.
- Duplicate JSON keys or huge JSON integers.
- Coverage outside requested range.
- Raw filename not matching symbol/interval Cartesian product.
- Raw manifest alias/hardlink with an artifact.
- `Path` string-prefix containment instead of component-aware containment.
- Symlink, junction, reparse, UNC/device namespace, or mapped-drive ambiguity.
- Trusting C2 snapshot as mutation authority; C3 must revalidate JIT.

### 10.4 Approved slice — C3B-2C durable staged conversion repair

State: `APPROVED_LOCAL` after Claude/GPT repair and independent source/runtime/full-gate review. C3B-2D design is unlocked; implementation has not started.

The rejected draft demonstrated these blockers:

1. Valid empty raw happy path failed during post-close staged verification.
2. Staging files were checked against final output parents.
3. Existing staging files were incorrectly required to be absent.
4. Conversion failure leaked raw/research/failure handles.
5. Public error retained `ConversionStreamError` in `__context__`.
6. Broad `except Exception` hid programmer defects.
7. Pre-write JIT validation did not bind descriptor/path identities or recheck final/manifest absence.
8. Raw post-read stability checked size incompletely and omitted full path/identity reinspection.
9. No C3B-2C test file existed at the rejected checkpoint.
10. Ruff and format gates were red.

Repair acceptance sequence:

```text
validate exact inputs
→ reinspect raw root/manifest/artifact and identity
→ JIT verify both zero-byte staging descriptor/path bindings
→ verify final targets + both manifest targets absent
→ open/fstat/bind raw descriptor
→ one bounded conversion pass with exact raw SHA-256
→ verify raw descriptor/path stability at EOF
→ flush + size/identity verify + fsync research
→ flush + size/identity verify + fsync failure
→ verify raw descriptor/path observable fingerprint after durability
→ attempt-close all three owned handles
→ post-close verify both staging files under staging parents
→ verify raw path fingerprint immediately before artifact construction
→ reconfirm final/manifest absence
→ construct and return ResearchFileArtifact
```

Required runtime proofs:

- Empty raw returns a valid artifact and two durable zero-byte staging files.
- Injected `ConversionStreamError` leaves raw/research/failure handles closed.
- Public error has `__cause__ is None` and `__context__ is None`.
- No payload write occurs after a JIT path/identity/target failure.
- No rename, replace, delete, cleanup, or manifest publication occurs.

Reviewer procedure after the active coder reports completion:

1. Inspect `git status` and exact diff/source.
2. Confirm `tests/unit/test_market_data_publication_conversion.py` exists and imports the module.
3. Run direct C3B-2C tests first.
4. Reproduce empty-raw and injected-converter probes independently.
5. Inspect ownership/finally paths; inject read/write/flush/fsync/close failures.
6. Run dependency chain, scoped Ruff/format/Mypy, then full gates.
7. Mark `APPROVED_LOCAL` only when source and runtime state machine agree.

Independent review of the reported repair reproduced three additional gaps:

1. **P1:** cleanup ownership starts after `sha256()`/reader construction, so `MemoryError` in that post-open gap leaks raw and both staging handles.
2. **P1:** raw stability is checked before output durability; same-size in-place mutation during fsync can return an artifact whose raw digest no longer matches the physical raw file.
3. **P2:** invalid direct `StagedConversionError` operation/category values raise a marker-leaking `KeyError` instead of failing closed with a sanitized error.

Independent gates on that source: direct `53 passed`, dependency chain `603 passed`, compile/Ruff/format/Mypy pass. Full pytest was intentionally not rerun after the adversarial runtime gate failed. Repair the same slice and add exact regressions before another review.

The bounded repair keeps the one-pass contract and adds observable raw metadata fingerprint checks after output durability and immediately before artifact construction. It must catch the reproduced same-size mutation, but it must not claim protection against a malicious writer that can preserve or restore all checked metadata; locking and a second read/hash pass remain outside this repair.

Final independent review passed on the live source: direct `80 passed`, dependency chain `630 passed`, and full repository `1436 passed`; compile/Ruff/format/Mypy were clean. Independent probes confirmed post-open critical cleanup, same-size physical mutation rejection after EOF, and fixed detached invalid-constructor handling. C3B-2C is therefore `APPROVED_LOCAL`; it remains uncommitted and does not imply promotion, pair-atomicity, recovery, or DATA-005 completion.

### 10.5 Approved slice — C3B-2D promotion

Start condition: C3B-2C is independently `APPROVED_LOCAL`.

Approved micro-slice split:

1. **C3B-2D-A/2 — `APPROVED_LOCAL`:** one package-private primitive for JIT-verified promotion of exactly one closed durable staging entry to its canonical final path. It owns Windows-local no-clobber `os.rename`, single-entry verification, and sanitized error translation. It does not own pair order or pair state.
2. **C3B-2D-B/2 — `APPROVED_LOCAL`:** compose the approved primitive failure-first then research-second, expose exact pair/partial observed progress, and define the crash boundary consumed by C5.

This split is architectural, not a new KPI task. It prevents the low-level no-clobber race logic and the two-entry crash state machine from competing for one coder context.

Design responsibility:

- Accept one durable staged artifact and its bound paths.
- Revalidate raw, staging, final parents, final target absence, identity, size, and same-volume assumptions immediately before each mutation.
- Publish failure sidecar first, research artifact second.
- Use Windows-local no-clobber rename semantics; do not use `os.replace()`.
- Verify each final entry after rename.
- Return exact physical state or a sanitized typed error.
- Never create the research manifest.
- Never claim pair-atomicity; crash after first rename is an explicit recoverable state for C5.

Primary failure hotspots:

- Last-moment target race causing overwrite.
- Cross-volume rename losing atomicity.
- Wrong promotion order exposing research without sidecar.
- Retrying after partial promotion and duplicating/clobbering files.
- Deleting unknown entries during rollback.
- Treating two successful renames as a committed dataset.

Performance is O(1) metadata work plus two same-volume renames per artifact. Durability remains bounded by filesystem/storage guarantees; Windows has no portable directory-fsync guarantee.

A/2 must remain package-private and must not be lazy-exported. Its exact write-set is only `_publication_promotion.py` plus its direct test file. It must not read/hash staged content; C4 verifies exact final bytes before publishing the final manifest. A/2 is intentionally Windows-local because Python stdlib `os.rename` does not provide portable cross-platform no-clobber semantics.

Independent A/2 blocker review confirmed a narrower Windows behavior than the original acceptance assumed. With two different source entries racing for one destination, Windows returns exactly one success and one WinError 183 without clobbering. With two synchronized calls using the identical canonical source and destination, both `os.rename()` calls can return success while only one physical move occurs. This reproduced `100/100` times in a standalone probe that did not import A/2.

No metadata-only postcondition can identify a unique invocation winner in the identical-source case because both callers share the same source baseline and observe the same final identity/state. Paw approved the correction on 2026-08-23: A/2 success means verified physical final state, the invariant is at most one physical move plus no clobber, and orchestration above the primitive is single-writer. `PromotedEntryState` is not an ownership/winner/lease/commit token. Cross-process claim/lease and stale-owner recovery remain C5 scope.

GPT repaired the approved observational contract, containment classification, identity/device availability downgrade, concurrency tests, and static gates. The first independent review passed direct `136`, dependency `671`, full `1572`, and repeated Windows concurrency probes, but found one P2 temporal size-drift classification defect. The bounded repair reordered complete previous/current drift before the static size contract only for the second observation and added one focused regression. Final independent review passed direct `137`, dependency `672`, full `1573`, scoped/full Ruff, scoped format, scoped/full Mypy, and two detached runtime probes preserving first-mismatch versus second-drift semantics. A/2 is therefore `APPROVED_LOCAL`.

B/2 then added only `_publication_pair.py` and its focused test file. Independent review matched both coder hashes, inspected the complete orchestrator, and ran real-Windows probes for success, first-entry failure, second-entry failure/partial physical state, and detached invalid construction. Direct B/2+A/2 passed `206`, the publication dependency chain passed `741`, and the full repository passed `1642`; Ruff, format, Mypy, private import, and lazy-export boundaries were clean. B/2 is therefore `APPROVED_LOCAL`. Its result remains sequential observational evidence, not pair atomicity, ownership, recovery, or dataset commit.

### 10.6 Next C4 — Dataset orchestration and manifest commit

Start condition is satisfied: single-artifact staging/conversion/promotion is independently approved.

Fixed dataset lifecycle:

```text
canonical raw manifest load + exact raw-manifest digest
  -> deterministic dataset work plan
  -> sequential prepare/convert/promote for every listed raw file
  -> final read-only verification of raw + research + failure exact bytes
  -> build complete ResearchDatasetManifest
  -> exclusive durable staging manifest
  -> final JIT verification
  -> no-clobber manifest rename last (sole dataset commit marker)
```

Implementation split:

| Slice | Responsibility | Mutation | Status |
|---|---|---|---|
| C4A/5 | Pure immutable work plan from exact `DownloadManifest`; bind canonical file identity, requested range, max line size, and expected line count | None | `APPROVED_LOCAL` — final review passed on 2026-08-27 |
| C4B/5 | Canonical bounded raw-manifest loader; exact-byte SHA-256, strict UTF-8/canonical round-trip, physical stability | Read-only | Ready for isolated assignment |
| C4C/5 | Final dataset verifier; reverify raw manifest plus every raw/research/failure file and compute aggregate hashes over actual concatenated bytes in canonical name order | Read-only | Locked |
| C4D/5 | Durable exclusive staging and Windows-local no-clobber publication of `research_manifest.json` as the final commit marker | Manifest staging + one manifest rename | Locked |
| C4E/5 | Public sequential dataset orchestrator composing C3, C4A-D, error translation, and end-to-end result | Composition only; mutations delegated | Locked |

The split is deliberate. C4A is frozen pure logic. Physical loading, exact-byte verification, and commit publication remain separate so a bug cannot simultaneously corrupt planning, artifact state, aggregate checksums, and the sole commit marker.

Responsibilities:

- Strictly load/validate the raw `DownloadManifest`.
- Build one `RawConversionContext`/`ResearchFilePlan` per expected raw file.
- Process artifacts sequentially in deterministic symbol/interval order for V1.
- Collect only physically verified `ResearchFileArtifact` values.
- Compute aggregate research/failure checksums over exact final bytes in canonical filename order.
- Build `ResearchDatasetManifest` with the original dataset identity and requested range.
- Write, flush, fsync, and no-clobber publish the final research manifest last.
- Treat the final manifest as the sole dataset commit marker.
- Never report complete when an expected artifact is missing, mismatched, unreadable, or only staged.

Primary failure hotspots:

- Treating requested symbols × intervals as expected files instead of using exact `raw_manifest.files`.
- Parsing symbol/interval back out of a filename or collapsing case-sensitive `1M` into `1m`.
- Using per-file actual coverage as the conversion range instead of the requested half-open dataset range.
- Manifest published before every artifact is verified.
- Aggregating artifact digest strings instead of exact file bytes.
- Sorting by processing order instead of canonical names.
- Quarantined-only artifacts incorrectly expanding accepted coverage.
- Dataset identity re-derived from accepted coverage rather than preserved from raw identity.
- Concurrent writers producing mixed artifact sets.
- Mutating or reusing a stale raw manifest between planning and final commit.
- Forgetting the valid empty-dataset path, where no C3 artifact call creates output directories.

Sequential V1 trades throughput for deterministic failure ownership. Complexity is O(total raw + output bytes + artifact count × path depth); memory should remain bounded by one artifact plus manifest metadata.

### 10.7 Future C5 — Recovery, rerun, CLI, and final DATA-005 acceptance

Start condition: C4 commit lifecycle is approved.

Responsibilities:

- Classify all crash states: directories only, one/two staging files, durable stage pair, one final promoted, two finals without manifest, committed manifest.
- Distinguish files owned by this dataset from unknown entries; never blind-delete.
- Verify an existing committed dataset and return deterministic cached success.
- Reject existing conflicting outputs.
- Resume only states with a documented proof; otherwise fail closed and require explicit operator recovery.
- Add the conversion CLI and a fake local end-to-end path.
- Demonstrate reproducible reruns in two independent temporary directories.
- Leave no unexplained `.tmp` files after handled non-crash failures.

Primary failure hotspots:

- “Resume” trusting filename/watermark without checksum and identity.
- Stale stage from another process mistaken for owned state.
- Cleanup crossing containment after a symlink/junction swap.
- Existing final pair treated as committed without manifest.
- CLI exit 0 on partial/incomplete publication.

Recovery adds metadata and checksum I/O but must not load whole files. Keep V1 single-writer and sequential until locking semantics are explicitly designed.

---

## 11. DATA-006 — Duplicate, gap, invalid-OHLC, and outlier detection

### Goal

Analyze committed research-format candles and emit deterministic quality findings without modifying, deduplicating, repairing, imputing, or deleting dataset records.

### Start gate

- DATA-005 committed-dataset contract is approved.
- Research manifest and physical checksum verification are available.
- Input ordering and one-file-per-symbol/interval contracts are explicit.

### Recommended micro-slices

#### Q1 — Quality contracts and objective invariants

Design immutable finding/result types, severity taxonomy, policy version, strict serialization, and the boundary between objective invalid data and statistical suspicion.

Objective failures include malformed research records, timestamp/interval mismatch, invalid OHLC bounds, negative volume/count, range violation, and checksum/read failure. These are not “outliers.”

#### Q2 — Duplicate and continuity scanner

- Natural key: source/exchange/market/symbol/interval/open time as defined by the research contract.
- Equal adjacent open time: duplicate finding; distinguish byte-identical duplicate from conflicting duplicate.
- Current open earlier than previous: ordering violation.
- Expected next open uses `interval_boundary_after()`, including leap February and year rollover for `1M`.
- Missing expected slots become explicit gap ranges with counts; do not invent candles.

Because DATA-005 output is monotonic per symbol/interval file, adjacent-state scanning can detect duplicates/gaps in O(1) memory. If the input violates ordering, report it instead of sorting the full dataset silently.

#### Q3 — Outlier policy

Outliers are suspicious observations, not automatically invalid records. Before code, Claude must submit a short design specifying:

- Feature being tested: return, range, volume, or another dimension.
- Trailing-only versus whole-dataset statistics.
- Warm-up, window size, minimum sample count, and zero/near-zero behavior.
- Decimal-only deterministic arithmetic.
- Threshold configuration and policy version recorded in results.
- Behavior for regime shifts, illiquid symbols, and sparse monthly data.

Preferred V1 is a bounded, deterministic, trailing-window robust method; never use an arbitrary universal percentage threshold as ingestion truth. If whole-dataset/two-sided statistics are selected for diagnostics, mark them explicitly as non-causal so Phase 4 cannot consume them as predictive features.

#### Q4 — Streaming file/dataset analyzer

Connect strict research parsing, manifest identity, continuity, duplicate/conflict, and outlier findings. One file failure must not produce a false clean dataset. Preserve partial findings and return an overall non-pass state.

### Required tests

- Clean fixed intervals and clean leap-February `1M`.
- Exact duplicate and conflicting duplicate.
- Single/multiple/leading/trailing gaps.
- Out-of-order record.
- Invalid OHLC, negative values, malformed type, bool-as-int, NaN/Infinity/scientific input behavior inherited from research schema.
- Empty dataset and all-quarantined dataset.
- Cross-symbol/interval isolation.
- Deterministic finding order and JSON.
- Outlier warm-up, zero MAD/dispersion, regime shift, extreme value, and insufficient sample count.
- Read/checksum failure never reports clean.
- No mutation and no network.

### Common implementation mistakes

- Reusing Phase 2 live-ingestion gap logic without adapting committed research contracts.
- Treating duplicates as silently dropped.
- Computing fixed duration for `1M`.
- Letting one suspicious outlier set objective integrity to failed without documented policy.
- Calculating statistics on future data and later exposing them as strategy features.
- Using float for return/threshold calculations.
- Loading multi-gigabyte files into memory to sort or compute global statistics.

### Ownership and review

Claude implements Q1–Q4 one slice at a time. ChatGPT reviews contract separation, calendar continuity, deterministic statistics, and adversarial false-pass cases. DeepSeek may critique the test matrix but does not write code under the current directive.

---

## 12. DATA-007 — Deterministic data-quality report

### Goal

Generate a reproducible, machine-readable and human-readable report that connects dataset identity, physical integrity, coverage, quality policy, counts, and findings.

### Required report content

- Report schema/version and quality-policy version.
- Dataset ID/version/schema and source lineage.
- Requested versus accepted coverage.
- Symbols/intervals and expected/observed file counts.
- Raw/research/failure/manifest checksums or references to their verified contracts.
- Records scanned, valid, quarantined, duplicated, conflicting, gap slots/ranges, invalid OHLC, and outlier counts.
- Per-symbol/interval summary and deterministic finding order.
- Scanner configuration, outlier window/threshold, and causal/non-causal declaration.
- Read/checksum/parser/operational failures.
- Overall outcome separate from DATA-005 conversion status.

### Status design guardrail

Do not silently extend `DatasetQualityStatus`. If existing `complete`, `partial`, `suspected_gaps`, and `failed` cannot represent outlier/report semantics, submit a design/change decision before changing metadata schema or version. Prefer a report-specific outcome until the metadata evolution is approved.

### Failure hotspots

- Overall PASS despite one unreadable/missing/mismatched file.
- Counts disagreeing with per-file details.
- Empty dataset receiving invented timestamps.
- Outliers omitted because they are “only warnings.”
- Raw payload/path/secret leaked into report errors.
- Current wall-clock time making JSON nondeterministic.
- Report checksum computed before final canonical serialization.

### Performance

Report generation should be O(number of findings + artifacts) after scanning, with deterministic bounded previews. Very large finding sets may require a summarized report plus a separate NDJSON findings sidecar rather than one unbounded JSON array; design this before implementation.

---

## 13. DATA-008 — Dataset fixtures for tests

### Goal

Provide versioned, deterministic fixtures that Phase 3 and Phase 4 can use without network access.

### Required fixture families

1. **Clean fixture:** several fixed candles, multiple symbols/intervals, exact checksums, complete manifest/report.
2. **Calendar fixture:** January→February, leap February 2024→March, December→January for `1M`.
3. **Adversarial quality fixture:** exact duplicate, conflicting duplicate, gap, ordering violation, invalid OHLC, negative value, and statistical outlier.
4. **Publication/recovery fixture:** empty raw, all-quarantined, partial conversion, zero-byte sidecar, and documented stale states where needed.

### Fixture contract

- Synthetic/public-shaped only; no credentials or private data.
- Store source/provenance and generator version.
- Include a deterministic generator command/script where practical.
- Check fixture bytes and expected checksums into tests.
- Test regeneration in a temporary directory and compare byte-for-byte.
- Tests consume fixtures read-only.

### Failure hotspots

- Expected values derived from the production helper being tested.
- Fixture regenerated with current time/random ordering.
- Huge fixture bloating Git and CI.
- Using a currently listed symbol set to claim survivorship-free historical coverage.
- Mixing malformed records into the clean baseline.

Keep fixtures small and representative; performance/load testing uses generated temporary data, not giant committed archives.

---

## 14. DATA-009 — Source, coverage, limitations, and retention documentation

### Goal

Document how to reproduce datasets and what they do and do not prove.

### Required documentation

- Public Binance Spot source/endpoints and acquisition date/range.
- Raw envelope and research schema versions.
- Symbol/interval selection and canonical naming.
- Inclusive raw versus exclusive canonical close semantics.
- UTC and `1M` calendar policy.
- Download, checksum, conversion, quality, and verification commands.
- Directory layout, manifests, commit marker, and recovery behavior.
- Coverage gaps, quarantine behavior, outlier policy, and known limitations.
- Raw/research/report retention, archival, capacity, and deletion authority.
- Reproducibility instructions and expected exit codes.
- Explicit exclusion of private/account/order/live data.
- Bias limitations: listing history, delistings, unavailable instruments, provider revisions, and missing data.

### Failure hotspots

- Documentation claiming CLI/options that code does not expose.
- Calling local test evidence “production verified.”
- Omitting retention size growth and cleanup authority.
- Publishing secrets or local absolute paths.
- Describing a partial file pair as a committed dataset.

Documentation must be checked against real `--help`, fake E2E output, and current schema. Do not copy stale design text after implementation changes.

---

## 15. DATA-010 — Lookahead and survivorship-bias review

### Goal

Perform the final read-only data-pipeline audit before Phase 4. ChatGPT owns this review by task wording; Claude implements only approved fixes discovered by the audit.

### Lookahead audit checklist

- A candle becomes available only after its exclusive close and configured receipt/execution delay.
- No strategy can trade at a candle's open using that candle's high/low/close/volume.
- Range filters and labels do not include future rows.
- Normalization, winsorization, imputation, and outlier thresholds disclose fit windows.
- Trailing features use prior/currently available data only.
- Dataset-wide statistics are marked diagnostic and cannot silently enter a predictive feature pipeline.
- Train/validation/test splits occur before fitting any data-derived parameters.
- Backfill/revision timestamps do not masquerade as historical availability.
- Quarantined or missing data does not get silently forward-filled.

### Survivorship audit checklist

- Symbol universe source and effective dates are documented.
- Current Binance listings are not projected backward as the historical universe.
- Delisted/renamed/merged assets and listing start dates are represented or declared unavailable.
- Missing symbols/periods are not silently excluded from performance claims.
- Stablecoin, quote-asset, and exchange-selection bias are documented.
- Dataset fixtures are not used as evidence of market-wide survivorship correctness.

### Required output

Create a dated audit under `docs/risk/` or another approved documentation location containing findings ordered by severity, exact source evidence, affected downstream Phase 4 assumptions, required fixes, residual risks, and a final PASS/BLOCKED verdict.

Any P0/P1 or false-pass finding blocks the Phase 3 exit gate. DATA-010 does not authorize Phase 4 implementation by itself; Paw must accept the Phase 3 exit review.

---

## 16. Phase 3 exit verification

All checks below must be complete before requesting Paw's phase-close approval:

- [ ] DATA-001…010 are independently reviewed and marked complete in the canonical plan after approval.
- [ ] No active review findings or unreviewed dirty production files remain.
- [ ] Full `pytest -q --no-cov` passes on final source.
- [ ] Full `ruff check .` passes.
- [ ] Full `ruff format --check .` passes.
- [ ] Full `mypy packages apps` passes.
- [ ] All Phase 3 lazy exports import successfully.
- [ ] CLI `--help` and documented exit codes are exercised.
- [ ] Fake end-to-end flow succeeds: raw fixture/download → checksum → conversion/publication → checksum → quality report.
- [ ] Two independent output directories produce byte-identical committed research datasets.
- [ ] Corruption, missing file, read failure, malformed record, partial publication, and stale staging never yield false PASS.
- [ ] No unexplained `.tmp`/staging files remain after handled non-crash scenarios.
- [ ] Crash-state behavior matches the recovery contract.
- [ ] Raw archive bytes/mtime remain unchanged by conversion and quality analysis.
- [ ] No network/private API/account/order/live-trading access occurs in tests.
- [ ] DATA-009 commands and paths match real output.
- [ ] DATA-010 bias review is PASS or all blocking findings are fixed and re-reviewed.
- [ ] Execution log is appended; prior history is not erased.
- [ ] Paw explicitly approves transition to Phase 4.

Release evidence must list exact commands, exit codes, test counts, files changed, residual risks, and whether work is merely local or committed.

---

## 17. Cross-cutting failure register

| Risk | Typical bug | Mandatory defense |
|---|---|---|
| Timestamp ambiguity | Magnitude inference or naive datetime | Explicit unit, aware UTC, integer arithmetic |
| Monthly interval | `1M` converted to `1m` or 30 days | Case-sensitive identity and calendar helper |
| Financial precision | Float coercion/rounding | String + Decimal, fixed output bound |
| Raw corruption | Converter rewrites raw archive | Read-only tests for bytes and mtime |
| Silent data loss | Invalid row dropped without record | Typed quarantine/failure sidecar |
| False integrity PASS | Read/checksum failure ignored | Fail-closed aggregate status tests |
| Digest confusion | Hashing metadata/digest strings instead of bytes | Explicit digest-domain contract |
| Exception leakage | `raise ... from None` inside active `except` | Deferred public raise; inspect cause/context/vars |
| Path escape | Traversal, symlink, junction, reparse | Lexical + physical containment and JIT revalidation |
| TOCTOU | Trusting old C2 snapshot | Recheck immediately before each mutation |
| Overwrite | `wb`, `touch`, or `os.replace` | Exclusive create and no-clobber promotion |
| Handle leak | Error before sequential close | One ownership boundary; attempt-close all handles |
| Fake atomicity | Treating file pair as committed | Final manifest is dataset commit marker |
| Crash ambiguity | Blind cleanup/resume | Explicit physical-state classifier |
| Outlier false positive | Universal threshold | Versioned, symbol-aware robust policy |
| Lookahead | Full-dataset fitted preprocessing | Split/fit chronology and causal metadata |
| Survivorship | Current symbols used historically | Time-aware universe or explicit limitation |
| Test false confidence | New module unimported | Direct test file + runtime probes + source review |
| Scope creep | Private API or Phase 4 logic enters data code | Write-set and import-boundary scan |

---

## 18. Agent-specific operating notes

### Claude implementation prompts

- One micro-slice per prompt.
- Prefer two to four production/test files; list conditional files separately.
- Begin with current baseline, exact write-set, required API, invariants, tests, gates, and stop conditions.
- Put later slices out of scope so Claude does not rush the active state machine.
- Run targeted tests before full suite.
- Report remediation encountered during gates, not only final green output.
- End with exact residual risk and confirmation that no next slice started.

Claude-specific review warning: long prompts can consume the context before verification. Split by actual lifecycle boundary, not arbitrary line count. A 1,000-line filesystem module is a maintenance risk even when tests pass; prefer a deep package-private seam over copied validation logic.

### ChatGPT/Codex review

- Read actual source and dirty state; never review only the coder report.
- Search for missing tests/import references.
- Exercise the real happy path and at least one failure-injection path.
- Inspect state transitions, resource ownership, exact bytes, exception context, and filesystem leftovers.
- Run targeted gates; run full gates only after targeted success.
- Findings first, with path/line evidence and severity.
- A green full suite is not proof if the new module is not collected/imported.

### DeepSeek consultation

Good uses under the current no-code role:

- Independent test-matrix critique.
- Mechanical contract comparison.
- DATA-009 documentation completeness check.
- DATA-010 bias-question brainstorming.

Require ChatGPT verification for any DeepSeek claim touching filesystem atomicity, causal statistics, schema evolution, or live-money risk.

---

## 19. Handoff update protocol

### 19.1 Active checkpoint ownership

`PHASE-3-active-checkpoint.md` is the single mutable execution snapshot. ChatGPT/Codex is its default writer and must refresh it:

1. Before issuing every implementation or repair prompt.
2. After receiving every coder completion report, changing the state only to `IMPLEMENTED_UNREVIEWED` until review finishes.
3. After independent review, recording `REVIEW_FINDINGS` or `APPROVED_LOCAL` and the single next action.

Coder reports are evidence inputs, not status authority. Coders must not edit the checkpoint, this master handoff, or mark their own work approved unless Paw explicitly assigns that documentation change.

### 19.2 Durable master-handoff updates

Update this file after every independently reviewed slice or durable architectural/ownership decision:

1. Refresh timestamp, Git branch/HEAD/status, and graph freshness note.
2. Change only the affected task/slice state.
3. Record exact files created/modified.
4. Record direct tests, targeted dependency tests, static gates, runtime probes, and full suite separately.
5. Record findings and repair status; do not erase rejected history.
6. Record residual risks and the single next authorized action.
7. If a schema/task/owner/phase change is required, link an approved KPI change request instead of editing the baseline silently.

Use this append-only execution history format:

| Date | Slice | Coder | Reviewer | Verdict | Exact evidence | Next action |
|---|---|---|---|---|---|---|

### Execution history not yet reflected in the KPI ledger

| Date | Slice | Coder | Reviewer | Verdict | Evidence summary | Next action |
|---|---|---|---|---|---|---|
| 2026-08-19/20 | DATA-005 research format, line kernel, stream, manifest and hardening | Claude/GPT Sol | ChatGPT | `APPROVED_LOCAL` | Focused and full tests reported green through each accepted slice; strict decimal/error/preview contracts preserved | Publication lifecycle |
| 2026-08-20/21 | DATA-005 C1, C2, C3B-1, C3B-2A, C3B-2B | Claude/GPT Sol | ChatGPT | `APPROVED_LOCAL` | Layout/preflight/staging suites and full gates reported green; shared physical seam established | C3B-2C |
| 2026-08-23 | DATA-005 C3B-2C initial draft | Claude | ChatGPT | `REVIEW_FINDINGS` | Independent probes found impossible happy path, handle leaks, exception retention, missing direct tests, Ruff/format failure | Repair same slice |
| 2026-08-23 | DATA-005 C3B-2C repair takeover | GPT | Pending independent ChatGPT/Codex review | `IMPLEMENTING` | Paw reassigned the existing repair prompt after Claude exhausted its token quota; `_publication_staging.py` and `_publication_conversion.py` are active production paths | Finish exact repair scope; no C3B-2D |
| 2026-08-23 | DATA-005 C3B-2C GPT repair review | GPT | ChatGPT/Codex | `REVIEW_FINDINGS` | Direct 53 and dependency 603 passed; static gates clean; probes reproduced post-open handle leak, post-EOF same-size raw mutation acceptance, and marker-leaking invalid error construction | Repair same C3B-2C slice; add regressions; no C3B-2D |
| 2026-08-23 | DATA-005 C3B-2C three-finding repair assignment | Claude primary / GPT fallback | ChatGPT/Codex pending | `IMPLEMENTING` | One identical bounded prompt; one-pass metadata-fingerprint policy; exact ownership and constructor regressions required | Finish repair, report evidence, then independent review |
| 2026-08-23 | DATA-005 C3B-2C final repair review | Claude/GPT fallback | ChatGPT/Codex | `APPROVED_LOCAL` | Direct 80, dependency 630, full 1436; full static gates clean; three independent adversarial probes passed with detached errors and closed handles | Design C3B-2D; no promotion code yet |
| 2026-08-23 | DATA-005 C3B-2D-A/2 assignment | Claude primary / GPT fallback | ChatGPT/Codex pending | `IMPLEMENTING` | Paw assigned Claude the bounded one-entry Windows no-clobber primitive; exact two-file write-set; GPT may continue only the identical remaining prompt after explicit transfer | Implement A/2 and return `IMPLEMENTED_UNREVIEWED` |
| 2026-08-23 | DATA-005 C3B-2D-A/2 blocker review | GPT partial | ChatGPT/Codex | `BLOCKED` | Standalone Windows probe reproduced identical-source double-success 100/100 while preserving one physical move; direct 111/2 red; containment classification and scoped static gates also unfinished | Paw chooses concurrency semantics; preserve partial files; no B/2 |
| 2026-08-23 | DATA-005 C3B-2D-A/2 contract decision and repair assignment | Claude primary / GPT fallback | ChatGPT/Codex pending | `IMPLEMENTING` | Paw approved physical no-clobber plus observational success; `PromotedEntryState` is not ownership; exact two-file repair preserves one rename/no lock | Repair A/2 and return `IMPLEMENTED_UNREVIEWED`; no B/2 |
| 2026-08-24 | DATA-005 C3B-2D-A/2 implementation review | GPT | ChatGPT/Codex | `REVIEW_FINDINGS` | Direct 136, dependency 671, full 1572 and repeated Windows probes pass; second-final size drift reproduces detached `verification_mismatch` instead of `concurrent_change` | Reorder second-snapshot drift check and add one regression; no B/2 |
| 2026-08-24 | DATA-005 C3B-2D-A/2 final repair review | GPT | ChatGPT/Codex | `APPROVED_LOCAL` | Direct 137, dependency 672, full 1573; scoped/full static gates pass; independent first-mismatch and phased second-drift probes return detached `verification_mismatch` and `concurrent_change` respectively | Design and assign bounded C3B-2D-B/2; keep C4/C5 locked |
| 2026-08-24 | DATA-005 C3B-2D-B/2 assignment | Claude primary / GPT fallback | ChatGPT/Codex pending | `IMPLEMENTING` | Two new files only; thin filesystem-free pair orchestrator; sequential observations and detached partial-progress errors; A/2 composition probe passed | Implement exact prompt and return `IMPLEMENTED_UNREVIEWED`; no C4/C5 |
| 2026-08-24 | DATA-005 C3B-2D-B/2 independent review | Claude/GPT fallback | ChatGPT/Codex | `APPROVED_LOCAL` | Hashes matched; independent Windows success/partial/error-detachment probes passed; direct 206, publication chain 741, full 1642; Ruff/format/Mypy/import boundaries clean | Design bounded C4 slices; keep C5/DATA-006 locked |
| 2026-08-24 | DATA-005 C4 design and C4A/5 assignment | ChatGPT/Codex | ChatGPT/Codex | `NOT_STARTED` | Five-slice lifecycle fixed after direct contract reads; C4A is a pure package-private two-file planning seam; graph freshness was stale/not-tracked so current source is authoritative | Transfer C4A prompt to Claude; no C4B/C5 |
| 2026-08-24 | DATA-005 C4A/5 fallback transfer | GPT | ChatGPT/Codex pending | `IMPLEMENTING` | Paw transferred the unchanged exact two-file C4A/5 prompt to GPT after Claude exhausted quota; GPT is the sole active writer and the live dirty tree must be preserved | Return `IMPLEMENTED_UNREVIEWED`; independent review before C4B/5 |
| 2026-08-27 | DATA-005 C4A/5 final review | GPT | ChatGPT/Codex | `APPROVED_LOCAL` | Final timezone lifecycle repair matched frozen production/planner hashes; focused 6, manifest/planner 400, dependency 538, full 1877; Ruff/format/Mypy/diff integrity clean | Assign C4B/5 in an isolated worktree |

Do not copy historical test totals forward as current evidence. Rerun gates after every source change.

---

## 20. Single next action

**Create an isolated worktree from the reviewed `main` checkpoint and assign only C4B/5. Keep C4C/5 through C5, DATA-006, and all approved C3/C4A contracts locked.**
