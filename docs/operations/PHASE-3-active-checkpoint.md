# Phase 3 Active Checkpoint

**Purpose:** Single mutable execution snapshot for the current Phase 3 slice
**Last refreshed:** 2026-08-27 15:34 ICT
**Canonical plan:** [`docs/KPI_PLAN.md`](../KPI_PLAN.md)
**Master handoff:** [`PHASE-3-execution-handoff.md`](PHASE-3-execution-handoff.md)
**Checkpoint owner:** ChatGPT/Codex reviewer and prompt coordinator

Read this file before issuing a prompt, accepting a coder report, reviewing a slice, or resuming after context loss. Refresh repository state before relying on this snapshot.

## Current execution state

| Field | Value |
|---|---|
| Phase | Phase 3 — Historical Data and Data Quality |
| KPI task | DATA-005 — Convert raw archives to research format |
| Slice | C4B/5 — canonical bounded raw-manifest loader |
| Status | `READY_FOR_ASSIGNMENT` — C4A/5 is independently `APPROVED_LOCAL`; no C4B writer has started |
| Coder | Unassigned until the coordinator creates an isolated worktree and exact C4B/5 prompt |
| Reviewer | ChatGPT/Codex |
| Branch / checkpoint parent | `main` / `3a491b7` |
| Next slice | Design and assign C4B/5 only; C4C/5 through C5 and DATA-006 remain locked |

Reviewed paths included in the DATA-005 checkpoint:

- `packages/market_data/datasets/_publication_staging.py` — approved dependency seam; preserve it.
- `packages/market_data/datasets/_publication_conversion.py` — approved C3B-2C implementation; preserve it.
- `tests/unit/test_market_data_publication_conversion.py` — approved C3B-2C tests; preserve them.
- `packages/market_data/datasets/_publication_promotion.py` — approved A/2 production path; preserve it.
- `tests/unit/test_market_data_publication_promotion.py` — approved A/2 focused tests; preserve them.
- `packages/market_data/datasets/_publication_pair.py` — approved B/2 package-private orchestrator; preserve it.
- `tests/unit/test_market_data_publication_pair.py` — approved B/2 focused tests; preserve them.
- `docs/operations/*` — reviewer-owned documentation; coder must not edit it.

C3B and C4A/5 are frozen at this reviewed checkpoint. No C4B/5 coder may start before receiving an isolated worktree, exact write-set, acceptance criteria, and stop conditions. C4C/5 through C5 and DATA-006 remain locked.

## Approved C3B-2D-B/2 review

Historical exact write-set:

- NEW `packages/market_data/datasets/_publication_pair.py`
- NEW `tests/unit/test_market_data_publication_pair.py`

B/2 is a package-private, filesystem-free orchestrator over the approved `promote_staged_entry_no_clobber()` primitive. It calls `failure` exactly once before `research` exactly once, returns two sequential verified observations on success, and exposes only truthful observed progress on failure. `failure_observed` means the failure primitive returned a valid observation before the research attempt; it is not a current simultaneous snapshot, rename ownership, pair commit, or recovery token.

Independent verdict on 2026-08-24: `APPROVED_LOCAL`. Production/test hashes matched the coder report. Independent real-Windows probes covered full success, first-child failure, second-child failure with the failure final preserved, and invalid-constructor sanitization. Final gates passed: direct B/2+A/2 `206`, publication chain `741`, full repository `1642`, scoped/full Ruff, scoped format, code-tree format, scoped/full Mypy, private import, and lazy-export absence. No source finding remains in B/2.

## Approved C4A/5 review — pure dataset work planning

Historical DATA-005 C3 interfaces are frozen. C4 is split into five implementation slices: pure deterministic planning, canonical raw-manifest loading, final exact-byte verification, manifest-last no-clobber publication, and top-level sequential composition.

Independent verdict on 2026-08-27: `APPROVED_LOCAL`. The final test-only repair replaced misleading timezone tests with per-instance second-observation, offset-sequence, stable-custom-timezone, and no-third-observation regressions. Production and planner hashes remained unchanged. Focused timezone tests passed `6`, manifest plus planner passed `400`, the dependency chain passed `538`, and the full repository passed `1877`; Ruff, format, Mypy, and diff integrity were clean. No C4A/5 blocker remains.

C4A/5 exact write-set:

- NEW `packages/market_data/datasets/_publication_dataset_plan.py`
- NEW `tests/unit/test_market_data_publication_dataset_plan.py`

C4A/5 is package-private and performs no filesystem, hashing, JSON parsing, staging, conversion, promotion, manifest publication, recovery, network, or environment work. It accepts one exact `DownloadManifest` plus `max_line_bytes`, reuses the existing conversion-manifest raw validator and centralized naming authority, and returns an immutable ordered work plan. Each work item binds one exact `ResearchFilePlan`, one exact `RawConversionContext`, and the expected raw line count.

Critical rules:

- Plan only `raw_manifest.files`; never manufacture the full symbols × intervals Cartesian product.
- Resolve identities through the existing canonical naming authority; never split filenames or duplicate `symbol.replace('/', '-')` logic.
- Preserve raw-manifest file order, which must already be sorted and unique.
- Every context uses the manifest requested half-open range, not each file's actual coverage range.
- Preserve case-sensitive `1m` versus `1M` identity.
- Reject bool/coercion/subclasses and revalidate direct `DownloadManifest` construction because that dependency has no `__post_init__`.
- Empty complete raw datasets produce an empty work tuple.
- Errors must be fixed, sanitized, detached, and valid under Python `-O`.

If using the existing package-private `_validate_raw_manifest` or `_raw_output_name` is incompatible, the coder must stop with an exact dependency finding. The coder may not copy their logic or modify `conversion_manifest.py` in this slice.

## Approved C3B-2D-A/2 contract and next boundary

C3B-2D is split to keep the mutation surface reviewable:

1. **A/2 (`APPROVED_LOCAL`):** one package-private primitive that promotes exactly one closed durable staging entry to its canonical final path using Windows-local no-clobber rename semantics.
2. **B/2 (`APPROVED_LOCAL`):** pair orchestration that calls the approved primitive failure-first then research-second and reports truthful sequential/partial observed progress. B/2 owns pair order and crash semantics; it does not mutate A/2 semantics or infer unique rename ownership from observational success.

A/2 historical write-set:

- NEW `packages/market_data/datasets/_publication_promotion.py`
- NEW `tests/unit/test_market_data_publication_promotion.py`

No other A/2 file was writable. B/2 used the separate historical two-file write-set above.

A/2 acceptance requires:

- Exact `ResearchPublicationLayout` and `ResearchFileArtifact` inputs plus exact `failure`/`research` entry kind; no coercion.
- Package-private API only; no lazy export and no user-facing publisher yet.
- Read-only snapshot preflight followed by JIT reinspection of raw identity/size, source stage identity/size/parent, final parent, both manifest targets, destination absence, and usable same-device metadata immediately before the single rename.
- Runtime fail-closed outside Windows because stdlib `os.rename` no-clobber semantics are not portable.
- Use `os.rename` exactly once on success; never `os.replace`, overwrite, unlink, delete, cleanup, truncate, content-read, content-hash, flush, fsync, or manifest publication.
- Reinspect the final regular file after rename; require canonical parent, exact size, and matching physical identity when metadata is usable; require the source path to be absent.
- Existing/racing destination must never be clobbered. Post-rename verification failure must leave physical state untouched for C5.
- Concurrency acceptance is physical, not caller-ownership based: at most one physical move and no destination overwrite/corruption. Identical concurrent source/destination callers may both return the same verified final state because Windows may report both identical `os.rename()` calls as successful.
- `PromotedEntryState` is observational evidence only. It is not a winner, lease, ownership, retry, or commit token. C3B-2D-B/2 and C4 must run under a single-writer policy until C5 owns cross-process coordination and stale-owner recovery.
- Sanitized typed errors with fixed operation/category mappings; no paths, values, errno, OS text, or lower-level exception retained in `str`, `repr`, `vars`, `__cause__`, or `__context__`.
- Tests cover Windows success for both entry kinds, zero-byte files, last-moment races, source/final/manifest redirection, cross-device metadata, post-rename mismatch, critical exception propagation, Python `-O`, and static forbidden-operation guards.

The accepted residual boundary is explicit: A/2 validates metadata and exact size but does not reread/hash staged content. C4 must verify exact final bytes before publishing the dataset manifest. A/2 is not pair-atomic and does not make a dataset committed.

## Independent C3B-2D-A/2 blocker review — 2026-08-23

Historical verdict at review time: `BLOCKED`. GPT correctly stopped instead of weakening the contract silently. Paw resolved the decision at 2026-08-23 21:46 ICT, so the slice is now back in `IMPLEMENTING` under the approved contract below.

Independent Windows probe, run without importing the promotion module:

- Same canonical source and destination, two synchronized callers: both `os.rename()` calls returned success in `100/100` runs; exactly one physical move occurred, source became absent, and destination contained the original source bytes.
- Two different sources racing for one destination: exactly one success and one `FileExistsError`/WinError 183 in `100/100` runs; the losing source remained and destination was not overwritten.

Therefore Windows preserves physical no-clobber, but a metadata-only primitive cannot produce a unique invocation-winner signal when concurrent callers submit the identical source/destination pair. Both callers observe the same source baseline, final identity, final bytes/size, source absence, and destination presence. Meeting `at most one invocation returns success` requires a lock, exclusive claim/lease, extra filesystem mutation, native coordination primitive, or higher-layer serialization—all outside the approved A/2 contract.

Additional current-source findings:

1. Post-rename containment/redirection is incorrectly classified as `verify_destination/io_failure` because `filesystem containment could not be verified` is included in the I/O-reason set; it should remain an unsafe-filesystem/concurrent-change classification.
2. A usable staging identity becoming unavailable at the promoted destination can currently return `PromotedEntryState(..., physical_identity=None)` instead of failing closed.
3. Direct suite is red: `111 passed, 2 failed`; the two failures are the impossible unique-winner assertion and the containment classification above.
4. Scoped Ruff has two test findings, format check reports both files unformatted, and scoped Mypy reports one Literal narrowing error. Compile passes.

Approved contract decision — Paw, 2026-08-23 21:46 ICT:

- Keep the exact one-rename/no-lock primitive.
- Define success as a verified final physical state, not proof that this invocation uniquely won the rename.
- Require only `at most one physical move; destination is never clobbered`; identical concurrent callers may observe the same verified final state.
- Require C3B-2D-B/C4 to run under a single-writer policy and never infer ownership from `PromotedEntryState`. Cross-process lease/claim and stale-owner recovery belong to C5.

The rejected alternative was unique invocation ownership via a cross-process claim/lease. Do not add locks, claims, leases, temporary ownership files, native dependencies, or C5 recovery behavior inside this two-file A/2 repair.

## Independent repair review — 2026-08-23

Verdict: `REVIEW_FINDINGS`. Existing tests and static gates are green, but adversarial probes found gaps not covered by the coder suite:

1. **P1 — ownership boundary starts too late.** `raw_stream` is opened before `sha256()` and `_HashingReader` construction, while the cleanup `try/finally` starts afterward. Injected `MemoryError` from `sha256()` left raw, research, and failure streams open and `pair.closed=False`.
2. **P1 — raw can change after EOF verification.** Raw stability is checked before output flush/fsync. A same-size in-place raw mutation during research fsync still returned an artifact whose `raw_sha256` did not match the physical raw file.
3. **P2 — error constructor is not fail-closed.** Invalid runtime operation/category values index `_ERROR_MESSAGES` directly, raising a `KeyError` that exposes the supplied marker instead of a sanitized staged-conversion error.

Independent evidence on the reviewed source:

- Direct C3B-2C suite: `53 passed`.
- DATA-005 publication dependency chain: `603 passed`.
- Compile, scoped Ruff, scoped format, and scoped Mypy: pass.
- Ownership probe: `raw_closed=False`, `research_closed=False`, `failure_closed=False`, `pair_closed=False`.
- Same-size mutation probe: artifact returned with `digest_matches_physical=False`.
- Invalid-constructor probe: `KeyError`, `marker_leaked=True`.
- Full suite was not rerun because the adversarial acceptance gate was already red; a green full suite cannot override these runtime findings.

Repair policy issued at this refresh:

- Move the ownership `try/finally` immediately after successful raw open, before digest/reader setup.
- Preserve one bounded raw pass. Add a strict raw metadata fingerprint and revalidate it after both output fsyncs and immediately before artifact construction so the reproduced same-size mutation fails closed.
- Do not claim that metadata revalidation eliminates metadata-preserving malicious mutation; exact prevention would require locking or a second read/hash pass and is outside this repair.
- Make invalid direct `StagedConversionError` construction fail with one fixed sanitized `ValueError`, without storing supplied values.
- Claude owns the repair first. If Claude exhausts quota, GPT continues from the same prompt and live dirty tree; neither model edits this checkpoint or self-approves.

## Final independent repair review — 2026-08-23

Verdict: `APPROVED_LOCAL`. No blocking finding remains in the bounded C3B-2C contract.

Independent evidence on final source:

- Direct C3B-2C suite: `80 passed`; no skip was reported.
- DATA-005 publication dependency chain: `630 passed`; no skip was reported.
- Independent ownership probe: injected post-open `sha256()` `MemoryError` propagated after raw/research/failure closure and `pair.closed=True`.
- Independent physical mutation probe: a same-size raw mutation during research fsync returned no artifact and raised detached `inspect_raw/concurrent_change`; all handles closed.
- Independent invalid-constructor probe: fixed sanitized `ValueError`, marker absent, no supplied attributes, and both cause/context `None`.
- Scoped compile, Ruff, format, and Mypy: pass.
- Full repository: `1436 passed`; full Ruff, `232 files already formatted`, and full Mypy over `176 source files`: pass.
- Graph generation does not track the active private/untracked files, so direct source plus runtime evidence is authoritative for this verdict.

Residual risk is intentional and documented: metadata fingerprinting detects observable same-size mutation but cannot defeat a malicious writer that preserves/restores all checked metadata; exact prevention requires locking or a second content pass. C3B-2C also does not provide pair-atomic promotion or crash recovery.

## Independent C3B-2D-A/2 implementation review — 2026-08-24

Verdict: `REVIEW_FINDINGS`. Core integrity is fail-closed and no P1 finding was found, but one reproducible P2 lifecycle-classification defect remains.

Finding:

- In `_inspect_promoted_destination()`, the exact-size contract is checked before comparing the second final snapshot with `previous`. If the first promoted-final inspection is correct and only `st_size` changes before the second inspection, the function raises detached `verify_destination/verification_mismatch` instead of `verify_destination/concurrent_change`. Identity/device/resolution drift in the same lifecycle is already classified as concurrent change. This distinction matters before B/2/C5 consume promotion state and recovery categories.

Required repair:

- For `previous is not None`, compare the complete previous/current snapshot before checking the static artifact-size contract. A size drift between final inspections must be `concurrent_change`; an incorrect size already present on the first final inspection remains `verification_mismatch`.
- Add one focused regression that phases destination `st_size`: first final observation exact, second final observation changed. Assert detached `verify_destination/concurrent_change`, final retained, source absent, and no rollback/cleanup.
- Preserve the exact two-file write-set and every observational/no-clobber contract already passing.

Independent evidence on the final GPT source:

- Final hashes match the coder report: production `2B9F94152A9592061EB47B686D5E2C038E65B9E78CBEDE373F47619E15CB6650`; test `C5824225956141FDDBE337C1395F3E26BB7716E224D1372DDB8B7E0989C40351`.
- Direct A/2 suite: `136 passed`; dependency chain: `671 passed`; full repository: `1572 passed`.
- Compile, scoped/full Ruff, scoped/full Mypy, and scoped format pass. Full format is red only on pre-existing out-of-scope `fix_helper.py` and `fix_symlink.py`.
- Independent 30-run probes: identical-source calls returned two observational successes in `30/30`; different sources produced exactly one winner and one detached `promote/entry_exists` in `30/30`.
- Independent identity-downgrade and post-rename-redirection probes failed closed with detached `verification_mismatch` and `concurrent_change`, respectively.
- Independent second-size-drift probe reproduced `verify_destination/verification_mismatch`; this is the sole current review finding.
- Graph coverage marks the active private files `not_tracked`; direct source and runtime evidence are authoritative.

## Final independent C3B-2D-A/2 repair review — 2026-08-24

Verdict: `APPROVED_LOCAL`. The bounded lifecycle-classification finding is fixed and no new finding was found in the exact two-file slice.

Independent evidence on final source:

- Final hashes match the coder report: production `7201317CB5E0268F9EF6B407F1BA22D5FC11612EE7D2733334D60A7469DF3FE2`; test `4B541FFD679DC42C4F55C6F5D8797CDB4452CF409889D36E76F5703B87E8A57D`.
- Direct A/2 suite: `137 passed`; publication dependency chain: `672 passed`; full repository: `1573 passed`.
- Compile, scoped/full Ruff, scoped format, scoped/full Mypy: pass. Full format was not rerun because pre-existing out-of-scope `fix_helper.py` and `fix_symlink.py` remain outside the approved write-set.
- Independent first-observation probe returned detached `verify_destination/verification_mismatch`, performed exactly one rename, retained the final entry, and left the source absent.
- Independent phased second-observation probe returned detached `verify_destination/concurrent_change`, performed exactly one rename, retained the changed final entry, and performed no rollback.
- The implementation checks complete previous/current snapshot drift before the static size contract only when a prior verified observation exists; first-observation artifact mismatch semantics remain unchanged.
- Graph transport was closed and both active A/2 files remain untracked, so direct current source, runtime probes, and executed gates are authoritative.

Accepted residual risks remain architectural rather than repair findings: `PromotedEntryState` is observational, identical concurrent callers do not receive unique ownership, A/2 is Windows-local and metadata-only, and one-entry promotion is not pair-atomic or a dataset commit. B/2/C4 must remain single-writer; C5 owns coordination and recovery.

## State-transition and ownership rule

The coder implements and reports evidence. The coder does not edit this checkpoint, change review status, or self-approve unless a prompt explicitly assigns a documentation-only change.

Only the reviewer records these transitions after inspecting live source:

```text
IMPLEMENTING
  -> IMPLEMENTED_UNREVIEWED  (coder reports completion)
  -> REVIEW_FINDINGS         (independent review finds a defect)
  -> APPROVED_LOCAL          (independent source, probe, and gate review passes)
```

A green test count is evidence, not approval.

## Mandatory checkpoint workflow

Before every implementation prompt, the reviewer:

1. Refreshes branch, HEAD, dirty files, active file existence, and coder availability.
2. Reads this checkpoint plus only the relevant master-handoff sections and source contracts.
3. Updates current slice, owner, write-set, acceptance criteria, stop conditions, and single next action here.
4. Issues one bounded prompt that points to this checkpoint instead of pasting the full master handoff.

After every coder report, the reviewer:

1. Marks the work `IMPLEMENTED_UNREVIEWED`; the report itself is not trusted as approval.
2. Reviews actual source/tests and runs adversarial probes plus proportional gates.
3. Records `REVIEW_FINDINGS` or `APPROVED_LOCAL` here.
4. Updates the master handoff only when the independent verdict or a durable architectural decision changes.

## Single next action

Create an isolated worktree from the reviewed `main` checkpoint and issue one bounded C4B/5 raw-manifest-loader prompt. Keep C4C/5 through C5 and DATA-006 locked.
