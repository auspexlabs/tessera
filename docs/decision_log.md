# Tessera Decision Ledger (ADR-Light)

Append-only ledger for decisions, deferrals, hypotheses, discoveries, incidents,
outages, and human corrections. Maintained per
[ADR-Light](https://github.com/auspexlabs/ADRLight): one grep-able file, causal
edges (`triggered_by`, `supersedes`, `resolves`), status-line-only edits to past
entries.

---

## Format

Entry types: DEC (decision), DEF (deferral), HYP (hypothesis), DIS (discovery),
INC (incident), OUT (outage), BOT (human correction). Each type has its own
number space (DEC-001, DEF-001, ...). IDs are allocated by appending an entry
(or stub) to this file — nowhere else.

Format changes are recorded as dated blockquote notes in this section, never by
rewriting old entries.

---

## Decisions

### DEC-001: Add Slate as Tessera's snapshot primitive (graduate the Auspice board)
- `id`: DEC-001
- `date`: 2026-06-14
- `status`: accepted
- `triggered_by`: live observation — the Auspice metrics board was the one multi-process boundary still outside Tessera, plus the directive to put every fitting MP boundary on Tessera ("a package between them is fine")
- `decision`: Add `tessera-slate`, a fifth primitive: a bytes-only, seqlock-protected, latest-value snapshot slot table (overwrite-in-place, random-access by index, no history). It is the snapshot peer of Pool/Ring/Channel. The typed/manifest layer stays in Auspice (`auspice-board`) layered on top.
- `rationale`: No existing primitive fits snapshot semantics — Ring is a lossy *stream* with history and per-reader cursors, Pool is lease-backed one-shot handoff, Channel is a reliable FIFO. A snapshot reader wants the latest value, not a stream. Building it as a primitive (not a layer-2 atop Ring) keeps the dependency graph a clean DAG: services may compose primitives, primitives never depend on each other.
- `impact`: New crate `crates/tessera-slate` (shipped — 23 unit tests + 1 doctest, concurrent cross-mapping hammer, clippy-clean under `-D warnings`; commit `f564ad0`). Slate implements its **own** seqlock so primitives stay independent — Ring is untouched. README + workspace `Cargo.toml` updated. The Auspice-side rework (auspice-schema layout split + auspice-board reimplemented over tessera-slate) is the remaining implementation, tracked in the work tracker, not here.
- `docs_updated`: `crates/tessera-slate/*`, `README.md`, `Cargo.toml`
- `related`: DEF-001

---

## Deferrals

### DEF-001: Defer heterogeneous / per-group slot sizing in Slate
- `id`: DEF-001
- `date`: 2026-06-14
- `status`: active
- `triggered_by`: DEC-001 — Slate v0.1 uses a single uniform `slot_size_bytes` for every slot
- `decision`: Ship Slate v0.1 with **uniform** slot sizes. Heterogeneous (arbitrary per-slot) or per-group (Ring-style size classes) sizing is deferred. Callers that vary record size pad every slot to the maximum; Slate's per-slot `length` field absorbs the variance, so no caller-side padding code is needed and reads return only the written bytes.
- `rationale`: For the intended snapshot-board use case (similarly-sized records) the uniform-padding overhead is negligible against system RAM — tens to a few hundred KB, dominated by one large field. Per-group sizing is a *moderate* change (on-disk geometry table, per-entry offset precompute and validation, a bifurcated config) for a benefit the use case does not need, on a primitive whose value is minimalism. It adds **no correctness risk** (the seqlock and 8-byte alignment are unaffected). Crucially it is cleanly evolvable: `FORMAT_VERSION` + the 40 reserved `GlobalHeader` bytes make it a **non-breaking v0.2 addition**, so deferring costs nothing in future flexibility.
- `impact`: README carries an explicit "known limitation (planned for v0.2)" note next to the Slate component so consumers aren't surprised.
- `revisit_when`: a caller needs heterogeneous slot sizes, OR uniform-padding waste becomes non-negligible for a real workload (e.g. thousands of slots with widely uneven sizes). If revisited, implement **per-group size classes** (mirroring Ring's sections), not arbitrary per-slot — it matches how callers structure data and is the proven pattern.
