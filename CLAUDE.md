# Tessera — agent guide

## Decision ledger

Record every architectural or policy decision — and deferrals, hypotheses,
discoveries, incidents, outages, and human corrections — in
[`docs/decision_log.md`](docs/decision_log.md), the append-only
[ADR-Light](https://github.com/auspexlabs/ADRLight) ledger.

- Append typed entries: `DEC` / `DEF` / `HYP` / `DIS` / `INC` / `OUT` / `BOT`.
- The ledger allocates all IDs (one number space per type).
- Never rewrite a past entry except its status line (e.g. `accepted` →
  `superseded by DEC-###`); corrections are new entries.
- At a decision moment, suggest an entry and confirm before appending; don't log
  trivia (naming, formatting, routine refactors).

## Primitive/service taxonomy

Pool, Ring, Channel, and Slate are **primitives** — independent, no primitive
depends on another. Sink is a **layer-2 service** that composes Pool + Channel.
Keep that boundary: share nothing between primitives (each carries its own
`namespace.rs` / `error.rs` / seqlock); only services may depend on primitives.
