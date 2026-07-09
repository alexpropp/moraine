# RFCs

Design records for moraine. An RFC is **required** for decisions that are
expensive to reverse — KV key layout, commit/transaction protocol, public API
shape — and optional for everything else. RFCs double as an ADR log: they
record *why* the project is the way it is.

## Process

1. Copy `0000-template.md` to `NNNN-kebab-title.md` (next free number).
2. Status lifecycle: `Draft` → `Accepted` (on sign-off) → `Implemented`.
   Replaced RFCs become `Superseded` with a pointer to their successor.
3. Accepted RFCs are binding until superseded. If implementation reveals a
   better design, update or supersede the RFC — don't silently diverge.

Design documents produced in brainstorming/design sessions are written
directly here as `Draft` RFCs; there is no separate specs directory.
