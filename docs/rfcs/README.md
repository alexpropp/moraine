# RFCs

Design records for moraine. An RFC is **required** for decisions that are
expensive to reverse — KV key layout, commit/transaction protocol, public API
shape — and optional for everything else. RFCs double as an ADR log: they
record *why* the project is the way it is.

## Process

1. Copy `0000-template.md` to `NNNN-kebab-title.md` (next free number).
2. RFCs carry no status field. Every RFC in this directory is the current
   design and is binding: if implementation reveals a better design, update
   the RFC (or replace it with a successor that points back) — don't
   silently diverge.

Design documents produced in brainstorming/design sessions are written
directly here as RFCs; there is no separate specs directory.
