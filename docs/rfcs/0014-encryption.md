# RFC 0014: Catalog and data encryption

- **Date:** 2026-07-09

## Summary

Encryption in a DuckLake-on-moraine deployment has two layers that are
routinely conflated, and the point of this RFC is to separate them cleanly.
The first is **data-file encryption**: DuckLake encrypts its Parquet files
and records the key material in catalog rows. moraine's job there is to be a
faithful conduit — store and return that material verbatim, decrypt nothing,
touch no data-file bytes. The second is **catalog-at-rest confidentiality**:
the SlateDB objects (SSTs, WAL) that hold the catalog itself live in object
storage and are, by default, plaintext.

The sharp insight this RFC is built around: **encrypting data files is close
to pointless while the catalog is plaintext.** The catalog holds the
data-encryption keys, the plaintext column min/max in `file_column_stats`
(RFC 0002 stores them verbatim), row counts, table and column names, and the
full schema. The catalog is as sensitive as the data it describes. Data-file
encryption is only meaningful *alongside* catalog-at-rest protection. For
launch we recommend delegating catalog-at-rest to object-store server-side
encryption and reserve design space in the framing header for moraine-level
envelope encryption later.

## Goals

- Separate DuckLake-owned data-file crypto from moraine-owned
  catalog-at-rest crypto, and be explicit about which is which.
- Carry DuckLake's data-file key material through moraine losslessly (RFC
  0006 row-faithfulness), inventing no crypto of moraine's own.
- State a threat model an operator can reason about, rather than an
  aspirational one.
- Give catalog-at-rest a recommended launch answer that needs **no new
  moraine machinery**, and a forward path that fits the existing on-disk
  format (RFC 0002) without a structural break.

Non-goals:

- moraine acting as a KMS. Keys are generated, stored, and rotated by
  DuckLake and the operator; moraine holds and uses what it is given.
- Encrypting Parquet, reading Parquet, or otherwise entering the data-file
  path — moraine never touches those bytes (RFC 0006, RFC 0007).
- Protecting against a compromised writer process or live memory scraping —
  see the threat model.
- Committing to moraine-level value-payload encryption in v1. This RFC
  reserves the design space; the decision is an open question.

## Background

DuckLake supports encrypted data files: the Parquet files are written
encrypted, and the encryption metadata — algorithm, key (or a reference to
one), IV/nonce — is persisted in the catalog, associated with the data-file
record. A reader retrieves that metadata from the catalog and decrypts the
file. The catalog is therefore the custodian of the keys to the data.

moraine is DuckLake's catalog (RFC 0006). It stores DuckLake's rows verbatim
in SlateDB (RFC 0002) and returns them unchanged; it never re-models or
interprets them. In particular, `file_column_stats` min/max values are stored
as DuckLake's exact strings and never parsed (RFC 0002, "statistics values
are stored faithfully"). SlateDB in turn persists the catalog as objects
(SSTs, WAL) in an object store.

The consequence, stated plainly: everything sensitive about the data is also
present, in the clear, in the catalog. So the two encryption layers are not
independent — the weaker one caps the effective protection of the stronger.

## Design

### Two layers

| Layer | Owner | Protects | moraine's role |
|---|---|---|---|
| Data-file encryption | DuckLake | Parquet bytes in object storage | Faithful conduit: store/return key material verbatim, decrypt nothing |
| Catalog-at-rest | moraine / operator | SlateDB SSTs + WAL (keys, stats, schema, names) | Choose and apply an at-rest scheme for the store objects |

The rest of this section takes each in turn, then the interactions.

### Data-file encryption: moraine as faithful conduit

DuckLake owns data-file crypto end to end. It chooses the algorithm,
generates the keys, encrypts the Parquet on write, and records the
encryption metadata in a catalog row (on or beside the `data_file` record).
On read it retrieves that metadata and decrypts.

moraine's entire responsibility is to **carry that metadata losslessly**. The
key material is fields of a `ducklake_*` row; RFC 0002 encodes those rows and
RFC 0006 makes moraine return them exactly as DuckLake wrote them. moraine:

- stores the encryption metadata as opaque row fields — it is not special to
  moraine, just more columns;
- never decrypts a data file, never derives or validates a key, never reads
  a Parquet byte (RFC 0007: moraine schedules object deletes, it does not
  read file contents);
- invents no encryption scheme of its own for data files.

This is row-faithfulness applied to crypto material: whatever key reference,
algorithm tag, or IV DuckLake puts in the row, moraine round-trips bit for
bit. Compaction and GC (RFC 0007) never read data bytes, so data-file
encryption is transparent to them — an encrypted file is scheduled for
deletion exactly as a plaintext one is.

### Catalog-at-rest: the layer that actually matters

Because the catalog holds the data keys and the plaintext stats, protecting
it is the load-bearing decision. Two options:

**(a) Object-store server-side encryption (SSE-KMS).** The bucket encrypts
SlateDB's objects at rest; keys are managed at the bucket/KMS. moraine writes
and reads plaintext to the object-store client, and the store handles
encryption transparently. No moraine crypto, no format change, no key
handling in the core — the entire cost is bucket configuration.

**(b) moraine-level envelope encryption of value payloads.** moraine encrypts
the protobuf **payload** of each value with a data-encryption key (DEK),
itself wrapped by a key-encryption key (KEK) from a KMS. Critically, the
fixed 5-byte framing header (4-byte `MRNE` magic + 1-byte encoding version,
RFC 0002) stays **plaintext**, so corruption detection and encoding-version
negotiation keep working on an encrypted store — a reader still fails loudly
on a wrong-kind or truncated value, and still refuses an encoding version it
does not understand, before it ever attempts to decrypt. Only the bytes after
the header are ciphertext.

| | (a) SSE-KMS | (b) Envelope encryption |
|---|---|---|
| New moraine machinery | none | DEK/KEK handling, per-value crypto |
| Format impact | none | signalled via header; payload opaque |
| Granularity | whole store object | per value |
| Key custody | bucket / KMS | moraine + KMS |
| Migration reads (RFC 0015) | unaffected | constrained — payloads opaque |

**Decision for launch: (a) SSE-KMS.** It gives catalog-at-rest confidentiality
with no new moraine surface, no on-disk format change, and no key material in
the core. It is the recommended launch answer, not the only path: (b) is
reserved as future work for deployments that need the store objects encrypted
independently of the bucket (e.g. the object store is itself untrusted, or
per-value key boundaries are required). When (b) is built, it is signalled
in-band — either through a new value **encoding-version** byte or a reserved
header bit — so a reader distinguishes an encrypted payload from a plaintext
one without guessing, exactly as the encoding version already gates format
evolution (RFC 0002).

### Threat model

State it plainly so operators do not over-trust it.

- **Protects:** the catalog and (via DuckLake's own scheme) the data files
  against an adversary with **read access to the object-store bucket** — a
  leaked bucket, a stolen backup, a mis-scoped IAM read. With SSE the bucket
  ciphertext is inert without the KMS grant; with envelope encryption the
  payloads are inert without the KEK.
- **Does not protect against:** a **compromised writer process**, which
  necessarily holds the keys it uses, nor **live memory scraping** of that
  process. An attacker who is the writer sees plaintext by construction.
- **Key custody is not moraine's.** DuckLake and the operator generate and
  rotate keys; moraine is not a KMS and originates no key material (Goals,
  Non-goals). Under (a) the keys never enter moraine at all; under (b)
  moraine uses a KEK it is handed to unwrap DEKs.

### Interactions

**Inlined data (RFC 0005)** lives *inside* the catalog — it is stored in
SlateDB values, not as Parquet in the object store. So protecting inlined
data is a special case of **catalog-at-rest**, not data-file encryption: it
is covered by (a) or (b) like any other value, and is *not* covered by
DuckLake's data-file scheme (there is no separate file to encrypt). If a
deployment relies on data-file encryption for confidentiality and also
inlines data, the inlined rows are only as protected as the catalog is —
another face of the central insight.

**Format migration (RFC 0015)** is genuinely constrained by (b). A structural
migration that must **read a value's fields** to relocate or reshape it cannot
do so if the payload is envelope-encrypted and opaque to the migrator — the
migrator would need the DEK/KEK to decrypt, re-read, re-encode, and
re-encrypt every value. This is a real limit on what migrations are possible
over an envelope-encrypted store, and a reason the header (magic + encoding
version) stays plaintext: header-only and key-only migrations remain possible
even when payloads are opaque. Flagged for RFC 0015; under (a) migrations are
unaffected because moraine sees plaintext.

**Compaction / GC (RFC 0007)** are transparent to data-file encryption (they
never read data bytes) and, under (a), to catalog-at-rest (the store handles
it). Under (b), compaction operates on already-encrypted payloads as opaque
bytes and needs no keys, since it relocates values without reading their
fields.

**Inlined-data compression (RFC 0005)** is quietly undermined by (b).
RFC 0005 keeps Arrow buffer compression off on the grounds that
"whatever cross-chunk redundancy remains is reclaimed by SlateDB's SST
block compression at rest" — but ciphertext does not compress, so
envelope-encrypting value payloads forfeits exactly that reclamation, and
inlined-data storage inflates by the redundancy SST compression was
absorbing. Under (a) nothing changes (SSE encrypts post-compression at the
object layer). If (b) is built, either the inflation is accepted or
per-value compression-before-encryption is added inside the envelope —
another cost on (b)'s side of the ledger, recorded here so the choice is
made with it in view.

## Open questions

- **Which catalog-at-rest strategy to commit to, and when.** (a) SSE is the
  launch recommendation; whether and when (b) envelope encryption is built
  depends on demand for store-object encryption independent of the bucket.
- **The plaintext-stats leak.** Even with data files encrypted, the catalog
  exposes column min/max verbatim (RFC 0002), plus row counts and schema.
  Encrypting the stats *values* would plug the leak but **break DuckLake's
  pruning**, because DuckLake reads those strings to skip files (RFC 0002,
  RFC 0009). The tension looks unavoidable: the stats are useful precisely
  because they are readable, and moraine does not interpret them. Catalog-at-
  rest (a)/(b) mitigates the leak against a bucket reader; against a party
  that can already read the catalog plaintext, the stats leak is inherent to
  having usable pruning. State the tension; do not pretend it away.
- **KEK source and KMS integration.** For both (a) and (b), where the key
  material comes from (cloud KMS, HSM, operator-supplied), and how moraine is
  handed the KEK under (b), is unspecified.
- **Header signalling and key rotation under (b).** If value-payload
  encryption is adopted, exactly how it is signalled in the framing header
  (a distinct encoding-version value vs. a reserved header bit), and how KEK
  rotation re-wraps values without rewriting payloads (re-wrap the DEK,
  leaving ciphertext untouched) versus a full re-encrypt.
- **Inlined encrypted data.** Whether inlined data (RFC 0005) that DuckLake
  considers "encrypted" needs any distinct handling beyond falling under
  catalog-at-rest, or whether the two notions collapse entirely.

## Alternatives considered

- **moraine inventing its own data-file encryption scheme.** Rejected:
  DuckLake owns data-file crypto, and moraine is row-faithful (RFC 0006) —
  it must not enter the data-file path (RFC 0007) or reinterpret DuckLake's
  encryption metadata. moraine carries key material; it does not originate a
  scheme.
- **Encrypting the whole SlateDB value opaquely, framing header included.**
  Rejected: it destroys the two properties the header exists to provide (RFC
  0002) — a corrupt or wrong-kind value would no longer fail loudly (it would
  be indistinguishable from ciphertext), and encoding-version negotiation
  would be impossible because the reader could not read the version without
  first decrypting. The header must stay plaintext.
- **Encrypting the stats values to plug the min/max leak.** Rejected: DuckLake
  reads those strings for file pruning (RFC 0002, RFC 0009); encrypting them
  breaks pruning. The leak is the cost of usable, uninterpreted stats.
- **Treating data-file encryption as sufficient on its own.** Rejected — the
  spine of this RFC. A plaintext catalog leaks the data keys themselves, plus
  plaintext stats, row counts, names, and schema. Data-file encryption
  without catalog-at-rest protection buys far less than it appears to.
