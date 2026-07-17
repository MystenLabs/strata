# Segment Record Index Design

## Goal

Avoid walking large segment data files when GC, recovery, or future maintenance tasks only need
record identity and layout metadata. A per-segment record index should let GC enumerate exact copy
candidates from compact metadata while leaving mutable liveness in the existing segment GC overlay.

## Non-Goals

- Do not duplicate retired, expired, or lifetime state in the record index.
- Do not make the index authoritative for payload bytes.
- Do not require existing sealed segments to be rewritten before they can be read.

## Proposed Files

Each sealed segment may have a sibling metadata file:

```text
<segment>.data
<segment>.idx
```

The index is append-only while the segment is open and immutable after seal. Segment state should
record the sealed index length and checksum once the index format is trusted enough for recovery and
GC to depend on it.

## Entry Contents

Each entry should contain immutable record facts:

```text
record_offset
encoded_record_len
payload_len
key
shard
payload_lsn
fixed_header_checksum or entry_checksum
```

The key, shard, payload LSN, and source range are enough to build exact GC `MapRef` operations after
the overlay determines that a record is still copy-eligible.

## GC Flow

Current implemented flow:

```text
prepare:
  build aggregate plan from SegmentGcSummary
  claim source segments

copy:
  scan segment data records
  merge with SegmentGcOverlay ranges
  copy selected payloads
  validate copied bytes against route estimates
```

Future indexed flow:

```text
prepare:
  unchanged

copy:
  scan segment.idx entries
  merge with SegmentGcOverlay ranges
  read selected payloads from segment.data
  validate copied bytes against route estimates
```

## Crash Consistency

The data file and index file must share a durable prefix. If data append succeeds but index append is
missing after a crash, recovery can either rebuild missing index entries from the data prefix or
truncate the segment to the last indexed durable offset. The chosen policy should be explicit before
the index becomes mandatory.

On seal, the store should fsync the index file and parent directory before publishing segment state
that references the index checksum. GC should trust an index only when segment state proves the data
and index were sealed together.

## Compatibility

Readers should support three cases:

```text
new sealed segment with valid index: use segment.idx
old sealed segment with no index: fall back to data-file scan
corrupt or mismatched index: return an error, or fall back only under an explicit repair mode
```

## Checksum Policy

The index can validate metadata integrity, but it cannot prove payload bytes are intact unless it
stores or references the record checksum. GC copy should still verify selected records before
publishing `MapRef`s. Skipped records do not need payload checksum verification during GC copy.

## Open Questions

- Should recovery rebuild missing index files eagerly or lazily?
- Should index checksums be per-entry, file-level, or both?
- Should the index include payload checksums to support selective verification without reading key
  trailers from the data file?
- Should index state live in `SegmentState` directly or in a separate versioned accounting manifest?
