# MEP: commit_timestamp for Correct MVCC/TTL/GC on Import and CDC Segments

- **Created:** 2026-03-24
- **Updated:** 2026-05-21
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** DataCoord | QueryNode | DataNode | Storage
- **Related Issues:** [milvus-io/milvus#48471](https://github.com/milvus-io/milvus/issues/48471)
- **Released:** TBD

## Summary

Add a `commit_timestamp` field to `SegmentInfo` (DataCoord) and propagate it through compaction plans, snapshot manifests, and `SegmentLoadInfo` (QueryCoord → QueryNode → C++ segcore). When non-zero, `commit_timestamp` is the effective transaction time for the segment, overriding stale segment-position and row timestamps for temporal decisions: MVCC snapshot visibility, delete filtering, GC eligibility, collection-level TTL compaction triggering, and delete-buffer anchoring.

`commit_timestamp` is a **temporary state**: it exists only between import and the first compaction. Compaction normalizes import segments by rewriting row timestamps to `commit_ts` in output binlogs and clearing `CommitTimestamp` to 0.

## Motivation

When Milvus imports a bulk-load batch (or replays CDC), each row carries a timestamp generated during the import process. These timestamps may be slightly older than the actual commit time. Every time-based check in the system uses `start_position.Timestamp` or `dml_position.Timestamp` (derived from these outdated row timestamps), seeing `T_old` instead of the actual commit time `T_commit`. This produces a family of correctness bugs:

| Bug | Root cause |
|-----|-----------|
| Snapshot over-inclusion | `GenSnapshot` filters by `start_position.Timestamp`; import segment with `T_old=1000` appears at `snapshotTs=3000` before it was logically committed at `T_commit=5000` |
| Premature L0 compaction | L0 target selection uses `start_position.Timestamp`; import segment is eligible for L0 runs before `T_commit` |
| Wrong TTL trigger | Collection-level TTL compaction trigger uses `binlog.TimestampFrom/TimestampTo`; import binlogs with outdated row timestamps look expired immediately after `isImporting` is cleared |
| Wrong MVCC visibility | C++ `mask_with_timestamps` uses raw `row.ts` for MVCC; a query at `mvcc_ts = T_mid` (where `T_old < T_mid < T_commit`) can see import rows that should be invisible until `T_commit` |
| Premature GC | GC eligibility uses `dml_position.Timestamp`; recently committed import segment with outdated `dml_position` may be garbage-collected |
| Wrong channel truncation | `TruncateChannelByTime` drops segments by `dml_position.Timestamp`; import segment dropped prematurely |

## Public Interfaces

### Proto field additions

**`pkg/proto/data_coord.proto` — `SegmentInfo`:**
```protobuf
// commit_timestamp is the transaction timestamp for import/CDC segments.
// When non-zero, it overrides start_position.Timestamp, dml_position.Timestamp,
// and binlog.TimestampFrom/TimestampTo for temporal decisions. Zero = normal
// segment. Cleared to 0 after compaction normalization.
uint64 commit_timestamp = 36;
```

**`pkg/proto/data_coord.proto` — `CompactionSegmentBinlogs`:**
```protobuf
// commit_timestamp mirrors SegmentInfo.commit_timestamp.
// DataNode uses it to protect import/CDC segment rows from premature TTL
// expiry and pre-commit delete application during compaction.
uint64 commit_timestamp = 14;
```

**`pkg/proto/data_coord.proto` — `SegmentDescription`:**
```protobuf
// commit_timestamp mirrors SegmentInfo.commit_timestamp.
// Preserved across snapshot/restore so temporal protections are not lost.
uint64 commit_timestamp = 18;
```

**`pkg/proto/query_coord.proto` — `SegmentLoadInfo`:**
```protobuf
// commit_timestamp mirrors data_coord.SegmentInfo.commit_timestamp.
// QueryNode uses it for: delete-buffer pinning, ListAfter calls, and
// passing to C++ segcore to overwrite the in-memory timestamp column.
uint64 commit_timestamp = 28;
```

**`pkg/proto/segcore.proto` — `SegmentLoadInfo`:**
```protobuf
// commit_timestamp propagated from QueryCoord to C++ segcore.
// Used in LoadFieldData to overwrite the timestamp column for import segments.
uint64 commit_timestamp = 24;
```

### New C API

```c
// Sets commit_ts_ on a segment when the load-info path is not sufficient.
CStatus
SegmentSetCommitTimestamp(CSegmentInterface c_segment, uint64_t commit_ts);
```

## Design Details

### Core Idea

An import batch is an atomic transaction. From the collection's perspective, every row in the batch came into existence at `T_commit` — the moment `isImporting` was cleared. `commit_timestamp` stores this value on the segment and acts as the authoritative time for all temporal decisions when non-zero.

Two helper functions in DataCoord encapsulate the override:

```go
// segmentEffectiveTs returns start_position.Timestamp, or commit_timestamp when non-zero.
func segmentEffectiveTs(seg *datapb.SegmentInfo) uint64 {
    if ts := seg.GetCommitTimestamp(); ts != 0 {
        return ts
    }
    return seg.GetStartPosition().GetTimestamp()
}

// segmentEffectiveDmlTs returns dml_position.Timestamp, or commit_timestamp when non-zero.
func segmentEffectiveDmlTs(seg *datapb.SegmentInfo) uint64 {
    if ts := seg.GetCommitTimestamp(); ts != 0 {
        return ts
    }
    return seg.GetDmlPosition().GetTimestamp()
}
```

An additional helper in `pkg/util/tsoutil` computes effective row timestamps:

```go
// EffectiveTimestamp returns max(rawTs, commitTs) when commitTs is non-zero.
func EffectiveTimestamp(rawTs, commitTs uint64) uint64 {
    if commitTs != 0 && commitTs > rawTs {
        return commitTs
    }
    return rawTs
}
```

### When `commit_timestamp` Is Set

TODO: Will be assigned by a 2PC commit flow in a companion PR. Currently `import_checker.go` has a placeholder.

`UpdateCommitTimestamp(segmentID, ts)` validates the invariant at the metadata boundary: when `ts != 0`, it must be greater than or equal to `max(binlog.TimestampTo)` across the segment's insert binlogs. This prevents segcore from observing an invalid `commit_ts` that would lower row timestamps. `ts == 0` is the reset path used by compaction completion and is always allowed.

### When `commit_timestamp` Is Cleared — Compaction Normalization

`commit_timestamp` is a **temporary state** that only exists between import and the first compaction. During compaction:

1. **All compaction paths** (mix, merge-sort/sort, clustering) rewrite row timestamps in output binlogs to `commit_ts` for rows from import segments.
2. **Completion mutations** in `meta.go` set `CommitTimestamp = 0` on the output segment and update `StartPosition`/`DmlPosition` timestamps via `normalizePositionTimestamp`.
3. After compaction, the segment is a **normal segment** with no special handling needed.

This design minimizes the surface area of `if commitTs != 0` checks — they only apply to segments between import and first compaction.

**Implementation:** `timestamp_overwrite.go` provides `overwriteRecordTimestamps` (wraps Arrow Record) and `wrapReaderWithTimestampOverwrite` (wraps RecordReader) to transparently overwrite timestamps in compaction data paths. The reader wrapper owns the wrapper lifecycle and releases the previously returned wrapper on `Next()` / `Close()`, avoiding Arrow timestamp-array leaks in reader-owned paths such as merge-sort and sort compaction.

### TTL Handling

**Per-row TTL field:** NOT affected by `commit_ts`. TTL field values represent the user's explicit expiration intent and are honored as-is, regardless of whether the segment is imported. No special handling needed.

**Collection-level TTL:** Uses `tsoutil.EffectiveTimestamp(row_ts, commit_ts)` = `max(row_ts, commit_ts)` as the effective row age, preventing premature expiration of import segments with outdated row timestamps.

### Delete and Upsert Handling

A delete or upsert with `ts < commit_ts` does **NOT** take effect on the import segment. At query time, segcore routes timestamp reads through `commit_ts`; during compaction, the entity filter compares `tsoutil.EffectiveTimestamp(pkTs, commitTs)` against the delete timestamp. Both paths preserve the same rule:
- `delete_ts < commit_ts` → `row_ts (= commit_ts) > delete_ts` → row not found → delete skipped ✓
- `delete_ts >= commit_ts` → `row_ts (= commit_ts) <= delete_ts` → row found → delete applied ✓

The delete-apply callback itself stays simple; timestamp readers and compaction filters provide the effective timestamp.

### DataCoord Fix Sites

| File | Site | Old | Fix |
|------|------|-----|-----|
| `handler.go` | `GenSnapshot` filter | `info.GetStartPosition().GetTimestamp()` | `segmentEffectiveTs(info.SegmentInfo)` |
| `compaction_task_l0.go` | L0 target selection | `info.GetStartPosition().GetTimestamp()` | `segmentEffectiveTs(info.SegmentInfo)` |
| `meta.go` | `TruncateChannelByTime` | `segment.GetDmlPosition().GetTimestamp()` | `segmentEffectiveDmlTs(segment.SegmentInfo)` |
| `garbage_collector.go` | GC eligibility | `segment.GetDmlPosition().GetTimestamp()` | `segmentEffectiveDmlTs(segment.SegmentInfo)` |
| `compaction_trigger.go` | TTL trigger | `binlog.TimestampTo / TimestampFrom` | `tsoutil.EffectiveTimestamp(binlogTs, commit_ts)` |
| `meta.go` | Compaction completion | Would otherwise carry stale import state | Set `CommitTimestamp = 0`, normalize positions |
| `snapshot.go` | Snapshot/restore manifest | `CommitTimestamp` field absent from Avro schema | Snapshot format V3 preserves `commit_timestamp` |

### QueryNode Fix Sites

A local helper mirrors the DataCoord pattern:

```go
func segmentEffectiveTs(info *querypb.SegmentLoadInfo) uint64 {
    if ts := info.GetCommitTimestamp(); ts != 0 {
        return ts
    }
    return info.GetStartPosition().GetTimestamp()
}
```

Applied at 4 call sites in `delegator_data.go`:
- `deleteBuffer.Pin(segmentEffectiveTs(info), ...)` — anchor point for streaming deletes
- `deleteBuffer.Unpin(segmentEffectiveTs(info), ...)` — symmetric unpin on segment release
- `deleteBuffer.ListAfter(segmentEffectiveTs(info))` — replay deletes since commit time
- `catchUpTs = segmentEffectiveTs(info)` — snapshot catch-up for empty-snapshot case

### C++ Segcore: Timestamp Override at Load and Read Time

**Rationale:** Import segments loaded through storage v2/v3 may keep a raw timestamp column in `fields_` even when the timestamp index is built from `commit_ts`. Therefore, the implementation must protect both the index and all readers that could otherwise read the raw column.

**Implementation:** In `ChunkedSegmentSealedImpl`, `commit_ts_` is set from `segcorepb.SegmentLoadInfo.commit_timestamp` in `SetLoadInfo` and can also be set through `SegmentSetCommitTimestamp` from Go. Loading paths initialize the timestamp index with `commit_ts_` when non-zero:

```cpp
// Storage v1 path (load_system_field_internal):
if (commit_ts_ != 0) {
    std::fill(timestamps.begin(), timestamps.end(), commit_ts_);
}
init_timestamp_index_owned(std::move(timestamps), num_rows);

// Storage v2 path (load_column_group_data_internal):
if (commit_ts_ != 0) {
    std::vector<Timestamp> ts(num_rows, commit_ts_);
    init_timestamp_index_owned(std::move(ts), num_rows);
} else {
    auto col = get_column(TimestampFieldID);
    init_timestamp_index_from_column(col, num_rows);
}
```

All timestamp consumers that can affect visibility, delete, or retrieval now route through `EffectiveCommitTs()`:

```cpp
std::optional<Timestamp>
EffectiveCommitTs() const {
    return commit_ts_ != 0 ? std::optional<Timestamp>{commit_ts_} : std::nullopt;
}
```

Applied reader paths:
- `search_batch_pks::read_ts` — drives delete / upsert callback matching.
- `bulk_subscript(SystemFieldType::Timestamp)` — returns `commit_ts` for retrieved system timestamp output.
- `mask_with_timestamps` — uses `commit_ts` for the per-row MVCC/TTL scan even when a raw v2/v3 timestamp column is present.

**Effect on query correctness** (with all `row.ts = commit_ts`):

| Check | Condition | Result |
|-------|-----------|--------|
| MVCC visibility | `row.ts > mvcc_ts` | Invisible for `mvcc_ts < commit_ts`, visible for `mvcc_ts ≥ commit_ts` ✓ |
| Collection-level TTL | `EffectiveTimestamp(row.ts, commit_ts)` | Uses `commit_ts` as logical age ✓ |
| Per-row TTL field | `current_time >= ttl_field_value` | Honored as-is — user expiration intent ✓ |
| Delete (ts < commit_ts) | `search_pk(pk, delete_ts)` → row not found | Delete correctly skipped ✓ |
| Delete (ts >= commit_ts) | `search_pk(pk, delete_ts)` → row found | Delete correctly applied ✓ |

**Trade-off:** Original `row.ts` values are no longer visible in the in-memory segment. They remain intact in on-disk binlogs until compaction. After compaction, `commit_ts` is baked as the physical row timestamp and `CommitTimestamp` is cleared — the segment becomes a normal segment.

**`SegmentGrowingImpl` is unaffected:** Growing segments receive rows from live DML where `row.ts ≈ current_timetick`. The `T_old ≪ T_commit` gap cannot occur.

### Propagation Chain

```
DataCoord:SegmentInfo.commit_timestamp
  → PackSegmentLoadInfo (querycoordv2/utils/types.go)
  → querypb.SegmentLoadInfo.commit_timestamp
  → ConvertToSegcoreSegmentLoadInfo (util/segcore/segment.go)
  → segcorepb.SegmentLoadInfo.commit_timestamp
  → NewSegmentWithLoadInfo / SegmentSetCommitTimestamp (C API)
  → ChunkedSegmentSealedImpl::SetLoadInfo / SetCommitTimestamp → commit_ts_
  → LoadFieldData: initialize timestamp index from commit_ts_
  → EffectiveCommitTs(): search_batch_pks, bulk_subscript(Timestamp), mask_with_timestamps
```

### Compaction Normalization Chain

```
Input segments with CommitTimestamp != 0
  → DataNode compactor reads rows
  → timestamp_overwrite.go: overwriteRecordTimestamps / wrapReaderWithTimestampOverwrite
  → EntityFilter uses EffectiveTimestamp(pkTs, commitTs) for delete filtering
  → Row timestamps rewritten to commit_ts in output binlogs
  → TimestampFrom/TimestampTo in output binlogs reflect commit_ts
  → DataCoord completion mutation:
    → CommitTimestamp = 0
    → StartPosition/DmlPosition normalized via normalizePositionTimestamp
  → Output segment is a normal segment
```

`normalizePositionTimestamp` intentionally bumps `Timestamp` without advancing `MsgID`, so the returned `MsgPosition` may violate the usual `Timestamp == TSO(MsgID)` invariant. This is safe here because the normalized position is only used as a fallback start/dml position for compaction-output segments and is consumed by timestamp-only callers such as GC, `TruncateChannelByTime`, and `GetEarliestTs`. Future WAL seek or resume-from-position code must not use this helper when it requires `MsgID` / timestamp consistency.

### Snapshot and Restore Compatibility

Snapshot manifests also preserve `commit_timestamp` so snapshot/restore of an un-compacted import segment does not silently lose MVCC/TTL/GC/delete protection.

The manifest schema is versioned because Avro binary decoding is positional:

| Snapshot format | Manifest schema | Notes |
|-----------------|-----------------|-------|
| V0/V1 | No `index_store_path_version`, no `commit_timestamp` | Legacy schema |
| V2 | Adds `index_store_path_version`, no `commit_timestamp` | Existing V2 manifests must keep decoding with this schema |
| V3 | Adds `commit_timestamp` | Current write schema |

New snapshots write `SnapshotFormatVersion = 3` and use the V3 schema. Reads dispatch by `SnapshotMetadata.format_version`: `0,1 → V1`, `2 → V2`, `3 → V3`, future versions rejected. `ManifestEntry.CommitTimestamp` is stored as Avro `long` / Go `int64` and converted to/from proto `uint64` at the snapshot boundary.

### Sites Confirmed Safe (No Change Needed)

| Site | Reason |
|------|--------|
| `handler.go:196` deleteCheckPoint | L0 segments only; conservative (earlier) checkpoint is safe |
| `meta.go:2219` `GetEarliestStartPositionOfGrowingSegments` | Growing segments only |
| `segment_allocation_policy.go` | Growing-segment sealing logic; import segments already sealed |
| `compaction_task_mix.go`, `compaction_task_clustering.go` | Neither uses `start_position.Timestamp` for selection |
| `delegator_data.go:854` `zap.Time` log field | Diagnostic output only |

## Compatibility, Deprecation, and Migration Plan

- **Backward compatible:** `commit_timestamp` is a new proto field with default value 0. All existing non-import segments have `commit_timestamp = 0`, and all code paths fall back to the original `start_position.Timestamp` / `dml_position.Timestamp` logic. Behavior for normal segments is identical to pre-change.
- **Rolling upgrade safe:** The field is optional. A coordinator running the new code alongside a node running the old code (or vice versa) simply ignores the field — old nodes see `commit_timestamp = 0` and use existing logic unchanged.
- **No migration required:** Existing import segments already completed before this change will have `commit_timestamp = 0`. They benefit from this fix only for new import operations after the upgrade.
- **Snapshot compatibility:** Existing V1/V2 snapshot manifests are decoded with their original schemas. New snapshot manifests use format V3 and preserve `commit_timestamp`.

## Test Plan

**Unit tests (Go):**
- `TestSegmentEffectiveTs_*` / `TestSegmentEffectiveDmlTs_*` — helper function correctness for import and normal segments
- `TestUpdateCommitTimestamp_*` — meta operator sets field correctly
- `TestGenSnapshot_ImportSegment_*` — excluded before commit_ts, included after
- `TestDeleteBuffer_PinsAtCommitTs` — delete buffer anchor uses commit_ts, not start_position.ts
- `Test_compactionTrigger_shouldDoSingleCompaction_CommitTimestamp` — TTL trigger uses commit_ts for import segments
- `TestOverwriteRecordTimestamps_*` / `TestWrapReaderWithTimestampOverwrite_*` — timestamp overwrite utilities
- `TestOverwriteReader_DrainsAndReleases` / `TestOverwriteReader_CloseEarly` — reader wrapper releases Arrow records correctly
- `TestSnapshotManifest_CommitTimestampRoundtripV3` — V3 snapshot manifest preserves `CommitTimestamp`
- `TestSnapshotManifest_LegacyV2NoCommitTimestamp` — legacy V2 manifest still decodes with `CommitTimestamp = 0`

**C++ tests (`test_commit_timestamp.cpp`):**
- `MVCC_RowsInvisibleBeforeCommitTs` — queries at `ts < commit_ts` see 0 rows; queries at `ts ≥ commit_ts` see all rows
- `TTL_RowsNotExpiredWhenCommitTsAboveTtl` — import segment not TTL-expired when `commit_ts > ttl_threshold`; control (no overwrite) correctly expires
- `Delete_PreCommitDeleteNotApplied` — delete at `ts < commit_ts` does NOT apply because row did not exist at delete time
- `NormalSegment_BehaviorUnchanged` — segments with `commit_ts=0` behave identically to pre-change
- V2/V3 fixture tests — raw timestamp column in `fields_` does not bypass `commit_ts` for MVCC, TTL, delete, or timestamp retrieval

**Integration tests (`CommitTimestampSuite`):**
- `TestMVCC_Visibility` — MVCC query before/at/after commit_ts
- `TestMVCC_StrongConsistency_CommitTsInPast` — strong consistency works once commit_ts is in the past
- `TestSearch_WithGuaranteeTs` — search respects commit_ts visibility
- `TestDelete_AfterCommitTs` / `TestDelete_BeforeCommitTs` — delete applies only after commit_ts
- `TestUpsert_AfterCommitTs` / `TestUpsert_BeforeCommitTs` — upsert delete side applies only after commit_ts
- `TestCompaction_NormalizesCommitTs` — compaction output has `CommitTimestamp = 0` and binlog timestamps rewritten
- `TestGC_ImportSegmentNotPrematurelyDropped` — import segment remains queryable after commit_ts protection
- `TestImport_CommitTimestampSetAfterCompletion` — currently skipped until the companion 2PC PR assigns commit_ts
- `TestImport_DataQueryableAfterCommit` — Strong-consistency query returns all imported rows

## Rejected Alternatives

### Approach B: Overlay in `mask_with_timestamps`

Instead of overwriting the timestamp column at load time, apply a per-row `max(row.ts, commit_ts)` in the `mask_with_timestamps` hot path:

```cpp
auto effective_ts = (commit_ts_ != 0) ? std::max(val, commit_ts_) : val;
mask[i] = effective_ts > timestamp;
```

**Rejected because:**
- Adds a branch + conditional max to every row in every query — measurable hot-path overhead.
- Requires changes in both the MVCC lambda and the TTL lambda, increasing the blast radius.
- Still does not fix the DataCoord-side bugs (snapshot, GC, compaction trigger) — those need the `segmentEffectiveTs` helpers regardless.
- The load-time overwrite achieves the same correctness with zero query hot-path overhead and a single implementation point.

### Approach C: Dual-timestamp segment (store both `row.ts` and `commit_ts`)

Keep original `row.ts` intact; thread `commit_ts` as a separate per-segment value through all query logic.

**Rejected because:**
- Every temporal decision site needs to be aware of two timestamps and pick the right one — far larger diff, higher risk of missed sites.
- No query semantics require access to the original `row.ts` after commit. The in-memory value is transient.
- Original `row.ts` is preserved on disk in binlogs until compaction.

## References

- Implementation PR: [milvus-io/milvus#48472](https://github.com/milvus-io/milvus/pull/48472)
- Tracking issue: [milvus-io/milvus#48471](https://github.com/milvus-io/milvus/issues/48471)
