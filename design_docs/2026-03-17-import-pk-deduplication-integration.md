# Primary Key Deduplication Integration: Import-in-Replication and PK Index

**Date:** 2026-03-17
**Status:** Design Review
**Author:** Collaborative Design Session

## Executive Summary

This document addresses the critical integration challenges between two Milvus features under development:

1. **Import in Replication** - Enables import operations in clusters with active CDC replication, using manual two-phase commit with `WaitingCommit` state
2. **Primary Key Index** - Provides O(1) PK lookup using BBHash with automatic deduplication on insert via StreamingNode

**The Problem:** When both features are enabled, there are multiple deduplication gaps and race conditions across the import lifecycle that can result in duplicate primary keys violating data integrity.

**The Solution:** Implement timestamp-ordered, comprehensive conflict detection that queries all segment sources (sealed, growing, and importing) with deterministic ordering semantics to guarantee correctness without perfect synchronization.

**Impact:** Ensures correct deduplication behavior across:
- Import vs. existing sealed/growing segments
- Import vs. concurrent insert operations at any point in import lifecycle
- Import vs. other concurrent import jobs
- Primary and secondary clusters in replication scenarios

## 1. Background

### 1.1 Import in Replication Feature

**Reference:** `2026-03-13-import-in-replication-design.md`

**Key aspects relevant to this document:**

**Import State Machine:**
```
Pending → Importing → IndexBuilding → WaitingCommit → Completed/Failed
```

**Import Data Timestamps:**
- Import data written with `rowTs = T_import` (from ImportMessage timestamp)
- Import segments have `visible_timestamp = T_commit` (future timestamp)
- Data not visible until CommitImportMessage received
- Visible timestamp approach: `effectiveTs = max(rowTs, visible_timestamp)` for filtering

**Two-Phase Commit Flow:**
```
T=1000  ImportMessage broadcast → job starts (Importing state)
        DataNode reads files, writes segments
        Segments: rowTs=1000, visible_timestamp=T_commit (future)

T=1000-2000  Import executing (Importing → IndexBuilding → WaitingCommit)

T=2000  Manual CommitImport RPC → CommitImportMessage broadcast
        visible_timestamp becomes effective
        Import data becomes visible
```

**Critical issue:** Import data written at T_import but logically belongs at T_commit, creating a timestamp inversion window where DML operations don't interact correctly.

### 1.2 PK Index Feature

**Reference:** `2026-03-16-primary-key-index-design.md`

**Key aspects relevant to this document:**

**Architecture:**
- DataNode builds BBHash index for sealed segments (shard-level)
- StreamingNode loads and serves PK queries (O(1) lookup)
- BBHash: 3-4 bits/key, perfect hash, static structure

**Automatic Deduplication (Section 3.4):**
```go
func (s *StreamingNode) ProcessInsertMsg(msg *InsertMsg) error {
    if !s.config.AutoDedupOnInsert {
        return s.stream.Append(msg)
    }

    for _, pk := range msg.PrimaryKeys {
        // Query shard-level PK index
        segs := s.pkIndex.Query(pk, msg.ShardID)

        if len(segs) > 0 {
            // PK exists in sealed segments - inject delete
            deleteMsg := &DeleteMsg{
                PrimaryKeys:  []PrimaryKey{pk},
                Timestamps:   []uint64{msg.Timestamp},  // Same TS as insert
                SegmentIDs:   segs,
            }
            s.stream.Append(deleteMsg)
        }
    }

    return s.stream.Append(msg)  // Forward insert
}
```

**Key properties:**
- Sequential processing per shard (no race conditions)
- Deletes injected with same timestamp as insert (upsert semantics)
- Only checks sealed segments (growing segments use bloom filter + query-time dedup)

**PK Index Coverage:**
- **Sealed segments:** BBHash index (rebuilt hourly or event-triggered)
- **Growing segments:** Current bloom filter system
- **Importing segments:** NOT covered in original design ⚠️

### 1.3 Integration Challenge

When both features are enabled:

1. **Import creates segments with future visibility** - Data exists but hidden
2. **PK index auto-dedup checks for conflicts** - But import segments not in index
3. **Result:** Concurrent inserts bypass deduplication, creating duplicates when import commits

The problem spans the **entire import lifecycle** (Pending → Importing → IndexBuilding → WaitingCommit), not just the WaitingCommit state.

## 2. Problem Statement

### 2.1 Core Deduplication Gaps

When import and insert operations involve the same primary keys, there are four critical time windows where deduplication can fail:

| Window | Import State | Duration | Dedup Gap |
|--------|-------------|----------|-----------|
| **Window 1** | Importing | 10s - 5min | Import segments not in PK index, inserts bypass dedup |
| **Window 2** | IndexBuilding | 10s - 2min | Same as Window 1 |
| **Window 3** | WaitingCommit | User-controlled | Same as Window 1 |
| **Window 4** | Before Import Start | N/A | Existing data conflicts with import PKs |

**Total exposure:** From ImportMessage broadcast until CommitImportMessage, plus historical data conflicts.

### 2.2 Failure Modes

**Failure Mode 1: Duplicate Creation**
- Import and insert both create segments with same PK
- Both segments become visible after commit
- Queries return duplicate results (violates PK uniqueness)

**Failure Mode 2: Delete Doesn't Apply**
- Import data has `visible_timestamp = T_commit` (future)
- User deletes PK at `T_delete` where `T_import < T_delete < T_commit`
- After commit: `effectiveTs = T_commit > T_delete`, delete doesn't apply
- Deleted data reappears (violates delete semantics)

**Failure Mode 3: UPSERT Creates Duplicates**
- Import segment in WaitingCommit (not visible)
- User upserts same PK
- UPSERT doesn't see hidden import data, doesn't inject delete for it
- Both versions survive after commit

**Failure Mode 4: Cross-Cluster Inconsistency**
- Primary and secondary both execute import
- Both experience same race conditions independently
- But timing differences may lead to different results
- Clusters diverge in PK uniqueness state

## 3. Detailed Failure Scenarios

### 3.1 Scenario: INSERT During Importing State (Window 1)

**Timeline:**

```
T=1000  ImportMessage broadcast → Import job starts (Pending → Importing)
        DataNode begins reading files, extracting data
        Import segments NOT created yet
        PK index has NO knowledge of import PKs

T=1200  ⚠️ User issues INSERT(PK=123)
        StreamingNode.ProcessInsertMsg():
          → Queries PK index for PK=123
          → PK index returns: [] (empty, no conflict detected)
          → Auto-dedup DOES NOT FIRE
          → INSERT forwarded to DataNode
          → Creates segment S_insert with PK=123, rowTs=1200

T=1500  Import continues writing data
        → Creates segment S_import with PK=123, rowTs=1000
        → Sets visible_timestamp=3000 (T_commit, future)
        → S_import still not in PK index (not sealed/indexed yet)

T=1800  Import completes data writing (Importing → IndexBuilding)
        Index building starts for import segments

T=2000  Index building completes (IndexBuilding → WaitingCommit)
        Import segments now fully built but still not visible

T=3000  User calls CommitImport RPC
        → CommitImportMessage broadcast
        → visible_timestamp=3000 set on S_import

Query PK=123 at T=3001:
  - S_insert: rowTs=1200, no deleteTs → VISIBLE ✅
  - S_import: rowTs=1000, visible_timestamp=3000, effectiveTs=3000 → VISIBLE ✅

❌ DUPLICATE! Both segments contain PK=123 and are visible
```

**Root cause:** PK index doesn't know about import PKs during Importing state (10s - 5min window).

### 3.2 Scenario: INSERT During WaitingCommit State (Window 3)

**Timeline:**

```
T=1000  Import enters WaitingCommit state
        Segments: S_import with PK=123, rowTs=1000, visible_timestamp=3000 (future)
        PK index still doesn't contain S_import (not indexed yet)

T=2000  ⚠️ User issues INSERT(PK=123)
        StreamingNode.ProcessInsertMsg():
          → Queries PK index for PK=123
          → PK index returns: [] (S_import not in index)
          → Auto-dedup DOES NOT FIRE
          → INSERT proceeds
          → Creates S_insert with PK=123, rowTs=2000

T=3000  User calls CommitImport RPC
        → CommitImportMessage broadcast
        → visible_timestamp=3000 set on S_import

Query PK=123 at T=3001:
  - S_insert: rowTs=2000, no deleteTs → VISIBLE ✅
  - S_import: rowTs=1000, visible_timestamp=3000, effectiveTs=3000 → VISIBLE ✅

❌ DUPLICATE! Both segments visible after commit
```

**Root cause:** Import segments in WaitingCommit not reflected in PK index until commit completes.

### 3.3 Scenario: PK Exists Before Import Starts (Window 4)

**Timeline:**

```
T=500   User INSERT(PK=123)
        → Creates segment S_old with PK=123, rowTs=500

T=800   Segment S_old sealed (Growing → Sealed)
        PK index rebuilt, now contains PK=123 → S_old

T=1000  Import starts with file containing PK=123
        Import Phase 1: Extract PKs from file → PK=123
        Import Phase 2: Query PK index for conflicts
          → Finds PK=123 in S_old (sealed segment)
          → Injects DeleteMsg(PK=123, ts=1000, targetSeg=S_old)
        Import Phase 3: Write import data
          → Creates S_import with PK=123, rowTs=1000, visible_timestamp=3000

T=3000  CommitImportMessage
        → visible_timestamp=3000 set on S_import

Query PK=123 at T=3001:
  - S_old: rowTs=500, deleteTs=1000 → DELETED (1000 > 500) ✅
  - S_import: rowTs=1000, visible_timestamp=3000, effectiveTs=3000 → VISIBLE ✅

✅ CORRECT: Only S_import visible, S_old deleted
```

**This case works if:**
1. PK index is up-to-date (contains S_old)
2. S_old is sealed (in BBHash)
3. Import preprocessing happens

**But fails if:**
- S_old is still growing (bloom filter check is probabilistic, may miss)
- PK index is stale (S_old sealed recently but not in BBHash yet)

### 3.4 Scenario: Race Between PK Registration and INSERT

**Timeline:**

```
T=1000  Import starts
        Phase 1: Extract PKs from file → PK=123
        Phase 2: Register importing PKs in PK index
          → importingPKs[jobID][PK=123] = [] (no segmentID yet)

T=1001  ⚠️ User INSERT(PK=123) arrives
        StreamingNode.ProcessInsertMsg():
          → Queries PK index for PK=123
          → Finds importingPKs[jobID][PK=123] (state=Importing, segmentID=0)
          → Auto-dedup FIRES
          → But no segment ID yet! Where to inject delete?
          → Option A: Store pending delete, apply later
          → Option B: Block insert (bad UX)
          → Option C: Skip dedup (creates duplicate)

T=1002  Import continues
        Phase 3: Query for conflicts with existing data
          → Queries sealed segments for PK=123
          → Queries growing segments for PK=123

        ❓ Does it find S_insert created at T=1001?

        Sub-case A: S_insert still growing
          → Bloom filter check (probabilistic)
          → May MISS S_insert
          → No DeleteMsg injected for S_insert

        Sub-case B: S_insert already sealed
          → BBHash query
          → But index might be stale (last rebuilt at T=800)
          → May MISS S_insert
          → No DeleteMsg injected for S_insert

T=1500  Import writes data
        → Creates S_import with PK=123, rowTs=1000, visible_timestamp=3000

T=3000  CommitImportMessage
        → visible_timestamp=3000 set on S_import

Query PK=123 at T=3001:
  - S_insert: rowTs=1001, no deleteTs → VISIBLE ✅
  - S_import: rowTs=1000, visible_timestamp=3000, effectiveTs=3000 → VISIBLE ✅

❌ DUPLICATE! Import preprocessing missed S_insert
```

**Root causes:**
1. **Segmentation not atomic** - PK registered before segment created
2. **Growing segment detection gap** - Bloom filter is probabilistic
3. **PK index staleness** - Sealed segments not immediately in BBHash

### 3.5 Scenario: PK Index Staleness

**Timeline:**

```
T=800   PK index last rebuilt
        Contains all sealed segments up to T=800

T=900   User INSERT(PK=123)
        → Creates segment S1 with PK=123, rowTs=900

T=950   Segment S1 sealed (Growing → Sealed)
        S1 now immutable, but not in PK index yet
        (Next rebuild scheduled for T=1600, 1-hour interval)

T=1000  Import starts with PK=123
        Import Phase: Query PK index for conflicts
          → Queries BBHash for PK=123
          → PK index doesn't contain S1 (index stale!)
          → Returns: [] (no conflict detected)
          → No DeleteMsg injected for S1

T=1500  Import writes data
        → Creates S_import with PK=123, rowTs=1000, visible_timestamp=3000

T=1600  PK index rebuilt
        Now includes S1 (finally)

T=3000  CommitImportMessage
        → visible_timestamp=3000 set on S_import

Query PK=123 at T=3001:
  - S1: rowTs=900, no deleteTs → VISIBLE ✅
  - S_import: rowTs=1000, visible_timestamp=3000, effectiveTs=3000 → VISIBLE ✅

❌ DUPLICATE! Import preprocessing missed S1 due to index staleness
```

**Root cause:** PK index rebuild interval (default 1 hour) creates staleness window where recently sealed segments not reflected in BBHash.

### 3.6 Scenario: Concurrent Import Jobs

**Timeline:**

```
T=1000  Import Job A starts with file containing PK=123
        → Registers importingPKs[jobA][PK=123]
        → Queries for conflicts: [] (no existing data)
        → Begins writing S_import_A

T=1001  Import Job B starts with file containing PK=123
        → Registers importingPKs[jobB][PK=123]
        → Queries for conflicts
          → Finds importingPKs[jobA][PK=123]

        ❓ What should Job B do?

        Option 1: Job B treats Job A as conflict, injects delete for S_import_A
        Option 2: Job A treats Job B as conflict, injects delete for S_import_B
        Option 3: Both treat each other as conflict, mutual deletion
        Option 4: Deterministic tiebreaker (timestamp or jobID)

Without proper handling:
  → Both jobs write data
  → Both jobs commit
  → Both segments visible

T=3000  Both jobs commit

Query PK=123:
  - S_import_A: rowTs=1000, visible_timestamp=3000 → VISIBLE ✅
  - S_import_B: rowTs=1001, visible_timestamp=3000 → VISIBLE ✅

❌ DUPLICATE! Two import jobs created same PK
```

**Root cause:** No coordination between concurrent import jobs for overlapping PKs.

### 3.7 Scenario: DELETE During WaitingCommit (Timestamp Inversion)

**Timeline:**

```
T=1000  Import writes data
        → S_import with PK=123, rowTs=1000, visible_timestamp=3000 (future)
        → Import enters WaitingCommit

T=2000  User issues DELETE(PK=123)
        → Proxy queries for PK=123 segments
        → Finds S_import (exists, but not visible)
        → Forwards DELETE to DataNode
        → Delete buffer records: [PK=123, deleteTs=2000]

T=3000  CommitImportMessage
        → visible_timestamp=3000 set on S_import

Query PK=123 at T=3001:
  Apply delete filter:
    - S_import: rowTs=1000, visible_timestamp=3000
    - effectiveTs = max(1000, 3000) = 3000
    - deleteTs = 2000
    - Compare: effectiveTs=3000 > deleteTs=2000
    - Delete DOES NOT APPLY (timestamp semantics)

❌ WRONG! Deleted data reappears after import commits
```

**Root cause:** visible_timestamp changes effectiveTs for filtering, making import data "immune" to deletes that happened between T_import and T_commit.

**Note:** This is a subset of "Issue 1: Row Reappears After DELETE" from the import-in-replication design document. Included here for completeness.

## 4. Proposed Solutions

### 4.1 Solution A: Timestamp-Ordered Comprehensive Conflict Detection ⭐ RECOMMENDED

**Core Idea:** Use import timestamp as ordering barrier and query ALL segment sources (sealed, growing, importing) with deterministic conflict resolution.

#### 4.1.1 Design Overview

**Key principles:**

1. **Timestamp barrier** - Import timestamp T_import from ImportMessage establishes ordering:
   - Any data with `rowTs <= T_import` is "before" import → import should replace it
   - Any data with `rowTs > T_import` is "after" import → newer data wins

2. **Comprehensive detection** - Query all sources, not just BBHash:
   - Sealed segments (via BBHash index)
   - Growing segments (direct query, bypass index)
   - Other importing jobs (via importingPKs map)

3. **Pre-registration** - Register importing PKs BEFORE writing data:
   - Extract PKs from files first
   - Register in PK index immediately
   - Write data second

4. **Deterministic concurrency** - Multiple imports with same PK:
   - Earlier timestamp wins
   - Tie-breaker: smaller jobID wins

#### 4.1.2 Implementation: Import Execution Flow

```go
func (task *importTask) Execute() error {
    // ============ PHASE 0: Establish timestamp barrier ============
    // Import timestamp from ImportMessage (broadcast time)
    importTimestamp := task.req.GetTs()  // e.g., T=1000

    log.Info("import execution started",
        zap.Int64("jobID", task.jobID),
        zap.Uint64("importTimestamp", importTimestamp),
        zap.Uint64("commitTimestamp", task.commitTimestamp))

    // ============ PHASE 1: Extract PKs from files ============
    log.Info("phase 1: extracting primary keys from files")
    importPKs := extractPrimaryKeysFromFiles(task.files)

    log.Info("extracted PKs", zap.Int("count", len(importPKs)))

    // ============ PHASE 2: Pre-register importing PKs ============
    // Register BEFORE writing any data
    // This makes PKs "visible" to auto-dedup immediately
    log.Info("phase 2: registering importing PKs in index")

    err := task.pkIndexService.RegisterImportingPKs(ctx, &RegisterImportingPKsRequest{
        JobID:            task.jobID,
        ShardID:          task.shardID,
        CollectionID:     task.collectionID,
        PKs:              importPKs,
        ImportTimestamp:  importTimestamp,
        CommitTimestamp:  task.commitTimestamp,  // visible_timestamp (future)
    })
    if err != nil {
        return errors.Wrap(err, "failed to register importing PKs")
    }

    log.Info("registered importing PKs", zap.Int("count", len(importPKs)))

    // ============ PHASE 3: Comprehensive conflict detection ============
    // Detect conflicts with ALL sources (sealed, growing, other imports)
    log.Info("phase 3: detecting conflicts with existing data")

    conflicts, err := task.detectConflicts(importPKs, importTimestamp)
    if err != nil {
        return errors.Wrap(err, "failed to detect conflicts")
    }

    log.Info("detected conflicts",
        zap.Int("conflictCount", len(conflicts.PKs)),
        zap.Int("affectedSegments", len(conflicts.SegmentIDs)))

    // Inject DeleteMsg for ALL conflicts at T_import
    // These deletes will be applied when segments are queried
    if len(conflicts.PKs) > 0 {
        deleteMsg := &DeleteMsg{
            CollectionID: task.collectionID,
            PartitionIDs: task.partitionIDs,
            PrimaryKeys:  conflicts.PKs,
            Timestamps:   makeTimestamps(len(conflicts.PKs), importTimestamp),
            SegmentIDs:   conflicts.SegmentIDs,  // Hint for efficient routing
        }

        err = task.stream.Append(deleteMsg)
        if err != nil {
            return errors.Wrap(err, "failed to inject delete message")
        }

        log.Info("injected delete message for conflicts")
    }

    // ============ PHASE 4: Write import data ============
    log.Info("phase 4: writing import segments")

    segments, err := task.writeImportSegments(
        importPKs,
        rowTimestamp:     importTimestamp,      // All rows get T_import
        visibleTimestamp: task.commitTimestamp, // Future T_commit
    )
    if err != nil {
        return errors.Wrap(err, "failed to write import segments")
    }

    log.Info("wrote import segments", zap.Int("segmentCount", len(segments)))

    // ============ PHASE 5: Update importing PKs with segment IDs ============
    // After segments flushed, update PK index with actual segment IDs
    // This allows subsequent operations to target specific segments
    log.Info("phase 5: updating importing PKs with segment IDs")

    pkToSegmentMap := task.buildPKSegmentMapping(segments)

    err = task.pkIndexService.UpdateImportingPKsWithSegments(ctx, &UpdateImportingPKsRequest{
        JobID:            task.jobID,
        PKSegmentMapping: pkToSegmentMap,
    })
    if err != nil {
        return errors.Wrap(err, "failed to update importing PKs with segments")
    }

    log.Info("updated importing PKs with segment IDs")

    return nil
}
```

#### 4.1.3 Implementation: Comprehensive Conflict Detection

```go
// ConflictResult contains PKs and segments that conflict with import
type ConflictResult struct {
    PKs        []PrimaryKey
    SegmentIDs []int64
    conflicts  map[PrimaryKey][]int64  // PK → list of conflicting segment IDs
}

func (r *ConflictResult) Add(pk PrimaryKey, segmentID int64) {
    if r.conflicts == nil {
        r.conflicts = make(map[PrimaryKey][]int64)
    }
    r.conflicts[pk] = append(r.conflicts[pk], segmentID)
}

func (r *ConflictResult) Finalize() {
    // Flatten map to lists
    for pk, segIDs := range r.conflicts {
        r.PKs = append(r.PKs, pk)
        r.SegmentIDs = append(r.SegmentIDs, segIDs...)
    }
}

// Detect conflicts with ALL sources (sealed, growing, other imports)
func (task *importTask) detectConflicts(
    importPKs []PrimaryKey,
    importTs uint64,
) (*ConflictResult, error) {
    conflicts := &ConflictResult{}

    for _, pk := range importPKs {
        // Query ALL sources (sealed + growing + importing)
        result, err := task.pkIndex.QueryAllSources(pk, task.shardID)
        if err != nil {
            return nil, errors.Wrap(err, "failed to query PK index")
        }

        for _, seg := range result.Segments {
            shouldDelete := task.shouldDeleteSegment(seg, importTs)
            if shouldDelete {
                conflicts.Add(pk, seg.SegmentID)
                log.Debug("conflict detected",
                    zap.Any("pk", pk),
                    zap.Int64("segmentID", seg.SegmentID),
                    zap.String("segmentState", seg.State.String()),
                    zap.Uint64("importTs", importTs))
            }
        }
    }

    conflicts.Finalize()
    return conflicts, nil
}

// Determine if segment should be deleted based on timestamp ordering
func (task *importTask) shouldDeleteSegment(
    seg *SegmentWithVisibility,
    importTs uint64,
) bool {
    switch seg.State {
    case SegmentStateSealed:
        // Sealed segment: check if PK existed at or before T_import
        // Use segment max timestamp as proxy for PK existence time
        if seg.MaxTimestamp <= importTs {
            // Segment sealed before import → conflict
            return true
        }

        // Segment sealed after import → newer data, don't delete
        return false

    case SegmentStateGrowing:
        // Growing segment: query directly for PK timestamp
        // Bypass PK index, go straight to segment
        pkTimestamp, err := task.queryGrowingSegmentForPK(seg.SegmentID, pk)
        if err != nil || pkTimestamp == 0 {
            // PK not found or error → no conflict
            return false
        }

        if pkTimestamp <= importTs {
            // PK inserted before import → conflict
            return true
        }

        // PK inserted after import → newer data, don't delete
        return false

    case SegmentStateImporting:
        // Another concurrent import job
        // Use deterministic ordering to avoid mutual deletion

        if seg.ImportTimestamp < importTs {
            // Earlier import → it wins, skip this PK in current import
            log.Info("skipping PK, earlier import job owns it",
                zap.Any("pk", pk),
                zap.Int64("earlierJobID", seg.JobID),
                zap.Uint64("earlierTs", seg.ImportTimestamp),
                zap.Int64("currentJobID", task.jobID),
                zap.Uint64("currentTs", importTs))

            // Mark this PK to skip in current import
            task.skipPKs[pk] = true
            return false
        }

        if seg.ImportTimestamp == importTs {
            // Same timestamp → use jobID as tiebreaker
            if seg.JobID < task.jobID {
                // Other import has smaller jobID → it wins
                log.Info("skipping PK, concurrent import with smaller jobID wins",
                    zap.Any("pk", pk),
                    zap.Int64("otherJobID", seg.JobID),
                    zap.Int64("currentJobID", task.jobID))

                task.skipPKs[pk] = true
                return false
            }
        }

        // Current import wins → delete other import's version
        log.Info("conflict with concurrent import, current job wins",
            zap.Any("pk", pk),
            zap.Int64("otherJobID", seg.JobID),
            zap.Uint64("otherTs", seg.ImportTimestamp),
            zap.Int64("currentJobID", task.jobID),
            zap.Uint64("currentTs", importTs))

        return true

    default:
        log.Warn("unknown segment state", zap.String("state", seg.State.String()))
        return false
    }
}

// Query growing segment directly for PK timestamp
func (task *importTask) queryGrowingSegmentForPK(
    segmentID int64,
    pk PrimaryKey,
) (uint64, error) {
    // Route to appropriate DataNode/StreamingNode
    seg := task.growingSegmentManager.GetSegment(segmentID)
    if seg == nil {
        return 0, errors.Errorf("segment %d not found", segmentID)
    }

    // Scan growing segment buffer for PK
    for _, row := range seg.buffer {
        if row.PK == pk {
            return row.Timestamp, nil
        }
    }

    return 0, nil  // PK not found
}
```

#### 4.1.4 Implementation: PK Index Service Extensions

```go
// PK Index Service with importing PKs support
type PKIndexService struct {
    sealedIndex   *BBHashIndex                           // Sealed segments (BBHash)
    growingIndex  map[PK][]SegmentID                     // Growing segments (existing BF)
    importingPKs  map[JobID]*ImportingPKIndex            // Importing PKs by job
    mu            sync.RWMutex
}

// ImportingPKIndex tracks PKs for a single import job
type ImportingPKIndex struct {
    JobID            int64
    ShardID          string
    CollectionID     int64
    ImportTimestamp  uint64
    CommitTimestamp  uint64
    PKToSegments     map[PrimaryKey][]int64  // PK → segment IDs (may be empty initially)
    RegisteredAt     time.Time
}

// RegisterImportingPKs adds PKs to importing index BEFORE segments exist
func (s *PKIndexService) RegisterImportingPKs(
    ctx context.Context,
    req *RegisterImportingPKsRequest,
) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.importingPKs == nil {
        s.importingPKs = make(map[JobID]*ImportingPKIndex)
    }

    // Create importing PK index for this job
    importIdx := &ImportingPKIndex{
        JobID:            req.JobID,
        ShardID:          req.ShardID,
        CollectionID:     req.CollectionID,
        ImportTimestamp:  req.ImportTimestamp,
        CommitTimestamp:  req.CommitTimestamp,
        PKToSegments:     make(map[PrimaryKey][]int64),
        RegisteredAt:     time.Now(),
    }

    // Register all PKs (segment IDs will be filled later)
    for _, pk := range req.PKs {
        importIdx.PKToSegments[pk] = []int64{}  // Empty slice, no segments yet
    }

    s.importingPKs[req.JobID] = importIdx

    log.Info("registered importing PKs",
        zap.Int64("jobID", req.JobID),
        zap.Int("pkCount", len(req.PKs)),
        zap.Uint64("importTs", req.ImportTimestamp),
        zap.Uint64("commitTs", req.CommitTimestamp))

    return nil
}

// UpdateImportingPKsWithSegments fills in segment IDs after segments created
func (s *PKIndexService) UpdateImportingPKsWithSegments(
    ctx context.Context,
    req *UpdateImportingPKsRequest,
) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    importIdx, ok := s.importingPKs[req.JobID]
    if !ok {
        return errors.Errorf("importing job %d not found in PK index", req.JobID)
    }

    // Update segment IDs for each PK
    for pk, segmentIDs := range req.PKSegmentMapping {
        importIdx.PKToSegments[pk] = segmentIDs
    }

    log.Info("updated importing PKs with segments",
        zap.Int64("jobID", req.JobID),
        zap.Int("pkCount", len(req.PKSegmentMapping)))

    return nil
}

// QueryAllSources queries sealed, growing, and importing segments
func (s *PKIndexService) QueryAllSources(
    pk PrimaryKey,
    shardID ShardID,
) (*PKLookupResult, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    result := &PKLookupResult{Segments: []SegmentWithVisibility{}}

    // 1. Query sealed segments (BBHash)
    if sealedSegs := s.sealedIndex.Query(pk); len(sealedSegs) > 0 {
        for _, segID := range sealedSegs {
            segInfo := s.getSegmentInfo(segID)
            result.Segments = append(result.Segments, SegmentWithVisibility{
                SegmentID:    segID,
                State:        SegmentStateSealed,
                MaxTimestamp: segInfo.MaxTimestamp,
            })
        }
    }

    // 2. Query growing segments (existing bloom filter fallback)
    if growingSegs, ok := s.growingIndex[pk]; ok {
        for _, segID := range growingSegs {
            result.Segments = append(result.Segments, SegmentWithVisibility{
                SegmentID: segID,
                State:     SegmentStateGrowing,
            })
        }
    }

    // 3. Query importing PKs (all active import jobs)
    for jobID, importIdx := range s.importingPKs {
        if segIDs, ok := importIdx.PKToSegments[pk]; ok {
            result.Segments = append(result.Segments, SegmentWithVisibility{
                SegmentID:        getFirstOrZero(segIDs),  // May be 0 if not assigned yet
                State:            SegmentStateImporting,
                JobID:            jobID,
                ImportTimestamp:  importIdx.ImportTimestamp,
                VisibleTimestamp: importIdx.CommitTimestamp,
            })
        }
    }

    return result, nil
}

// PromoteImportingToSealed moves importing PKs to sealed index on commit
func (s *PKIndexService) PromoteImportingToSealed(
    jobID int64,
    commitTs uint64,
) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    importIdx, ok := s.importingPKs[jobID]
    if !ok {
        return errors.Errorf("importing job %d not found", jobID)
    }

    // Move PKs to sealed index
    // This will be done during next BBHash rebuild
    // For now, just mark as committed
    importIdx.CommitTimestamp = commitTs

    // Clean up from importingPKs map
    // (Keep for a grace period for debugging, then GC)
    go func() {
        time.Sleep(10 * time.Minute)
        s.mu.Lock()
        delete(s.importingPKs, jobID)
        s.mu.Unlock()
        log.Info("cleaned up importing PK index", zap.Int64("jobID", jobID))
    }()

    log.Info("promoted importing PKs to sealed",
        zap.Int64("jobID", jobID),
        zap.Uint64("commitTs", commitTs))

    return nil
}

// RemoveImportingJob cleans up on import abort
func (s *PKIndexService) RemoveImportingJob(jobID int64) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    delete(s.importingPKs, jobID)

    log.Info("removed importing job from PK index", zap.Int64("jobID", jobID))

    return nil
}
```

#### 4.1.5 Implementation: StreamingNode Auto-Dedup (Unchanged)

The StreamingNode auto-dedup logic **does not need to change**. It already queries the PK index, which now includes importing PKs:

```go
func (s *StreamingNode) ProcessInsertMsg(msg *InsertMsg) error {
    if !s.config.AutoDedupOnInsert {
        return s.stream.Append(msg)
    }

    for _, pk := range msg.PrimaryKeys {
        // Query now returns importing PKs automatically!
        result := s.pkIndex.QueryAllSources(pk, msg.ShardID)

        for _, seg := range result.Segments {
            if seg.State == SegmentStateImporting {
                log.Info("dedup against importing segment",
                    zap.Int64("jobID", seg.JobID),
                    zap.Any("pk", pk),
                    zap.Uint64("importTs", seg.ImportTimestamp))

                // If segment not assigned yet (Phase 2), can't route delete
                // Store pending delete, apply when segment created
                if seg.SegmentID == 0 {
                    s.pendingDeleteManager.Add(seg.JobID, pk, msg.Timestamp)
                    continue
                }
            }

            // Normal delete injection for segments with IDs
            deleteMsg := &DeleteMsg{
                CollectionID: msg.CollectionID,
                PrimaryKeys:  []PrimaryKey{pk},
                Timestamps:   []uint64{msg.Timestamp},
                SegmentIDs:   []int64{seg.SegmentID},
            }
            s.stream.Append(deleteMsg)
        }
    }

    return s.stream.Append(msg)
}
```

#### 4.1.6 Implementation: Pending Delete Manager

For handling inserts when segment IDs not assigned yet:

```go
type PendingDeleteManager struct {
    pending map[JobID]map[PrimaryKey][]uint64  // jobID → PK → delete timestamps
    mu      sync.RWMutex
}

func (m *PendingDeleteManager) Add(jobID int64, pk PrimaryKey, deleteTs uint64) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if m.pending == nil {
        m.pending = make(map[JobID]map[PrimaryKey][]uint64)
    }
    if m.pending[jobID] == nil {
        m.pending[jobID] = make(map[PrimaryKey][]uint64)
    }

    m.pending[jobID][pk] = append(m.pending[jobID][pk], deleteTs)
}

func (m *PendingDeleteManager) FlushForJob(
    jobID int64,
    pkToSegmentMap map[PrimaryKey][]int64,
    stream MessageStream,
) error {
    m.mu.Lock()
    pending := m.pending[jobID]
    delete(m.pending, jobID)
    m.mu.Unlock()

    if len(pending) == 0 {
        return nil
    }

    // Now we have segment IDs, inject deletes
    for pk, deleteTimestamps := range pending {
        segIDs := pkToSegmentMap[pk]
        if len(segIDs) == 0 {
            log.Warn("no segment for pending delete", zap.Any("pk", pk))
            continue
        }

        for _, deleteTs := range deleteTimestamps {
            deleteMsg := &DeleteMsg{
                PrimaryKeys:  []PrimaryKey{pk},
                Timestamps:   []uint64{deleteTs},
                SegmentIDs:   segIDs,
            }
            stream.Append(deleteMsg)
        }
    }

    log.Info("flushed pending deletes",
        zap.Int64("jobID", jobID),
        zap.Int("pkCount", len(pending)))

    return nil
}
```

Import task calls `FlushForJob` after Phase 5 (segment IDs assigned):

```go
// After updating importing PKs with segment IDs
err = task.pendingDeleteManager.FlushForJob(
    task.jobID,
    pkToSegmentMap,
    task.stream,
)
```

#### 4.1.7 Complete Timeline Example

**Scenario: INSERT before, during, and after import**

```
T=500   User INSERT(PK=123)
        → Creates S_old, rowTs=500

T=800   S_old sealed

T=1000  ImportMessage (ts=1000) → Import starts
        Phase 1: Extract PKs → finds PK=123
        Phase 2: Register importing PKs
                 → importingPKs[jobA][PK=123] = []
        Phase 3: Detect conflicts
                 → Query sealed: finds S_old
                 → Check: rowTs=500 <= importTs=1000? YES → conflict
                 → Inject DeleteMsg(PK=123, ts=1000, target=S_old)

T=1200  User INSERT(PK=123)
        StreamingNode queries PK index
        → Finds importingPKs[jobA][PK=123] (segmentID=0)
        → Stores pending delete: pendingDeletes[jobA][PK=123] = [1200]
        → Forwards INSERT → creates S_insert_1, rowTs=1200

T=1500  Import Phase 4: Write segments
        → Creates S_import, rowTs=1000, visible_timestamp=3000
        Phase 5: Update with segment IDs
        → importingPKs[jobA][PK=123] = [S_import]
        → Flush pending deletes
        → Inject DeleteMsg(PK=123, ts=1200, target=S_import)

T=2000  User INSERT(PK=123)
        StreamingNode queries PK index
        → Finds importingPKs[jobA][PK=123] (segmentID=S_import)
        → Inject DeleteMsg(PK=123, ts=2000, target=S_import)
        → Forwards INSERT → creates S_insert_2, rowTs=2000

T=3000  CommitImportMessage (ts=3000)
        → visible_timestamp=3000 set on S_import
        → Promote importing PKs to sealed

Query PK=123 at T=3001:
  - S_old: rowTs=500, deleteTs=1000 → DELETED (1000 > 500)
  - S_import: rowTs=1000, visible_timestamp=3000, deleteTs=1200 → DELETED (1200 > 1000)
  - S_insert_1: rowTs=1200, deleteTs=2000 → DELETED (2000 > 1200)
  - S_insert_2: rowTs=2000, no deleteTs → VISIBLE ✅

✅ CORRECT: Only S_insert_2 (latest version) visible
```

#### 4.1.8 Advantages

1. **Comprehensive coverage** - Handles all scenarios:
   - ✅ INSERT before import starts
   - ✅ INSERT during Importing state
   - ✅ INSERT during IndexBuilding state
   - ✅ INSERT during WaitingCommit state
   - ✅ Growing segments not in BBHash
   - ✅ PK index staleness
   - ✅ Concurrent import jobs

2. **Timestamp-based ordering** - Eliminates race conditions:
   - No perfect synchronization needed
   - Uses ImportMessage timestamp as ordering barrier
   - Deterministic conflict resolution

3. **No DML blocking** - Good UX:
   - Inserts/deletes work throughout import lifecycle
   - Auto-dedup happens transparently
   - Latest data always wins

4. **Cross-cluster consistency** - Replication-safe:
   - Same logic on primary and secondary
   - Deterministic outcomes (timestamp + jobID ordering)
   - Both clusters arrive at same state

5. **Incremental complexity** - Clean integration:
   - StreamingNode auto-dedup unchanged
   - Extends existing PK index with importingPKs map
   - Reuses delete message mechanism

#### 4.1.9 Disadvantages

1. **Memory overhead** - Importing PKs in memory:
   - ~16 bytes per PK (int64) or 16-64 bytes (varchar)
   - For 1B PKs: ~16 GB memory during import
   - Temporary (cleared on commit/abort)
   - Comparable to import data size

2. **Two-phase import** - Extract PKs first:
   - Must read files twice (PKs first, then full data)
   - Adds latency to import start (seconds to minutes)
   - File format must support PK extraction without full parse

3. **Growing segment queries** - Direct queries needed:
   - Must query growing segments directly (not via PK index)
   - Requires segment-level PK lookup API
   - May be slow if growing segment large

4. **Pending delete complexity** - Delayed delete injection:
   - Need PendingDeleteManager for pre-segment-assignment inserts
   - Adds code complexity
   - Small memory overhead (typically < 1% of import PKs)

### 4.2 Solution B: Block DML During Import Lifecycle

**Core Idea:** Reject all DML operations (insert, upsert, delete) during active import jobs to avoid conflicts entirely.

#### 4.2.1 Design Overview

```go
func (s *StreamingNode) ProcessInsertMsg(msg *InsertMsg) error {
    // Check if any import job is active for this collection
    activeJobs := s.meta.GetActiveImportJobs(msg.CollectionID)

    if len(activeJobs) > 0 {
        return merr.WrapErrImportInProgress(
            "DML blocked during import execution, retry after import completes",
            zap.Int("activeJobCount", len(activeJobs)))
    }

    // Proceed with normal processing
    return s.processInsertNormal(msg)
}
```

**Blocking scope:**
- Block: INSERT, UPSERT, DELETE operations
- Allow: Query operations (read-only)
- Duration: From ImportMessage until CommitImportMessage/AbortImportMessage

#### 4.2.2 Advantages

1. **Simple implementation** - No complex conflict detection
2. **Guaranteed correctness** - No duplicates possible
3. **Low memory overhead** - No importing PKs map needed

#### 4.2.3 Disadvantages

1. **Bad user experience** - All writes fail during import:
   - Small imports: 10-30 seconds blocked
   - Large imports: 1-5 minutes or more blocked
   - Unpredictable for users (depends on import size)

2. **Breaks existing workflows**:
   - Real-time ingestion pipelines fail
   - Application writes fail unpredictably
   - No graceful degradation

3. **Not suitable for production**:
   - High-throughput systems can't tolerate write blocking
   - Import becomes a disruptive operation
   - Users forced to schedule imports during maintenance windows

4. **Doesn't solve all issues**:
   - Still need conflict detection for data existing before import
   - Doesn't handle concurrent import jobs

#### 4.2.4 Verdict

❌ **Not recommended** - UX impact too severe for production systems.

May be acceptable for:
- Batch/offline systems with scheduled maintenance windows
- Low-throughput test/dev environments
- Initial v1 implementation (with explicit warning to users)

### 4.3 Solution C: Eventual Consistency with Compaction Cleanup

**Core Idea:** Accept temporary duplicates during import, rely on compaction to clean up later.

#### 4.3.1 Design Overview

1. **Import and insert both succeed** - No deduplication during import lifecycle
2. **Duplicates coexist temporarily** - Both versions visible after import commits
3. **Compaction detects and removes** - EntityFilter keeps highest timestamp version
4. **Queries handle duplicates** - Return latest version based on timestamp

#### 4.3.2 Compaction Logic

```go
// Existing compaction logic already handles this
type EntityFilter struct {
    deletedPKs map[PrimaryKey]uint64  // PK → delete timestamp
    latestPKs  map[PrimaryKey]uint64  // PK → latest insert timestamp
}

func (f *EntityFilter) ShouldKeep(row *Row) bool {
    // Check delete
    if deleteTs, deleted := f.deletedPKs[row.PK]; deleted {
        if row.Timestamp < deleteTs {
            return false  // Deleted
        }
    }

    // Check duplicate (keep only latest version)
    if latestTs, exists := f.latestPKs[row.PK]; exists {
        if row.Timestamp < latestTs {
            return false  // Older version, discard
        }
    }

    return true
}
```

#### 4.3.3 Query Handling

```go
// QueryNode must handle multiple versions per PK
func (s *Segment) QueryByPK(pk PrimaryKey) []Row {
    rows := s.scanForPK(pk)
    if len(rows) <= 1 {
        return rows
    }

    // Multiple versions found - return latest
    latestRow := rows[0]
    for _, row := range rows[1:] {
        if row.Timestamp > latestRow.Timestamp {
            latestRow = row
        }
    }

    return []Row{latestRow}
}
```

#### 4.3.4 Advantages

1. **Simplest implementation** - No coordination needed
2. **No DML blocking** - All operations proceed normally
3. **No memory overhead** - No importing PKs tracking
4. **Works with existing compaction** - Reuses EntityFilter logic

#### 4.3.5 Disadvantages

1. **Temporary duplicates visible** - Violates PK uniqueness:
   - Users see duplicate results in queries
   - Aggregation counts incorrect
   - Inconsistent behavior (depends on compaction timing)

2. **Inconsistent results** - Query results depend on compaction:
   - Before compaction: duplicates visible
   - After compaction: duplicates removed
   - Non-deterministic user experience

3. **Compaction overhead** - Extra work:
   - More data to scan (duplicate rows)
   - More CPU/memory for deduplication
   - Increased compaction frequency needed

4. **Cross-cluster divergence** - Replication issues:
   - Primary and secondary may compact at different times
   - Inconsistent duplicate visibility between clusters
   - Can't guarantee identical query results

5. **Violates user expectations** - PK uniqueness is fundamental:
   - Users expect PK uniqueness enforced immediately
   - Temporary duplicates may break application logic
   - Not acceptable for most use cases

#### 4.3.6 Verdict

❌ **Not recommended** - Violates PK uniqueness guarantees.

May be acceptable for:
- Append-only analytics workloads (no PK uniqueness requirement)
- Systems with very frequent compaction (< 1 minute)
- Temporary workaround during feature development

## 5. Recommendation

### 5.1 Recommended Solution: Solution A (Timestamp-Ordered Conflict Detection)

**Rationale:**

1. **Correctness** - Handles all scenarios with deterministic outcomes
2. **Performance** - No DML blocking, good UX
3. **Consistency** - Works correctly in replication scenarios
4. **Maintainability** - Clean integration with existing systems

**Implementation priority:**

**Phase 1: Foundation (Week 1-2)**
- Implement ImportingPKIndex data structure
- Extend PKIndexService with RegisterImportingPKs, QueryAllSources
- Add comprehensive conflict detection logic
- Unit tests for conflict detection

**Phase 2: Integration (Week 3-4)**
- Integrate import preprocessing with conflict detection
- Implement PendingDeleteManager for early-phase inserts
- Update import execution flow (5 phases)
- Integration tests for import + insert scenarios

**Phase 3: Optimization (Week 5-6)**
- Optimize growing segment queries (add index if needed)
- Tune memory usage for importing PKs
- Performance testing at scale (1B PKs)
- Memory profiling and optimization

**Phase 4: Production Rollout (Week 7-8)**
- Feature flag control (enable per collection)
- Monitoring and metrics
- A/B testing
- Gradual rollout to production

### 5.2 Alternative for V1: Solution B (Block DML)

If timeline is aggressive or resources limited, consider **Solution B** for initial v1:

**Pros for v1:**
- Much simpler implementation (days, not weeks)
- Guarantees correctness
- Buys time for proper Solution A implementation

**Must include:**
- Clear warning to users about write blocking
- Documentation of blocked duration (estimate based on import size)
- Explicit opt-in (disabled by default)
- Roadmap commitment to Solution A for v2

## 6. Implementation Details

### 6.1 Data Structures

#### SegmentWithVisibility

```go
type SegmentState int

const (
    SegmentStateSealed SegmentState = iota
    SegmentStateGrowing
    SegmentStateImporting
)

type SegmentWithVisibility struct {
    SegmentID        int64
    State            SegmentState
    JobID            int64        // For importing segments
    ImportTimestamp  uint64       // For importing segments
    VisibleTimestamp uint64       // For importing segments
    MaxTimestamp     uint64       // For sealed segments
}
```

#### PKLookupResult

```go
type PKLookupResult struct {
    Segments []SegmentWithVisibility
}
```

### 6.2 API Extensions

#### PK Index Service

```go
type PKIndexService interface {
    // Existing methods
    Query(pk PrimaryKey, shardID ShardID) []SegmentID

    // New methods for import integration
    RegisterImportingPKs(ctx context.Context, req *RegisterImportingPKsRequest) error
    UpdateImportingPKsWithSegments(ctx context.Context, req *UpdateImportingPKsRequest) error
    QueryAllSources(pk PrimaryKey, shardID ShardID) (*PKLookupResult, error)
    PromoteImportingToSealed(jobID int64, commitTs uint64) error
    RemoveImportingJob(jobID int64) error
}

type RegisterImportingPKsRequest struct {
    JobID            int64
    ShardID          string
    CollectionID     int64
    PKs              []PrimaryKey
    ImportTimestamp  uint64
    CommitTimestamp  uint64
}

type UpdateImportingPKsRequest struct {
    JobID            int64
    PKSegmentMapping map[PrimaryKey][]int64
}
```

#### Growing Segment Manager

```go
type GrowingSegmentManager interface {
    // Query PK timestamp in growing segment
    QueryPKTimestamp(segmentID int64, pk PrimaryKey) (uint64, error)

    // Batch query for efficiency
    BatchQueryPKTimestamp(segmentID int64, pks []PrimaryKey) (map[PrimaryKey]uint64, error)
}
```

### 6.3 Configuration

```yaml
pk_index:
  # Enable automatic deduplication on insert
  auto_dedup_on_insert: true

  # Enable import integration (register importing PKs)
  import_integration_enabled: true

  # Memory limit for importing PKs (per job)
  importing_pks_memory_limit_gb: 20

  # Pending delete manager settings
  pending_delete_max_entries: 1000000
  pending_delete_flush_interval_ms: 1000

import:
  # Enable PK pre-registration before writing data
  pk_preregistration_enabled: true

  # Enable comprehensive conflict detection
  comprehensive_conflict_detection: true

  # Query growing segments directly (bypass PK index)
  query_growing_segments_for_conflicts: true
```

### 6.4 Metrics and Monitoring

**Import Metrics:**
```
# Phase durations
import_pk_extraction_duration_seconds
import_pk_registration_duration_seconds
import_conflict_detection_duration_seconds
import_data_write_duration_seconds

# Conflict detection
import_conflicts_detected_total{type="sealed|growing|importing"}
import_pks_skipped_total  # PKs skipped due to earlier import

# Memory usage
import_importing_pks_memory_bytes
import_pending_deletes_count
```

**PK Index Metrics:**
```
# Importing PKs tracking
pk_index_importing_jobs_active
pk_index_importing_pks_total
pk_index_importing_pks_memory_bytes

# Query performance
pk_index_query_duration_ms{source="sealed|growing|importing"}
pk_index_query_segments_returned{state="sealed|growing|importing"}
```

**Auto-Dedup Metrics:**
```
# Deduplication actions
auto_dedup_deletes_injected_total{target="sealed|growing|importing"}
auto_dedup_pending_deletes_stored_total
auto_dedup_pending_deletes_flushed_total
```

## 7. Testing Strategy

### 7.1 Unit Tests

**Conflict Detection:**
- Import PK exists in sealed segment
- Import PK exists in growing segment
- Import PK exists in another importing job (earlier timestamp)
- Import PK exists in another importing job (same timestamp, jobID tiebreaker)
- Import PK doesn't exist anywhere

**PK Index Service:**
- RegisterImportingPKs adds to importingPKs map
- QueryAllSources returns sealed + growing + importing segments
- UpdateImportingPKsWithSegments fills segment IDs correctly
- PromoteImportingToSealed cleans up and triggers rebuild
- RemoveImportingJob removes on abort

**Pending Delete Manager:**
- Add pending deletes for pre-segment-assignment inserts
- Flush pending deletes when segments available
- Cleanup on job completion

### 7.2 Integration Tests

**End-to-End Import + Insert:**
- INSERT before import starts → import deletes old version
- INSERT during Importing state → pending delete, applied after segment created
- INSERT during WaitingCommit → delete injected to import segment
- INSERT after commit → normal auto-dedup

**Concurrent Import Jobs:**
- Two imports with same PKs, earlier timestamp wins
- Two imports with same PKs and timestamp, smaller jobID wins
- Import A starts, Import B starts, both complete correctly

**Cross-Cluster Replication:**
- Primary and secondary both import same data
- Same inserts on both clusters
- Both arrive at identical state after commit

### 7.3 Performance Tests

**Import with Conflict Detection:**
- Import 1B PKs with 0%, 10%, 50%, 100% conflict rate
- Measure phase durations (extraction, registration, conflict detection, write)
- Memory usage during import

**Concurrent Inserts During Import:**
- Sustained insert load (100k ops/sec) during import
- Measure insert latency P50/P95/P99
- Measure pending delete queue depth

**Growing Segment Query Performance:**
- Query growing segments with 1M, 10M, 100M rows
- Batch query optimization (query all PKs in one call)

### 7.4 Chaos Tests

**Import Failure Scenarios:**
- Import aborted during Importing state (cleanup importingPKs)
- Import aborted during WaitingCommit (cleanup importingPKs)
- DataNode crashes during import (restart, cleanup)
- StreamingNode crashes (reload importingPKs from persistent state)

**Race Condition Tests:**
- 1000 concurrent inserts with same PK during import start
- Multiple imports with overlapping PKs starting simultaneously
- Import + insert + delete mixed workload

## 8. References

### 8.1 Related Design Documents

- **Import in Replication:** `2026-03-13-import-in-replication-design.md`
  - Section 4.2: Write Consistency Solution (visible_timestamp)
  - Section 5: Outstanding Consistency Issues
  - Issue 1: Row Reappears After DELETE

- **Primary Key Index:** `2026-03-16-primary-key-index-design.md`
  - Section 3.4: Automatic Deduplication on Insert
  - Section 4.1: BBHash Index Structure
  - Section 9.5: Consistency Model for PK Index

### 8.2 Relevant Code Locations

**Current Milvus Codebase:**
- `/internal/datacoord/ddl_callbacks_import.go` - Import execution callbacks
- `/internal/datanode/importv2/` - Import task execution
- `/internal/querynodev2/pkoracle/` - Current PK lookup (bloom filter)
- `/internal/streamingnode/` - Message processing and auto-dedup (future)

**Files to Create/Modify:**
- New: `/internal/datacoord/pk_index_service.go` - ImportingPKIndex implementation
- New: `/internal/datacoord/pending_delete_manager.go` - Pending delete tracking
- Modify: `/internal/datanode/importv2/import_task.go` - Add 5-phase execution
- Modify: `/internal/datacoord/ddl_callbacks_import.go` - Register/promote importing PKs

---

**End of Design Document**
