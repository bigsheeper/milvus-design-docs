# Milvus 复制场景下的 Import 设计方案

**日期:** 2026-03-13
**作者:** Design Collaboration Session
**状态:** Draft v1.4

---

## 背景

为了支持 Milvus 集群间的主备容灾和跨区域数据复制,Milvus 2.6 引入了基于 CDC (Change Data Capture) 的复制机制。然而,当前版本中 Import 操作与 CDC 复制功能互斥 —— 当集群启用主备复制时,Import 操作会被明确阻止 (`internal/datacoord/ddl_callbacks_import.go:121-123`):

```go
// Import in replicating cluster is not supported yet
if channelAssignment.ReplicateConfiguration != nil &&
   len(channelAssignment.ReplicateConfiguration.GetClusters()) > 1 {
    return merr.WrapErrImportFailed("import in replicating cluster is not supported yet")
}
```

这一限制严重影响了企业用户的数据迁移和导入场景,尤其是在需要持续保持主备同步的生产环境中。

### 核心目标

本设计方案旨在解除 Import 与 CDC 复制的互斥限制,在保持数据一致性的前提下,实现以下核心目标:

1. **解除复制阻塞** - 允许在 CDC 复制激活状态下执行 Import 操作
2. **强一致性** - 所有集群上的导入数据要么全部可见、要么全部不可见(All-or-nothing)
3. **手动协调** - 操作员手动控制导入数据的可见性切换时机
4. **CDC 兼容** - 修复按 vchannel 粒度的 TimeTick 以支持 Checkpoint 恢复
5. **最小化复杂度** - 复用现有的 Broadcast 和 CDC 机制

### 非目标

- 主备集群间的自动协调
- 在从集群(Follower)上执行 Import
- 破坏向后兼容性的 API 变更

---

## 架构概览

### 高层流程

**复制集群(手动 Commit):**

```
┌─────────────────────────────────────────────────────────────┐
│                     主集群 (PRIMARY)                         │
│  用户 → Proxy → DataCoord.ImportV2()                        │
│         ↓                                                    │
│  DataCoord 广播 ImportMessage (通过 CDC)                    │
│         ↓                                                    │
│  ImportJob: Pending → ... → IndexBuilding → WaitingCommit  │
│                                                              │
│  用户调用: CommitImport(jobID) 或 AbortImport(jobID)        │
│         ↓                                                    │
│  DataCoord 广播: CommitImportMessage / AbortImportMessage   │
└─────────────────────────────────────────────────────────────┘
                           │
                           │ CDC 复制
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                   从集群 (SECONDARIES)                       │
│  Proxy 接收 ImportMessage → DataCoord.importV1AckCallback  │
│         ↓                                                    │
│  ImportJob: Pending → ... → IndexBuilding → WaitingCommit  │
│         ↓                                                    │
│  接收 CommitImportMessage 或 AbortImportMessage             │
│         ↓                                                    │
│  状态转换: WaitingCommit → Completed 或 Failed              │
└─────────────────────────────────────────────────────────────┘
```

**非复制集群(自动 Commit):**

```
┌─────────────────────────────────────────────────────────────┐
│                   非复制集群                                 │
│  用户 → Proxy → DataCoord.ImportV2()                        │
│         ↓                                                    │
│  ImportJob: Pending → ... → IndexBuilding → WaitingCommit  │
│         ↓                                                    │
│  DataCoord 自动广播 CommitImportMessage                     │
│         ↓                                                    │
│  状态转换: WaitingCommit → Completed (无缝)                 │
│                                                              │
│  结果: 用户看到 IndexBuilding → Completed (快速)            │
└─────────────────────────────────────────────────────────────┘
```

### 核心设计决策

1. **统一状态机 + 条件触发** - 所有集群使用相同的状态机 (IndexBuilding → WaitingCommit → Completed)
2. **非复制集群自动提交** - 非复制集群自动广播 CommitImportMessage (向后兼容)
3. **复制集群手动提交** - 复制集群等待用户调用 CommitImport RPC
4. **用户负责一致性检查** - 用户必须手动检查所有集群处于 WaitingCommit 后才能调用 CommitImport (无自动验证)
5. **复用现有 ImportMessage** - 不需要新的 PreImport 消息类型
6. **全局统一 JobID** - 所有集群使用相同的 JobID
7. **共享对象存储** - 所有集群从同一个 S3/MinIO 位置读取导入文件
8. **独立执行** - 每个集群独立管理自己的 ImportJob 生命周期
9. **幂等操作** - Commit/abort 可以安全重试

---

## 状态机变更

### 当前状态机 (8 个状态)

```
Pending → PreImporting → Importing → Sorting → IndexBuilding → Completed
            ↓              ↓           ↓            ↓              ↓
                             Failed (任意阶段)
```

### 新状态机 (9 个状态)

```
Pending → PreImporting → Importing → Sorting → IndexBuilding → WaitingCommit → Completed
            ↓              ↓           ↓            ↓               ↓              ↓
                             Failed (任意阶段,或显式 abort)
```

### 新增状态: WaitingCommit

**用途:** 在使导入 Segment 可查询之前的关卡,等待提交触发器(自动或手动)。

**进入条件:**
- IndexBuilding 成功完成
- **所有集群**都会进入此状态(统一状态机)

**退出条件:**
- 接收到 `CommitImportMessage` → `Completed`
- 接收到 `AbortImportMessage` → `Failed`
- 超时(默认 1 小时) → `Failed` 并自动清理

**提交触发行为:**
- **非复制集群:** DataCoord 在进入 WaitingCommit 时自动广播 `CommitImportMessage` (无缝自动提交)
- **复制集群:** 等待用户调用 `CommitImport(jobID)` RPC (手动提交)

**状态特征:**
- Segment 已存在,Binlog 已写入对象存储
- Segment 已建立索引
- Segment 标记 `importing = true` (不可查询)
- Job 元数据持久化在 etcd

**向后兼容性:**
- 非复制集群: 自动提交保持现有行为 (IndexBuilding → WaitingCommit → 自动广播 → Completed)
- 现有部署无需修改(未启用 CDC 复制)

### 状态转换规则

| 源状态 | 目标状态 | 触发器 | 备注 |
|--------|----------|--------|------|
| IndexBuilding | WaitingCommit | 自动(所有集群) | 统一状态机 - 所有集群进入 WaitingCommit |
| WaitingCommit | Completed | CommitImportMessage 广播 | 自动广播(非复制)或手动RPC(复制) |
| WaitingCommit | Failed | AbortImportMessage 广播 | 手动触发 |
| WaitingCommit | Failed | 超时(默认1小时) | 自动安全机制 |
| 任意状态(除Completed/Failed) | Failed | AbortImportMessage | 允许随时手动 abort |

---

## 一致性模型与用户职责

### 设计哲学

本设计提供**手动协调的最终强一致性**:
- 所有集群使用**相同的 JobID** 执行**相同的导入作业**
- **用户是协调者** - 负责检查就绪状态并触发提交
- **无自动验证** - 主集群不会在提交前查询从集群状态
- **幂等广播** - Commit/abort 消息可以安全重试

### 一致性保证

**系统提供的保证:**
1. ✅ **原子本地转换** - 每个集群的状态转换 (WaitingCommit → Completed) 是原子的
2. ✅ **广播交付** - CommitImportMessage 通过 CDC 交付到所有集群(至少一次语义)
3. ✅ **幂等操作** - 多次接收 CommitImportMessage 是安全的(已 Completed 则为空操作)
4. ✅ **Segment 可见性控制** - Segment 在 WaitingCommit → Completed 转换前不可查询

**系统不提供的保证:**
1. ❌ **无自动从集群验证** - 主集群在广播前不检查从集群是否处于 WaitingCommit
2. ❌ **无分布式事务** - 没有确保原子性的 2PC 协调器
3. ❌ **无自动回滚** - 如果某个从集群失败,其他集群不会自动回滚
4. ❌ **无跨集群状态同步** - 集群之间不会等待彼此

### 用户职责

**用户必须:**

1. **在调用 CommitImport 前手动检查所有集群:**
   ```bash
   # 检查主集群
   curl "http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   # 输出: {"state": "WaitingCommit"}

   # 检查从集群1
   curl "http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   # 输出: {"state": "WaitingCommit"}

   # 检查从集群2
   curl "http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   # 输出: {"state": "WaitingCommit"}

   # 所有集群必须都处于 WaitingCommit 才能提交!
   ```

2. **处理部分失败:**
   - 如果任何从集群卡在 IndexBuilding → 等待或调用 `AbortImport`
   - 如果任何从集群转换为 Failed → 调用 `AbortImport` 清理所有集群

3. **在部分广播失败时重试:**
   - 如果 CommitImport 成功但某些从集群仍处于 WaitingCommit → 重试 CommitImport
   - 幂等 - 可以安全多次重试

4. **监控分歧:**
   - 在 CommitImport 后,验证所有集群都转换为 Completed
   - 如果检测到分歧(部分 Completed,部分 WaitingCommit) → 重试 CommitImport

### 为什么选择手动协调?

**设计理由:**
1. **简单性** - 无需复杂的分布式协调协议
2. **可见性** - 操作员完全可见和可控
3. **灵活性** - 操作员可以决定等待、中止或调查
4. **生产友好** - 许多企业运维更喜欢显式控制而非自动行为
5. **故障处理** - 操作员可以根据上下文做出决策(例如:"从集群正在维护,无论如何都继续")

**接受的权衡:**
- 用户必须执行手动检查(容易出错)
- 无自动就绪状态强制执行
- 操作员错误的可能性(在从集群未就绪时提交)

### 一致性场景

**场景 1: 正常路径**
```
主集群: IndexBuilding → WaitingCommit ✅
从集群1: IndexBuilding → WaitingCommit ✅
从集群2: IndexBuilding → WaitingCommit ✅

用户检查所有集群: 全部处于 WaitingCommit
用户调用 CommitImport(jobID)
主集群广播 CommitImportMessage

主集群: WaitingCommit → Completed ✅
从集群1: WaitingCommit → Completed ✅
从集群2: WaitingCommit → Completed ✅

结果: 实现强一致性
```

**场景 2: 从集群未就绪**
```
主集群: WaitingCommit ✅
从集群1: WaitingCommit ✅
从集群2: 仍在 Importing ❌

用户检查所有集群: 从集群2 未就绪!
用户选项:
  A) 等待从集群2赶上
  B) 调用 AbortImport (清理所有集群)
  C) 调查为什么从集群2慢

用户暂不调用 CommitImport。
```

**场景 3: 用户过早提交(错误)**
```
主集群: WaitingCommit ✅
从集群1: WaitingCommit ✅
从集群2: 仍在 Importing ❌

用户错误地调用 CommitImport (未检查从集群2)
主集群广播 CommitImportMessage

主集群: WaitingCommit → Completed ✅
从集群1: WaitingCommit → Completed ✅
从集群2: 在 Importing 时接收 CommitImportMessage 💥

从集群2会怎样?
→ 见下一节 "处理乱序消息"
```

**场景 4: 部分广播失败**
```
主集群: WaitingCommit → Completed ✅
从集群1: WaitingCommit → Completed ✅
从集群2: 网络问题,CommitImportMessage 丢失 ❌

用户检测到分歧(轮询显示从集群2仍处于 WaitingCommit)
用户重试: CommitImport(jobID)
主集群重新广播 CommitImportMessage (幂等)

从集群2: 接收消息 → WaitingCommit → Completed ✅

结果: 通过重试实现最终一致性
```

### 处理乱序消息

**问题:** 如果 CommitImportMessage 到达时集群不处于 WaitingCommit 怎么办?

**当前设计决策:** **基于当前状态处理消息**

**实现:**
```go
func (c *DDLCallbacks) commitImportAckCallback(ctx context.Context, result message.BroadcastResultCommitImportMessageV1) error {
    jobID := result.Message.MustBody().GetJobId()
    job := c.importMeta.GetJob(jobID)

    if job == nil {
        // Job 不存在 - 已清理或错误的集群
        log.Warn("收到不存在 Job 的 commit import 消息,忽略",
            zap.Int64("job_id", jobID))
        return nil
    }

    state := job.GetState()
    switch state {
    case internalpb.ImportJobStateV2_WaitingCommit:
        // 期望情况: 正常处理
        return c.processCommitImport(ctx, jobID)

    case internalpb.ImportJobStateV2_Completed:
        // 已完成(重复消息) - 幂等空操作
        log.Info("Job 已完成,忽略重复的 commit 消息",
            zap.Int64("job_id", jobID))
        return nil

    default:
        // Job 尚未处于 WaitingCommit (太早) 或已 Failed
        log.Error("收到 commit import 消息但 Job 不在 WaitingCommit",
            zap.Int64("job_id", jobID),
            zap.String("current_state", state.String()))

        // 选项 1: 丢弃消息(当前设计 - 用户错误)
        return merr.WrapErrImportFailed(
            fmt.Sprintf("Job %d 处于状态 %s,无法提交", jobID, state))

        // 选项 2: 排队消息稍后处理(未来增强)
        // c.pendingCommits.Store(jobID, result.Message)
        // return nil
    }
}
```

**当前行为:** 如果 CommitImportMessage 到达太早 → 记录错误,丢弃消息,Job 保持当前状态

**后果:** 用户犯错(提交太早) → 从集群保持当前状态 → 超时 → 自动中止

**用户恢复:** 调用 `AbortImport` 清理所有集群,修复慢的从集群,重试 Import

---

## 写入一致性: Import 数据时间戳

### 问题陈述

**关键问题:**

当 Import 执行时,所有导入的行都被分配一个**单一时间戳**,来自 ImportMessage 广播时间 (`task.req.GetTs()`)。此时间戳在 Segment flush 期间写入存储并持久化在 binlog 中。

```go
// 文件: internal/datanode/importv2/util.go (lines 188-226)
func AppendSystemFieldsData(task *ImportTask, data *storage.InsertData, rowNum int) error {
    tss := make([]int64, rowNum)
    ts := int64(task.req.GetTs())  // 所有行获得 T_import
    for i := 0; i < rowNum; i++ {
        tss[i] = ts
    }
    data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
}
```

**问题时间线:**

```
T=1000: ImportMessage 广播 → import 执行
        → 所有行写入 binlog,时间戳 = 1000
        → Segment 处于 WaitingCommit (importing=true, 不可查询)

T=2000: 用户执行: DELETE pk=2
        → 写入 Delta log: (pk=2, ts=2000)
        → Delete 仅应用于可见 Segment (import segment 隐藏)

T=3000: 用户调用 CommitImport → CommitImportMessage 广播
        → Segment 变为可查询 (importing=false)
        → QueryNode 过滤: row.ts=1000 < delete.ts=2000
        → DELETE 生效! pk=2 的行被删除

结果: T=2000 的 DELETE 删除了在那时"逻辑上不存在"的数据
      (在 WaitingCommit 中隐藏)。
```

### 为什么当前行为在语义上是错误的

**语义期望:**

- Import 数据应被视为在 T_commit (变为可查询时)"出现"
- T_commit 之前的 DML 操作不应影响 import 数据
- 只有 T_commit 之后的 DML 操作才应生效

**当前现实:**

- Import 数据有 T_import (广播时的旧时间戳)
- T_import 和 T_commit 之间的 DML 操作错误地影响 import 数据
- 时间戳顺序不反映逻辑可见性顺序

**违反:**

1. **因果性违反**: T=2000 的 DELETE 删除了"尚不存在"的数据(隐藏到 T=3000)
2. **跨集群不一致**: 主集群用户立即看到 DML 结果,但从集群在 CDC 延迟后才看到
3. **重放问题**: 如果 import 被中止并重试,DML 操作在重试时会有不同的应用效果

### 解决方案: Segment 级可见时间戳

**设计选择: 分析中的方法 C**

使用**Segment 级元数据**覆盖行时间戳用于过滤,通过 Compaction 最终标准化。

**为什么选择这种方法:**

✅ **无昂贵重写** - 提交是即时的(仅元数据更新)
✅ **不可变 binlog** - 原始数据不变
✅ **正确语义** - DML 过滤正确工作
✅ **向后兼容** - 非 import segment 照常工作
✅ **自然粒度** - Segment 已经是管理单元
✅ **最终清理** - Compaction 随时间标准化数据
✅ **干净回滚** - Abort 只是删除 segment
✅ **最小更改** - 一个元数据字段 + QueryNode 过滤逻辑

### 实现设计

#### Proto 变更

**文件: `pkg/v2/proto/datapb/segment.proto`**

```protobuf
message SegmentInfo {
    // ... 现有字段 ...

    // visible_timestamp 覆盖行级时间戳用于过滤。
    // 用于 import segment 以确保 DML 操作遵循逻辑可见性顺序。
    //
    // 当设置(非零)时:
    // - QueryNode 使用此时间戳进行过滤而非 row.timestamp
    // - 确保 visible_timestamp 之前的 DML 操作不影响此 segment
    // - 确保 visible_timestamp 之后的 DML 操作正确应用
    //
    // 当为零时:
    // - 正常 segment (非 import),使用 row.timestamp 进行过滤
    //
    // 生命周期:
    // - 接收 CommitImportMessage 时设置为 T_commit
    // - Compaction 重写行时间戳后清除(设为 0)
    optional uint64 visible_timestamp = X;
}
```

#### DataCoord 变更

**文件: `internal/datacoord/ddl_callbacks_import.go`**

```go
// commitImportAckCallback 处理 CommitImportMessage 广播的确认。
// 将 Job 转换为 Completed 并使 segment 以正确的可见时间戳可查询。
func (c *DDLCallbacks) commitImportAckCallback(ctx context.Context, result message.BroadcastResultCommitImportMessageV1) error {
    body := result.Message.MustBody()
    jobID := body.GetJobId()

    // 从 CommitImportMessage 广播获取时间戳
    commitTimestamp := result.GetMaxTimeTick()  // 广播的 T_commit

    log.Ctx(ctx).Info("处理 commit import ack",
        zap.Int64("job_id", jobID),
        zap.Uint64("commit_timestamp", commitTimestamp))

    // 1. 将 Job 状态更新为 Completed
    err := c.importMeta.UpdateJobState(jobID, internalpb.ImportJobStateV2_Completed)
    if err != nil {
        return err
    }

    // 2. 标记所有 segment 为可查询,visible_timestamp = T_commit
    job := c.importMeta.GetJob(jobID)
    if job == nil {
        return merr.WrapErrImportJobNotExist(jobID)
    }

    for _, task := range job.GetTasks() {
        for _, segmentID := range task.GetSegmentIDs() {
            segment := c.meta.GetSegment(segmentID)
            if segment != nil && segment.GetImporting() {
                // 原子更新: 设置 visible_timestamp 并清除 importing 标志
                err := c.meta.UpdateSegmentVisibility(segmentID, commitTimestamp, false)
                if err != nil {
                    log.Ctx(ctx).Error("更新 segment 可见性失败",
                        zap.Int64("segment_id", segmentID), zap.Error(err))
                    return err
                }
            }
        }
    }

    log.Ctx(ctx).Info("Import Job 成功提交",
        zap.Int64("job_id", jobID),
        zap.Uint64("visible_timestamp", commitTimestamp),
        zap.Int("segment_count", len(job.GetTasks())))

    return nil
}
```

**新 Meta 方法:**

```go
// 文件: internal/datacoord/meta.go

// UpdateSegmentVisibility 原子更新 segment 的 visible_timestamp 和 importing 标志。
// 在 import commit 期间使用,使 segment 以正确的时间戳语义可查询。
func (m *meta) UpdateSegmentVisibility(segmentID int64, visibleTimestamp uint64, importing bool) error {
    m.Lock()
    defer m.Unlock()

    segment := m.segments.GetSegment(segmentID)
    if segment == nil {
        return merr.WrapErrSegmentNotFound(segmentID)
    }

    // 克隆 segment 进行更新
    cloned := proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
    cloned.VisibleTimestamp = visibleTimestamp
    cloned.Importing = importing

    // 持久化到 etcd
    err := m.catalog.AlterSegment(m.ctx, cloned)
    if err != nil {
        return err
    }

    // 更新内存
    m.segments.SetSegment(segmentID, NewSegmentInfo(cloned))

    log.Info("更新 segment 可见性",
        zap.Int64("segment_id", segmentID),
        zap.Uint64("visible_timestamp", visibleTimestamp),
        zap.Bool("importing", importing))

    return nil
}
```

#### QueryNode 变更

**文件: `internal/querynodev2/segments/segment.go` 或相关过滤代码**

```go
// GetEffectiveTimestamp 返回用于 DML 过滤的时间戳。
// 对于 import segment,如果设置了 visible_timestamp 则返回它;否则返回行时间戳。
func (s *Segment) GetEffectiveTimestamp(rowTimestamp uint64) uint64 {
    // 检查 segment 级覆盖
    if s.segmentInfo.GetVisibleTimestamp() != 0 {
        return s.segmentInfo.GetVisibleTimestamp()
    }

    // 正常 segment: 使用行时间戳
    return rowTimestamp
}

// ApplyDelete 基于有效时间戳语义过滤行。
func (s *Segment) ApplyDelete(pks []PrimaryKey, deleteTss []uint64) {
    for i, pk := range pks {
        deleteTs := deleteTss[i]

        // 查找匹配的行
        rowOffsets := s.pkIndex.Query(pk)
        for _, offset := range rowOffsets {
            rowTs := s.timestampField.Get(offset)

            // 使用有效时间戳进行比较
            effectiveTs := s.GetEffectiveTimestamp(rowTs)

            // Delete 在以下情况生效: effectiveTs <= deleteTs
            if effectiveTs <= deleteTs {
                s.deleteBuffer.Add(offset)
            }
        }
    }
}
```

#### Compaction 变更

**文件: `internal/datanode/compaction/mix_compactor.go` 或相关 compaction 代码**

```go
// CompactSegments 执行 L0/L1 compaction,标准化 import segment。
func (c *MixCompactor) CompactSegments(segments []*datapb.SegmentInfo) (*datapb.CompactionResult, error) {
    // ... 现有 compaction 逻辑 ...

    // 对于每个被 compact 的 segment
    for _, segment := range segments {
        visibleTs := segment.GetVisibleTimestamp()

        if visibleTs != 0 {
            // 这是带覆盖时间戳的 import segment
            log.Info("在 compaction 期间标准化 import segment",
                zap.Int64("segment_id", segment.GetID()),
                zap.Uint64("visible_timestamp", visibleTs))

            // 将行时间戳重写为 visible_timestamp
            for rowOffset := 0; rowOffset < segment.NumRows; rowOffset++ {
                originalTs := segment.GetTimestamp(rowOffset)
                segment.SetTimestamp(rowOffset, visibleTs)
            }

            // 在输出 segment 元数据中清除 visible_timestamp
            // (行时间戳现在反映正确值)
            outputSegment.VisibleTimestamp = 0
        }

        // ... 继续使用标准化时间戳进行 compaction ...
    }

    return compactionResult, nil
}
```

### 跨集群一致性

**这如何实现写入一致性:**

```
主集群:
T=1000: ImportMessage → import 执行 → 行写入 ts=1000
T=2000: DELETE pk=2 → delta log (pk=2, ts=2000)
T=3000: CommitImportMessage (广播 ts=3000)
        → segment.visible_timestamp = 3000
        → segment.importing = false
        → QueryNode: effectiveTs=3000 > deleteTs=2000 → DELETE 不生效 ✓

从集群 (通过 CDC):
T=1000: ImportMessage → import 执行 → 行写入 ts=1000
T=3000: CommitImportMessage 到达 (ts=3000)
        → segment.visible_timestamp = 3000
        → segment.importing = false
T=3000+lag: DELETE 消息到达 (pk=2, ts=2000)
        → QueryNode: effectiveTs=3000 > deleteTs=2000 → DELETE 不生效 ✓

结果: 两个集群具有相同的过滤行为!
```

**关键属性:**

1. **相同 visible_timestamp**: 两个集群从同一广播消息设置 `segment.visible_timestamp = T_commit`
2. **相同 DML 时间戳**: DML 操作在两个集群上具有相同的时间戳(通过 CDC 复制)
3. **相同过滤逻辑**: `effectiveTs > deleteTs` 在所有集群上产生相同结果
4. **顺序无关**: DELETE 在 CommitImportMessage 之前或之后到达都无关紧要 - 过滤逻辑是一致的

### 示例场景

**场景 1: Commit 前 DELETE**

```
T=1000: Import 执行 → 行: (pk=1, ts=1000), (pk=2, ts=1000), (pk=3, ts=1000)
T=2000: DELETE pk=2 → delta log: (pk=2, ts=2000)
T=3000: CommitImport → segment.visible_timestamp = 3000

Commit 后查询:
- 行 pk=1: effectiveTs=3000, 无 delete → 可见 ✓
- 行 pk=2: effectiveTs=3000, deleteTs=2000, 3000 > 2000 → 可见 ✓ (delete 不生效)
- 行 pk=3: effectiveTs=3000, 无 delete → 可见 ✓

结果: 所有 3 行可见 (正确 - DELETE 在 commit 前)
```

**场景 2: Commit 后 DELETE**

```
T=1000: Import 执行 → 行: (pk=1, ts=1000), (pk=2, ts=1000), (pk=3, ts=1000)
T=3000: CommitImport → segment.visible_timestamp = 3000
T=4000: DELETE pk=2 → delta log: (pk=2, ts=4000)

Delete 后查询:
- 行 pk=1: effectiveTs=3000, 无 delete → 可见 ✓
- 行 pk=2: effectiveTs=3000, deleteTs=4000, 3000 < 4000 → 已删除 ✓ (delete 生效)
- 行 pk=3: effectiveTs=3000, 无 delete → 可见 ✓

结果: 2 行可见 (正确 - DELETE 在 commit 后)
```

### 实现影响总结

| 组件 | 变更 | 复杂度 |
|------|------|--------|
| **SegmentInfo Proto** | 添加 `visible_timestamp` 字段 | 低 (1个字段) |
| **DataCoord Commit** | 设置 `visible_timestamp = T_commit` | 低 (~10 行代码) |
| **Meta 操作** | 添加 `UpdateSegmentVisibility()` | 低 (~30 行代码) |
| **QueryNode 过滤** | 对 delete 使用有效时间戳 | 中 (~50 行代码) |
| **Compaction** | 标准化时间戳,清除元数据 | 中 (~50 行代码) |

**总估计代码行数:** ~150 行

**风险级别:** 低-中
- QueryNode 过滤逻辑是关键路径
- 需要仔细测试时间戳语义
- Compaction 标准化不紧急(可以稍后添加)

**性能影响:**
- ✅ Import 期间无额外成本(仅元数据写入)
- ✅ Commit 期间无额外成本(与之前相同)
- ✅ QueryNode 中成本最小(每个 delete 操作一个额外的 if 检查)
- ✅ 无额外存储成本(每个 segment 元数据一个 uint64)

---

## 已知一致性问题与权衡

### 关键问题识别

`visible_timestamp` 解决方案解决了基本的写入一致性问题,但引入了需要仔细考虑的**新语义歧义**。

---

### 问题 1: 行在 DELETE 后重新出现 ⚠️ 关键

**场景:**
```
T=1000: Import 数据: (pk=2, field=100, ts=1000)
        → Segment A: 隐藏 (importing=true)

T=2000: 用户 INSERT: (pk=2, field=200, ts=2000)
        → Segment B: growing segment, 立即可查询
        → 查询结果: pk=2, field=200 ✓

T=2500: 用户 DELETE pk=2 (ts=2500)
        → Delta log: (pk=2, ts=2500)
        → 应用于 Segment B: rowTs=2000 < deleteTs=2500 → 已删除 ✓
        → 查询结果: 无 (pk=2 已删除) ✓

T=3000: CommitImport → Segment A 变为可见
        → Segment A: visible_timestamp=3000, importing=false

Commit 后查询:
- Segment A: (pk=2, field=100, rowTs=1000, effectiveTs=3000)
  → DELETE 检查: effectiveTs=3000 > deleteTs=2500 → 未删除
- Segment B: (pk=2, field=200, ts=2000) → 在 T=2500 标记为已删除
- 合并结果: 仅返回 Segment A 行
- **结果: pk=2, field=100 可见! (行在被删除后重新出现!)**
```

**根本原因:**

DELETE 行为的语义歧义:

**解释 A: "DELETE 应用于删除时可见的数据"**
- T=2500 的 DELETE 仅操作可见数据 (Segment B)
- Import 数据稍后在 T=3000 变为可见 (在 delete 之后)
- DELETE 不应用于 import → Import 行出现 (似乎不对)

**解释 B: "DELETE 应用于时间戳 < delete 时间戳的所有数据"**
- T=2500 的 DELETE 应删除 rowTs < 2500 的所有 pk=2
- Import 数据有 rowTs=1000 < 2500 → 应该被删除
- 但 visible_timestamp 阻止了这一点 → Import 行出现 (也似乎不对)

**用户期望:**
- "我在 T=2500 删除了 pk=2,它应该保持删除状态"
- 看到 pk=2 在 T=3000 重新出现违反了这个期望

---

### 可能的解决方案

### 解决方案 1: 阻止 WaitingCommit 期间的 DML ⚠️

**设计:**
- 当集合上的任何 import job 进入 WaitingCommit 状态时
- 拒绝该集合上的所有 DML 操作 (INSERT/DELETE/UPSERT)
- 返回错误: "DML 操作被阻止,因为 import job 待提交"
- 在 CommitImport 或 AbortImport 后解除阻止

**优点:**
- ✅ 完全避免所有语义歧义问题
- ✅ 实现简单 (~100 行代码)
- ✅ 用户清晰的错误消息
- ✅ 无需复杂的时间戳逻辑
- ✅ 保证所有场景的一致性

**缺点:**
- ❌ **糟糕的用户体验** - 用户在 import commit 窗口期间无法执行 DML
- ❌ 需要用户快速完成 import 或有阻塞应用程序的风险
- ❌ 不适用于多个 import job (一个阻塞所有 DML)
- ❌ 如果 import 卡在 WaitingCommit 可能导致应用程序停机
- ❌ 对 CDC 用例不理想 (持续复制 + 偶尔 import)

**风险级别:** 低 (简单,安全)
**用户影响:** 高 (阻塞操作)

---

### 解决方案 2: 接受"行重新出现"行为(记录为限制)

**设计:**
- 保持 visible_timestamp 解决方案原样
- 在 API 文档和操作指南中明确记录 WaitingCommit 期间 DML 的"最终可见性"语义
- 在 API 文档中添加警告

**文档:**
```
警告: WaitingCommit 阶段的 DML 操作:

如果在 import job 处于 WaitingCommit 状态时执行 DML 操作
(INSERT/DELETE/UPSERT),将应用以下行为:

1. DELETE: 删除仅影响删除时可见的数据。
   Import 数据在 commit 时变为可见,不会被先前的 DELETE 操作删除,
   即使时间戳表明应该删除。

2. UPSERT: Upsert 仅操作可见数据。如果 upsert 的 PK 存在于
   隐藏的 import 数据中,commit 后可能看到意外结果。

3. INSERT: Insert 创建新行。如果相同 PK 存在于 import 数据中,
   去重将优先 DML 操作(更新的时间戳)。

建议: 等待所有 import job 完成(或中止)后再对同一集合执行 DML 操作。
```

**优点:**
- ✅ 无阻塞 - 始终允许 DML
- ✅ 无额外实现复杂性
- ✅ 适用于当前 visible_timestamp 设计
- ✅ 性能不受影响

**缺点:**
- ❌ 用户的意外行为 (delete 后行重新出现)
- ❌ 需要用户理解复杂的时序语义
- ❌ 可能导致生产中的数据质量问题
- ❌ 问题发生时难以调试
- ❌ 不直观 - 违反用户期望

**风险级别:** 中 (复杂语义)
**用户影响:** 中 (意外行为,文档负担)

---

### 推荐矩阵

| 解决方案 | 一致性 | 复杂度 | 用户影响 | 可行性 |
|----------|--------|--------|----------|--------|
| **1. 阻止 DML** | ✅ 完美 | ✅ 低 | ❌ 高 (阻塞) | ✅ 高 |
| **2. 记录限制** | ❌ 差 | ✅ 无 | ⚠️ 中 (混淆) | ✅ 高 |

---

## 决策需要

**我们需要选择:**

1. **解决方案 1 (阻止 DML)** - 最安全、最简单,但用户体验差
2. **解决方案 2 (记录)** - 带已知限制发布,稍后迭代

**v1 推荐方法:**
- 从**解决方案 1 (阻止 DML)** 开始以保证正确性
- 为勇敢的用户添加配置标志以禁用阻塞
- 如果用户需求证明复杂性合理,则为 v2 计划更复杂的解决方案

---

## Protocol Buffer 定义

### 新消息类型

**文件: `pkg/v2/proto/msg.proto`**

```protobuf
message CommitImportMessageHeader {
    int64 job_id = 1;
}

message CommitImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}

message AbortImportMessageHeader {
    int64 job_id = 1;
}

message AbortImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}
```

### 新 RPC 定义

**文件: `pkg/v2/proto/data_coord.proto`**

```protobuf
service DataCoord {
    // ... 现有方法 ...

    rpc CommitImport(CommitImportRequest) returns(common.Status) {}
    rpc AbortImport(AbortImportRequest) returns(common.Status) {}
}

message CommitImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}

message AbortImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}
```

### 增强现有 Proto

**文件: `pkg/v2/proto/internal.proto`**

```protobuf
enum ImportJobStateV2 {
    None = 0;
    Pending = 1;
    PreImporting = 2;
    Importing = 3;
    Sorting = 4;
    IndexBuilding = 5;
    WaitingCommit = 6;  // 新状态
    Completed = 7;
    Failed = 8;
}

message ImportRequestInternal {
    // ... 现有字段 ...

    // 已弃用: 改用 vchannel_timestamps
    uint64 data_timestamp = 10 [deprecated=true];

    // 新: 用于 CDC checkpoint 恢复的按 vchannel 时间戳
    map<string, uint64> vchannel_timestamps = 11;
}

message ImportJob {
    // ... 现有字段 ...

    // 已弃用
    uint64 data_timestamp = 10 [deprecated=true];

    // 新: 按 vchannel 时间戳
    map<string, uint64> vchannel_timestamps = 11;
}
```

---

## RPC 实现

### CommitImport RPC

**文件: `internal/datacoord/server.go`**

```go
// CommitImport 提交 import job,使导入的 segment 在所有复制集群上可查询。
// 只能在此集群的 job 处于 WaitingCommit 状态时调用。
// 通过 CDC 向所有 vchannel 广播 CommitImportMessage。
//
// 重要: 此 RPC 不验证从集群状态。
// 用户必须在调用此 RPC 前手动确保所有集群处于 WaitingCommit。
// 在每个集群上调用 GetImportProgress 以验证就绪状态。
func (s *Server) CommitImport(ctx context.Context, req *datapb.CommitImportRequest) (*commonpb.Status, error) {
    log := log.Ctx(ctx).With(zap.String("job_id", req.GetJobId()))

    // 1. 验证请求
    if req.GetJobId() == "" {
        return merr.Status(merr.WrapErrParameterInvalidMsg("job_id 是必需的")), nil
    }

    // 2. 从 meta 获取 import job
    job := s.importMeta.GetJob(req.GetJobId())
    if job == nil {
        return merr.Status(merr.WrapErrImportJobNotExist(req.GetJobId())), nil
    }

    // 3. 验证此集群的 job 状态是 WaitingCommit
    // 注意: 不检查从集群 - 用户职责
    if job.GetState() != internalpb.ImportJobStateV2_WaitingCommit {
        return merr.Status(merr.WrapErrImportFailed(
            fmt.Sprintf("Job %s 处于状态 %s,期望 WaitingCommit",
                req.GetJobId(), job.GetState()))), nil
    }

    // 4. 向所有 vchannel 广播 CommitImportMessage (包括通过 CDC 的从集群)
    log.Info("广播 commit import 消息")
    err := s.broadcastCommitImport(ctx, job.GetJobID(), job.GetCollectionID(), job.GetVchannels())
    if err != nil {
        log.Error("广播 commit import 失败", zap.Error(err))
        return merr.Status(err), nil
    }

    log.Info("Commit import 消息广播成功")
    return merr.Success(), nil
}
```

### AbortImport RPC

```go
// AbortImport 中止 import job,标记为 Failed 并清理 segment。
// 可以在除 Completed 外的任何状态下调用。
// 通过 CDC 向所有 vchannel 广播 AbortImportMessage。
func (s *Server) AbortImport(ctx context.Context, req *datapb.AbortImportRequest) (*commonpb.Status, error) {
    log := log.Ctx(ctx).With(zap.String("job_id", req.GetJobId()))

    // 1. 验证请求
    if req.GetJobId() == "" {
        return merr.Status(merr.WrapErrParameterInvalidMsg("job_id 是必需的")), nil
    }

    // 2. 从 meta 获取 import job
    job := s.importMeta.GetJob(req.GetJobId())
    if job == nil {
        return merr.Status(merr.WrapErrImportJobNotExist(req.GetJobId())), nil
    }

    // 3. 验证状态 - 可以中止除终止状态外的任何状态
    state := job.GetState()
    if state == internalpb.ImportJobStateV2_Completed {
        return merr.Status(merr.WrapErrImportFailed(
            fmt.Sprintf("Job %s 已完成,无法中止", req.GetJobId()))), nil
    }
    if state == internalpb.ImportJobStateV2_Failed {
        log.Info("Job 已失败,abort 是空操作(幂等)")
        return merr.Success(), nil
    }

    // 4. 向所有 vchannel 广播 AbortImportMessage
    log.Info("广播 abort import 消息", zap.String("state", state.String()))
    err := s.broadcastAbortImport(ctx, job.GetJobID(), job.GetCollectionID(), job.GetVchannels())
    if err != nil {
        log.Error("广播 abort import 失败", zap.Error(err))
        return merr.Status(err), nil
    }

    log.Info("Abort import 消息广播成功")
    return merr.Success(), nil
}
```

---

## 按 VChannel TimeTick 支持

### 问题陈述

当前代码在 `ddl_callbacks_import.go:74`:
```go
DataTimestamp: result.GetMaxTimeTick(), // TODO: 未来使用按 vchannel TimeTick,CDC 必须支持。
```

**问题:** 对所有 vchannel 使用 `MaxTimeTick` 破坏了 CDC 的按通道 checkpoint 恢复语义。当从集群重启时,CDC 需要在每个 vchannel 上独立从确切位置恢复。

### 解决方案

在 ImportJob 中存储按 vchannel 时间戳而不是单个 MaxTimeTick。

**代码变更: `internal/datacoord/ddl_callbacks_import.go`**

```go
func (c *DDLCallbacks) importV1AckCallback(ctx context.Context, result message.BroadcastResultImportMessageV1) error {
    body := result.Message.MustBody()

    // 确保 Schema.DbName 已填充
    if body.Schema != nil {
        body.Schema.DbName = body.DbName
    }

    // 从广播结果构建按 vchannel 时间戳 map
    vchannelTimestamps := make(map[string]uint64)
    vchannels := make([]string, 0, len(result.Results))
    for vchannel, br := range result.Results {
        if funcutil.IsControlChannel(vchannel) {
            continue
        }
        vchannels = append(vchannels, vchannel)
        vchannelTimestamps[vchannel] = br.TimeTick  // 按 vchannel 而不是 max
    }

    importResp, err := c.createImportJobFromAck(ctx, &internalpb.ImportRequestInternal{
        DbID:               0, // deprecated
        CollectionID:       body.GetCollectionID(),
        CollectionName:     body.GetCollectionName(),
        PartitionIDs:       body.GetPartitionIDs(),
        ChannelNames:       vchannels,
        Schema:             body.GetSchema(),
        Files:              convertFiles(body.GetFiles()),
        Options:            funcutil.Map2KeyValuePair(body.GetOptions()),
        VchannelTimestamps: vchannelTimestamps,  // 新: 按 vchannel map
        JobID:              body.GetJobID(),
    })

    return merr.CheckRPCCall(importResp, err)
}
```

**影响:**
- ImportJob 存储 `map<string, uint64> vchannel_timestamps` 而不是单个 `data_timestamp`
- 创建 segment 时,使用特定 vchannel 的时间戳
- CDC checkpoint 恢复使用精确的按通道位置
- 对外部 API 无变更(对用户透明)
- `data_timestamp` 字段已弃用但为向后兼容保留

---

## ImportChecker 状态转换逻辑

### 修改的检查循环

**文件: `internal/datacoord/import_checker.go`**

```go
func (c *importChecker) checkJobs() {
    for _, job := range c.meta.GetJobBy() {
        switch job.GetState() {
        case internalpb.ImportJobStateV2_Pending:
            c.tryPreImport(job)
        case internalpb.ImportJobStateV2_PreImporting:
            c.checkPreImportResult(job)
        case internalpb.ImportJobStateV2_Importing:
            c.checkImportResult(job)
        case internalpb.ImportJobStateV2_Sorting:
            c.checkSortingResult(job)
        case internalpb.ImportJobStateV2_IndexBuilding:
            c.checkIndexBuildingResult(job)
        case internalpb.ImportJobStateV2_WaitingCommit:
            // 新状态处理
            c.checkWaitingCommitTimeout(job)
        }
    }
}
```

### IndexBuilding → WaitingCommit 转换

```go
func (c *importChecker) checkIndexBuildingResult(job *ImportJob) {
    // ... 现有 index building 检查 ...

    if allIndexesBuilt {
        // 转换到 WaitingCommit (统一对所有集群)
        err := c.meta.UpdateJobState(job.JobID, internalpb.ImportJobStateV2_WaitingCommit)
        if err != nil {
            log.Error("更新 job 到 WaitingCommit 失败",
                zap.Int64("job_id", job.JobID), zap.Error(err))
            return
        }

        log.Info("Import job 进入 WaitingCommit",
            zap.Int64("job_id", job.JobID))

        // 如果非复制集群: 自动广播 CommitImportMessage
        if !c.isReplicating() {
            log.Info("非复制集群: 自动提交 import")
            err := c.server.broadcastCommitImport(
                context.Background(),
                job.JobID,
                job.CollectionID,
                job.Vchannels)
            if err != nil {
                log.Error("自动 commit import 失败",
                    zap.Int64("job_id", job.JobID), zap.Error(err))
                // 不失败整个 job - 将重试
            }
        } else {
            log.Info("复制集群: 等待手动 CommitImport",
                zap.Int64("job_id", job.JobID))
        }
    }
}
```

### WaitingCommit 超时处理

```go
func (c *importChecker) checkWaitingCommitTimeout(job *ImportJob) {
    timeout := paramtable.Get().DataCoordCfg.ImportWaitingCommitTimeout.GetAsDuration(time.Hour)

    if time.Since(job.StateTransitionTime) > timeout {
        log.Warn("Import job 在 WaitingCommit 超时,自动中止",
            zap.Int64("job_id", job.JobID),
            zap.Duration("timeout", timeout))

        // 广播 AbortImportMessage
        err := c.server.broadcastAbortImport(
            context.Background(),
            job.JobID,
            job.CollectionID,
            job.Vchannels)
        if err != nil {
            log.Error("自动 abort import 失败",
                zap.Int64("job_id", job.JobID), zap.Error(err))
        }
    }
}
```

---

## 监控指标

| 指标名称 | 类型 | 单位 | 标签 | 描述 |
|---------|------|------|------|------|
| milvus_import_job_state_duration | Histogram | seconds | state, cluster_id | 各状态持续时间 |
| milvus_import_jobs_waiting_commit | Gauge | number | cluster_id | WaitingCommit 状态的 job 数 |
| milvus_import_commit_latency | Histogram | ms | cluster_id | CommitImport RPC 延迟 |
| milvus_import_commit_failures_total | Counter | number | cluster_id, reason | Commit 失败总数 |
| milvus_import_abort_total | Counter | number | cluster_id, reason | Abort 调用总数 |
| milvus_import_timeout_total | Counter | number | cluster_id | WaitingCommit 超时总数 |

---

## 测试策略

### 功能测试

1. **基本流程测试**
   - 非复制集群: 验证自动 commit (IndexBuilding → WaitingCommit → auto-broadcast → Completed)
   - 复制集群: 验证手动 commit (WaitingCommit → 用户调用 CommitImport → Completed)

2. **一致性测试**
   - 主从数据一致性: 在多个集群上验证导入数据相同
   - 跨集群 JobID: 验证所有集群使用相同 JobID
   - Segment 可见性: 验证 WaitingCommit 期间 segment 不可查询

3. **失败场景测试**
   - AbortImport: 测试在各状态下 abort
   - 超时: 验证 WaitingCommit 超时自动 abort
   - 部分失败: 测试一个从集群失败时的行为
   - 网络分区: 测试 CDC 消息丢失和重试

4. **写入一致性测试**
   - DELETE 在 commit 前: 验证 delete 不影响 import 数据
   - DELETE 在 commit 后: 验证 delete 正确应用
   - 混合 DML: 测试 INSERT + DELETE + UPSERT 与 import 的交互

### 性能测试

1. **延迟测试**
   - 测量 CommitImport RPC 延迟 (目标 < 1s)
   - 测量 WaitingCommit → Completed 转换时间

2. **吞吐量测试**
   - 并发多个 import job
   - 大文件导入 (GB 级别)

3. **压力测试**
   - 长时间运行 (24+ 小时)
   - 持续 DML + 定期 import

---

## 未来工作

### Phase 2 增强

1. **自动一致性验证**
   - 主集群在广播前检查所有从集群状态
   - 如果所有从集群不是 WaitingCommit 则返回错误

2. **更复杂的 DML 处理**
   - DML 重放机制(解决行重新出现问题)
   - 临时集合方法

3. **改进的可观测性**
   - 每集群状态可视化
   - 跨集群一致性仪表板
   - 自动异常检测

### 长期考虑

1. **部分导入**
   - 仅导入部分 vchannel
   - Vchannel 级协调

2. **增量导入**
   - 追加到现有 segment
   - Delta 导入支持

3. **跨区域优化**
   - 本地对象存储缓存
   - 智能 Segment 路由

---

## 参考文档

- [Milvus CDC 2.6 Design](link-to-cdc-design)
- [Streaming Service Design](link-to-streaming-design)
- [Import V2 Design](link-to-import-v2-design)

---

## 附录 A: 用户操作指南

### 在复制集群上导入数据

**前提条件:**
- 主从集群已配置 CDC 复制
- 共享对象存储访问

**步骤:**

1. **在主集群上启动导入:**
   ```bash
   curl -X POST "http://primary:19530/v2/vectordb/jobs/import/create" \
     -d '{"collectionName": "my_collection", "files": ["s3://bucket/data.parquet"]}'
   # 响应: {"jobId": "123"}
   ```

2. **监控所有集群上的进度:**
   ```bash
   # 主集群
   watch -n 5 'curl "http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=123"'

   # 从集群1
   watch -n 5 'curl "http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=123"'

   # 从集群2
   watch -n 5 'curl "http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=123"'
   ```

3. **等待所有集群达到 WaitingCommit:**
   ```bash
   # 所有集群应显示:
   {"jobId": "123", "state": "WaitingCommit", "progress": 100}
   ```

4. **提交导入(仅在主集群):**
   ```bash
   curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
     -d '{"jobId": "123"}'
   ```

5. **验证完成:**
   ```bash
   # 检查所有集群 - 应该都是 Completed
   curl "http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   curl "http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   curl "http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   ```

### 处理失败

**如果某个集群卡在 IndexBuilding:**
```bash
# 选项 1: 等待它赶上
# 选项 2: 中止并重试
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/abort" \
  -d '{"jobId": "123"}'

# 验证所有集群都已中止
# 调查失败原因
# 修复后重试导入
```

**如果 CommitImport 后检测到分歧:**
```bash
# 重试 commit (幂等)
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -d '{"jobId": "123"}'

# 如果仍然失败,联系支持人员
```

---

## 附录 B: 操作运维清单

### 部署前

- [ ] 验证所有集群具有相同的 PChannel 数量
- [ ] 确认共享对象存储访问
- [ ] 配置 `ImportWaitingCommitTimeout` (默认 1 小时)
- [ ] 设置监控告警

### 操作期间

- [ ] 监控 `milvus_import_jobs_waiting_commit` 指标
- [ ] 检查 `milvus_import_timeout_total` 以查看过期的 job
- [ ] 验证 CDC 复制延迟 < 预期阈值
- [ ] 确保对象存储有足够容量

### 故障排查

**症状: 从集群卡在 IndexBuilding**
- 检查对象存储访问
- 验证从集群资源 (CPU/内存)
- 检查 QueryNode 日志以查找索引构建错误

**症状: CommitImport 超时**
- 检查网络连接
- 验证 WAL 服务健康
- 检查 DataCoord 日志以查找广播错误

**症状: 数据不一致**
- 验证 JobID 在所有集群上相同
- 检查 CDC 复制状态
- 比较 segment 元数据 (visible_timestamp)

---

**文档结束**
