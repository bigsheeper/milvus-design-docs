# Milvus 复制场景下的 Import 设计方案

**日期:** 2026-03-13
**作者:** Design Collaboration Session
**状态:** Draft v1.5

---

## 一、背景与目标

### 1.1 背景

为了支持 Milvus 集群间的主备容灾和跨区域数据复制，Milvus 2.6 引入了基于 CDC (Change Data Capture) 的复制机制。然而，当前版本中 Import 操作与 CDC 复制功能互斥 —— 当集群启用主备复制时，Import 操作会被明确阻止 (`internal/datacoord/ddl_callbacks_import.go:121-123`):

```go
// Import in replicating cluster is not supported yet
if channelAssignment.ReplicateConfiguration != nil &&
   len(channelAssignment.ReplicateConfiguration.GetClusters()) > 1 {
    return merr.WrapErrImportFailed("import in replicating cluster is not supported yet")
}
```

该限制严重影响企业级数据迁移和批量导入场景，尤其是在需要保持主备集群持续同步的生产环境中。

### 1.2 核心目标

本设计方案旨在解除 Import 与 CDC 复制的互斥限制，在保持数据一致性的前提下，实现以下核心目标:

1. **解除复制阻塞** - 在 CDC 复制激活状态下允许 Import 操作执行
2. **强一致性保证** - 跨集群导入数据可见性的原子性语义（All-or-nothing）
3. **显式协调机制** - 通过 RPC 接口显式控制导入数据的可见性切换
4. **CDC 兼容性** - 基于 per-vchannel TimeTick 实现精确的 Checkpoint 恢复语义
5. **架构简洁性** - 复用现有 Broadcast 和 CDC 消息传播机制

### 1.3 非目标

- 主备集群间的自动协调
- 在从集群(Follower)上执行 Import
- 破坏向后兼容性的 API 变更

---

## 二、问题场景分析

当 Import 数据在不同集群以不同时间完成时，会导致主备数据不一致。考虑以下场景：

```
主集群：
T=1000: 主集群执行 Import
        → ImportMessage 广播，开始导入数据
        → 导入数据: (pk=1,2,3,......)
        → 导入数据的 row.timestamp=T1000
        → 数据处于 Uncommitted 状态（不可查询）

T=20000: 主集群 Import 完成
         → 数据变为 Completed 状态
         → 数据变为可查询
         → 用户查询: pk=1,2,3 都可见

T=50000: 用户在主集群执行 DELETE pk=2
         → DELETE 消息写入 WAL，delete.timestamp=T50000
         → 主集群根据时间戳逻辑应用删除
         → 由于 row.timestamp=T1000 < delete.timestamp=T50000
         → pk=2 被删除
         → 用户查询: 只有 pk=1, pk=3 可见

从集群：
T=1000: 从集群通过 CDC 接收到 ImportMessage
        → 开始导入相同的数据
        → 数据处于 Uncommitted 状态

T=30000: 从集群 Import 完成（晚于主集群）
         → 数据变为 Completed 状态
         → 数据变为可查询
         → 用户查询: pk=1,2,3 都可见

T=61000: 从集群通过 CDC 收到 DELETE 消息 (delete.timestamp=T50000)
         → 从集群根据时间戳逻辑应用删除
         → 由于 row.timestamp=T1000 < delete.timestamp=T50000
         → pk=2 被删除
         → 用户查询: 只有 pk=1, pk=3 可见

结果：主备数据一致性依赖于 Import 完成时间
      - 若 Import 在 DELETE 之前完成：数据被正确删除
      - 若 Import 在 DELETE 之后完成：数据无法被删除
      - 主备集群的 Import 完成时间不同 → 主备数据不一致
```

**核心问题**：Import 数据使用 `row.timestamp` 作为数据可见性判断的唯一标准，而 `row.timestamp` 是 ImportMessage 广播时间，不能反映数据的真实可见时间（Import 完成时间），导致跨集群一致性无法保证。

---

## 三、解决方案

### 3.1 核心思路

**问题本质**：跨集群物理执行顺序不可控（CDC 延迟、Compaction 触发时机差异）导致数据可见性不一致。

**解决思路**：引入事务时间基准，解耦物理执行与数据可见性。

**核心机制**：两阶段提交协议（Two-Phase Commit Protocol）
- **Prepare 阶段**：各集群独立执行物理数据准备（写入、索引构建）
- **Commit 阶段**：统一逻辑可见时间，所有集群原子地切换数据可见性

### 3.2 三个关键设计

#### 3.2.1 状态扩展：Uncommitted 与 Committing

**新增两个状态**：
- **Uncommitted**：数据物理准备完成，等待 Commit
- **Committing**：CommitImportMessage 已广播，等待所有 vchannel 处理完成

**Uncommitted 状态**

**作用**：将"物理准备完成"与"数据可见"解耦。

**特征**：
- Segment 已写入对象存储并完成索引构建
- 标记 `importing=true`，查询不可见
- Job 元数据持久化在 etcd

**Committing 状态**

**作用**：协调多 vchannel 场景下的 Commit 操作。

**触发**：平台侧调用 CommitImport RPC 后，广播 CommitImportMessage 到所有 vchannel。

**特征**：
- CommitImport RPC 立即返回成功（< 100ms）
- 后台异步等待所有 vchannel 确认完成
- DML 消息继续正常处理，不阻塞

**状态转换**：
```
IndexBuilding → Uncommitted → Committing → Completed
                ↓
    收到 AbortImportMessage → Failed
```

**触发机制**：
- **复制集群**：等待平台侧调用 CommitImport RPC
- **非复制集群**：自动广播 CommitImportMessage（向后兼容）

#### 3.2.2 commit_timestamp（Commit Time）

**问题**：`row.timestamp` 是 Write Time（数据写入存储的时间），各集群不同。

**方案**：`segment.commit_timestamp` 是 Commit Time（数据事务提交的时间），所有集群相同。

**时间戳系统**：

| 时间戳类型 | 存储位置 | 语义 | 使用场景 |
|-----------|---------|------|----------|
| **Write Time**<br>`row.timestamp` | Binlog 行数据 | 数据写入对象存储的时间 | 存储层操作、Compaction 重写 |
| **Commit Time**<br>`commit_timestamp` | SegmentInfo 元数据 | 数据事务提交的时间 | 时序判断、DML 过滤、CDC、GC |

**使用规则**：所有涉及时序判断的逻辑使用 effective timestamp：

```go
func GetEffectiveTimestamp(segment *SegmentInfo, rowTimestamp uint64) uint64 {
    if segment.CommitTimestamp != 0 {
        return segment.CommitTimestamp  // Import segment: 使用 Commit Time
    }
    return rowTimestamp  // 正常 segment: 使用 Write Time
}
```

**生命周期**：
1. **Import 阶段**：`commit_timestamp = 0`, `importing = true`（数据不可见）
2. **Commit 阶段**：`commit_timestamp = T_commit`, `importing = false`（数据可见）
3. **Compaction 后**：`commit_timestamp = 0`（row.timestamp 已标准化）

#### 3.2.3 显式 Commit 控制

**CommitImport RPC**：平台侧确认所有集群就绪后显式触发提交。

**广播机制**：CommitImportMessage 通过 Collection 的所有 vchannel 传播，CDC 链路复制到从集群。

**处理流程**：
1. **Uncommitted → Committing**：CommitImport RPC 广播消息，立即返回
2. **Committing → Completed**：所有 vchannel 处理完成后转换

**vchannel 本地原子操作**：
- 更新元数据：`segment.commit_timestamp = T_commit`
- 清除标记：`segment.importing = false`
- 确认完成：标记该 vchannel 已处理

### 3.3 方案如何解决一致性问题

**问题场景（无 commit_timestamp）：**

```
主集群：
T=1000: 主集群执行 Import
        → ImportMessage 广播，开始导入数据
        → 导入数据: (pk=1,2,3,......)
        → 导入数据的 row.timestamp=T1000

T=2000: 主集群执行 DELETE pk=2
        → DELETE 消息写入 WAL，时间戳 ts=2000
......

T=50000: 主集群 Import 完成
        → 导入数据可见，可见时间=50000

T=51000: 主集群触发 L0Compaction
        → pk=2 Deleted!

备集群：
T=1000: 备集群通过 CDC 接收到 ImportMessage
        → ImportMessage 广播，开始导入数据
        → 导入数据: (pk=1,2,3,......)
        → 导入数据的 row.timestamp=T1000

T=2000: 备集群通过 CDC 收到 DELETE 消息 (ts=2000)
        → DELETE 消息写入 WAL，时间戳 ts=2000
......

T=60000: 备集群触发 L0Compaction
        → Import 还未完成
        → pk=2 NOT Deleted!

T=61000: 备集群 Import 完成
        → 导入数据可见，可见时间=61000
        → pk=2 Imported!

结果:
主备数据不一致!
```

**方案解决（引入 commit_timestamp）：**

```
主集群：
T=1000: Import 执行，row.timestamp=1000（Write Time）
        → Uncommitted, segment.commit_timestamp=0
T=2000: DELETE pk=2, delete.ts=2000
T=3000: CommitImport 广播，segment.commit_timestamp=3000（Commit Time）
        → 系统时序判断: effective_ts (3000) > delete.ts (2000)
        → pk=2 可见 ✓

备集群：
T=1000: Import 执行，row.timestamp=1000（Write Time）
        → Uncommitted, segment.commit_timestamp=0
T=2000: DELETE pk=2, delete.ts=2000
T=3000: CommitImport 广播，segment.commit_timestamp=3000（Commit Time，与主集群相同）
        → 系统时序判断: effective_ts (3000) > delete.ts (2000)
        → pk=2 可见 ✓

结果:
主备数据一致！
```

**解决本质**：引入 `commit_timestamp` 作为系统级 Commit Time，所有集群使用统一的 commit_timestamp 进行时序判断，消除物理执行顺序（如 Compaction 触发时机）差异导致的不一致。

### 3.4 多 vchannel 一致性处理

**问题**：Import job 跨多个 vchannel 时，如何协调 CommitImportMessage 的处理？

**核心挑战**：
1. **原子性问题**：v1 处理完就标记 job=Completed，v2/v3 还在处理 → job 状态不一致
2. **DML 消费问题**：commit 期间是否阻塞 DML？阻塞影响吞吐，不阻塞如何保证写一致性？
3. **写一致性问题**：v1 已设置 commit_timestamp，v2 还没设置，DELETE 操作在两个 vchannel 上行为是否一致？

**方案：Committing 状态 + 异步等待 + 不阻塞 DML**

Committing 状态（已在 3.2.1 引入）在这里解决多 vchannel 协调问题：
- **CommitImport RPC 立即返回**：不等待所有 vchannel，直接返回成功
- **Background checker 异步监控**：定期检查所有 vchannel 是否确认完成，全部完成后转到 Completed

**完整流程**：

```
┌─────────────────────────────────────────────────────────────┐
│ T1: 平台侧调用 CommitImport RPC                             │
│     DataCoord 处理：                                        │
│     ├─ 初始化跟踪：{v1: false, v2: false, v3: false}       │
│     ├─ 状态转换：Uncommitted → Committing               │
│     ├─ 广播 CommitImportMessage 到所有 vchannel           │
│     └─ 立即返回 success ✅ (< 100ms)                       │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ↓                   ↓                   ↓
  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
  │ vchannel v1 │     │ vchannel v2 │     │ vchannel v3 │
  │             │     │             │     │             │
  │ T2: 收到消息│     │ T3: 收到消息│     │ T4: 收到消息│
  │ 设置 commit │     │ (延迟)      │     │ (延迟)      │
  │ _timestamp  │     │ 设置 commit │     │ 设置 commit │
  │ 标记确认 ✅ │     │ _timestamp  │     │ _timestamp  │
  │             │     │ 标记确认 ✅ │     │ 标记确认 ✅ │
  │ 继续消费DML │     │ 继续消费DML │     │ 继续消费DML │
  └─────────────┘     └─────────────┘     └─────────────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              ↓
          ┌───────────────────────────────────────┐
          │ T5: Background Checker 检测           │
          │     AllVChannelsCommitted? → true     │
          │     状态转换：Committing → Completed  │
          └───────────────────────────────────────┘
```

**关键点**：
- **DML 不阻塞**：各 vchannel 独立处理 CommitImportMessage，不阻塞后续 DML 消费
- **写一致性保证**：
  - 已设置 commit_timestamp 的 vchannel：DELETE 判断使用 Commit Time
  - 未设置的 vchannel：Segment 仍标记 importing=true，DELETE 不可见该数据
  - 最终所有 vchannel 设置完成后，行为一致
- **Job 状态一致性**：通过 Committing 中间状态避免过早标记 Completed

**实现要点**：
- **异步转换检查**：DataCoord 后台 goroutine 定期扫描 Committing 状态的 job
- **vchannel 确认机制**：每个 vchannel 处理完成后更新 etcd 确认标记
- **状态转换条件**：所有 vchannel 确认完成 → Committing → Completed

### 3.5 完整流程

#### 3.5.1 复制集群导入流程

**关键步骤**：
1. **Prepare**：各集群执行 Import → Uncommitted
2. **Coordination**：平台侧轮询确认所有集群状态
3. **Commit**：广播 CommitImportMessage → 统一可见

**复制集群导入流程图**：

```
┌──────────────────────────────────────────────────────────────────┐
│                         主集群 (Primary)                          │
│                                                                    │
│  平台侧调用: ImportV2(jobID, files, options)                      │
│         ↓                                                          │
│  DataCoord 验证请求，生成 JobID                                   │
│         ↓                                                          │
│  广播 ImportMessage(jobID, files) → 所有 vchannel                │
│         ↓                                                          │
│  DataNode 执行: 读取对象存储 → 写入 binlog (row.ts=T_import)     │
│         ↓                                                          │
│  Job 状态: Pending → Importing → IndexBuilding → Uncommitted   │
│         ↓                                                          │
│  segment.importing=true, commit_timestamp=0 (数据不可见)          │
│         ↓                                                          │
│  [等待平台侧调用 CommitImport]                                     │
│         ↓                                                          │
│  平台侧调用: CommitImport(jobID)                                  │
│         ↓                                                          │
│  广播 CommitImportMessage(jobID, ts=T_commit) → 所有 vchannel    │
│         ↓                                                          │
│  原子更新: segment.commit_timestamp=T_commit, importing=false     │
│         ↓                                                          │
│  Job 状态: Uncommitted → Completed                              │
└──────────────────────────────────────────────────────────────────┘
                           │
                           │ CDC 链路传播
                           ↓
┌──────────────────────────────────────────────────────────────────┐
│                       从集群 (Secondaries)                        │
│                                                                    │
│  接收 ImportMessage(jobID, files) ← CDC 链路                     │
│         ↓                                                          │
│  DataCoord 创建相同 JobID 的 Import Job                          │
│         ↓                                                          │
│  DataNode 执行: 读取对象存储 → 写入 binlog (row.ts=T_import)     │
│         ↓                                                          │
│  Job 状态: Pending → Importing → IndexBuilding → Uncommitted   │
│         ↓                                                          │
│  segment.importing=true, commit_timestamp=0 (数据不可见)          │
│         ↓                                                          │
│  [等待 CommitImportMessage]                                        │
│         ↓                                                          │
│  接收 CommitImportMessage(jobID, ts=T_commit) ← CDC 链路         │
│         ↓                                                          │
│  原子更新: segment.commit_timestamp=T_commit, importing=false     │
│         ↓                                                          │
│  Job 状态: Uncommitted → Completed                              │
└──────────────────────────────────────────────────────────────────┘
```

#### 3.5.2 非复制集群导入流程（向后兼容）

**自动提交机制**：ImportChecker 检测到非复制配置，自动广播 CommitImportMessage。

**流程图**：

```
┌──────────────────────────────────────────────────────────────────┐
│                      非复制集群 (Standalone)                      │
│                                                                    │
│  平台侧调用: ImportV2(jobID, files, options)                      │
│         ↓                                                          │
│  Job 状态: Pending → Importing → IndexBuilding → Uncommitted   │
│         ↓                                                          │
│  ImportChecker 检测非复制配置                                     │
│         ↓                                                          │
│  自动广播 CommitImportMessage(jobID, ts=T_commit)                │
│         ↓                                                          │
│  原子更新: segment.commit_timestamp=T_commit, importing=false     │
│         ↓                                                          │
│  Job 状态: Uncommitted → Completed (自动，无需平台侧干预)      │
└──────────────────────────────────────────────────────────────────┘
```

用户观察到的行为：IndexBuilding → Completed（平滑过渡，无感知）

### 3.6 技术实现细节

#### 3.6.1 commit_timestamp 元数据定义

```protobuf
message SegmentInfo {
    // ... 现有字段 ...

    // commit_timestamp: Segment 的 Commit Time（事务提交时间戳）
    //
    // 【核心概念】
    // 两层时间戳系统：
    //   - row.timestamp (Write Time)      = 数据写入存储的时间
    //   - commit_timestamp (Commit Time)  = 数据事务提交的时间
    //
    // 【使用规则】
    // 系统中任何涉及以下判断的逻辑，都必须使用 commit_timestamp（如果非零）：
    //   1. 时序比较：数据是否在某个时间点"存在"
    //   2. 因果关系：操作 A 是否发生在操作 B 之前/之后
    //   3. 一致性：跨集群的时间戳一致性判断
    //   4. 过滤逻辑：DML 是否应该影响某行数据
    //
    // 【影响的组件】
    //   - QueryNode: DML 过滤、时间旅行查询、一致性快照
    //   - DataCoord: Segment 时间范围管理、Compaction 触发、GC 决策
    //   - CDC/Replication: Checkpoint 计算、复制进度判断
    //   - 监控/诊断: Segment 时间范围展示、延迟统计
    //
    // 【生命周期】
    //   1. Import 执行阶段: commit_timestamp = 0, importing = true (不可查询)
    //   2. Commit 提交阶段: commit_timestamp = T_commit, importing = false (可查询)
    //   3. Compaction 标准化阶段: 重写 row.timestamp = commit_timestamp, 清除 commit_timestamp = 0
    //
    // 【重要说明】
    //   - commit_timestamp = 0: Write Time 即 Commit Time（正常 segment）
    //   - commit_timestamp != 0: Commit Time 覆盖 Write Time（import segment）
    optional uint64 commit_timestamp = X;
}
```

**Effective Timestamp 计算**：

```go
func GetEffectiveTimestamp(segment *SegmentInfo, rowTimestamp uint64) uint64 {
    if segment.CommitTimestamp != 0 {
        return segment.CommitTimestamp  // Import segment: 使用 Commit Time
    }
    return rowTimestamp  // 正常 segment: 使用 Write Time
}
```

#### 3.6.2 RPC 接口

**CommitImport RPC**：

```protobuf
rpc CommitImport(CommitImportRequest) returns(common.Status) {}

message CommitImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}
```

- **功能**：提交处于 Uncommitted 状态的 Import Job
- **约束**：仅在主集群调用；Job 必须处于 Uncommitted 状态；幂等操作
- **执行**：广播 CommitImportMessage → 各集群原子更新元数据

**AbortImport RPC**：

```protobuf
rpc AbortImport(AbortImportRequest) returns(common.Status) {}

message AbortImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}
```

- **功能**：中止 Import Job，清理所有集群数据和元数据
- **约束**：仅在主集群调用；可在任何非终止状态调用；幂等操作
- **执行**：广播 AbortImportMessage → 标记 Segment Dropped → 触发 GC

#### 3.6.3 消息类型

**CommitImportMessage**：

```protobuf
message CommitImportMsg {
    commonpb.MsgBase base = 1;  // 包含 T_commit（Commit Time）
    int64 job_id = 2;
}
```

广播目标：Collection 的所有 vchannel，通过 CDC 链路传播至从集群。

**AbortImportMessage**：

```protobuf
message AbortImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}
```

广播目标：Collection 的所有 vchannel，通过 CDC 链路传播至从集群。

#### 3.6.4 组件改造

| 组件 | 改造内容 | 复杂度 |
|------|----------|--------|
| **Meta 存储** | • SegmentInfo proto 新增 `commit_timestamp` 字段<br>• 引入两层时间戳系统（Write Time + Commit Time） | 低 |
| **DataCoord** | • 新增 Uncommitted/Committing 状态处理<br>• 实现 CommitImport/AbortImport RPC<br>• 自动提交逻辑（非复制集群）<br>• Commit 时设置 segment.commit_timestamp | 中 |
| **QueryNode** | • DML 过滤逻辑使用 effective_timestamp<br>• 时间旅行查询使用 effective_timestamp<br>• 一致性快照使用 effective_timestamp | 中 |
| **ImportChecker** | • Uncommitted 状态检查<br>• 自动提交判断逻辑<br>• 多 vchannel 确认状态跟踪 | 低 |
| **Compaction** | • 标准化 import segment 时间戳（重写 row.timestamp）<br>• 清除 commit_timestamp 元数据 | 低 |
| **CDC/Replication** | • Checkpoint 计算使用 commit_timestamp<br>• 复制进度判断使用 commit_timestamp | 低 |
| **Proto 定义** | • 新增消息类型<br>• 新增 RPC 定义<br>• 状态枚举扩展 | 低 |

---

## 四、平台侧工作流

### 4.1 复制集群导入数据流程

#### 步骤 1: 启动导入（主集群）

```bash
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/create" \
  -H "Content-Type: application/json" \
  -d '{
    "collectionName": "my_collection",
    "files": ["s3://bucket/data.parquet"]
  }'

# 响应示例:
{
  "status": {"error_code": "Success"},
  "jobId": "job-123456"
}
```

**说明:** ImportMessage 会自动通过 CDC 复制到所有从集群。

#### 步骤 2: 监控所有集群进度

使用 GetImportProgress API 监控各集群状态：

```bash
# 主集群
watch -n 5 'curl "http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=job-123456"'

# 从集群 1
watch -n 5 'curl "http://secondary-1:19530/v2/vectordb/jobs/import/get_progress?jobId=job-123456"'

# 从集群 2
watch -n 5 'curl "http://secondary-2:19530/v2/vectordb/jobs/import/get_progress?jobId=job-123456"'
```

**等待所有集群都达到 Uncommitted 状态：**

```json
{
  "jobId": "job-123456",
  "state": "Uncommitted",
  "progress": 100
}
```

#### 步骤 3: 提交导入（主集群）

**重要:** 只有在所有集群都处于 Uncommitted 时才能提交！

```bash
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -H "Content-Type: application/json" \
  -d '{
    "jobId": "job-123456"
  }'

# 响应示例:
{
  "status": {"error_code": "Success"}
}
```

#### 步骤 4: 验证完成

检查所有集群是否都转换为 Completed 状态：

```bash
# 所有集群应该返回:
{
  "jobId": "job-123456",
  "state": "Completed",
  "progress": 100,
  "importedRows": 1000000
}
```

### 4.2 处理异常情况

#### 情况 1: 某个从集群卡在 IndexBuilding

**原因:** 可能是资源不足、对象存储访问慢等。

**处理:**

```bash
# 选项 A: 等待该集群赶上（推荐）
# 继续监控，通常会自动恢复

# 选项 B: 中止并重试
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/abort" \
  -d '{"jobId": "job-123456"}'

# 验证所有集群都转换为 Failed
# 调查失败原因并修复
# 重新发起 import
```

#### 情况 2: CommitImport 后发现某个从集群仍是 Uncommitted

**原因:** 网络问题导致 CommitImportMessage 丢失。

**处理:**

```bash
# 重试 CommitImport（幂等操作）
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -d '{"jobId": "job-123456"}'

# 再次检查所有集群状态
```

### 4.3 非复制集群导入流程

对于没有启用 CDC 复制的集群，操作流程与之前完全一致：

```bash
# 1. 启动导入
curl -X POST "http://standalone:19530/v2/vectordb/jobs/import/create" \
  -d '{"collectionName": "my_collection", "files": ["s3://bucket/data.parquet"]}'

# 2. 监控进度（Uncommitted 状态会自动跳过）
watch -n 5 'curl "http://standalone:19530/v2/vectordb/jobs/import/get_progress?jobId=xxx"'

# 3. 等待完成（无需手动 commit）
# 响应: {"state": "Completed", ...}
```

### 4.4 平台侧操作检查清单

**导入前检查:**

- [ ] 确认所有集群可以访问共享对象存储
- [ ] 验证 CDC 复制链路正常
- [ ] 检查各集群资源充足（CPU、内存、磁盘）

**导入期间监控:**

- [ ] 定期检查所有集群的 job 状态
- [ ] 监控 CDC 复制延迟
- [ ] 关注 DataCoord 和 QueryNode 日志

**提交前确认:**

- [ ] 所有集群的 job 状态都是 Uncommitted
- [ ] 所有集群的进度都是 100%
- [ ] 检查 importedRows 数量一致

**提交后验证:**

- [ ] 所有集群的 job 状态都是 Completed
- [ ] 在各集群上查询数据，验证一致性
- [ ] 检查监控指标无异常

---

## 五、测试策略

### 5.1 功能测试

#### 7.1.1 基本流程测试

**测试用例 1: 非复制集群自动提交**

```
目标: 验证非复制集群的向后兼容性
步骤:
  1. 在非复制集群上启动 import
  2. 监控状态转换: Pending → Importing → IndexBuilding → Uncommitted → Completed
  3. 验证 Uncommitted 状态立即自动转换为 Completed（无用户干预）
  4. 验证数据可查询
验收:
  - 用户无需调用 CommitImport
  - 行为与旧版本一致
```

**测试用例 2: 复制集群手动提交（正常路径）**

```
目标: 验证两阶段提交的完整流程
配置: 1 主集群 + 2 从集群
步骤:
  1. 在主集群启动 import
  2. 监控所有集群状态，等待都达到 Uncommitted
  3. 验证 Uncommitted 状态下数据不可查询
  4. 在主集群调用 CommitImport
  5. 验证所有集群转换为 Completed
  6. 验证所有集群数据可查询且一致
验收:
  - 所有集群使用相同 JobID
  - 所有集群同时完成提交
  - 数据一致性 100%
```

#### 7.1.2 一致性测试

**测试用例 3: DELETE 在 commit 前（语义正确性）**

```
目标: 验证 commit_timestamp 正确处理 commit 前的 DELETE
步骤:
  1. Import 数据: (pk=1, field="A"), (pk=2, field="B"), (pk=3, field="C")
  2. 等待达到 Uncommitted（数据隐藏）
  3. 执行 DELETE pk=2
  4. 验证查询无结果（数据隐藏）
  5. CommitImport
  6. 查询数据
验收:
  - Commit 后查询到 pk=1, pk=2, pk=3（DELETE 不生效）
  - 主备集群结果一致
```

**测试用例 4: DELETE 在 commit 后（语义正确性）**

```
目标: 验证 commit 后的 DELETE 正确应用
步骤:
  1. Import 数据: (pk=1, field="A"), (pk=2, field="B"), (pk=3, field="C")
  2. 等待 Uncommitted 并 CommitImport
  3. 验证查询到 3 条数据
  4. 执行 DELETE pk=2
  5. 查询数据
验收:
  - 删除后只查询到 pk=1, pk=3
  - 主备集群结果一致
```

**测试用例 5: 跨集群时间戳一致性**

```
目标: 验证主备集群使用相同的 commit_timestamp
步骤:
  1. 在主集群 import
  2. 在主集群 DELETE pk=2（T=2000）
  3. CommitImport（T=3000）
  4. 等待从集群接收 CommitImportMessage 和 DELETE 消息（顺序可能不同）
  5. 在主集群和所有从集群上查询
验收:
  - 所有集群的 segment.commit_timestamp 相同
  - 所有集群的查询结果相同（pk=2 可见）
```

#### 7.1.3 失败场景测试

**测试用例 6: AbortImport**

```
目标: 验证中止操作正确清理
步骤:
  1. Import 到 Uncommitted
  2. 调用 AbortImport
  3. 验证所有集群转换为 Failed
  4. 验证数据不可查询
  5. 验证 segment 被清理
验收:
  - Job 状态为 Failed
  - 数据完全不可见
  - etcd 元数据清理
```

**测试用例 7: 从集群未就绪时提交（平台操作错误）**

```
目标: 验证过早提交的行为
配置: 1 主 + 2 从
步骤:
  1. Import，主集群和从集群1 到达 Uncommitted
  2. 从集群2 人为延迟（断网或暂停进程），仍在 IndexBuilding
  3. 用户错误地调用 CommitImport（未检查从集群2）
  4. 观察各集群状态
验收:
  - 主集群和从集群1 转换为 Completed
  - 从集群2 收到 CommitImportMessage 时：
    * 当前设计：记录错误，丢弃消息
    * 最终超时并 Failed
  - 出现主备不一致（符合预期 - 用户错误）
```

**测试用例 8: 网络分区和消息重传**

```
目标: 验证消息丢失和重试机制
步骤:
  1. Import 到 Uncommitted
  2. 模拟从集群2网络故障
  3. 主集群 CommitImport（从集群2 收不到消息）
  4. 主集群和从集群1 转换为 Completed
  5. 发现从集群2 仍是 Uncommitted
  6. 恢复网络
  7. 重试 CommitImport
验收:
  - 重试后从集群2 转换为 Completed
  - 幂等性：重复 commit 不会导致错误
  - 最终所有集群一致
```

### 5.2 性能测试

**测试用例 9: 延迟测试**

```
目标: 验证 CommitImport 的延迟
配置: 1 主 + 3 从
数据: 1 GB 数据（100万行）
测量:
  - CommitImport RPC 响应时间
  - 从 Uncommitted 到 Completed 的转换时间
  - CDC 消息传播延迟
目标:
  - RPC 响应 < 1s
  - 状态转换 < 5s
```

**测试用例 10: 大规模并发导入**

```
目标: 验证多个 import job 并发
配置: 1 主 + 2 从
数据: 10 个并发 job，每个 500MB
步骤:
  1. 同时启动 10 个 import
  2. 监控所有 job 到达 Uncommitted
  3. 逐个或批量 CommitImport
  4. 验证所有 job 成功完成
验收:
  - 无资源竞争导致的失败
  - 所有 job 最终一致
```

**测试用例 11: 长时间压力测试**

```
目标: 验证稳定性
配置: 1 主 + 2 从
持续时间: 24 小时
操作:
  - 持续 DML 操作（INSERT/DELETE）
  - 每 30 分钟执行一次 import
负载:
  - DML QPS: 1000
  - Import 频率: 每次 100MB
验收:
  - 无内存泄漏
  - 无 job 卡死
  - 数据一致性保持
```

### 5.3 兼容性测试

**测试用例 12: 版本升级**

```
目标: 验证从旧版本升级
场景:
  - 旧版本: 不支持 import in replication
  - 新版本: 支持 Uncommitted
步骤:
  1. 旧版本集群运行 import（非复制）
  2. 滚动升级到新版本
  3. 启用 CDC 复制
  4. 执行 import with replication
验收:
  - 升级过程无数据丢失
  - 新功能正常工作
  - 非复制模式仍正常（向后兼容）
```

---

## 六、已知限制与后续优化

### 8.1 当前版本的限制

#### 限制 1: 无自动一致性验证

**问题描述:**
CommitImport RPC 执行时不会跨集群验证所有从集群的 Job 状态。若在从集群未达到 Uncommitted 状态时调用该 RPC，将导致跨集群数据可见性不一致。

**系统行为:**
- 主集群执行提交，从集群接收到 CommitImportMessage 时仍处于 Importing/IndexBuilding 状态
- 从集群丢弃该消息（状态不匹配），继续执行 Import
- 最终主集群数据可见，从集群 Job 因超时或其他原因失败

**缓解措施:**
- 提供标准化的操作流程文档和状态检查脚本
- 在可观测性系统中实现跨集群状态聚合视图
- 通过 API 网关层实现集群状态预检查逻辑

#### 限制 2: Uncommitted 期间的 DML 交互语义

**问题描述:**
Uncommitted 状态下的 Segment 不参与查询，但与后续 DML 操作存在主键冲突风险。当 Commit 后 Segment 可见时，可能违背 DML 操作的语义预期。

**场景示例:**

```
T=1000: Import segment 包含 pk=2，状态=Uncommitted（查询不可见）
T=2000: INSERT pk=2（写入成功，因 Import segment 查询不可见）
T=2500: DELETE pk=2（删除 T=2000 的 INSERT）
T=3000: CommitImport → Import segment 的 pk=2 变为可见
→ 结果: pk=2 存在（Import 数据），违背 T=2500 DELETE 的预期
```

**当前策略:**
该语义问题在文档中明确声明，但系统不做运行时阻止。

**推荐实践:**
- Import 与 DML 操作时间隔离：Import 完成后再执行 DML
- 数据空间隔离：使用不同 Partition 避免主键冲突域重叠

### 8.2 性能限制

#### 限制 3: Segment 元数据更新的 etcd 写入开销

**问题描述:**
CommitImport 需要对所有 Import Segment 的 SegmentInfo 执行 etcd 写操作，设置 `commit_timestamp` 字段并清除 `importing` 标记。当单个 Job 产生大量 Segment 时（如数百个），该操作的延迟随 Segment 数量线性增长。

**性能影响:**
- 单 Job 产生 100+ Segment 时，Commit 操作延迟可能达到秒级
- etcd 集群的写入 QPS 上限成为系统吞吐瓶颈
- 大规模并发 Import 场景下，Commit 操作可能相互阻塞

**缓解措施:**
- 实现 Segment 元数据批量更新事务（单次 etcd txn 更新多个 SegmentInfo）
- 通过调整 Import 参数控制单 Job 的 Segment 数量（如 `segment_max_size`）
- 考虑采用分层元数据架构，减少 etcd 写入压力

---

**文档版本:** v1.5
**最后更新:** 2026-03-17
