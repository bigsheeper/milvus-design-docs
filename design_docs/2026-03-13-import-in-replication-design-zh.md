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

### 2.1 核心难点：主备数据一致性

在主备复制环境下，Import 操作面临的核心挑战是**如何保证主备集群数据的强一致性**。具体来说，存在以下难点：

#### 难点 1: 导入时机的不确定性

由于 CDC 复制链路存在固有延迟，主从集群的 Import 任务执行完成时间存在时间差。若主集群 Import 完成后立即使数据可查询，而从集群仍处于 Import 执行阶段，将导致：

- **主集群**: 查询结果包含导入数据
- **从集群**: 查询结果不包含导入数据（处于 Importing 状态）
- **一致性违背**: 跨集群数据可见性不一致

#### 难点 2: DML 操作与 Import 数据的时序冲突

更严重的问题发生在 **DML 操作（INSERT/DELETE/UPSERT）与 Import 数据交互时**。考虑以下场景：

**场景示例：DELETE 操作与 Import 数据的冲突**

```
初始状态：
- 主集群和从集群都为空

T=1000: 主集群执行 Import
        → ImportMessage 广播，开始导入数据
        → 导入数据: (pk=1, field="A"), (pk=2, field="B"), (pk=3, field="C")
        → 此时数据处于"隐藏"状态（importing=true），不可查询

T=1500: 从集群通过 CDC 接收到 ImportMessage，开始导入
        → 导入相同的数据
        → 数据也处于"隐藏"状态

T=2000: 用户在主集群执行 DELETE pk=2
        → DELETE 消息写入 WAL，时间戳 ts=2000
        → 主集群的 DELETE 只能删除"可见"的数据
        → 由于 Import 数据还是隐藏的，DELETE 操作"看不到" pk=2
        → DELETE 执行成功，但实际没有删除任何数据

T=2500: 主集群 Import 完成，数据变为可查询
        → 用户查询：pk=1, pk=2, pk=3 都可见
        → 问题：pk=2 应该在 T=2000 被删除，但现在还在！

T=3000: 从集群通过 CDC 收到 DELETE 消息 (ts=2000)
        → 从集群 Import 也完成了，数据可查询
        → 但是从集群会应用 DELETE 吗？
```

**问题的根源：时间戳语义不一致**

当前 Milvus 的 Import 实现有一个关键问题：**导入的数据使用 ImportMessage 广播时的时间戳**，而不是数据变为可查询时的时间戳。

```go
// 文件: internal/datanode/importv2/util.go (lines 188-226)
func AppendSystemFieldsData(task *ImportTask, data *storage.InsertData, rowNum int) error {
    tss := make([]int64, rowNum)
    ts := int64(task.req.GetTs())  // 所有行使用 T_import (广播时间)
    for i := 0; i < rowNum; i++ {
        tss[i] = ts
    }
    data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
}
```

这导致了时间顺序的混乱：

```
T=1000: Import 数据写入，row.timestamp = 1000（逻辑上应该不可见）
T=2000: DELETE pk=2, delete.timestamp = 2000
T=3000: Import 数据提交，变为可查询

查询时的过滤逻辑:
    if row.timestamp <= delete.timestamp:
        delete_row()  # 1000 <= 2000，应该删除

预期: pk=2 应该被删除（因为 row.ts=1000 < delete.ts=2000）
实际: pk=2 在 T=3000 才出现，在 T=2000 时"逻辑上不存在"
```

**核心矛盾：**
- Import 数据的 `row.timestamp` 是 T_import（早期时间）
- 但数据的"逻辑可见时间"是 T_commit（晚期时间）
- DML 操作在中间发生，应该怎么处理？

**跨集群不一致性：**

更糟糕的是，主备集群可能会有不同的行为：

```
主集群:
T=1000: Import 开始，数据隐藏
T=2000: DELETE pk=2（没有删除隐藏数据）
T=3000: Import 提交，pk=2 出现
→ 结果: pk=2 可见

从集群:
T=1000: Import 开始，数据隐藏
T=3000: Import 提交，pk=2 出现
T=3100: CDC 延迟后收到 DELETE (ts=2000)
        → 由于 row.ts=1000 < delete.ts=2000
        → pk=2 被删除
→ 结果: pk=2 不可见

最终: 主备数据不一致！
```

### 2.2 问题总结

Import 在复制场景下的核心难点：

1. **时机同步问题**: 主备集群 Import 任务不会同时完成，需要协调提交时机
2. **时间戳语义问题**: Import 数据的时间戳不能反映其逻辑可见性
3. **DML 交互问题**: Import 期间的 DML 操作可能导致主备不一致
4. **用户预期违反**: DELETE 后的数据可能"复活"，违反用户预期

---

## 三、解决方案：两阶段提交协议

### 3.1 方案核心思想

我们引入一个**手动两阶段提交协议**来解决上述问题：

**阶段 1: 准备阶段（Prepare）**
- 各集群独立执行 Import 任务，完成数据物理写入和索引构建
- Import 完成后，状态转换至 **WaitingCommit**
- WaitingCommit 状态语义：数据已持久化到对象存储，但 `importing=true` 标记阻止查询可见
- 各集群在此状态阻塞，等待外部提交信号

**阶段 2: 提交阶段（Commit）**
- 平台侧通过 GetImportProgress API 轮询，确认所有集群达到 WaitingCommit
- 在主集群调用 `CommitImport` RPC 触发提交
- 主集群广播 `CommitImportMessage`，通过 CDC 链路传播至从集群
- 各集群接收消息后，执行原子状态转换：
  - 设置 `segment.commit_timestamp` 元数据
  - 清除 `segment.importing` 标记
  - Job 状态转换：WaitingCommit → Completed

**关键特性:**
- **统一 JobID**: 所有集群使用相同的 JobID，确保处理的是同一个导入任务
- **手动协调**: 平台侧负责确认所有集群就绪后才提交（不自动验证）
- **CDC 广播**: 利用现有 CDC 机制传播提交消息
- **原子可见性**: 每个集群本地的状态转换是原子的
- **多 VChannel 广播**: CommitImportMessage 和 AbortImportMessage 通过 collection 的所有 vchannel 广播，确保与 ImportMessage 一致的传播路径

### 3.2 方案如何解决一致性问题

#### 解决时机同步问题

通过 WaitingCommit 状态，我们将"数据准备完成"和"数据可查询"两个时间点解耦：

```
主集群:
T=1000: Import 完成 → WaitingCommit（数据隐藏）
T=2000: 等待从集群...
T=3000: 收到 CommitImportMessage → Completed（数据可见）

从集群:
T=1200: Import 完成 → WaitingCommit（数据隐藏）
T=3000: 收到 CommitImportMessage → Completed（数据可见）

结果: 两个集群同时在 T=3000 让数据可查询
```

#### 解决时间戳语义问题

我们引入 **segment 级的 `commit_timestamp` 元数据**，作为**系统级的逻辑时间戳**：

```protobuf
message SegmentInfo {
    // ... 现有字段 ...

    // commit_timestamp: Import 数据的提交时间戳（系统级时序语义）
    //
    // 语义定义:
    //   - row.timestamp      = 物理写入时间（Import 执行时写入 binlog）
    //   - commit_timestamp  = 提交时间（CommitImport 时设置，数据的逻辑存在时间）
    //
    // 命名动机:
    //   直接对应 CommitImport 操作，表示两阶段提交中的 commit 时间点
    //
    // 使用规则:
    //   系统中任何涉及"时序判断"、"因果关系"、"数据可见性"的逻辑，
    //   都应该使用 commit_timestamp（如果设置）而非 row.timestamp
    //
    // 影响范围:
    //   - QueryNode: DML 过滤、时间旅行查询、一致性快照
    //   - DataCoord: Compaction 触发、Segment 时间范围管理、GC 决策
    //   - CDC/Replication: Checkpoint 计算、复制进度判断
    //   - 监控/诊断: Segment 时间范围显示、延迟统计
    //
    // 生命周期:
    //   - 为 0: 正常 segment（未提交或已标准化）
    //   - 非 0: import segment（已提交但未标准化）
    //   - Compaction 重写 row.timestamp 后清除（设为 0）
    optional uint64 commit_timestamp = X;
}
```

**工作原理:**

```
T=1000: Import 开始，行写入 binlog，row.timestamp = 1000（物理时间）
        → Segment 处于 WaitingCommit，importing=true
        → segment.commit_timestamp = 0（逻辑时间未设置）

T=2000: DELETE pk=2, delete.timestamp = 2000
        → QueryNode 看不到隐藏的 segment，DELETE 不应用

T=3000: CommitImport 广播，广播时间戳 T_commit = 3000
        → DataCoord 设置: segment.commit_timestamp = 3000（逻辑时间）
        → Segment 变为可查询: importing=false

T=4000: 用户查询 pk=2
        → 系统统一使用逻辑时间:
            effective_ts = segment.commit_timestamp ?: row.timestamp
            effective_ts = 3000（使用逻辑时间）

        → DELETE 检查: effective_ts (3000) > delete.ts (2000)
        → 结论: DELETE 不生效，pk=2 可见
```

**语义修正:**
- Import 数据的**物理写入时间** = T_import（1000）→ 存储在 binlog 中
- Import 数据的**逻辑存在时间** = T_commit（3000）→ 存储在 segment 元数据中
- 系统所有时序判断使用**逻辑时间**，确保语义正确
- T_commit 之前的 DML 操作（T=2000 的 DELETE）不影响 import 数据
- T_commit 之后的 DML 操作才会正确应用

**系统级影响:**

| 组件 | 使用场景 | 行为变化 |
|------|----------|----------|
| **QueryNode** | DML 过滤、时间旅行查询 | 使用 `commit_timestamp` 判断行的逻辑存在时间 |
| **DataCoord** | Compaction 决策、Segment 管理 | 使用 `commit_timestamp` 确定 segment 的逻辑时间范围 |
| **CDC** | Checkpoint 计算、复制进度 | 使用 `commit_timestamp` 判断数据是否"逻辑提交" |
| **Monitoring** | 时间范围展示、延迟统计 | 显示 `commit_timestamp` 作为 segment 的逻辑时间 |

#### 跨集群一致性

**关键机制：统一的逻辑时间基准**

由于所有集群从同一个 `CommitImportMessage` 广播设置相同的 `commit_timestamp`，因此建立了**统一的逻辑时间基准**，消除了物理执行顺序的差异：

```
主集群:
T=1000: Import, row.ts = 1000（物理时间）
T=2000: DELETE pk=2, delete.ts = 2000
T=3000: CommitImport, segment.commit_timestamp = 3000（逻辑时间）
→ 系统时序判断: effective_ts (3000) > delete.ts (2000) → pk=2 可见

从集群:
T=1200: Import, row.ts = 1000（物理时间）
T=3000: CommitImport, segment.commit_timestamp = 3000（逻辑时间，与主集群相同）
T=3100: CDC 收到 DELETE, delete.ts = 2000
→ 系统时序判断: effective_ts (3000) > delete.ts (2000) → pk=2 可见

结果: 主备行为一致！✓
```

**为什么能保证一致性？**

1. **统一的逻辑时间戳**
   - 所有集群的 `segment.commit_timestamp = T_commit`（来自同一广播）
   - 无论物理执行顺序如何，逻辑时间统一

2. **统一的时序判断规则**
   - 所有组件都使用 `effective_ts = commit_timestamp ?: row.timestamp`
   - DML 操作的应用与否仅取决于逻辑时间比较

3. **与物理顺序无关**
   - 即使主集群 Compaction 在 Import 完成后触发
   - 即使从集群 Compaction 在 Import 完成前触发
   - 由于都使用逻辑时间判断，结果一致

**对比：如果没有 commit_timestamp**

```
主集群:
T=50000: Import 完成，row.ts = 1000
T=51000: Compaction 触发
         → DELETE (ts=2000) vs row (ts=1000)
         → 1000 < 2000 → DELETE 应用 → pk=2 被删除 ✓

从集群:
T=60000: Compaction 触发（Import 还未完成）
         → Import segment 不参与 compaction
T=61000: Import 完成，row.ts = 1000
         → DELETE (ts=2000) 已经应用过
         → 但 Import segment 不受影响 → pk=2 存在 ✗

结果: 主备不一致！
```

**commit_timestamp 解决的本质问题：**
- 将"物理时间"与"逻辑时间"解耦
- 让系统的时序判断不再依赖不可控的物理执行顺序（如 Compaction 触发时机）
- 通过统一的逻辑时间戳，建立跨集群的一致性基准

### 3.3 高层流程图

**复制集群（手动 Commit）:**

```
┌─────────────────────────────────────────────────────────────┐
│                     主集群 (PRIMARY)                         │
│  用户 → Proxy → DataCoord.ImportV2()                        │
│         ↓                                                    │
│  DataCoord 广播 ImportMessage (通过 CDC)                    │
│         ↓                                                    │
│  ImportJob: Pending → Importing → IndexBuilding            │
│         ↓                                                    │
│  进入新状态: WaitingCommit (数据已写入但不可查询)          │
│         ↓                                                    │
│  用户检查所有集群状态，确认都到达 WaitingCommit            │
│         ↓                                                    │
│  用户调用: CommitImport(jobID)                              │
│         ↓                                                    │
│  DataCoord 广播: CommitImportMessage (ts = T_commit)        │
└─────────────────────────────────────────────────────────────┘
                           │
                           │ CDC 复制
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                   从集群 (SECONDARIES)                       │
│  Proxy 接收 ImportMessage → DataCoord 处理                  │
│         ↓                                                    │
│  ImportJob: Pending → Importing → IndexBuilding            │
│         ↓                                                    │
│  进入: WaitingCommit (等待提交信号)                         │
│         ↓                                                    │
│  接收 CommitImportMessage (ts = T_commit)                   │
│         ↓                                                    │
│  原子操作:                                                   │
│    - 设置 segment.commit_timestamp = T_commit              │
│    - 设置 segment.importing = false                         │
│    - 状态转换: WaitingCommit → Completed                    │
└─────────────────────────────────────────────────────────────┘
```

**非复制集群（自动 Commit，向后兼容）:**

```
┌─────────────────────────────────────────────────────────────┐
│                   非复制集群                                 │
│  用户 → Proxy → DataCoord.ImportV2()                        │
│         ↓                                                    │
│  ImportJob: Pending → Importing → IndexBuilding            │
│         ↓                                                    │
│  进入: WaitingCommit                                        │
│         ↓                                                    │
│  DataCoord 自动广播 CommitImportMessage（无需用户干预）    │
│         ↓                                                    │
│  状态转换: WaitingCommit → Completed                        │
│         ↓                                                    │
│  结果: 用户看到 IndexBuilding → Completed（平滑过渡）      │
└─────────────────────────────────────────────────────────────┘
```

---

## 四、方案改造

### 4.1 状态机扩展

**当前状态机 (8 个状态):**

```
Pending → PreImporting → Importing → Sorting → IndexBuilding → Completed
            ↓              ↓           ↓            ↓              ↓
                             Failed (任意阶段)
```

**新状态机 (9 个状态):**

```
Pending → PreImporting → Importing → Sorting → IndexBuilding → WaitingCommit → Completed
            ↓              ↓           ↓            ↓               ↓              ↓
                             Failed (任意阶段，或显式 abort)
```

**新增状态: WaitingCommit**

| 属性 | 说明 |
|------|------|
| **用途** | 在使导入数据可查询之前的检查点，等待提交信号 |
| **进入条件** | IndexBuilding 成功完成（所有集群统一） |
| **状态特征** | • Segment 已写入存储<br>• Segment 已建立索引<br>• Segment 标记 `importing=true`（不可查询）<br>• Job 元数据持久化在 etcd |
| **退出条件** | • 收到 `CommitImportMessage` → `Completed`<br>• 收到 `AbortImportMessage` → `Failed` |
| **提交触发** | • 非复制集群: 自动广播（向后兼容）<br>• 复制集群: 等待用户调用 `CommitImport` RPC |

### 4.2 Segment 元数据扩展：逻辑时间戳

**设计原则:**

Milvus 引入**两层时间戳系统**来正确处理 Import 数据的时序语义：

| 时间戳类型 | 存储位置 | 语义 | 使用场景 |
|-----------|---------|------|----------|
| **物理时间戳**<br>`row.timestamp` | Binlog 中的每一行 | 数据写入存储的物理时间 | Compaction 重写、物理存储 |
| **逻辑时间戳**<br>`segment.commit_timestamp` | Segment 元数据 | 数据的逻辑可见时间 | 所有时序判断、DML 过滤、CDC |

**SegmentInfo 新增字段:**

```protobuf
message SegmentInfo {
    // ... 现有字段 ...

    // commit_timestamp: Segment 的提交时间戳（系统级时序语义）
    //
    // 【核心概念】
    // 这不是 QueryNode 的实现细节，而是系统级的时序语义：
    //   - row.timestamp      = 物理写入时间（Import 执行时写入 binlog）
    //   - commit_timestamp  = 提交时间（CommitImport 时设置，表示数据的逻辑存在时间）
    //
    // 命名动机：
    //   - "commit" 直接对应 CommitImport 操作（两阶段提交的 commit 阶段）
    //   - 类似分布式事务中的 commit_ts，表示事务提交的时间点
    //   - 与 WaitingCommit 状态形成完整的语义闭环：
    //     * WaitingCommit: 准备阶段，数据已写入但未提交
    //     * CommitImport: 提交操作
    //     * commit_timestamp: 提交时间戳
    //
    // 【使用规则】
    // 系统中任何涉及以下判断的逻辑，都必须使用 commit_timestamp（如果非零）：
    //   1. 时序比较：数据是否在某个时间点"存在"
    //   2. 因果关系：操作 A 是否发生在操作 B 之前/之后
    //   3. 一致性：跨集群的时间戳一致性判断
    //   4. 过滤逻辑：DML 是否应该影响某行数据
    //
    // 【影响的组件】（必须使用 commit_timestamp）
    //   - QueryNode:
    //       * DML 过滤（DELETE/UPDATE 是否应用）
    //       * 时间旅行查询（GuaranteeTimestamp 过滤）
    //       * 一致性快照（ServiceTimestamp 过滤）
    //   - DataCoord:
    //       * Segment 时间范围管理（min/max timestamp）
    //       * Compaction 触发条件（segment 是否可 compact）
    //       * GC 决策（segment 是否可删除）
    //   - CDC/Replication:
    //       * Checkpoint 计算（复制进度判断）
    //       * 复制延迟统计（数据同步进度）
    //   - 监控/诊断:
    //       * Segment 时间范围展示
    //       * 数据延迟统计
    //       * 时序一致性检查
    //
    // 【生命周期】
    //   1. Import 执行阶段（WaitingCommit 之前）:
    //      - commit_timestamp = 0（未提交）
    //      - importing = true
    //      - 数据物理存在但逻辑上未提交，不可查询
    //
    //   2. Commit 提交阶段（收到 CommitImportMessage）:
    //      - commit_timestamp = T_commit（CommitImportMessage 的广播时间戳）
    //      - importing = false
    //      - 数据已提交，变为可查询
    //      - 系统使用 commit_timestamp 作为数据的逻辑时间基准
    //
    //   3. Compaction 标准化阶段:
    //      - Compaction 重写每行的 row.timestamp = commit_timestamp
    //      - 完成后清除: commit_timestamp = 0
    //      - 物理时间与提交时间统一，回归正常 segment
    //
    // 【重要说明】
    //   - commit_timestamp = 0: 表示物理时间即逻辑时间（正常 segment）
    //   - commit_timestamp != 0: 表示逻辑时间覆盖物理时间（import segment）
    //   - 物理时间用于存储层，逻辑时间用于语义层
    optional uint64 commit_timestamp = X;
}
```

**工作流程（两阶段提交）:**

```
阶段 1: Prepare（Import 执行 + WaitingCommit）
  - Import 写入数据: row.timestamp = T_import（物理时间）
  - 进入 WaitingCommit: segment.commit_timestamp = 0（未提交）
  - 状态标记: segment.importing = true（不可查询）
  → 系统行为：数据已准备好（prepared），但未提交（not committed）

阶段 2: Commit（CommitImport 广播）
  - 接收 CommitImportMessage: 广播时间戳 = T_commit
  - 设置提交时间: segment.commit_timestamp = T_commit
  - 状态转换: segment.importing = false（可查询）
  - 物理时间不变: row.timestamp 仍为 T_import
  → 系统行为：数据已提交（committed），所有时序判断使用 T_commit

阶段 3: Compaction（异步标准化）
  - 重写物理时间: row.timestamp ← segment.commit_timestamp
  - 清除提交时间: segment.commit_timestamp = 0
  → 系统行为：物理时间 = 提交时间，回归正常 segment
```

**示例：effective_timestamp 计算**

所有组件应该使用统一的逻辑：

```go
// 获取 segment 中某行数据的有效时间戳
// 如果 segment 已提交（commit_timestamp != 0），使用提交时间
// 否则使用物理写入时间
func GetEffectiveTimestamp(segment *SegmentInfo, rowTimestamp uint64) uint64 {
    if segment.CommitTimestamp != 0 {
        // Import segment 已提交：使用提交时间作为数据的逻辑存在时间
        return segment.CommitTimestamp
    }
    // 正常 segment 或未提交：使用物理写入时间
    return rowTimestamp
}

// 应用场景示例:
// - QueryNode: DELETE 过滤时判断 effectiveTs <= deleteTs
//   → 使用提交时间判断 DELETE 是否应该影响 import 数据
// - DataCoord: 判断 segment 是否超过 retention period
//   → 使用提交时间作为 segment 的逻辑创建时间
// - CDC: 判断数据是否已经提交并可以复制
//   → commit_timestamp != 0 表示数据已提交
```

### 4.3 新增 RPC 接口

#### 4.3.1 CommitImport RPC

**功能：** 提交处于 WaitingCommit 状态的 import job，通过广播 CommitImportMessage 使所有集群的数据同步可查询。

**接口签名：**

```protobuf
rpc CommitImport(CommitImportRequest) returns(common.Status) {}

message CommitImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}
```

**调用约束：**
- 仅在主集群调用
- Job 必须处于 WaitingCommit 状态
- 不自动验证从集群状态（需平台侧手动确认）
- 幂等操作，可安全重试

**执行流程：**
1. 验证 JobID 存在且状态为 WaitingCommit
2. 广播 CommitImportMessage 到所有 vchannel（通过 CDC 传播）
3. 各集群接收消息后原子地更新 segment 元数据：
   - 设置 `segment.commit_timestamp = T_commit`
   - 设置 `segment.importing = false`
   - 转换状态：WaitingCommit → Completed

#### 4.3.2 AbortImport RPC

**功能：** 中止 import job，清理所有集群的导入数据和元数据。

**接口签名：**

```protobuf
rpc AbortImport(AbortImportRequest) returns(common.Status) {}

message AbortImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}
```

**调用约束：**
- 仅在主集群调用
- 可在任何非终止状态（Completed 和 Failed 除外）调用
- 幂等操作，重复调用返回成功

**执行流程：**
1. 验证 JobID 存在且未处于终止状态
2. 广播 AbortImportMessage 到所有 vchannel
3. 各集群接收消息后执行清理：
   - 标记所有 import segment 为 Dropped
   - 更新 Job 状态为 Failed
   - 触发 GC 清理 binlog 和索引文件

**使用场景：**
- 从集群长时间卡在某个状态，需要主动清理
- 检测到 import 数据异常，需要回滚
- 平台侧决策不再继续当前 import 操作

### 4.4 新增消息类型

**CommitImportMessage:**

```protobuf
message CommitImportMsg {
    commonpb.MsgBase base = 1;  // 包含时间戳 T_commit
    int64 job_id = 2;
}
```

**AbortImportMessage:**

```protobuf
message AbortImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}
```

### 4.5 组件改造概览

| 组件 | 改造内容 | 复杂度 |
|------|----------|--------|
| **Meta 存储**<br>（核心改造） | • SegmentInfo proto 新增 `commit_timestamp` 字段<br>• 引入两层时间戳系统（物理 + 逻辑）<br>• UpdateSegmentVisibility 方法 | 低 |
| **DataCoord** | • 新增 WaitingCommit 状态处理<br>• 实现 CommitImport/AbortImport RPC<br>• 自动提交逻辑（非复制集群）<br>• Commit 时设置 segment.commit_timestamp<br>• Compaction 决策使用 commit_timestamp<br>• Segment 时间范围管理使用 commit_timestamp | 中 |
| **QueryNode** | • DML 过滤逻辑使用 effective_timestamp (commit_timestamp ?: row.timestamp)<br>• 时间旅行查询使用 effective_timestamp<br>• 一致性快照使用 effective_timestamp | 中 |
| **ImportChecker** | • WaitingCommit 状态检查和超时处理<br>• 自动提交判断逻辑 | 低 |
| **Compaction** | • 标准化 import segment 时间戳（重写 row.timestamp）<br>• 清除 commit_timestamp 元数据（设为 0） | 低 |
| **CDC/Replication** | • Checkpoint 计算使用 commit_timestamp<br>• 复制进度判断使用 commit_timestamp | 低 |
| **Monitoring** | • Segment 时间范围展示使用 commit_timestamp<br>• 延迟统计使用 commit_timestamp | 低 |
| **Proto 定义** | • 新增消息类型<br>• 新增 RPC 定义<br>• 状态枚举扩展 | 低 |

**核心原则：**
- `commit_timestamp` 是**系统级的逻辑时间戳**，不是某个组件的实现细节
- 所有涉及"时序判断"、"因果关系"、"数据可见性"的逻辑都必须使用它
- 物理时间戳（`row.timestamp`）仅用于存储层（binlog 读写、Compaction 重写）

### 4.6 向后兼容性

**非复制集群（无 CDC）:**
- WaitingCommit 状态自动提交，用户无感知
- 用户观察到的行为：IndexBuilding → Completed（与之前一致）
- 无需修改现有 API 调用

**复制集群（有 CDC）:**
- 解除 import 阻塞限制
- 引入新的用户操作：手动调用 CommitImport
- 用户观察到的行为：IndexBuilding → WaitingCommit → 用户提交 → Completed

---

## 五、API 和 Protocol Buffer 定义

### 5.1 新增 RPC 接口

#### CommitImport RPC

**功能:** 提交处于 WaitingCommit 状态的 import job，使数据在所有复制集群上可查询。

**接口定义:**

```protobuf
service DataCoord {
    // ... 现有方法 ...

    rpc CommitImport(CommitImportRequest) returns(common.Status) {}
    rpc AbortImport(AbortImportRequest) returns(common.Status) {}
}

message CommitImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;  // Import job ID
}

message AbortImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;  // Import job ID
}
```

**调用要求:**

1. 只能在主集群上调用
2. 调用前必须确保所有集群的 job 都处于 WaitingCommit 状态
3. 幂等操作，可以安全重试

**返回值:**

```json
{
  "error_code": "Success",
  "reason": ""
}
```

可能的错误:
- `ImportJobNotExist`: Job ID 不存在
- `InvalidState`: Job 不在 WaitingCommit 状态
- `BroadcastFailed`: 消息广播失败（可重试）

#### AbortImport RPC

**功能:** 中止 import job，清理所有集群上的导入数据。

**调用要求:**

1. 只能在主集群上调用
2. 可以在任何非终止状态（除 Completed 外）下调用
3. 幂等操作，可以安全重试

### 5.2 消息类型定义

#### CommitImportMessage

```protobuf
message CommitImportMessageHeader {
    int64 job_id = 1;
}

message CommitImportMsg {
    commonpb.MsgBase base = 1;  // 包含 T_commit 时间戳
    int64 job_id = 2;
}
```

**广播目标:** 所有 vchannel（通过 CDC 复制到从集群）

#### AbortImportMessage

```protobuf
message AbortImportMessageHeader {
    int64 job_id = 1;
}

message AbortImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}
```

**广播目标:** 所有 vchannel（通过 CDC 复制到从集群）

### 5.3 状态枚举扩展

```protobuf
enum ImportJobStateV2 {
    None = 0;
    Pending = 1;
    PreImporting = 2;
    Importing = 3;
    Sorting = 4;
    IndexBuilding = 5;
    WaitingCommit = 6;  // 新增状态
    Completed = 7;
    Failed = 8;
}
```

### 5.4 GetImportProgress API 返回值

**现有 API 扩展:**

```json
{
  "status": {
    "error_code": "Success"
  },
  "jobId": "123",
  "state": "WaitingCommit",  // 新增状态
  "progress": 100,
  "collectionName": "my_collection",
  "reason": "",
  "completeTime": "",
  "importedRows": 1000000
}
```

**平台侧通过此 API 检查所有集群状态，确认都达到 WaitingCommit 后再调用 CommitImport。**

---

## 六、平台侧工作流

### 6.1 复制集群导入数据流程

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

**等待所有集群都达到 WaitingCommit 状态：**

```json
{
  "jobId": "job-123456",
  "state": "WaitingCommit",
  "progress": 100
}
```

#### 步骤 3: 提交导入（主集群）

**重要:** 只有在所有集群都处于 WaitingCommit 时才能提交！

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

### 6.2 处理异常情况

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

#### 情况 2: CommitImport 后发现某个从集群仍是 WaitingCommit

**原因:** 网络问题导致 CommitImportMessage 丢失。

**处理:**

```bash
# 重试 CommitImport（幂等操作）
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -d '{"jobId": "job-123456"}'

# 再次检查所有集群状态
```

### 6.3 非复制集群导入流程

对于没有启用 CDC 复制的集群，操作流程与之前完全一致：

```bash
# 1. 启动导入
curl -X POST "http://standalone:19530/v2/vectordb/jobs/import/create" \
  -d '{"collectionName": "my_collection", "files": ["s3://bucket/data.parquet"]}'

# 2. 监控进度（WaitingCommit 状态会自动跳过）
watch -n 5 'curl "http://standalone:19530/v2/vectordb/jobs/import/get_progress?jobId=xxx"'

# 3. 等待完成（无需手动 commit）
# 响应: {"state": "Completed", ...}
```

### 6.4 平台侧操作检查清单

**导入前检查:**

- [ ] 确认所有集群可以访问共享对象存储
- [ ] 验证 CDC 复制链路正常
- [ ] 检查各集群资源充足（CPU、内存、磁盘）

**导入期间监控:**

- [ ] 定期检查所有集群的 job 状态
- [ ] 监控 CDC 复制延迟
- [ ] 关注 DataCoord 和 QueryNode 日志

**提交前确认:**

- [ ] 所有集群的 job 状态都是 WaitingCommit
- [ ] 所有集群的进度都是 100%
- [ ] 检查 importedRows 数量一致

**提交后验证:**

- [ ] 所有集群的 job 状态都是 Completed
- [ ] 在各集群上查询数据，验证一致性
- [ ] 检查监控指标无异常

---

## 七、测试策略

### 7.1 功能测试

#### 7.1.1 基本流程测试

**测试用例 1: 非复制集群自动提交**

```
目标: 验证非复制集群的向后兼容性
步骤:
  1. 在非复制集群上启动 import
  2. 监控状态转换: Pending → Importing → IndexBuilding → WaitingCommit → Completed
  3. 验证 WaitingCommit 状态立即自动转换为 Completed（无用户干预）
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
  2. 监控所有集群状态，等待都达到 WaitingCommit
  3. 验证 WaitingCommit 状态下数据不可查询
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
  2. 等待达到 WaitingCommit（数据隐藏）
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
  2. 等待 WaitingCommit 并 CommitImport
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
  1. Import 到 WaitingCommit
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
  1. Import，主集群和从集群1 到达 WaitingCommit
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
  1. Import 到 WaitingCommit
  2. 模拟从集群2网络故障
  3. 主集群 CommitImport（从集群2 收不到消息）
  4. 主集群和从集群1 转换为 Completed
  5. 发现从集群2 仍是 WaitingCommit
  6. 恢复网络
  7. 重试 CommitImport
验收:
  - 重试后从集群2 转换为 Completed
  - 幂等性：重复 commit 不会导致错误
  - 最终所有集群一致
```

### 7.2 性能测试

**测试用例 9: 延迟测试**

```
目标: 验证 CommitImport 的延迟
配置: 1 主 + 3 从
数据: 1 GB 数据（100万行）
测量:
  - CommitImport RPC 响应时间
  - 从 WaitingCommit 到 Completed 的转换时间
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
  2. 监控所有 job 到达 WaitingCommit
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

### 7.3 兼容性测试

**测试用例 12: 版本升级**

```
目标: 验证从旧版本升级
场景:
  - 旧版本: 不支持 import in replication
  - 新版本: 支持 WaitingCommit
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

## 八、已知限制与后续优化

### 8.1 当前版本的限制

#### 限制 1: 无自动一致性验证

**问题描述:**
CommitImport RPC 执行时不会跨集群验证所有从集群的 Job 状态。若在从集群未达到 WaitingCommit 状态时调用该 RPC，将导致跨集群数据可见性不一致。

**系统行为:**
- 主集群执行提交，从集群接收到 CommitImportMessage 时仍处于 Importing/IndexBuilding 状态
- 从集群丢弃该消息（状态不匹配），继续执行 Import
- 最终主集群数据可见，从集群 Job 因超时或其他原因失败

**缓解措施:**
- 提供标准化的操作流程文档和状态检查脚本
- 在可观测性系统中实现跨集群状态聚合视图
- 通过 API 网关层实现集群状态预检查逻辑

#### 限制 2: WaitingCommit 期间的 DML 交互语义

**问题描述:**
WaitingCommit 状态下的 Segment 不参与查询，但与后续 DML 操作存在主键冲突风险。当 Commit 后 Segment 可见时，可能违背 DML 操作的语义预期。

**场景示例:**

```
T=1000: Import segment 包含 pk=2，状态=WaitingCommit（查询不可见）
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
