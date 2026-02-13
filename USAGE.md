# HuggingGraph Lineage Query Engine 使用指南

## 项目概述

这是一个 **in-memory lineage-specific query engine**，支持对 HuggingGraph 进行高效的 lineage 查询。

## 项目结构

```
project/
  data/
    edges.tsv              # Phase 1 生成的边列表
    stats.json             # 统计信息
    node2id.json          # 节点名称到 ID 的映射
    id2node.txt           # ID 到节点名称的映射
  src/
    parser.py             # Phase 1: DOT 文件解析器
    graph.py              # Phase 2: 图加载和内存结构
    engine.py             # Phase 3: 查询引擎 API
  benchmark/
    gen_workload.py       # Phase 4: Workload 生成器
    run_bench.py          # Phase 4: Benchmark 运行器
  workloads/              # 生成的 workload 文件
  results/                # Benchmark 结果
```

## 快速开始

### 1. Phase 1: 解析 DOT 文件

```bash
python src/parser.py
```

这会生成：
- `data/edges.tsv`: 边列表文件
- `data/stats.json`: 统计信息

### 2. Phase 2: 加载图到内存

```bash
python src/graph.py
```

这会：
- 从 TSV 文件加载图
- 构建 ID 映射和邻接表
- 保存映射文件到 `data/`

### 3. Phase 3: 使用查询引擎

```python
from src.graph import Graph
from src.engine import QueryEngine

# 加载图
graph = Graph.from_tsv("data/edges.tsv")

# 创建查询引擎
engine = QueryEngine(graph, use_epoch_visited=False)

# 执行查询
result = engine.descendants("mistralai/Mistral-7B-v0.1", max_hops=2)
print(f"找到 {result.visited_count} 个节点，耗时 {result.elapsed_ms:.2f} ms")

result = engine.ancestors("some-node", max_hops=3)
result = engine.k_hop("some-node", k=5, direction="out")
```

### 4. Phase 4: 运行 Benchmark

#### 生成 Workload

```bash
# 生成混合 workload（80% 随机 + 20% 热点）
python benchmark/gen_workload.py --type mixed --num 1000 --output workloads/workload.jsonl

# 生成随机 workload
python benchmark/gen_workload.py --type random --num 500 --output workloads/random.jsonl

# 生成热点 workload
python benchmark/gen_workload.py --type hot --num 500 --output workloads/hot.jsonl
```

#### 运行 Benchmark

```bash
# Baseline 版本
python benchmark/run_bench.py --workload workloads/workload.jsonl \
    --output results/baseline_results.csv \
    --stats results/baseline_stats.json

# 使用 epoch-based visited 优化
python benchmark/run_bench.py --workload workloads/workload.jsonl \
    --output results/optimized_results.csv \
    --stats results/optimized_stats.json \
    --epoch
```

## API 文档

### QueryEngine

#### `ancestors(node, max_hops=None, limit=None)`

查找节点的所有祖先（上游节点）。

- `node`: 节点名称（字符串）
- `max_hops`: 最大跳数，None 表示全图可达
- `limit`: 结果数量限制，None 表示无限制
- 返回: `QueryResult` 对象

#### `descendants(node, max_hops=None, limit=None)`

查找节点的所有后代（下游节点）。

参数同上。

#### `k_hop(node, k, direction="out")`

k-hop 查询。

- `node`: 节点名称
- `k`: 跳数
- `direction`: "out" (下游) 或 "in" (上游)
- 返回: `QueryResult` 对象

#### `shortest_path(src, dst, direction="out")`

查找最短路径。

- `src`: 源节点
- `dst`: 目标节点
- `direction`: "out" (下游) 或 "in" (上游)
- 返回: 路径（节点 ID 列表）或 None

### QueryResult

查询结果结构：

- `nodes`: 节点 ID 集合
- `visited_count`: 访问的节点数
- `hops_reached`: 达到的最大跳数
- `elapsed_ms`: 查询耗时（毫秒）

## 性能优化

### 优化 1: Epoch-based Visited

使用 epoch 计数器代替每次查询清空 visited 集合，减少内存分配开销。

```python
engine = QueryEngine(graph, use_epoch_visited=True)
```

### 未来优化（Phase 5）

- **CSR/CSC 存储**: 使用压缩稀疏行格式提高 cache locality
- **热点缓存**: 对高频节点缓存查询结果
- **Degree-aware traversal**: 针对 super-node 的特殊处理

## 统计信息

根据 `data/stats.json`，当前图包含：

- **节点数**: 390,303
- **边数**: 447,047 (去重后)
- **自环数**: 15,477
- **脏节点数**: 520

**Top 出度节点**:
1. `mistralai/Mistral-7B-v0.1`: 1,093 出度
2. `meta-llama/Meta-Llama-3-8B-Instruct`: 1,003 出度
3. `HuggingFaceH4/ultrachat_200k`: 988 出度

## 示例结果

运行 100 个查询的 benchmark 结果（baseline）：

- **QPS**: ~50,000 queries/second
- **P50 延迟**: < 0.01 ms
- **P95 延迟**: < 0.01 ms
- **P99 延迟**: ~1 ms
- **平均访问节点数**: 30

## 注意事项

1. 图很大（40万+节点），首次加载需要几秒钟
2. 对于热点节点（高出度），查询可能访问数千个节点
3. 建议使用 `max_hops` 限制查询范围，避免全图遍历

## 下一步

- [ ] 实现 CSR/CSC 存储优化
- [ ] 实现热点节点缓存
- [ ] 添加动态更新支持
- [ ] 性能对比报告

