"""
Phase 4.2: Benchmark 运行器
执行 workload 并收集性能指标
"""

import json
import time
import statistics
import sys
from typing import List, Dict
import argparse
import csv
from pathlib import Path

# 添加 src 目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from graph import Graph
from engine import QueryEngine


class BenchmarkRunner:
    """运行 benchmark 并收集指标"""
    
    def __init__(self, graph: Graph, engine: QueryEngine):
        self.graph = graph
        self.engine = engine
        self.results = []
    
    def run_query(self, query: Dict) -> Dict:
        """执行单个查询"""
        qtype = query["qtype"]
        node = query["node"]
        k = query.get("k", None)
        
        if qtype == "descendants":
            result = self.engine.descendants(node, max_hops=k)
        elif qtype == "ancestors":
            result = self.engine.ancestors(node, max_hops=k)
        elif qtype == "k_hop":
            direction = query.get("direction", "out")
            result = self.engine.k_hop(node, k=k, direction=direction)
        else:
            raise ValueError(f"未知查询类型: {qtype}")
        
        return {
            "qtype": qtype,
            "node": node,
            "k": k,
            "node_count": result.visited_count,
            "hops_reached": result.hops_reached,
            "latency_ms": result.elapsed_ms
        }
    
    def run_workload(self, workload_file: str):
        """运行整个 workload"""
        print(f"正在加载 workload: {workload_file}")
        
        queries = []
        with open(workload_file, 'r', encoding='utf-8') as f:
            for line in f:
                queries.append(json.loads(line.strip()))
        
        print(f"共 {len(queries)} 个查询")
        print("开始执行 benchmark...\n")
        
        start_time = time.time()
        latencies = []
        
        for i, query in enumerate(queries):
            if (i + 1) % 100 == 0:
                print(f"  已执行 {i + 1}/{len(queries)} 个查询...")
            
            result = self.run_query(query)
            self.results.append(result)
            latencies.append(result["latency_ms"])
        
        total_time = time.time() - start_time
        
        # 计算统计信息
        if latencies:
            latencies_sorted = sorted(latencies)
            n = len(latencies_sorted)
            
            stats = {
                "total_queries": len(queries),
                "total_time_seconds": total_time,
                "qps": len(queries) / total_time,
                "latency_p50_ms": latencies_sorted[n // 2],
                "latency_p95_ms": latencies_sorted[int(n * 0.95)],
                "latency_p99_ms": latencies_sorted[int(n * 0.99)],
                "latency_avg_ms": statistics.mean(latencies),
                "latency_min_ms": min(latencies),
                "latency_max_ms": max(latencies),
                "avg_visited_nodes": statistics.mean(r["node_count"] for r in self.results)
            }
        else:
            stats = {}
        
        return stats
    
    def save_results(self, results_file: str, stats_file: str = None):
        """保存结果到 CSV 和统计文件"""
        # 保存详细结果
        with open(results_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "qtype", "node", "k", "node_count", "hops_reached", "latency_ms"
            ])
            writer.writeheader()
            writer.writerows(self.results)
        
        print(f"\n详细结果已保存: {results_file}")
        
        # 保存统计信息
        if stats_file:
            latencies = [r["latency_ms"] for r in self.results]
            if latencies:
                latencies_sorted = sorted(latencies)
                n = len(latencies_sorted)
                
                stats = {
                    "total_queries": len(self.results),
                    "latency_p50_ms": latencies_sorted[n // 2],
                    "latency_p95_ms": latencies_sorted[int(n * 0.95)],
                    "latency_p99_ms": latencies_sorted[int(n * 0.99)],
                    "latency_avg_ms": statistics.mean(latencies),
                    "latency_min_ms": min(latencies),
                    "latency_max_ms": max(latencies),
                    "avg_visited_nodes": statistics.mean(r["node_count"] for r in self.results)
                }
                
                with open(stats_file, 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, ensure_ascii=False)
                
                print(f"统计信息已保存: {stats_file}")
                print(f"\n性能统计:")
                print(f"  总查询数: {stats['total_queries']}")
                print(f"  P50 延迟: {stats['latency_p50_ms']:.2f} ms")
                print(f"  P95 延迟: {stats['latency_p95_ms']:.2f} ms")
                print(f"  P99 延迟: {stats['latency_p99_ms']:.2f} ms")
                print(f"  平均延迟: {stats['latency_avg_ms']:.2f} ms")
                print(f"  平均访问节点数: {stats['avg_visited_nodes']:.0f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行 benchmark")
    parser.add_argument("--workload", required=True,
                       help="Workload 文件路径")
    parser.add_argument("--output", default="results/results.csv",
                       help="结果输出文件")
    parser.add_argument("--stats", default="results/stats.json",
                       help="统计信息输出文件")
    parser.add_argument("--epoch", action="store_true",
                       help="使用 epoch-based visited 优化")
    
    args = parser.parse_args()
    
    # 加载图
    print("正在加载图...")
    graph = Graph.from_tsv("data/edges.tsv")
    
    # 创建查询引擎
    print(f"\n创建查询引擎 (epoch_visited={args.epoch})...")
    engine = QueryEngine(graph, use_epoch_visited=args.epoch)
    
    # 运行 benchmark
    runner = BenchmarkRunner(graph, engine)
    stats = runner.run_workload(args.workload)
    
    # 保存结果
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.stats).parent.mkdir(parents=True, exist_ok=True)
    runner.save_results(args.output, args.stats)
    
    if stats:
        print(f"\n总体性能:")
        print(f"  QPS: {stats['qps']:.2f}")
        print(f"  总耗时: {stats['total_time_seconds']:.2f} 秒")

