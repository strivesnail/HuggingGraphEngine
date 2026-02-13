"""
Phase 4.1: Workload 生成器
生成不同类型的查询 workload
"""

import json
import random
import sys
from pathlib import Path
from collections import Counter
from typing import List, Dict
import argparse

# 添加 src 目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from graph import Graph


class WorkloadGenerator:
    """生成查询 workload"""
    
    def __init__(self, graph: Graph):
        self.graph = graph
        self.all_nodes = list(range(graph.num_nodes()))
        
        # 计算节点出度
        self.out_degrees = [len(graph.get_out_neighbors(i)) for i in self.all_nodes]
        self.max_out_degree = max(self.out_degrees) if self.out_degrees else 0
        
        # 找出热点节点（top 1% 出度）
        sorted_indices = sorted(range(len(self.out_degrees)), 
                               key=lambda i: self.out_degrees[i], 
                               reverse=True)
        top_1_percent = max(1, len(sorted_indices) // 100)
        self.hot_nodes = sorted_indices[:top_1_percent]
    
    def generate_random_workload(self, num_queries: int, 
                                 k_values: List[int]) -> List[Dict]:
        """生成随机 workload"""
        queries = []
        for _ in range(num_queries):
            node_id = random.choice(self.all_nodes)
            node_name = self.graph.get_node_name(node_id)
            k = random.choice(k_values)
            query_type = random.choice(["descendants", "ancestors", "k_hop"])
            
            if query_type == "k_hop":
                direction = random.choice(["out", "in"])
                queries.append({
                    "qtype": "k_hop",
                    "node": node_name,
                    "k": k,
                    "direction": direction
                })
            else:
                queries.append({
                    "qtype": query_type,
                    "node": node_name,
                    "k": k
                })
        
        return queries
    
    def generate_hot_workload(self, num_queries: int,
                            k_values: List[int]) -> List[Dict]:
        """生成热点节点 workload"""
        queries = []
        for _ in range(num_queries):
            node_id = random.choice(self.hot_nodes)
            node_name = self.graph.get_node_name(node_id)
            k = random.choice(k_values)
            query_type = random.choice(["descendants", "ancestors", "k_hop"])
            
            if query_type == "k_hop":
                direction = random.choice(["out", "in"])
                queries.append({
                    "qtype": "k_hop",
                    "node": node_name,
                    "k": k,
                    "direction": direction
                })
            else:
                queries.append({
                    "qtype": query_type,
                    "node": node_name,
                    "k": k
                })
        
        return queries
    
    def generate_mixed_workload(self, num_queries: int,
                               k_values: List[int],
                               random_ratio: float = 0.8) -> List[Dict]:
        """生成混合 workload"""
        num_random = int(num_queries * random_ratio)
        num_hot = num_queries - num_random
        
        random_queries = self.generate_random_workload(num_random, k_values)
        hot_queries = self.generate_hot_workload(num_hot, k_values)
        
        queries = random_queries + hot_queries
        random.shuffle(queries)  # 打乱顺序
        
        return queries
    
    def save_workload(self, queries: List[Dict], filename: str):
        """保存 workload 到 JSONL 文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            for query in queries:
                f.write(json.dumps(query, ensure_ascii=False) + '\n')
        print(f"Workload 已保存: {filename} ({len(queries)} 个查询)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="生成查询 workload")
    parser.add_argument("--type", choices=["random", "hot", "mixed"], 
                      default="mixed", help="Workload 类型")
    parser.add_argument("--num", type=int, default=1000, 
                       help="查询数量")
    parser.add_argument("--k", nargs="+", type=int, 
                       default=[2, 3, 5, 10], help="k 值列表")
    parser.add_argument("--output", default="workloads/workload.jsonl",
                       help="输出文件路径")
    
    args = parser.parse_args()
    
    # 加载图
    print("正在加载图...")
    graph = Graph.from_tsv("data/edges.tsv")
    
    # 生成 workload
    print(f"\n生成 {args.type} workload...")
    generator = WorkloadGenerator(graph)
    
    if args.type == "random":
        queries = generator.generate_random_workload(args.num, args.k)
    elif args.type == "hot":
        queries = generator.generate_hot_workload(args.num, args.k)
    else:  # mixed
        queries = generator.generate_mixed_workload(args.num, args.k)
    
    # 保存
    generator.save_workload(queries, args.output)
    
    # 统计信息
    qtype_counter = Counter(q["qtype"] for q in queries)
    print(f"\nWorkload 统计:")
    print(f"  总查询数: {len(queries)}")
    print(f"  查询类型分布: {dict(qtype_counter)}")

