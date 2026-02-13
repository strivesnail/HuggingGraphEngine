"""
Phase 2: 图加载到内存
构建 ID 映射和邻接表结构
"""

import json
from typing import List, Dict, Set, Optional
import time


class Graph:
    """内存中的图结构，支持快速查询"""
    
    def __init__(self):
        # ID 映射
        self.node2id: Dict[str, int] = {}
        self.id2node: List[str] = []
        
        # 邻接表
        self.out_adj: List[List[int]] = []  # 出边邻接表（用于 descendants）
        self.in_adj: List[List[int]] = []   # 入边邻接表（用于 ancestors）
        
        # 边标签（可选，用于后续分析）
        self.edge_labels: Dict[tuple, str] = {}  # (src_id, dst_id) -> label
    
    def add_node(self, node: str) -> int:
        """添加节点，返回节点 ID"""
        if node in self.node2id:
            return self.node2id[node]
        
        node_id = len(self.id2node)
        self.node2id[node] = node_id
        self.id2node.append(node)
        self.out_adj.append([])
        self.in_adj.append([])
        return node_id
    
    def add_edge(self, src: str, dst: str, label: Optional[str] = None):
        """添加边"""
        src_id = self.add_node(src)
        dst_id = self.add_node(dst)
        
        # 避免重复边
        if dst_id not in self.out_adj[src_id]:
            self.out_adj[src_id].append(dst_id)
            self.in_adj[dst_id].append(src_id)
            
            if label:
                self.edge_labels[(src_id, dst_id)] = label
    
    def get_node_id(self, node: str) -> Optional[int]:
        """获取节点 ID"""
        return self.node2id.get(node)
    
    def get_node_name(self, node_id: int) -> Optional[str]:
        """根据 ID 获取节点名称"""
        if 0 <= node_id < len(self.id2node):
            return self.id2node[node_id]
        return None
    
    def get_out_neighbors(self, node_id: int) -> List[int]:
        """获取出边邻居"""
        if 0 <= node_id < len(self.out_adj):
            return self.out_adj[node_id]
        return []
    
    def get_in_neighbors(self, node_id: int) -> List[int]:
        """获取入边邻居"""
        if 0 <= node_id < len(self.in_adj):
            return self.in_adj[node_id]
        return []
    
    def get_degree(self, node_id: int) -> tuple:
        """获取节点的入度和出度"""
        return (len(self.in_adj[node_id]), len(self.out_adj[node_id]))
    
    def num_nodes(self) -> int:
        """返回节点数"""
        return len(self.id2node)
    
    def num_edges(self) -> int:
        """返回边数"""
        return sum(len(adj) for adj in self.out_adj)
    
    def save_mapping(self, node2id_file: str = "data/node2id.json", 
                     id2node_file: str = "data/id2node.txt"):
        """保存 ID 映射到文件"""
        # 保存 node2id
        with open(node2id_file, 'w', encoding='utf-8') as f:
            json.dump(self.node2id, f, indent=2, ensure_ascii=False)
        
        # 保存 id2node（每行一个节点名）
        with open(id2node_file, 'w', encoding='utf-8') as f:
            for node in self.id2node:
                f.write(f"{node}\n")
        
        print(f"ID 映射已保存:")
        print(f"  - {node2id_file}")
        print(f"  - {id2node_file}")
    
    @classmethod
    def from_tsv(cls, tsv_file: str) -> 'Graph':
        """
        从 TSV 文件加载图
        
        Args:
            tsv_file: edges.tsv 文件路径
            
        Returns:
            Graph 对象
        """
        print(f"正在从 TSV 文件加载图: {tsv_file}")
        start_time = time.time()
        
        graph = cls()
        
        with open(tsv_file, 'r', encoding='utf-8') as f:
            # 跳过头部
            header = f.readline()
            
            edge_count = 0
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split('\t')
                if len(parts) >= 2:
                    src = parts[0]
                    dst = parts[1]
                    label = parts[2] if len(parts) > 2 else None
                    graph.add_edge(src, dst, label)
                    edge_count += 1
                    
                    if edge_count % 50000 == 0:
                        print(f"  已加载 {edge_count:,} 条边...")
        
        elapsed = time.time() - start_time
        print(f"\n[完成] 图加载完成!")
        print(f"  节点数: {graph.num_nodes():,}")
        print(f"  边数: {graph.num_edges():,}")
        print(f"  加载时间: {elapsed:.2f} 秒")
        
        return graph


if __name__ == "__main__":
    # 从 TSV 文件加载图
    graph = Graph.from_tsv("data/edges.tsv")
    
    # 保存 ID 映射
    graph.save_mapping()
    
    # 显示一些统计信息
    print(f"\n图统计:")
    print(f"  节点数: {graph.num_nodes():,}")
    print(f"  边数: {graph.num_edges():,}")
    
    # 显示前几个节点的信息
    if graph.num_nodes() > 0:
        print(f"\n前5个节点示例:")
        for i in range(min(5, graph.num_nodes())):
            node_name = graph.get_node_name(i)
            in_deg, out_deg = graph.get_degree(i)
            print(f"  [{i}] {node_name}")
            print(f"      入度: {in_deg}, 出度: {out_deg}")

