"""
Phase 3: Lineage Query Engine
实现 ancestors, descendants, k_hop 等查询接口
"""

import time
from typing import List, Set, Optional, Dict
from collections import deque
from dataclasses import dataclass

from graph import Graph


@dataclass
class QueryResult:
    """查询结果结构"""
    nodes: Set[int]  # 节点 ID 集合
    visited_count: int  # 访问的节点数
    hops_reached: int  # 达到的最大跳数
    elapsed_ms: float  # 查询耗时（毫秒）
    
    def to_dict(self):
        """转换为字典"""
        return {
            "node_count": len(self.nodes),
            "visited_count": self.visited_count,
            "hops_reached": self.hops_reached,
            "elapsed_ms": self.elapsed_ms
        }


class QueryEngine:
    """Lineage 查询引擎"""
    
    def __init__(self, graph: Graph, use_epoch_visited: bool = False):
        """
        初始化查询引擎
        
        Args:
            graph: Graph 对象
            use_epoch_visited: 是否使用 epoch-based visited（优化1）
        """
        self.graph = graph
        self.use_epoch_visited = use_epoch_visited
        
        # Epoch-based visited（优化1）
        if use_epoch_visited:
            self.visited_epoch = [0] * graph.num_nodes()
            self.current_epoch = 0
        else:
            self.visited_epoch = None
            self.current_epoch = None
    
    def _mark_visited(self, node_id: int):
        """标记节点为已访问"""
        if self.use_epoch_visited:
            self.visited_epoch[node_id] = self.current_epoch
        else:
            # 在 BFS 中使用 visited set
            pass
    
    def _is_visited(self, node_id: int) -> bool:
        """检查节点是否已访问"""
        if self.use_epoch_visited:
            return self.visited_epoch[node_id] == self.current_epoch
        else:
            # 在 BFS 中使用 visited set
            return False
    
    def _start_query(self):
        """开始新查询（epoch-based）"""
        if self.use_epoch_visited:
            self.current_epoch += 1
    
    def ancestors(self, node: str, max_hops: Optional[int] = None, 
                  limit: Optional[int] = None) -> QueryResult:
        """
        查找节点的所有祖先（上游节点）
        
        Args:
            node: 节点名称
            max_hops: 最大跳数，None 表示全图可达
            limit: 结果数量限制，None 表示无限制
            
        Returns:
            QueryResult 对象
        """
        start_time = time.time()
        self._start_query()
        
        node_id = self.graph.get_node_id(node)
        if node_id is None:
            return QueryResult(nodes=set(), visited_count=0, hops_reached=0, 
                             elapsed_ms=(time.time() - start_time) * 1000)
        
        visited = set() if not self.use_epoch_visited else None
        result_nodes = set()
        queue = deque([(node_id, 0)])  # (node_id, hop)
        
        if not self.use_epoch_visited:
            visited.add(node_id)
        else:
            self._mark_visited(node_id)
        
        result_nodes.add(node_id)
        max_hop_reached = 0
        
        while queue:
            current, hop = queue.popleft()
            max_hop_reached = max(max_hop_reached, hop)
            
            # 检查限制
            if limit and len(result_nodes) >= limit:
                break
            
            # 检查最大跳数
            if max_hops is not None and hop >= max_hops:
                continue
            
            # 遍历入边邻居（前驱节点）
            for neighbor in self.graph.get_in_neighbors(current):
                if self.use_epoch_visited:
                    if self._is_visited(neighbor):
                        continue
                    self._mark_visited(neighbor)
                else:
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                
                result_nodes.add(neighbor)
                queue.append((neighbor, hop + 1))
        
        elapsed_ms = (time.time() - start_time) * 1000
        return QueryResult(
            nodes=result_nodes,
            visited_count=len(result_nodes),
            hops_reached=max_hop_reached,
            elapsed_ms=elapsed_ms
        )
    
    def descendants(self, node: str, max_hops: Optional[int] = None,
                   limit: Optional[int] = None) -> QueryResult:
        """
        查找节点的所有后代（下游节点）
        
        Args:
            node: 节点名称
            max_hops: 最大跳数，None 表示全图可达
            limit: 结果数量限制，None 表示无限制
            
        Returns:
            QueryResult 对象
        """
        start_time = time.time()
        self._start_query()
        
        node_id = self.graph.get_node_id(node)
        if node_id is None:
            return QueryResult(nodes=set(), visited_count=0, hops_reached=0,
                             elapsed_ms=(time.time() - start_time) * 1000)
        
        visited = set() if not self.use_epoch_visited else None
        result_nodes = set()
        queue = deque([(node_id, 0)])  # (node_id, hop)
        
        if not self.use_epoch_visited:
            visited.add(node_id)
        else:
            self._mark_visited(node_id)
        
        result_nodes.add(node_id)
        max_hop_reached = 0
        
        while queue:
            current, hop = queue.popleft()
            max_hop_reached = max(max_hop_reached, hop)
            
            # 检查限制
            if limit and len(result_nodes) >= limit:
                break
            
            # 检查最大跳数
            if max_hops is not None and hop >= max_hops:
                continue
            
            # 遍历出边邻居（后继节点）
            for neighbor in self.graph.get_out_neighbors(current):
                if self.use_epoch_visited:
                    if self._is_visited(neighbor):
                        continue
                    self._mark_visited(neighbor)
                else:
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                
                result_nodes.add(neighbor)
                queue.append((neighbor, hop + 1))
        
        elapsed_ms = (time.time() - start_time) * 1000
        return QueryResult(
            nodes=result_nodes,
            visited_count=len(result_nodes),
            hops_reached=max_hop_reached,
            elapsed_ms=elapsed_ms
        )
    
    def k_hop(self, node: str, k: int, direction: str = "out") -> QueryResult:
        """
        k-hop 查询
        
        Args:
            node: 节点名称
            k: 跳数
            direction: "out" (下游) 或 "in" (上游)
            
        Returns:
            QueryResult 对象
        """
        if direction == "out":
            return self.descendants(node, max_hops=k)
        elif direction == "in":
            return self.ancestors(node, max_hops=k)
        else:
            raise ValueError(f"direction 必须是 'out' 或 'in'，得到: {direction}")
    
    def shortest_path(self, src: str, dst: str, direction: str = "out") -> Optional[List[int]]:
        """
        查找最短路径
        
        Args:
            src: 源节点
            dst: 目标节点
            direction: "out" (下游) 或 "in" (上游)
            
        Returns:
            路径（节点 ID 列表），如果不存在则返回 None
        """
        self._start_query()
        
        src_id = self.graph.get_node_id(src)
        dst_id = self.graph.get_node_id(dst)
        
        if src_id is None or dst_id is None:
            return None
        
        if src_id == dst_id:
            return [src_id]
        
        visited = set() if not self.use_epoch_visited else None
        queue = deque([(src_id, [src_id])])  # (node_id, path)
        
        if not self.use_epoch_visited:
            visited.add(src_id)
        else:
            self._mark_visited(src_id)
        
        while queue:
            current, path = queue.popleft()
            
            # 选择邻居方向
            if direction == "out":
                neighbors = self.graph.get_out_neighbors(current)
            else:
                neighbors = self.graph.get_in_neighbors(current)
            
            for neighbor in neighbors:
                if neighbor == dst_id:
                    return path + [neighbor]
                
                if self.use_epoch_visited:
                    if self._is_visited(neighbor):
                        continue
                    self._mark_visited(neighbor)
                else:
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                
                queue.append((neighbor, path + [neighbor]))
        
        return None  # 无路径


if __name__ == "__main__":
    # 加载图
    print("正在加载图...")
    graph = Graph.from_tsv("data/edges.tsv")
    
    # 创建查询引擎（baseline）
    print("\n创建查询引擎 (baseline)...")
    engine = QueryEngine(graph, use_epoch_visited=False)
    
    # 测试查询
    test_nodes = [
        "mistralai/Mistral-7B-v0.1",  # 高出度节点
        "meta-llama/Meta-Llama-3-8B-Instruct",
    ]
    
    for node in test_nodes:
        if graph.get_node_id(node) is None:
            print(f"\n节点 '{node}' 不存在，跳过")
            continue
        
        print(f"\n测试节点: {node}")
        
        # descendants 查询
        print("  descendants (k=2):")
        result = engine.descendants(node, max_hops=2)
        print(f"    节点数: {result.visited_count:,}")
        print(f"    跳数: {result.hops_reached}")
        print(f"    耗时: {result.elapsed_ms:.2f} ms")
        
        # ancestors 查询
        print("  ancestors (k=2):")
        result = engine.ancestors(node, max_hops=2)
        print(f"    节点数: {result.visited_count:,}")
        print(f"    跳数: {result.hops_reached}")
        print(f"    耗时: {result.elapsed_ms:.2f} ms")
        
        # k-hop 查询
        print("  k_hop (k=3, direction='out'):")
        result = engine.k_hop(node, k=3, direction="out")
        print(f"    节点数: {result.visited_count:,}")
        print(f"    跳数: {result.hops_reached}")
        print(f"    耗时: {result.elapsed_ms:.2f} ms")

