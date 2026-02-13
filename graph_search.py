"""
HuggingGraph 检索工具
支持对图结构进行各种检索操作
"""

import networkx as nx
from networkx.drawing.nx_pydot import read_dot
import pickle
import os
from typing import List, Set, Optional


class HuggingGraphSearcher:
    """HuggingGraph 图检索类"""
    
    def __init__(self, dot_file: str = "HuggingGraph.dot", cache_file: str = "graph_cache.pkl"):
        """
        初始化图检索器
        
        Args:
            dot_file: DOT 文件路径
            cache_file: 缓存文件路径（用于加速后续加载）
        """
        self.dot_file = dot_file
        self.cache_file = cache_file
        self.G = None
        self._load_graph()
    
    def _load_graph(self):
        """加载图（优先使用缓存）"""
        if os.path.exists(self.cache_file):
            print(f"从缓存加载图: {self.cache_file}")
            with open(self.cache_file, 'rb') as f:
                self.G = pickle.load(f)
            print(f"✅ 图已加载: {self.G.number_of_nodes():,} 个节点, {self.G.number_of_edges():,} 条边")
        else:
            print(f"正在从 DOT 文件加载图: {self.dot_file}")
            print("这可能需要几分钟时间...")
            self.G = read_dot(self.dot_file)
            # 转换为有向图（如果还不是）
            if not isinstance(self.G, nx.DiGraph):
                self.G = self.G.to_directed()
            
            print(f"✅ 图已加载: {self.G.number_of_nodes():,} 个节点, {self.G.number_of_edges():,} 条边")
            
            # 保存缓存
            print(f"正在保存缓存到: {self.cache_file}")
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.G, f)
            print("✅ 缓存已保存")
    
    def search_node(self, keyword: str, limit: int = 10) -> List[str]:
        """
        搜索包含关键词的节点
        
        Args:
            keyword: 搜索关键词
            limit: 返回结果数量限制
            
        Returns:
            匹配的节点列表
        """
        keyword_lower = keyword.lower()
        matches = [node for node in self.G.nodes() 
                  if keyword_lower in str(node).lower()]
        return matches[:limit]
    
    def get_node_neighbors(self, node: str, direction: str = "both") -> dict:
        """
        获取节点的邻居
        
        Args:
            node: 节点名称
            direction: "in" (入边), "out" (出边), "both" (双向)
            
        Returns:
            包含前驱和后继的字典
        """
        if node not in self.G:
            return {"error": f"节点 '{node}' 不存在"}
        
        result = {
            "node": node,
            "in_degree": self.G.in_degree(node),
            "out_degree": self.G.out_degree(node),
        }
        
        if direction in ["in", "both"]:
            result["predecessors"] = list(self.G.predecessors(node))
        
        if direction in ["out", "both"]:
            result["successors"] = list(self.G.successors(node))
        
        return result
    
    def find_path(self, source: str, target: str) -> Optional[List[str]]:
        """
        查找两个节点之间的最短路径
        
        Args:
            source: 源节点
            target: 目标节点
            
        Returns:
            路径列表，如果不存在则返回 None
        """
        if source not in self.G:
            return None
        if target not in self.G:
            return None
        
        try:
            path = nx.shortest_path(self.G, source, target)
            return path
        except nx.NetworkXNoPath:
            return None
    
    def forward_trace(self, node: str, max_depth: int = 3) -> dict:
        """
        前向追踪：查找从该节点出发的所有下游节点（依赖链）
        
        Args:
            node: 起始节点
            max_depth: 最大追踪深度
            
        Returns:
            按深度组织的节点字典
        """
        if node not in self.G:
            return {"error": f"节点 '{node}' 不存在"}
        
        visited = set()
        result = {0: [node]}
        visited.add(node)
        
        current_level = [node]
        
        for depth in range(1, max_depth + 1):
            next_level = []
            for n in current_level:
                for successor in self.G.successors(n):
                    if successor not in visited:
                        visited.add(successor)
                        next_level.append(successor)
            
            if not next_level:
                break
            
            result[depth] = next_level
            current_level = next_level
        
        return result
    
    def backward_trace(self, node: str, max_depth: int = 3) -> dict:
        """
        后向追踪：查找指向该节点的所有上游节点（依赖链）
        
        Args:
            node: 起始节点
            max_depth: 最大追踪深度
            
        Returns:
            按深度组织的节点字典
        """
        if node not in self.G:
            return {"error": f"节点 '{node}' 不存在"}
        
        visited = set()
        result = {0: [node]}
        visited.add(node)
        
        current_level = [node]
        
        for depth in range(1, max_depth + 1):
            next_level = []
            for n in current_level:
                for predecessor in self.G.predecessors(n):
                    if predecessor not in visited:
                        visited.add(predecessor)
                        next_level.append(predecessor)
            
            if not next_level:
                break
            
            result[depth] = next_level
            current_level = next_level
        
        return result
    
    def get_subgraph(self, node: str, depth: int = 2) -> nx.DiGraph:
        """
        获取以某个节点为中心的子图
        
        Args:
            node: 中心节点
            depth: 子图深度
            
        Returns:
            子图对象
        """
        if node not in self.G:
            return nx.DiGraph()
        
        nodes_to_include = {node}
        current_nodes = {node}
        
        for _ in range(depth):
            next_nodes = set()
            for n in current_nodes:
                next_nodes.update(self.G.successors(n))
                next_nodes.update(self.G.predecessors(n))
            nodes_to_include.update(next_nodes)
            current_nodes = next_nodes
        
        return self.G.subgraph(nodes_to_include).copy()
    
    def get_statistics(self) -> dict:
        """获取图的基本统计信息"""
        return {
            "nodes": self.G.number_of_nodes(),
            "edges": self.G.number_of_edges(),
            "is_directed": self.G.is_directed(),
            "density": nx.density(self.G),
        }


# 示例使用
if __name__ == "__main__":
    print("=" * 60)
    print("HuggingGraph 检索工具")
    print("=" * 60)
    
    # 初始化检索器
    searcher = HuggingGraphSearcher()
    
    # 显示统计信息
    stats = searcher.get_statistics()
    print(f"\n📊 图统计信息:")
    print(f"  节点数: {stats['nodes']:,}")
    print(f"  边数: {stats['edges']:,}")
    print(f"  图密度: {stats['density']:.6f}")
    
    # 示例1: 搜索节点
    print(f"\n🔍 示例1: 搜索包含 'llama' 的节点")
    llama_nodes = searcher.search_node("llama", limit=5)
    for i, node in enumerate(llama_nodes, 1):
        print(f"  {i}. {node}")
    
    # 示例2: 获取节点邻居
    if llama_nodes:
        print(f"\n📌 示例2: 获取节点 '{llama_nodes[0]}' 的邻居")
        neighbors = searcher.get_node_neighbors(llama_nodes[0], direction="both")
        print(f"  入度: {neighbors['in_degree']}, 出度: {neighbors['out_degree']}")
        if 'predecessors' in neighbors and neighbors['predecessors']:
            print(f"  前驱节点 (前5个): {neighbors['predecessors'][:5]}")
        if 'successors' in neighbors and neighbors['successors']:
            print(f"  后继节点 (前5个): {neighbors['successors'][:5]}")
    
    # 示例3: 前向追踪
    if llama_nodes:
        print(f"\n➡️  示例3: 前向追踪 '{llama_nodes[0]}' (深度=2)")
        forward = searcher.forward_trace(llama_nodes[0], max_depth=2)
        for depth, nodes in forward.items():
            if isinstance(nodes, list):
                print(f"  深度 {depth}: {len(nodes)} 个节点")
                if nodes:
                    print(f"    示例: {nodes[0]}")
    
    # 示例4: 后向追踪
    if llama_nodes:
        print(f"\n⬅️  示例4: 后向追踪 '{llama_nodes[0]}' (深度=2)")
        backward = searcher.backward_trace(llama_nodes[0], max_depth=2)
        for depth, nodes in backward.items():
            if isinstance(nodes, list):
                print(f"  深度 {depth}: {len(nodes)} 个节点")
                if nodes:
                    print(f"    示例: {nodes[0]}")
    
    print(f"\n✅ 检索工具已就绪！")
    print(f"\n💡 使用方法:")
    print(f"  from graph_search import HuggingGraphSearcher")
    print(f"  searcher = HuggingGraphSearcher()")
    print(f"  # 然后调用各种检索方法")

