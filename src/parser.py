"""
Phase 1: DOT 文件解析器
将 Graphviz DOT 文件解析为边列表，并生成统计信息
"""

import re
import json
from collections import defaultdict, Counter
from typing import Tuple, Optional
import time


class DOTParser:
    """解析 DOT 文件并生成边列表"""
    
    # 匹配 DOT 边的正则表达式
    # 格式: "src" -> "dst" [label="xxx"];
    EDGE_PATTERN = re.compile(r'"([^"]+)"\s*->\s*"([^"]+)"\s*(?:\[label="([^"]+)"\])?')
    
    def __init__(self, dot_file: str, filter_self_loops: bool = True):
        """
        初始化解析器
        
        Args:
            dot_file: DOT 文件路径
            filter_self_loops: 是否过滤自环
        """
        self.dot_file = dot_file
        self.filter_self_loops = filter_self_loops
        self.stats = {
            "edge_count_raw": 0,
            "edge_count_dedup": 0,
            "self_loop_count": 0,
            "label_distribution": Counter(),
            "dirty_nodes": set(),  # 末尾带点的节点
            "node_count": 0,
        }
    
    def parse_edge(self, line: str) -> Optional[Tuple[str, str, str]]:
        """
        解析一行，提取边信息
        
        Returns:
            (src, dst, label) 或 None
        """
        line = line.strip()
        
        # 跳过空行和注释
        if not line or line.startswith('//') or line.startswith('#'):
            return None
        
        # 跳过 digraph 声明和结束大括号
        if line.startswith('digraph') or line == '}':
            return None
        
        # 移除末尾的分号
        if line.endswith(';'):
            line = line[:-1]
        
        match = self.EDGE_PATTERN.search(line)
        if not match:
            return None
        
        src = match.group(1)
        dst = match.group(2)
        label = match.group(3) if match.group(3) else "trained_on"  # 默认 label
        
        return (src, dst, label)
    
    def is_dirty_node(self, node: str) -> bool:
        """检查是否为脏节点（末尾带点）"""
        return node.endswith('.')
    
    def parse(self, output_tsv: str = "data/edges.tsv", output_stats: str = "data/stats.json"):
        """
        解析 DOT 文件并生成边列表
        
        Args:
            output_tsv: 输出 TSV 文件路径
            output_stats: 输出统计文件路径
        """
        print(f"开始解析 DOT 文件: {self.dot_file}")
        start_time = time.time()
        
        edges_seen = set()  # 用于去重
        all_nodes = set()
        out_degree_counter = Counter()  # 用于统计出度
        
        with open(self.dot_file, 'r', encoding='utf-8') as f_in, \
             open(output_tsv, 'w', encoding='utf-8') as f_out:
            
            # 写入 TSV 头部
            f_out.write("src\tdst\tlabel\n")
            
            line_num = 0
            for line in f_in:
                line_num += 1
                if line_num % 100000 == 0:
                    print(f"  已处理 {line_num:,} 行...")
                
                edge = self.parse_edge(line)
                if edge is None:
                    continue
                
                src, dst, label = edge
                self.stats["edge_count_raw"] += 1
                
                # 检查自环
                if src == dst:
                    self.stats["self_loop_count"] += 1
                    if self.filter_self_loops:
                        continue
                
                # 检查脏节点
                if self.is_dirty_node(src):
                    self.stats["dirty_nodes"].add(src)
                if self.is_dirty_node(dst):
                    self.stats["dirty_nodes"].add(dst)
                
                # 去重
                edge_key = (src, dst, label)
                if edge_key in edges_seen:
                    continue
                edges_seen.add(edge_key)
                
                # 写入 TSV
                f_out.write(f"{src}\t{dst}\t{label}\n")
                
                # 更新统计
                self.stats["edge_count_dedup"] += 1
                self.stats["label_distribution"][label] += 1
                all_nodes.add(src)
                all_nodes.add(dst)
                out_degree_counter[src] += 1
        
        # 计算节点数
        self.stats["node_count"] = len(all_nodes)
        
        # 计算 top 出度节点
        top_out_degree = out_degree_counter.most_common(10)
        self.stats["top_out_degree_nodes"] = [
            {"node": node, "out_degree": degree} 
            for node, degree in top_out_degree
        ]
        
        # 转换 dirty_nodes 为列表（JSON 序列化需要）
        self.stats["dirty_nodes"] = list(self.stats["dirty_nodes"])
        self.stats["dirty_node_count"] = len(self.stats["dirty_nodes"])
        
        # 转换 label_distribution 为字典
        self.stats["label_distribution"] = dict(self.stats["label_distribution"])
        
        # 添加处理时间
        elapsed = time.time() - start_time
        self.stats["parse_time_seconds"] = round(elapsed, 2)
        
        # 保存统计信息
        with open(output_stats, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
        
        print(f"\n[完成] 解析完成!")
        print(f"  原始边数: {self.stats['edge_count_raw']:,}")
        print(f"  去重后边数: {self.stats['edge_count_dedup']:,}")
        print(f"  节点数: {self.stats['node_count']:,}")
        print(f"  自环数: {self.stats['self_loop_count']:,}")
        print(f"  脏节点数: {self.stats['dirty_node_count']:,}")
        print(f"  处理时间: {elapsed:.2f} 秒")
        print(f"\n  输出文件:")
        print(f"    - {output_tsv}")
        print(f"    - {output_stats}")


if __name__ == "__main__":
    parser = DOTParser("HuggingGraph.dot", filter_self_loops=True)
    parser.parse()

