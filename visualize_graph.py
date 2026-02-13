import networkx as nx
from networkx.drawing.nx_pydot import read_dot
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端

print("正在加载 HuggingGraph.dot 文件...")
try:
    # 加载图形
    G = read_dot("HuggingGraph.dot")
    
    # 打印基本信息
    print(f"\n图形统计信息:")
    print(f"节点数: {G.number_of_nodes():,}")
    print(f"边数: {G.number_of_edges():,}")
    
    # 显示前10个节点
    nodes_list = list(G.nodes())[:10]
    print(f"\n前10个节点示例:")
    for i, node in enumerate(nodes_list, 1):
        print(f"  {i}. {node}")
    
    # 创建子图（前100个节点及其连接）
    print(f"\n正在创建子图可视化...")
    subset_nodes = list(G.nodes())[:100]
    H = G.subgraph(subset_nodes)
    
    print(f"子图节点数: {H.number_of_nodes()}")
    print(f"子图边数: {H.number_of_edges()}")
    
    # 可视化子图
    plt.figure(figsize=(20, 20))
    pos = nx.spring_layout(H, k=1, iterations=50)
    nx.draw(H, pos, with_labels=False, node_size=50, node_color='lightblue', 
            edge_color='gray', alpha=0.6, width=0.5)
    plt.title(f"HuggingGraph 子图可视化\n(前100个节点, 共{H.number_of_edges()}条边)", 
              fontsize=16, pad=20)
    plt.tight_layout()
    plt.savefig("HuggingGraph_subgraph.png", dpi=150, bbox_inches='tight')
    print(f"\n子图可视化已保存为: HuggingGraph_subgraph.png")
    
    # 导出子图为 DOT 格式
    nx.nx_pydot.write_dot(H, "subgraph_100nodes.dot")
    print(f"子图 DOT 文件已保存为: subgraph_100nodes.dot")
    
    print("\n✅ 完成！")
    
except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()

