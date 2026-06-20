// Package deadlock implements wait-for graph + cycle detection
// for the v0.7.1 Batch 2 H6 M6 死锁检测(防 LLM 互骂)
//
// 设计(per A2 §2.2 + H6 subtask doc):
// - 节点 = agent 任务(task_id)
// - 边 = 等待关系(A 等待 B 完成 → A→B 边)
// - 用 Kahn 算法(BFS 拓扑排序)检测环
// - 干死锁后,选最弱 trust 节点杀掉重试
//
// 实施进度:
// - v0.7.1: dry-run 模式(只告警不杀,默认 true)
// - v0.8.0: 真杀模式(dryRun=false,per A2 锁定)
package deadlock

import (
	"sort"
)

// WaitForGraph 表示 agent 任务的等待关系图
//
// 节点:task_id (string)
// 边:从等待者 → 被等待者(adj[from] = []to,表示 from 等待 to)
type WaitForGraph struct {
	adj map[string][]string // adj[from] = []to,from 等待 to
}

// NewWaitForGraph 创建空图
func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{
		adj: make(map[string][]string),
	}
}

// AddEdge 加 1 条等待边:from 等待 to
//
// 例: A 等待 B 完成 → AddEdge("A", "B")
func (g *WaitForGraph) AddEdge(from, to string) {
	if from == to {
		return // 自环不算死锁(同一个任务等自己无意义)
	}
	if !g.hasEdge(from, to) {
		g.adj[from] = append(g.adj[from], to)
	}
}

// hasEdge 检查 from→to 是否已存在
func (g *WaitForGraph) hasEdge(from, to string) bool {
	for _, t := range g.adj[from] {
		if t == to {
			return true
		}
	}
	return false
}

// RemoveEdge 删 1 条边(节点完成任务时调用)
func (g *WaitForGraph) RemoveEdge(from, to string) {
	edges := g.adj[from]
	for i, t := range edges {
		if t == to {
			g.adj[from] = append(edges[:i], edges[i+1:]...)
			return
		}
	}
}

// RemoveNode 删 1 个节点(任务完成或被杀时清理)
func (g *WaitForGraph) RemoveNode(node string) {
	delete(g.adj, node)
	// 同时删所有指向该节点的边
	for from, edges := range g.adj {
		filtered := edges[:0]
		for _, to := range edges {
			if to != node {
				filtered = append(filtered, to)
			}
		}
		g.adj[from] = filtered
	}
}

// Nodes 返回所有节点列表
func (g *WaitForGraph) Nodes() []string {
	// 收集 from + to(避免漏掉只有入度没出度的节点)
	seen := make(map[string]bool)
	for from, edges := range g.adj {
		seen[from] = true
		for _, to := range edges {
			seen[to] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out) // 稳定输出
	return out
}

// EdgeCount 返回边数
func (g *WaitForGraph) EdgeCount() int {
	n := 0
	for _, edges := range g.adj {
		n += len(edges)
	}
	return n
}

// HasCycles 检测图中是否有环(死锁)
//
// 算法:Kahn's algorithm(BFS 拓扑排序)
// - 统计每个节点的入度
// - 入度 0 的入队列,逐个弹出并减少邻居入度
// - 若最终处理节点数 < 总节点数 → 有环
//
// 返回:有环=true,环上的节点列表=[]string
func (g *WaitForGraph) HasCycles() (bool, []string) {
	nodes := g.Nodes()
	if len(nodes) == 0 {
		return false, nil
	}

	// 1. 统计入度
	inDegree := make(map[string]int, len(nodes))
	for _, n := range nodes {
		inDegree[n] = 0
	}
	for _, edges := range g.adj {
		for _, to := range edges {
			inDegree[to]++
		}
	}

	// 2. 入度 0 入队列
	queue := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if inDegree[n] == 0 {
			queue = append(queue, n)
		}
	}

	// 3. BFS 拓扑
	processed := 0
	for len(queue) > 0 {
		// 每次取最小节点(确定性输出)
		sort.Strings(queue)
		node := queue[0]
		queue = queue[1:]

		processed++
		for _, to := range g.adj[node] {
			inDegree[to]--
			if inDegree[to] == 0 {
				queue = append(queue, to)
			}
		}
	}

	// 4. 未处理的节点 = 环上节点
	if processed < len(nodes) {
		deadlocked := make([]string, 0)
		for _, n := range nodes {
			if inDegree[n] > 0 {
				deadlocked = append(deadlocked, n)
			}
		}
		return true, deadlocked
	}
	return false, nil
}

// Snapshot 返回图的字符串表示(用于日志)
func (g *WaitForGraph) Snapshot() string {
	nodes := g.Nodes()
	if len(nodes) == 0 {
		return "WaitForGraph(empty)"
	}
	out := "WaitForGraph{"
	for _, n := range nodes {
		out += n + "→["
		for i, to := range g.adj[n] {
			if i > 0 {
				out += ","
			}
			out += to
		}
		out += "];"
	}
	return out + "}"
}