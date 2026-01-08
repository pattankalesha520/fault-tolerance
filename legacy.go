package main
import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
type Node struct {
	ID       int
	Active   bool
	Load     float64
	Replicas int
	mu       sync.Mutex
}
type ReplicaManager struct {
	nodes []*Node
}

func (rm *ReplicaManager) Monitor() {
	for {
		for _, node := range rm.nodes {
			node.mu.Lock()
			if node.Active && rand.Float64() < 0.1 {
				node.Active = false
			} else if !node.Active && rand.Float64() < 0.05 {
				node.Active = true
			}
			node.Load = rand.Float64() * 100
			node.mu.Unlock()
		}
		time.Sleep(time.Second)
	}
}
func (rm *ReplicaManager) CheckAndRecover() {
	for {
		for _, node := range rm.nodes {
			node.mu.Lock()
			if !node.Active {
				rm.Recover(node)
			}
			node.mu.Unlock()
		}
		time.Sleep(2 * time.Second)
	}
}
func (rm *ReplicaManager) Recover(node *Node) {
	for _, n := range rm.nodes {
		if n.Active {
			n.mu.Lock()
			n.Replicas++
			n.mu.Unlock()
			break
		}
	}
	node.Active = true
	node.Replicas = 0
}
func (rm *ReplicaManager) Display() {
	for {
		fmt.Println("------ Cluster Status ------")
		for _, n := range rm.nodes {
			status := "Active"
			if !n.Active {
				status = "Failed"
			}
			n.mu.Lock()
			fmt.Printf("Node %02d | Status: %-7s | Load: %6.2f%% | Replicas: %d\n", n.ID, status, n.Load, n.Replicas)
			n.mu.Unlock()
		}
		time.Sleep(3 * time.Second)
	}
}
func createNodes(count int) []*Node {
	nodes := make([]*Node, count)
	for i := 0; i < count; i++ {
		nodes[i] = &Node{
			ID:       i + 1,
			Active:   true,
			Load:     rand.Float64() * 100,
			Replicas: 0,
		}
	}
	return nodes
}
func main() {
	rand.Seed(time.Now().UnixNano())
	nodes := createNodes(5)
	rm := &ReplicaManager{nodes: nodes}
	go rm.Monitor()
	go rm.CheckAndRecover()
	rm.Display()
}
