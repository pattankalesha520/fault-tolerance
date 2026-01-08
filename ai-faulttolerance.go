package main
import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	ID       int
	Active   bool
	TotalMB  int
	UsedMB   int
	Replicas int
	mu       sync.Mutex
}

type FS struct {
	mu     sync.Mutex
	window int
	data   map[int][]float64
}

func NewFS(w int) *FS { return &FS{window: w, data: make(map[int][]float64)} }
func (f *FS) Add(id, used int) {
	f.mu.Lock()
	s := f.data[id]
	s = append(s, float64(used))
	if len(s) > f.window {
		s = s[len(s)-f.window:]
	}
	f.data[id] = s
	f.mu.Unlock()
}
func (f *FS) Get(id int) []float64 {
	f.mu.Lock()
	out := append([]float64(nil), f.data[id]...)
	f.mu.Unlock()
	return out
}

type Pred struct{}
func NewPred() *Pred { return &Pred{} }
func (p *Pred) Predict(s []float64) float64 {
	n := len(s)
	if n == 0 { return 0 }
	if n == 1 { return s[0] }
	var sx, sy, sxx, sxy float64
	for i := 0; i < n; i++ { x := float64(i); y := s[i]; sx += x; sy += y; sxx += x*x; sxy += x*y }
	den := float64(n)*sxx - sx*sx
	if math.Abs(den) < 1e-9 { return sy / float64(n) }
	a := (float64(n)*sxy - sx*sy) / den
	b := (sy - a*sx) / float64(n)
	pred := a*float64(n) + b
	if pred < 0 { pred = 0 }
	return pred
}

type RL struct {
	mu     sync.Mutex
	Q      map[string]map[string]float64
	acts   []string
	alpha  float64
	gamma  float64
	eps    float64
}

func NewRL() *RL {
	return &RL{Q: make(map[string]map[string]float64), acts: []string{"INCR","DECR","MIGRATE"}, alpha: 0.3, gamma: 0.9, eps: 0.2}
}







func (r *RL) choose(st string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rand.Float64() < r.eps { return r.acts[rand.Intn(len(r.acts))] }
	if _, ok := r.Q[st]; !ok { r.Q[st] = map[string]float64{}; for _, a := range r.acts { r.Q[st][a] = 0 } }
	best, bv := r.acts[0], r.Q[st][r.acts[0]]
	for _, a := range r.acts { if r.Q[st][a] > bv { best, bv = a, r.Q[st][a] } }
	return best
}
func (r *RL) learn(st, a string, rew float64, nxt string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.Q[st]; !ok { r.Q[st] = map[string]float64{}; for _, act := range r.acts { r.Q[st][act] = 0 } }
	if _, ok := r.Q[nxt]; !ok { r.Q[nxt] = map[string]float64{}; for _, act := range r.acts { r.Q[nxt][act] = 0 } }
	mx := r.Q[nxt][r.acts[0]]
	for _, act := range r.acts { if r.Q[nxt][act] > mx { mx = r.Q[nxt][act] } }
	old := r.Q[st][a]
	r.Q[st][a] = old + r.alpha*(rew + r.gamma*mx - old)
}

type RM struct {
	nodes []*Node
	fs    *FS
	pred  *Pred
	rl    *RL
	met   chan [2]int
}

func NewRM(nodes []*Node, fs *FS, pred *Pred, rl *RL, met chan [2]int) *RM {
	return &RM{nodes: nodes, fs: fs, pred: pred, rl: rl, met: met}
}

func (rm *RM) monitor(stop <-chan struct{}) {
	t := time.NewTicker(300 * time.Millisecond)
	for {
		select {
		case <-stop: return
		case <-t.C:
			for _, n := range rm.nodes {
				n.mu.Lock()
				if n.Active && rand.Float64() < 0.06 { n.Active = false }
				if !n.Active && rand.Float64() < 0.04 { n.Active = true }
				n.UsedMB += rand.Intn(51) - 25
				if n.UsedMB < 0 { n.UsedMB = 0 }
				if n.UsedMB > n.TotalMB { n.UsedMB = n.TotalMB }
				n.mu.Unlock()
				rm.met <- [2]int{n.ID, n.UsedMB}
			}
		}
	}
}

func bucket(r float64) string {
	if r > 0.85 { return "H" }
	if r > 0.6 { return "M" }
	return "L"
}

func (rm *RM) controller(stop <-chan struct{}) {
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-stop: return
		case m := <-rm.met:
			rm.fs.Add(m[0], m[1])
		case <-t.C:
			for _, n := range rm.nodes {
				s := rm.fs.Get(n.ID)
				p := rm.pred.Predict(s)
				risk := 0.0
				if n.TotalMB > 0 { risk = float64(n.UsedMB)/float64(n.TotalMB) + p/float64(n.TotalMB) }
				st := fmt.Sprintf("N%d_%s", n.ID, bucket(risk))
				act := rm.rl.choose(st)
				rm.exec(n, act)
			}
		}
	}
}

func (rm *RM) exec(n *Node, act string) {
	start := time.Now()
	switch act {
	case "INCR":
		for _, x := range rm.nodes {
			x.mu.Lock()
			if x.Active && x.ID != n.ID { x.Replicas++; x.mu.Unlock(); break }
			x.mu.Unlock()
		}
	case "DECR":
		for _, x := range rm.nodes {
			x.mu.Lock()
			if x.Replicas > 0 { x.Replicas--; x.mu.Unlock(); break }
			x.mu.Unlock()
		}
	case "MIGRATE":
		for _, x := range rm.nodes {
			x.mu.Lock()
			if x.Active && x.ID != n.ID { x.UsedMB = int(float64(x.UsedMB) * 0.9); x.mu.Unlock(); break }
			x.mu.Unlock()
		}
	}
	el := time.Since(start).Milliseconds()
	rew := rm.eval(n, el)
	nextS := fmt.Sprintf("N%d_%s", n.ID, bucket(rm.pred.Predict(rm.fs.Get(n.ID))/float64(n.TotalMB)+float64(n.UsedMB)/float64(n.TotalMB)))
	rm.rl.learn(fmt.Sprintf("N%d_%s", n.ID, bucket(float64(n.UsedMB)/float64(n.TotalMB))), act, rew, nextS)
}

func (rm *RM) eval(n *Node, execMs int64) float64 {
	sum := 0.0
	for _, x := range rm.nodes { x.mu.Lock(); sum += float64(x.Replicas); x.mu.Unlock() }
	over := sum / float64(len(rm.nodes))
	pen := float64(execMs)/100.0 + over*2.0
	return 100.0 - pen
}

func printStatus(nodes []*Node) {
	fmt.Println("----- Cluster Status -----")
	for _, n := range nodes {
		n.mu.Lock()
		st := "Active"
		if !n.Active { st = "Failed" }
		util := 0.0
		if n.TotalMB > 0 { util = float64(n.UsedMB)/float64(n.TotalMB)*100.0 }
		fmt.Printf("N%02d | %s | Tot:%4dMB | Use:%4dMB | Util:%5.1f%% | Rep:%d\n", n.ID, st, n.TotalMB, n.UsedMB, util, n.Replicas)
		n.mu.Unlock()
	}
	fmt.Println("--------------------------")
}

func createNodes(c int) []*Node {
	nodes := make([]*Node, c)
	for i := 0; i < c; i++ {
		t := 1024 + rand.Intn(1024)
		u := 256 + rand.Intn(t/2)
		nodes[i] = &Node{ID: i + 1, Active: true, TotalMB: t, UsedMB: u}
	}
	return nodes
}

func main() {
	rand.Seed(time.Now().UnixNano())
	nodes := createNodes(7)
	met := make(chan [2]int, 1024)
	fs := NewFS(12)
	pred := NewPred()
	rl := NewRL()
	rm := NewRM(nodes, fs, pred, rl, met)
	stop := make(chan struct{})
	go rm.monitor(stop)
	go rm.controller(stop)
	ticker := time.NewTicker(3 * time.Second)
	done := time.After(40 * time.Second)
	for {
		select {
		case <-done:
			close(stop)
			time.Sleep(300 * time.Millisecond)
			printStatus(nodes)
			return
		case <-ticker.C:
			printStatus(nodes)
		}
	}
}
