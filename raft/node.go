package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Role int

const (
	Replica Role = iota
	Master
)

type logEntry struct {
	Key   string
	Value string
	Op    string
	Term  int
}

type Node struct {
	mu         sync.RWMutex
	id         int
	role       Role
	port       int
	address    string
	peers      []string
	masterID   int
	data       map[string]string
	isAlive    bool
	term       int
	votedFor   int
	logEntries []logEntry
	lastLogIdx int
	aliveAtLastPing []bool
}

func NewNode(id, port int, peers []string) *Node {
  alive := make([]bool, len(peers))
  for i := range alive {
	  alive[i] = true // в начале считаем всех живыми
  }
	return &Node{
		id:         id,
		port:       port,
		address:    fmt.Sprintf("http://localhost:%d", port),
		peers:      peers,
		data:       make(map[string]string),
		role:       Replica,
		masterID:   -1,
		isAlive:    true,
		term:       0,
		votedFor:   -1,
		logEntries: make([]logEntry, 0),
		lastLogIdx: -1,
    aliveAtLastPing: alive,
	}
}

func (n *Node) logPrefix() string {
	return fmt.Sprintf("[node:%d,role:%v,mst:%d,t:%d]", n.id, n.role, n.masterID, n.term)
}
func (n *Node) logState() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	keys := []string{}
	for k := range n.data {
		keys = append(keys, k)
	}
	log.Printf("[%d] CURRENT ROLE=%v MASTER_ID=%d KEYS=%v", n.id, n.role, n.masterID, keys)
	for k, v := range n.data {
		log.Printf("[%d]    %s = %s", n.id, k, v)
	}
}


// ======================= WORK WITH DATA =======================
func (n *Node) handleData(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/data/")
	switch r.Method {
	case "GET":
		// Если мастер — редиректим на реплику
		if n.role == Master {
			var replicas []string
			for i, addr := range n.peers {
				if i != n.id && addr != "" {
					replicas = append(replicas, addr)
				}
			}
			if len(replicas) == 0 {
				http.Error(w, "no replicas", 503)
				return
			}
			pick := replicas[rand.Intn(len(replicas))]
			w.Header().Set("Location", pick+"/data/"+key)
			w.WriteHeader(http.StatusFound)
			return
		}

		n.mu.RLock()
		value, ok := n.data[key]
		n.mu.RUnlock()

		// Нашли ключ
		if ok {
			resp := map[string]interface{}{
				"key":   key,
				"value": value,
			}
			json.NewEncoder(w).Encode(resp)
			return
		}

		// Просто не найдено на реплике
		http.Error(w, "not found", 404)
		return
	case "PUT":
		if n.role != Master {
			http.Error(w, "not master", 403)
			return
		}
		if key == "" || key == "/" {
			// batch put
			var req map[string]string
			err := json.NewDecoder(r.Body).Decode(&req)
			if err != nil {
				http.Error(w, "bad request", 400)
				return
			}
			entries := make([]logEntry, 0, len(req))
			n.mu.Lock()
			for k, v := range req {
				n.data[k] = v
				le := logEntry{Key: k, Value: v, Op: "PUT", Term: n.term}
				n.logEntries = append(n.logEntries, le)
				n.lastLogIdx++
				entries = append(entries, le)
			}
			idx := n.lastLogIdx - len(entries)
			n.mu.Unlock()
			n.replicateLog(entries, idx)
			w.WriteHeader(201)
			return
		}
		var req struct{ Value string }
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		n.mu.Lock()
		n.data[key] = req.Value
		le := logEntry{Key: key, Value: req.Value, Op: "PUT", Term: n.term}
		n.logEntries = append(n.logEntries, le)
		n.lastLogIdx++
		idx := n.lastLogIdx - 1
		n.mu.Unlock()
		n.replicateLog([]logEntry{le}, idx-1)
		w.WriteHeader(201)
	case "POST":
		if n.role != Master {
			http.Error(w, "not master", 403)
			return
		}
		var cas struct {
			Expect string `json:"expect"`
			Update string `json:"update"`
		}
		err := json.NewDecoder(r.Body).Decode(&cas)
		if err == nil && cas.Expect != "" {
			n.mu.Lock()
			current, ok := n.data[key]
			if ok && current == cas.Expect {
				n.data[key] = cas.Update
				le := logEntry{Key: key, Value: cas.Update, Op: "CAS", Term: n.term}
				n.logEntries = append(n.logEntries, le)
				n.lastLogIdx++
				idx := n.lastLogIdx - 1
				n.mu.Unlock()
				n.replicateLog([]logEntry{le}, idx-1)
				w.WriteHeader(200)
				w.Write([]byte(`{"result":"swapped"}`))
				return
			}
			n.mu.Unlock()
			http.Error(w, "CAS failed: value mismatch", 409)
			return
		}
	case "DELETE":
		if n.role != Master {
			http.Error(w, "not master", 403)
			return
		}
		n.mu.Lock()
		delete(n.data, key)
		le := logEntry{Key: key, Value: "", Op: "DELETE", Term: n.term}
		n.logEntries = append(n.logEntries, le)
		n.lastLogIdx++
		idx := n.lastLogIdx - 1
		n.mu.Unlock()
		n.replicateLog([]logEntry{le}, idx-1)
		w.WriteHeader(204)
	default:
		w.WriteHeader(405)
	}
}

func (n *Node) replicateLog(newEntries []logEntry, prevLogIdx int) {
	for i, addr := range n.peers {
		if i == n.id || addr == "" {
			continue
		}
		go func(addr string) {
			client := http.Client{Timeout: 1 * time.Second}
			body := map[string]interface{}{
				"LogEntries": newEntries,
				"PrevLogIdx": prevLogIdx,
			}
			b, _ := json.Marshal(body)
			resp, err := client.Post(addr+"/replicate", "application/json", strings.NewReader(string(b)))
			if err != nil {
				log.Printf("%s failed replicate to %s: %v", n.logPrefix(), addr, err)
				return
			}
			if resp != nil {
				resp.Body.Close()
			}
		}(addr)
	}
}

func (n *Node) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		LogEntries []logEntry
		PrevLogIdx int
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// Записываем только если наш lastLogIdx == PrevLogIdx
	if n.lastLogIdx != req.PrevLogIdx {
		go n.ensureLogIsSyncedFromMaster()
		http.Error(w, "log index mismatch", 409)
		return
	}
	for _, entry := range req.LogEntries {
		n.logEntries = append(n.logEntries, entry)
		n.lastLogIdx++
		switch entry.Op {
		case "PUT", "POST", "CAS":
			n.data[entry.Key] = entry.Value
		case "DELETE":
			delete(n.data, entry.Key)
		}
	}
	w.WriteHeader(200)
}
// ===============================================================

// ======================= LEADERSHIP ============================
func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.isAlive {
		http.Error(w, "down", 503)
		return
	}
	fmt.Fprintf(w, "ok %d %d", n.id, n.term)
}

func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	term, _ := strconv.Atoi(q.Get("term"))
	candidate, _ := strconv.Atoi(q.Get("candidate"))

	n.mu.Lock()
	defer n.mu.Unlock()
	if term > n.term {
		n.term = term
		n.votedFor = -1
		n.masterID = candidate
	}
	if n.votedFor == -1 || n.votedFor == candidate {
		n.votedFor = candidate
		log.Printf("Voted for: %d", candidate)
		w.WriteHeader(200)
		return
	}
	w.WriteHeader(409)
}

func (n *Node) monitorMaster() {
	for {
		time.Sleep(2 * time.Second)
		n.mu.RLock()
		if n.role == Master {
			n.mu.RUnlock()
			continue
		}
		mID := n.masterID
		mAddr := ""
		if mID >= 0 && mID < len(n.peers) {
			mAddr = n.peers[mID]
		}
		n.mu.RUnlock()

		if mAddr == "" {
			log.Printf("%s: no master known, start election", n.logPrefix())
			n.startElection()
			continue
		}

		client := http.Client{Timeout: 1 * time.Second}
		url := mAddr + "/ping"
		resp, err := client.Get(url)
		if err != nil || resp.StatusCode != 200 {
			log.Printf("%s: master %s unreachable, start election", n.logPrefix(), mAddr)
			n.startElection()
			continue
		}
		resp.Body.Close()
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.term++
	myTerm := n.term
	candidateID := n.id
	n.votedFor = candidateID
	n.mu.Unlock()

	var votes int32 = 1
	var wg sync.WaitGroup
	var peers = n.peers

	for i, addr := range peers {
		if i == candidateID || addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string, i int) {
			defer wg.Done()
			client := &http.Client{Timeout: 1 * time.Second}
			resp, err := client.Post(fmt.Sprintf("%s/vote?term=%d&candidate=%d", addr, myTerm, candidateID), "", nil)
			if err == nil && resp.StatusCode == 200 {
				votes++
			}
			if resp != nil {
				resp.Body.Close()
			}
		}(addr, i)
	}
	wg.Wait()

	if votes > int32(len(peers)/2) {
		n.mu.Lock()
		n.role = Master
		n.masterID = n.id
		n.votedFor = n.id
		log.Printf("%s: became master! Votes: %d", n.logPrefix(), votes)
		n.mu.Unlock()
		for i, addr := range peers {
			if i == n.id || addr == "" {
				continue
			}
			go func(addr string) {
				client := http.Client{Timeout: 1 * time.Second}
				client.Post(addr+"/notify_master?id="+strconv.Itoa(n.id), "", nil)
			}(addr)
		}
	}
  n.logState()
}

func (n *Node) handleNotifyMaster(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  mID, _ := strconv.Atoi(q.Get("id"))
  n.mu.Lock()
  wasMaster := n.role == Master
  n.masterID = mID
  if n.id != mID {
      n.role = Replica
  }
  lastLogIdx := n.lastLogIdx
  logLength := len(n.logEntries)
  n.mu.Unlock()
  if lastLogIdx < 0 || logLength == 0 || wasMaster {
      n.ensureLogIsSyncedFromMaster()
  }
  n.logState()
  w.WriteHeader(200)
}

func (n *Node) ensureLogIsSyncedFromMaster() {
	go func() {
		for {
			n.mu.RLock()
			shouldSync := (n.lastLogIdx < 0 || len(n.logEntries) == 0)
			mID := n.masterID
			addr := ""
			if mID >= 0 && mID < len(n.peers) {
				addr = n.peers[mID]
			}
			self := n.address
			n.mu.RUnlock()
			// Лог не пуст и индекс > 0 — всё ок, можно прекратить retry
			if !shouldSync {
				return
			}
			// Не знаем мастера или мастер это мы
			if addr == "" || addr == self {
				time.Sleep(1 * time.Second)
				continue
			}
			// Пытаемся скачать полный лог
			resp, err := http.Get(addr + "/full_log")
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			var body struct {
				Log  []logEntry
				Term int
			}
			err = json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			// Обновляем состояние
			n.mu.Lock()
			n.logEntries = make([]logEntry, len(body.Log))
			copy(n.logEntries, body.Log)
			n.data = make(map[string]string)
			n.lastLogIdx = -1
			n.term = body.Term
			for _, entry := range n.logEntries {
				n.lastLogIdx++
				switch entry.Op {
				case "PUT", "POST":
					n.data[entry.Key] = entry.Value
				case "DELETE":
					delete(n.data, entry.Key)
				}
			}
			n.mu.Unlock()
			log.Printf("%s full resync from master done: %d log entries", n.logPrefix(), len(body.Log))
			return
		}
	}()
}

func (n *Node) handleFullLog(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	type fullLogResp struct {
		Log  []logEntry
		Term int
	}
	json.NewEncoder(w).Encode(fullLogResp{Log: n.logEntries, Term: n.term})
}
// ===============================================================


// ======================= RECOVERY ===============================
func (n *Node) monitorReplicas() {
	for {
		time.Sleep(2 * time.Second)
		n.mu.RLock()
		if n.role != Master {
			n.mu.RUnlock()
			time.Sleep(2 * time.Second)
			continue
		}
		peers := append([]string(nil), n.peers...)
		n.mu.RUnlock()
		for i, addr := range peers {
			if i == n.id || addr == "" {
				continue
			}
			go func(i int, addr string) {
				client := http.Client{Timeout: 1 * time.Second}
				resp, err := client.Get(addr + "/ping")
				n.mu.Lock()
				alivePrev := n.aliveAtLastPing[i]
				isAlive := false
				if err == nil && resp != nil && resp.StatusCode == 200 {
					isAlive = true
					resp.Body.Close()
				}
				if !alivePrev && isAlive { // Был мертв, а потом ожил!
					log.Printf("%s: detected recovery of node %d, sending catchup_log", n.logPrefix(), i)
					go n.sendFullLogToReplica(addr)
				}
				n.aliveAtLastPing[i] = isAlive
				n.mu.Unlock()
			}(i, addr)
		}
	}
}

// полный лог на восстановившуюся реплику
func (n *Node) sendFullLogToReplica(addr string) {
	n.mu.RLock()
	body := map[string]interface{}{
		"LogEntries": n.logEntries,
		"Term":       n.term,
	}
	b, _ := json.Marshal(body)
	n.mu.RUnlock()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(addr+"/catchup_log", "application/json", strings.NewReader(string(b)))
	if err != nil {
		log.Printf("%s: catchup_log send failed to %s: %v", n.logPrefix(), addr, err)
		return
	}
	resp.Body.Close()
}

// принимаем лог + data
func (n *Node) handleCatchupLog(w http.ResponseWriter, r *http.Request) {
	type reqStruct struct {
		LogEntries []logEntry
		Term       int
	}
	var req reqStruct
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	n.mu.Lock()
	n.logEntries = make([]logEntry, len(req.LogEntries))
	copy(n.logEntries, req.LogEntries)
	n.term = req.Term
	n.data = make(map[string]string)
	n.lastLogIdx = -1
	for _, entry := range n.logEntries {
		n.lastLogIdx++
		switch entry.Op {
		case "PUT", "POST":
			n.data[entry.Key] = entry.Value
		case "DELETE":
			delete(n.data, entry.Key)
		}
	}
	n.mu.Unlock()
	log.Printf("%s: catchup_log applied! loglen=%d", n.logPrefix(), len(req.LogEntries))
  n.logState()
	w.WriteHeader(200)
}
// ================================================================


// ======================= LOG ====================================
func (n *Node) handleLog(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	json.NewEncoder(w).Encode(n.logEntries)
}
// ======================= WhoIsMaster ============================
func (n *Node) handleWhoIsMaster(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	masterID := n.masterID
	masterAddr := ""
	if masterID >= 0 && masterID < len(n.peers) {
		masterAddr = n.peers[masterID]
	}
	n.mu.RUnlock()
	json.NewEncoder(w).Encode(map[string]string{"master": masterAddr})
}
// ================================================================


func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: node <id> <port> <peer1,peer2,...>")
		os.Exit(1)
	}
	id, _ := strconv.Atoi(os.Args[1])
	port, _ := strconv.Atoi(os.Args[2])
	peers := strings.Split(os.Args[3], ",")

	node := NewNode(id, port, peers)

	// ВСЕГДА начинай как реплика !!!
	node.role = Replica
	node.masterID = -1

	// При запуске: ищем мастера среди всех нод
	masterFound := false
	for _, addr := range peers {
		if addr == node.address {
			continue
		}
		resp, err := http.Get(addr + "/whois_master")
		if err == nil && resp.StatusCode == 200 {
			var ans struct{ Master string }
			_ = json.NewDecoder(resp.Body).Decode(&ans)
			resp.Body.Close()
			for i, a := range peers {
				if a == ans.Master && ans.Master != "" {
					node.masterID = i
					masterFound = true
					break
				}
			}
		}
		if masterFound {
			break
		}
	}
	go node.monitorMaster()
	go node.monitorReplicas()

	http.HandleFunc("/data/", node.handleData)
	http.HandleFunc("/replicate", node.handleReplicate)
	http.HandleFunc("/ping", node.handlePing)
	http.HandleFunc("/vote", node.handleVote)
	http.HandleFunc("/whois_master", node.handleWhoIsMaster)
	http.HandleFunc("/notify_master", node.handleNotifyMaster)
	http.HandleFunc("/log", node.handleLog)
	http.HandleFunc("/full_log", node.handleFullLog)
	http.HandleFunc("/catchup_log", node.handleCatchupLog)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", port)}
	log.Printf("%s starting HTTP on :%d", node.logPrefix(), port)
	err := srv.ListenAndServe()
	if err != nil {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
