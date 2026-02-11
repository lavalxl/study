package main

import (
    "bytes"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strings"
    "sync"
)


type VectorClock map[string]int64

func (vc VectorClock) Copy() VectorClock {
    out := make(VectorClock, len(vc))
    for k, v := range vc {
        out[k] = v
    }
    return out
}
func (vc VectorClock) Merge(other VectorClock) {
    for k, v := range other {
        if v > vc[k] {
            vc[k] = v
        }
    }
}

type PatchMsg struct {
    Ops    []Operation `json:"ops"`
    VClock VectorClock `json:"vclock"`
}
type Operation struct {
    Key     string  `json:"key"`
    Value   *string `json:"value,omitempty"`
    Deleted bool    `json:"deleted"`
    Lamport int64   `json:"lamport"`
    From    string  `json:"from"`
}
type Entry struct {
    Value   *string
    Deleted bool
    Lamport int64
    From    string
}

type CRDT struct {
    mu     sync.Mutex
    data   map[string]Entry
    clock  int64
    id     string // порт
    vclock VectorClock
    peers  []string
}


func NewCRDT(id string, peerPorts []string) *CRDT {
    vclock := make(VectorClock)
    vclock[id] = 0
    for _, p := range peerPorts {
        if p != id && p != "" {
            vclock[p] = 0
        }
    }
    return &CRDT{data: map[string]Entry{}, id: id, vclock: vclock, peers: peerPorts}
}

func (c *CRDT) GetVisible() map[string]string {
    out := make(map[string]string)
    for k, v := range c.data {
        if !v.Deleted && v.Value != nil {
            out[k] = *v.Value
        }
    }
    return out
}

func (c *CRDT) NextLamport() int64 {
    c.clock++
    c.vclock[c.id] = c.clock
    return c.clock
}

func (c *CRDT) Apply(op Operation) {
    prev, ok := c.data[op.Key]
    if !ok || op.Lamport > prev.Lamport || (op.Lamport == prev.Lamport && op.From > prev.From) {
        c.data[op.Key] = Entry{
            Value:   op.Value,
            Deleted: op.Deleted,
            Lamport: op.Lamport,
            From:    op.From,
        }
    }
    if op.Lamport > c.vclock[op.From] {
        c.vclock[op.From] = op.Lamport
    }
    if op.From == c.id && op.Lamport > c.clock {
        c.clock = op.Lamport
    }
}

func (c *CRDT) DoPatchPeer(addr string, pm PatchMsg) (VectorClock, error) {
    b, _ := json.Marshal(pm)
    req, _ := http.NewRequest("PATCH", addr, bytes.NewReader(b))
    req.Header.Set("Content-Type", "application/json")
    res, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer res.Body.Close()
    var ack struct {
        VClock VectorClock `json:"vclock"`
    }
    if err := json.NewDecoder(res.Body).Decode(&ack); err != nil {
        return nil, err
    }
    return ack.VClock, nil
}

// ------ Handlers -----

func PatchHandler(crdt *CRDT) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "PATCH" {
            http.Error(w, "only PATCH", 405)
            return
        }
        var upd map[string]*json.RawMessage
        b, _ := io.ReadAll(r.Body)
        if err := json.Unmarshal(b, &upd); err != nil {
            http.Error(w, "bad json", 400)
            return
        }
        crdt.mu.Lock()
        batchLamport := crdt.NextLamport()
        ops := make([]Operation, 0, len(upd))
        for k, raw := range upd {
            var del bool
            var sval *string
            if raw == nil || string(*raw) == "null" {
                del = true
            } else {
                var v string
                if err := json.Unmarshal(*raw, &v); err == nil {
                    sval = &v
                } else {
                    http.Error(w, "bad value for "+k, 400)
                    crdt.mu.Unlock()
                    return
                }
            }
            op := Operation{Key: k, Value: sval, Deleted: del, Lamport: batchLamport, From: crdt.id}
            crdt.Apply(op)
            ops = append(ops, op)
        }
        vclockPrev := crdt.vclock.Copy()
        crdt.mu.Unlock()

        for _, peerPort := range crdt.peers {
            if peerPort == crdt.id || peerPort == "" {
                continue
            }
            addr := "http://127.0.0.1:" + peerPort + "/peer_patch"
            pm := PatchMsg{
                Ops:    ops,
                VClock: vclockPrev,
            }
            ackVClock, err := crdt.DoPatchPeer(addr, pm)
            if err == nil {
                crdt.mu.Lock()
                crdt.vclock.Merge(ackVClock)
                crdt.mu.Unlock()
                log.Printf("Merged vclock from peer %s : %v", peerPort, ackVClock)
            }
        }

        w.Header().Set("Content-Type", "application/json")
        crdt.mu.Lock()
        json.NewEncoder(w).Encode(crdt.GetVisible())
        crdt.mu.Unlock()
    }
}

func PeerPatchHandler(crdt *CRDT) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "PATCH" {
            http.Error(w, "only PATCH", 405)
            return
        }
        var pm PatchMsg
        if err := json.NewDecoder(r.Body).Decode(&pm); err != nil {
            http.Error(w, "bad ops+clock", 400)
            return
        }
        crdt.mu.Lock()
        for _, op := range pm.Ops {
            crdt.Apply(op)
        }
        crdt.clock++
        crdt.vclock[crdt.id] = crdt.clock
        myClock := crdt.vclock.Copy()
        crdt.mu.Unlock()
        resp := struct {
            VClock VectorClock `json:"vclock"`
        }{myClock}
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(resp)
    }
}

func DumpHandler(crdt *CRDT) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        crdt.mu.Lock()
        out := crdt.GetVisible()
        crdt.mu.Unlock()
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(out)
    }
}

func VClockHandler(crdt *CRDT) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        crdt.mu.Lock()
        out := crdt.vclock.Copy()
        crdt.mu.Unlock()
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(out)
    }
}

// ------ MAIN ------

func main() {
    var port string
    var peers string
    flag.StringVar(&port, "port", "", "порт текущей реплики")
    flag.StringVar(&peers, "peers", "", "порты peer-реплик, через запятую (не включая свой)")
    flag.Parse()

    if port == "" {
        fmt.Fprintf(os.Stderr, "usage: %s -port 8081 -peers 8082,8083\n", os.Args[0])
        os.Exit(1)
    }
    peerPorts := []string{}
    if peers != "" {
        peerPorts = strings.Split(peers, ",")
    }
    crdt := NewCRDT(port, peerPorts)
    mux := http.NewServeMux()
    mux.HandleFunc("/patch", PatchHandler(crdt))
    mux.HandleFunc("/peer_patch", PeerPatchHandler(crdt))
    mux.HandleFunc("/dump", DumpHandler(crdt))
    mux.HandleFunc("/vclock", VClockHandler(crdt))

    log.Printf("Listening at :%s, peers: %v", port, peerPorts)
    log.Fatal(http.ListenAndServe(":"+port, mux))
}
