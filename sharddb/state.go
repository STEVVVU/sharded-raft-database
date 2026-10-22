package sharddb

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const Shards = 4
const MaxValue = 1024
const MaxKeys = 64
const MaxClients = 64

type Receipt struct {
	Seq         uint64
	Fingerprint string
	Value       string
}
type Shard struct {
	Epoch   uint64
	Mode    string
	Values  map[string]string
	Clients map[string]Receipt
}
type Owner struct {
	Group string
	Epoch uint64
}
type Migration struct {
	Shard    int
	From, To string
	Epoch    uint64
	Phase    int
}
type State struct {
	Owners [Shards]Owner
	Parts  [Shards]*Shard
	Move   *Migration
}
type Command struct {
	Op                        string
	Shard                     int
	Epoch                     uint64
	Group, Client, Key, Value string
	Seq                       uint64
	Part                      *Shard
	Move                      *Migration
	Phase                     int
}
type Result struct {
	Error string `json:"error,omitempty"`
	Value string `json:"value,omitempty"`
	State *State `json:"state,omitempty"`
	Part  *Shard `json:"part,omitempty"`
}
type FSM struct {
	mu    sync.Mutex
	State State
	Group string
}

func KeyShard(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % Shards)
}
func NewFSM(group string) *FSM {
	f := &FSM{Group: group}
	for i := 0; i < Shards; i++ {
		g := "a"
		if i%2 == 1 {
			g = "b"
		}
		f.State.Owners[i] = Owner{g, 1}
		if group == g {
			f.State.Parts[i] = &Shard{1, "serving", map[string]string{}, map[string]Receipt{}}
		}
	}
	return f
}
func copyJSON[T any](v T) T {
	b, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(b, &out)
	return out
}
func failure(s string) Result { return Result{Error: s} }
func (f *FSM) Apply(log *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		return failure("invalid command")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.apply(c)
}
func (f *FSM) apply(c Command) Result {
	if c.Op == "config" {
		if f.Group != "controller" {
			return failure("wrong role")
		}
		s := copyJSON(f.State)
		return Result{State: &s}
	}
	if c.Shard < 0 || c.Shard >= Shards {
		return failure("invalid shard")
	}
	if f.Group == "controller" {
		switch c.Op {
		case "begin":
			if c.Move == nil || c.Move.Shard != c.Shard || c.Move.To == c.Move.From || (c.Move.To != "a" && c.Move.To != "b") {
				return failure("invalid migration")
			}
			if f.State.Move != nil {
				if *f.State.Move == *c.Move {
					return Result{}
				}
				return failure("migration busy")
			}
			o := f.State.Owners[c.Shard]
			if o.Group != c.Move.From || o.Epoch+1 != c.Move.Epoch || c.Move.Epoch > 1_000_000 || c.Move.Phase != 0 {
				return failure("stale epoch")
			}
			f.State.Move = copyJSON(c.Move)
			return Result{}
		case "phase":
			m := f.State.Move
			if m == nil || m.Epoch != c.Epoch || m.Shard != c.Shard {
				return failure("stale migration")
			}
			if c.Phase == m.Phase {
				return Result{}
			}
			if c.Phase != m.Phase+1 {
				return failure("phase conflict")
			}
			if c.Phase == 3 {
				f.State.Owners[c.Shard] = Owner{m.To, m.Epoch}
			}
			if c.Phase == 6 {
				f.State.Move = nil
			} else {
				m.Phase = c.Phase
			}
			return Result{}
		}
		return failure("wrong role")
	}
	p := f.State.Parts[c.Shard]
	switch c.Op {
	case "install":
		if p != nil && p.Epoch == c.Epoch {
			return Result{}
		}
		if p != nil && p.Epoch >= c.Epoch {
			return failure("stale install")
		}
		if c.Part == nil || c.Part.Mode != "frozen" || c.Part.Epoch+1 != c.Epoch || !validPart(c.Part, c.Shard) {
			return failure("invalid transfer")
		}
		p = copyJSON(c.Part)
		p.Epoch = c.Epoch
		p.Mode = "staged"
		f.State.Parts[c.Shard] = p
		return Result{}
	case "freeze":
		if p == nil || p.Epoch+1 != c.Epoch {
			return failure("stale freeze")
		}
		if p.Mode != "serving" && p.Mode != "frozen" {
			return failure("not serving")
		}
		p.Mode = "frozen"
		return Result{Part: copyJSON(p)}
	case "activate":
		if p == nil || p.Epoch != c.Epoch || (p.Mode != "staged" && p.Mode != "serving") {
			return failure("stale activation")
		}
		p.Mode = "serving"
		return Result{}
	case "cleanup":
		if p == nil {
			return Result{}
		}
		if p.Epoch+1 != c.Epoch || (p.Mode != "frozen" && p.Mode != "retired") {
			return failure("stale cleanup")
		}
		// Keep the epoch tombstone to reject delayed installs after a later move.
		p.Mode = "retired"
		p.Values = map[string]string{}
		p.Clients = map[string]Receipt{}
		return Result{}
	case "get", "put", "append":
		if p == nil || p.Epoch != c.Epoch || p.Mode != "serving" {
			return failure("wrong owner")
		}
		if KeyShard(c.Key) != c.Shard || len(c.Key) == 0 || len(c.Key) > 256 || len(c.Value) > MaxValue || len(c.Client) == 0 || len(c.Client) > 64 || c.Seq == 0 {
			return failure("invalid operation")
		}
		identity, _ := json.Marshal([3]string{c.Op, c.Key, c.Value})
		fingerprint := fmt.Sprintf("%x", sha256.Sum256(identity))
		if old, ok := p.Clients[c.Client]; ok {
			if c.Seq < old.Seq {
				return failure("stale sequence")
			}
			if c.Seq == old.Seq {
				if old.Fingerprint != fingerprint {
					return failure("identity conflict")
				}
				return Result{Value: old.Value}
			}
		} else if len(p.Clients) >= MaxClients {
			return failure("client capacity")
		}
		value := p.Values[c.Key]
		if c.Op != "get" {
			if _, ok := p.Values[c.Key]; !ok && len(p.Values) >= MaxKeys {
				return failure("key capacity")
			}
			if c.Op == "append" {
				value += c.Value
			} else {
				value = c.Value
			}
			if len(value) > MaxValue {
				return failure("value capacity")
			}
			p.Values[c.Key] = value
		}
		p.Clients[c.Client] = Receipt{c.Seq, fingerprint, value}
		return Result{Value: value}
	}
	return failure("unknown operation")
}

func validPart(p *Shard, index int) bool {
	if p.Values == nil || p.Clients == nil || len(p.Values) > MaxKeys || len(p.Clients) > MaxClients {
		return false
	}
	for key, value := range p.Values {
		if len(key) == 0 || len(key) > 256 || KeyShard(key) != index || len(value) > MaxValue {
			return false
		}
	}
	for client, r := range p.Clients {
		if len(client) == 0 || len(client) > 64 || r.Seq == 0 || len(r.Value) > MaxValue || len(r.Fingerprint) != 64 {
			return false
		}
	}
	return true
}

type snapshot []byte

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, e := json.Marshal(f.State)
	return snapshot(b), e
}
func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if _, e := sink.Write(s); e != nil {
		_ = sink.Cancel()
		return e
	}
	return sink.Close()
}
func (s snapshot) Release() {}
func (f *FSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var s State
	if e := json.NewDecoder(io.LimitReader(r, 16<<20)).Decode(&s); e != nil {
		return e
	}
	f.mu.Lock()
	f.State = s
	f.mu.Unlock()
	return nil
}
