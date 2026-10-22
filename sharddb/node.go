package sharddb

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Peer struct{ ID, Group, HTTP, Raft, Dir string }
type Config struct {
	Token string
	Nodes []Peer
}
type Node struct {
	Config    Config
	Me        Peer
	FSM       *FSM
	Raft      *raft.Raft
	transport *raft.NetworkTransport
	disk      *boltdb.BoltStore
	server    *http.Server
	slots     chan struct{}
}

func Validate(c Config) error {
	if len(c.Token) < 24 || len(c.Token) > 128 || len(c.Nodes) != 9 {
		return fmt.Errorf("require capability token and nine nodes")
	}
	ids := map[string]bool{}
	addresses := map[string]bool{}
	directories := map[string]bool{}
	groups := map[string]int{}
	for _, p := range c.Nodes {
		if p.ID == "" || len(p.ID) > 64 || ids[p.ID] || p.Dir == "" {
			return fmt.Errorf("invalid node")
		}
		ids[p.ID] = true
		dir, e := filepath.Abs(p.Dir)
		if e != nil {
			return e
		}
		dir = strings.ToLower(filepath.Clean(dir))
		if directories[dir] {
			return fmt.Errorf("node directories must be distinct")
		}
		directories[dir] = true
		groups[p.Group]++
		for _, a := range []string{p.HTTP, p.Raft} {
			h, _, e := net.SplitHostPort(a)
			if e != nil || net.ParseIP(h) == nil || !net.ParseIP(h).IsLoopback() || addresses[a] {
				return fmt.Errorf("require distinct loopback addresses")
			}
			addresses[a] = true
		}
	}
	if groups["a"] != 3 || groups["b"] != 3 || groups["controller"] != 3 {
		return fmt.Errorf("require three members per group")
	}
	return nil
}
func Open(c Config, id string) (*Node, error) {
	if e := Validate(c); e != nil {
		return nil, e
	}
	var me Peer
	for _, p := range c.Nodes {
		if p.ID == id {
			me = p
		}
	}
	if me.ID == "" {
		return nil, fmt.Errorf("unknown node")
	}
	if e := os.MkdirAll(me.Dir, 0700); e != nil {
		return nil, e
	}
	n := &Node{Config: c, Me: me, FSM: NewFSM(me.Group), slots: make(chan struct{}, 16)}
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(id)
	rc.SnapshotThreshold = 64
	rc.TrailingLogs = 16
	rc.LogOutput = os.Stderr
	addr, e := net.ResolveTCPAddr("tcp", me.Raft)
	if e != nil {
		return nil, e
	}
	n.transport, e = raft.NewTCPTransport(me.Raft, addr, 3, 2*time.Second, os.Stderr)
	if e != nil {
		return nil, e
	}
	n.disk, e = boltdb.New(boltdb.Options{Path: filepath.Join(me.Dir, "raft.db")})
	if e != nil {
		n.transport.Close()
		return nil, e
	}
	snaps, e := raft.NewFileSnapshotStore(me.Dir, 2, os.Stderr)
	if e != nil {
		n.disk.Close()
		n.transport.Close()
		return nil, e
	}
	existing, e := raft.HasExistingState(n.disk, n.disk, snaps)
	if e != nil {
		n.disk.Close()
		n.transport.Close()
		return nil, e
	}
	n.Raft, e = raft.NewRaft(rc, n.FSM, n.disk, n.disk, snaps, n.transport)
	if e != nil {
		n.disk.Close()
		n.transport.Close()
		return nil, e
	}
	if !existing {
		members := raft.Configuration{}
		for _, p := range c.Nodes {
			if p.Group == me.Group {
				members.Servers = append(members.Servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Raft)})
			}
		}
		if e = n.Raft.BootstrapCluster(members).Error(); e != nil {
			n.Close()
			return nil, e
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handle)
	n.server = &http.Server{Addr: me.HTTP, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 4 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4096}
	listener, e := net.Listen("tcp", me.HTTP)
	if e != nil {
		n.Close()
		return nil, e
	}
	go func() {
		if e := n.server.Serve(listener); e != nil && e != http.ErrServerClosed {
			log.Print(e)
		}
	}()
	if me.Group == "controller" {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				if n.Raft.State() == raft.Shutdown {
					return
				}
				if n.Raft.State() == raft.Leader {
					n.step()
				}
			}
		}()
	}
	return n, nil
}
func (n *Node) Close() {
	if n.server != nil {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		_ = n.server.Shutdown(ctx)
		c()
	}
	if n.Raft != nil {
		_ = n.Raft.Shutdown().Error()
	}
	if n.transport != nil {
		_ = n.transport.Close()
	}
	if n.disk != nil {
		_ = n.disk.Close()
	}
}
func (n *Node) apply(c Command) Result {
	b, e := json.Marshal(c)
	if e != nil {
		return failure(e.Error())
	}
	future := n.Raft.Apply(b, 2*time.Second)
	if e = future.Error(); e != nil {
		return failure(e.Error())
	}
	return future.Response().(Result)
}
func (n *Node) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+n.Config.Token)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	select {
	case n.slots <- struct{}{}:
		defer func() { <-n.slots }()
	default:
		http.Error(w, "busy", 429)
		return
	}
	if r.URL.Path == "/status" && r.Method == "GET" {
		leader, _ := n.Raft.LeaderWithID()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": n.Me.ID, "group": n.Me.Group, "leader": leader, "state": n.Raft.State().String()})
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method", 405)
		return
	}
	if n.Raft.State() != raft.Leader {
		http.Error(w, "not leader", 409)
		return
	}
	var c Command
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(&c); e != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	if d.Decode(&struct{}{}) != io.EOF {
		http.Error(w, "trailing body", 400)
		return
	}
	var result Result
	switch r.URL.Path {
	case "/op":
		if c.Op != "get" && c.Op != "put" && c.Op != "append" {
			result = failure("invalid operation")
		} else {
			result = n.apply(c)
		}
	case "/config":
		result = n.apply(Command{Op: "config"})
	case "/move":
		result = n.apply(Command{Op: "begin", Shard: c.Shard, Move: c.Move})
	case "/internal":
		result = n.apply(c)
	case "/snapshot":
		if e := n.Raft.Snapshot().Error(); e != nil {
			result = failure(e.Error())
		}
	default:
		http.Error(w, "not found", 404)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
func (n *Node) rpc(group string, c Command) (Result, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	body, _ := json.Marshal(c)
	for _, p := range n.Config.Nodes {
		if p.Group != group {
			continue
		}
		req, _ := http.NewRequest("POST", "http://"+p.HTTP+"/internal", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+n.Config.Token)
		response, e := client.Do(req)
		if e != nil {
			continue
		}
		var result Result
		e = json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&result)
		response.Body.Close()
		if response.StatusCode != 200 || e != nil {
			continue
		}
		if result.Error != "" {
			return result, fmt.Errorf("%s", result.Error)
		}
		return result, nil
	}
	return Result{}, fmt.Errorf("group unavailable")
}
func (n *Node) step() Result {
	r := n.apply(Command{Op: "config"})
	if r.Error != "" {
		return r
	}
	m := r.State.Move
	if m == nil {
		return Result{}
	}
	c := Command{Shard: m.Shard, Epoch: m.Epoch}
	var e error
	switch m.Phase {
	case 0:
		c.Op = "freeze"
		_, e = n.rpc(m.From, c)
	case 1:
		c.Op = "freeze"
		var donor Result
		donor, e = n.rpc(m.From, c)
		if e == nil {
			c.Op = "install"
			c.Part = donor.Part
			_, e = n.rpc(m.To, c)
		}
	case 2:
	case 3:
		c.Op = "activate"
		_, e = n.rpc(m.To, c)
	case 4:
		c.Op = "cleanup"
		_, e = n.rpc(m.From, c)
	case 5:
	default:
		return failure("invalid phase")
	}
	if e != nil {
		return failure(e.Error())
	}
	return n.apply(Command{Op: "phase", Shard: m.Shard, Epoch: m.Epoch, Phase: m.Phase + 1})
}
