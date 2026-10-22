package sharddb

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func apply(f *FSM, c Command) Result {
	b, _ := json.Marshal(c)
	return f.Apply(&raft.Log{Data: b}).(Result)
}
func TestMigrationFencingAndReceipts(t *testing.T) {
	a, b := NewFSM("a"), NewFSM("b")
	key := ""
	for _, k := range []string{"a", "b", "c", "d"} {
		if KeyShard(k) == 0 {
			key = k
		}
	}
	if key == "" {
		t.Fatal("fixture")
	}
	c := Command{Op: "append", Shard: 0, Epoch: 1, Key: key, Value: "X", Client: "c", Seq: 1}
	if r := apply(a, c); r.Error != "" || r.Value != "X" {
		t.Fatal(r)
	}
	frozen := apply(a, Command{Op: "freeze", Shard: 0, Epoch: 2})
	if frozen.Part == nil {
		t.Fatal(frozen)
	}
	if apply(a, c).Error == "" {
		t.Fatal("frozen owner served")
	}
	transfer := Command{Op: "install", Shard: 0, Epoch: 2, Part: frozen.Part}
	if apply(b, transfer).Error != "" || apply(b, transfer).Error != "" {
		t.Fatal("install retry")
	}
	c.Epoch = 2
	if apply(b, c).Error == "" {
		t.Fatal("staged owner served")
	}
	if apply(b, Command{Op: "activate", Shard: 0, Epoch: 2}).Error != "" {
		t.Fatal("activate")
	}
	if r := apply(b, c); r.Value != "X" || r.Error != "" {
		t.Fatal("duplicate", r)
	}
	snap, e := b.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	restored := NewFSM("b")
	raw := snap.(snapshot)
	if e = restored.Restore(io.NopCloser(strings.NewReader(string(raw)))); e != nil {
		t.Fatal(e)
	}
	if r := apply(restored, c); r.Value != "X" || r.Error != "" {
		t.Fatal("restored receipt", r)
	}
	back := apply(b, Command{Op: "freeze", Shard: 0, Epoch: 3})
	if apply(a, Command{Op: "cleanup", Shard: 0, Epoch: 2}).Error != "" {
		t.Fatal("cleanup")
	}
	if apply(a, Command{Op: "install", Shard: 0, Epoch: 3, Part: back.Part}).Error != "" {
		t.Fatal("return install")
	}
	if apply(a, Command{Op: "cleanup", Shard: 0, Epoch: 2}).Error == "" {
		t.Fatal("stale cleanup accepted")
	}
	if apply(a, transfer).Error == "" {
		t.Fatal("stale install accepted")
	}
}
func TestControllerCAS(t *testing.T) {
	f := NewFSM("controller")
	m := &Migration{0, "a", "b", 2, 0}
	c := Command{Op: "begin", Shard: 0, Move: m}
	if apply(f, c).Error != "" || apply(f, c).Error != "" {
		t.Fatal("intent retry")
	}
	if apply(f, Command{Op: "phase", Shard: 0, Epoch: 2, Phase: 2}).Error == "" {
		t.Fatal("skipped phase")
	}
	for i := 1; i <= 6; i++ {
		if r := apply(f, Command{Op: "phase", Shard: 0, Epoch: 2, Phase: i}); r.Error != "" {
			t.Fatal(r)
		}
	}
	if f.State.Owners[0] != (Owner{"b", 2}) || f.State.Move != nil {
		t.Fatal(f.State)
	}
}

func TestBoundsAndIdentity(t *testing.T) {
	f := NewFSM("a")
	key := "a"
	for KeyShard(key) != 0 {
		key += "a"
	}
	c := Command{Op: "put", Shard: 0, Epoch: 1, Key: key, Value: "one", Client: "client", Seq: 1}
	if apply(f, c).Error != "" {
		t.Fatal("write")
	}
	c.Value = "two"
	if apply(f, c).Error != "identity conflict" {
		t.Fatal("conflicting retry")
	}
	c.Seq = 2
	c.Value = strings.Repeat("x", MaxValue+1)
	if apply(f, c).Error == "" {
		t.Fatal("oversized value")
	}
	c.Seq = 2
	c.Value = "two"
	if apply(f, c).Error != "" {
		t.Fatal("next sequence")
	}
	c.Seq = 1
	if apply(f, c).Error != "stale sequence" {
		t.Fatal("stale write")
	}
	malformed := &Shard{Epoch: 1, Mode: "frozen"}
	if apply(NewFSM("b"), Command{Op: "install", Shard: 0, Epoch: 2, Part: malformed}).Error == "" {
		t.Fatal("nil maps accepted")
	}
	for i := 0; i < MaxClients; i++ {
		c.Client = fmt.Sprint(i)
		c.Seq = 1
		apply(f, c)
	}
	c.Client = "overflow"
	if apply(f, c).Error != "client capacity" {
		t.Fatal("unbounded ledger")
	}
}

func TestResumeEveryDurablePhase(t *testing.T) {
	for phase := 0; phase <= 5; phase++ {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			f := NewFSM("controller")
			m := &Migration{0, "a", "b", 2, 0}
			apply(f, Command{Op: "begin", Shard: 0, Move: m})
			for step := 1; step <= phase; step++ {
				if r := apply(f, Command{Op: "phase", Shard: 0, Epoch: 2, Phase: step}); r.Error != "" {
					t.Fatal(r)
				}
			}
			snap, e := f.Snapshot()
			if e != nil {
				t.Fatal(e)
			}
			next := NewFSM("controller")
			if e = next.Restore(io.NopCloser(strings.NewReader(string(snap.(snapshot))))); e != nil {
				t.Fatal(e)
			}
			if next.State.Move == nil || next.State.Move.Phase != phase {
				t.Fatal("phase lost")
			}
			for step := phase + 1; step <= 6; step++ {
				if r := apply(next, Command{Op: "phase", Shard: 0, Epoch: 2, Phase: step}); r.Error != "" {
					t.Fatal(r)
				}
			}
			if next.State.Move != nil || next.State.Owners[0] != (Owner{"b", 2}) {
				t.Fatal("resume failed")
			}
		})
	}
}
