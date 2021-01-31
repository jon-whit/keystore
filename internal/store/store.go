package store

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	opTimeout           = 1 * time.Second
	setKeyOp            = "set"
	deleteKeyOp         = "delete"
)

var ErrKeyNotFound = fmt.Errorf("key not found")

// command represents an oplog command persisted to the replicated log.
type command struct {
	Op    string
	Key   string
	Value interface{}
}

// Store implements a key/value store. All changes are made via Raft consensus.
type Store struct {
	raftBindAddr   string
	raftStorageDir string
	inmem          bool
	db             map[string]interface{}
	mu             sync.Mutex
	raft           *raft.Raft
}

type StoreOption func(s *Store)

func InmemStore() StoreOption {
	return func(s *Store) {
		s.inmem = true
	}
}

func RaftBindAddr(addr string) StoreOption {
	return func(s *Store) {
		s.raftBindAddr = addr
	}
}

func RaftStorageDir(dir string) StoreOption {
	return func(s *Store) {
		s.raftStorageDir = dir
	}
}

func NewStore(opts ...StoreOption) *Store {

	s := &Store{
		db: map[string]interface{}{},
	}

	for _, applyOptTo := range opts {
		applyOptTo(s)
	}

	return s
}

// Open sets up the Raft node and opens the store.
//
// If `enableSingle` is set, and there are no existing peers, then
// this node becomes the first node, and therefore leader, of the cluster.
// `localID` should be the server identifier for this node.
func (s *Store) Open(enableSingle bool, localID string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft TCP communication
	addr, err := net.ResolveTCPAddr("tcp", s.raftBindAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(s.raftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftStorageDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return err
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore
	if s.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.raftStorageDir, "raft.db"))
		if err != nil {
			return fmt.Errorf("Failed to initialize the Bolt Store: %s", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("Failed to initialize the Raft node: %v", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

func (s *Store) Get(key string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if val, ok := s.db[key]; ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

func (s *Store) Set(key string, value interface{}) error {

	if s.raft.State() != raft.Leader {
		return fmt.Errorf("Writes must be directed toward the Raft Leader")
	}

	c := command{
		Op:    setKeyOp,
		Key:   key,
		Value: value,
	}
	cmd, err := json.Marshal(c)
	if err != nil {

	}

	f := s.raft.Apply(cmd, opTimeout)
	return f.Error()
}

func (s *Store) Delete(key string) error {

	if s.raft.State() != raft.Leader {
		return fmt.Errorf("Deletes must be directed toward the Raft Leader")
	}

	c := command{
		Op:  deleteKeyOp,
		Key: key,
	}
	cmd, err := json.Marshal(c)
	if err != nil {

	}

	f := s.raft.Apply(cmd, opTimeout)
	return f.Error()
}

type fsm Store

func (f *fsm) Apply(log *raft.Log) interface{} {

	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal FSM command: %s", err.Error()))
	}

	switch cmd.Op {
	case setKeyOp:
		return f.applySet(cmd.Key, cmd.Value)
	case deleteKeyOp:
		return f.applyDelete(cmd.Key)
	default:
		panic(fmt.Sprintf("Unrecognized FSM command received: (%s)", cmd.Op))
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, fmt.Errorf("fsm.Snapshot not implemented")
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("fsm.Restore not implemented")
}

func (f *fsm) applySet(key string, value interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.db, key)
	return nil
}

type fsmSnapshot struct {
	db map[string]interface{}
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return fmt.Errorf("fsmSnapshot.Persist not implemented")
}

func (f *fsmSnapshot) Release() {}
