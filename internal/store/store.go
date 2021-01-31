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

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = fmt.Errorf("not leader")

	// ErrKeyNotFound is returned when a read attempt is made to a key that
	// doesn't exist.
	ErrKeyNotFound = fmt.Errorf("key not found")
)

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
// If `enableBootstrap` is true, and there are no existing peers, then
// this node becomes the first node, and therefore leader, of the cluster.
// If `` is non-empty, then a connection attempt is made to the
// existing cluster.
//
// `localID` should be the server identifier for this node.
func (s *Store) Open(enableBootstrap bool, localID string) error {
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

	if enableBootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := s.raft.BootstrapCluster(configuration)
		return f.Error()
	}

	return nil
}

func (s *Store) Close(wait bool) error {

	f := s.raft.Shutdown()

	if wait {
		if err := f.Error(); err != nil {
			return err
		}
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

func (s *Store) Join(nodeID, addr string) error {

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(nodeID)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == serverID || srv.Address == serverAddr {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == serverAddr && srv.ID == serverID {
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return err
				//return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(serverID, serverAddr, 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	return nil
}

func (s *Store) Remove(nodeID string) error {
	return fmt.Errorf("store.Remove not implemented")
}

// LeaderAddr returns the address of the current leader. Returns a
// blank string if there is no leader.
func (s *Store) LeaderAddr() string {
	return string(s.raft.Leader())
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	addr := s.LeaderAddr()
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return "", err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}

	return "", nil
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
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the underlying db.
	o := make(map[string]interface{})
	for k, v := range f.db {
		o[k] = v
	}

	return &fsmSnapshot{db: o}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {

	o := make(map[string]interface{})
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.db = o
	return nil
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
	err := func() error {
		b, err := json.Marshal(f.db)
		if err != nil {
			return err
		}

		// Write the data to the sink
		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
