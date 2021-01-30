package store

import (
	"fmt"

	"github.com/hashicorp/raft"
	keystore "github.com/jon-whit/keystore/internal"
)

// store implements a key/value store. All changes are made via Raft consensus.
type store struct {
	db   map[string]interface{}
	raft *raft.Raft
}

func NewStore() (keystore.Store, error) {
	return &store{}, nil
}

func (s *store) Get(key string) (interface{}, error) {
	return nil, fmt.Errorf("store.Get not implemented")
}

func (s *store) Set(key string, value interface{}) error {
	return fmt.Errorf("store.Set not implemented")
}

func (s *store) Delete(key string) error {
	return fmt.Errorf("store.Delete not implemented")
}
