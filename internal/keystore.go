package keystore

import (
	"context"

	kspb "github.com/jon-whit/keystore/api/protos/keystore/v1alpha1"
	"github.com/jon-whit/keystore/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Store defines the interface to manage key/value pairs.
type Store interface {
	Get(key string) (interface{}, error)
	Set(key string, value interface{}) error
	Delete(key string) error
}

// Keystore implements the key/value storage service for the v1alpha1 keystore API.
type Keystore struct {
	kspb.UnimplementedKeystoreServer

	store Store
}

func NewKeystore(s Store) (*Keystore, error) {
	return &Keystore{
		store: s,
	}, nil
}

func (ks *Keystore) Get(ctx context.Context, in *kspb.GetRequest) (*kspb.GetResponse, error) {

	val, err := ks.store.Get(in.GetKey())
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Error(codes.NotFound, err.Error())
		}

		return nil, err
	}

	protoVal, err := structpb.NewValue(val)
	if err != nil {
		// todo: handle error
		return nil, err
	}

	response := kspb.GetResponse{
		Key:   in.GetKey(),
		Value: protoVal,
	}

	return &response, nil
}

func (ks *Keystore) Set(ctx context.Context, in *kspb.SetRequest) (*emptypb.Empty, error) {

	err := ks.store.Set(in.GetKey(), in.GetValue().AsInterface())
	if err != nil {
		// todo: handle error
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ks *Keystore) Delete(ctx context.Context, in *kspb.DeleteRequest) (*emptypb.Empty, error) {

	err := ks.store.Delete(in.GetKey())
	if err != nil {
		// todo: handle error
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
