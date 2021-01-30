//go:generate protoc -I ../../api/protos --go_out=../../api/protos --go_opt=paths=source_relative --go-grpc_out=../../api/protos --go-grpc_opt=paths=source_relative keystore/v1alpha1/keystore.proto
package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	kspb "github.com/jon-whit/keystore/api/protos/keystore/v1alpha1"
	keystore "github.com/jon-whit/keystore/internal"
	"github.com/jon-whit/keystore/internal/store"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultServicePort = ":50051"
)

func main() {

	log.Infof("Starting TCP listener on port '%v'", defaultServicePort)
	lis, err := net.Listen("tcp", defaultServicePort)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port '%v': %v", defaultServicePort, err)
	}

	store, err := store.NewStore()
	if err != nil {
		log.Fatalf("Failed to initialize the key/value Store: %v", err)
	}

	ks, err := keystore.NewKeystore(store)
	if err != nil {
		log.Fatalf("Failed to instantiate the Keystore service: %v", err)
	}

	server := grpc.NewServer()
	kspb.RegisterKeystoreServer(server, ks)
	reflection.Register(server)

	serverErr := make(chan error)
	go func() {
		if err := server.Serve(lis); err != nil {
			serverErr <- err
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		log.Errorf("Failed to start the server: %v", err)
	case sig := <-sig:
		log.Infof("'%v' caught. Terminating gracefully..", sig)
		server.GracefulStop()
	}
}
