//go:generate protoc -I ../../api/protos --go_out=../../api/protos --go_opt=paths=source_relative --go-grpc_out=../../api/protos --go-grpc_opt=paths=source_relative keystore/v1alpha1/keystore.proto
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	kspb "github.com/jon-whit/keystore/api/protos/keystore/v1alpha1"
	keystore "github.com/jon-whit/keystore/internal"
	"github.com/jon-whit/keystore/internal/store"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultServiceAddr = ":50051"
	defaultRaftAddr    = ":12000"
)

var inmem bool
var serverAddr string
var raftAddr string
var joinAddr string
var nodeID string

func init() {
	flag.BoolVar(&inmem, "inmem", false, "Use in-memory storage for Raft")
	flag.StringVar(&serverAddr, "saddr", defaultServiceAddr, "The Keystore service bind address")
	flag.StringVar(&raftAddr, "raddr", defaultRaftAddr, "The Raft Store bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&nodeID, "id", "", "Node ID")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s [options] <raft-storage-dir> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatalf("The Raft storage directory is required")
	}

	raftStorageDir := flag.Arg(0)
	if raftStorageDir == "" {
		log.Fatalf("The Raft storage directory is required")
	}

	if err := os.MkdirAll(raftStorageDir, 0700); err != nil {
		log.Fatalf("Failed to create the Raft storage directory: %v", err)
	}

	storeOpts := []store.StoreOption{
		store.RaftBindAddr(raftAddr),
		store.RaftStorageDir(raftStorageDir),
	}

	if inmem {
		storeOpts = append(storeOpts, store.InmemStore())
	}

	store := store.NewStore(storeOpts...)
	if err := store.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("Failed to open the key/value Raft Store: %v", err)
	}

	ks, err := keystore.NewKeystore(store)
	if err != nil {
		log.Fatalf("Failed to instantiate the Keystore service: %v", err)
	}

	server := grpc.NewServer()
	kspb.RegisterKeystoreServer(server, ks)
	kspb.RegisterRaftStoreServer(server, ks)
	reflection.Register(server)

	log.Infof("Starting TCP listener on port '%v'", serverAddr)
	lis, err := net.Listen("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port '%v': %v", serverAddr, err)
	}

	serverErr := make(chan error)
	go func() {
		if err := server.Serve(lis); err != nil {
			serverErr <- err
		}
	}()

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("Failed to join Raft node at '%s' to the cluster: %s", joinAddr, err.Error())
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		log.Errorf("Failed to start the server: %v", err)
	case sig := <-sig:
		log.Infof("'%v' caught. Terminating gracefully..", sig)
		server.GracefulStop()
		store.Close(true)
	}
}

func join(joinAddr, raftAddr, nodeID string) error {

	conn, err := grpc.Dial(joinAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := kspb.NewRaftStoreClient(conn)

	_, err = client.Join(context.Background(), &kspb.JoinRequest{
		NodeId:     nodeID,
		ServerAddr: raftAddr,
	})
	if err != nil {
		return err
	}

	return nil
}
