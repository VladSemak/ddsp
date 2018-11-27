package storage

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"storage/pb"
)

const Timeout = 5*time.Second

type Storage interface {
	Put(k RecordID, d []byte) error
	Get(k RecordID) ([]byte, error)
	Del(k RecordID) error
}

type Server struct {
	addr string
	st   Storage
	srv  *grpc.Server
}

func NewServer(st Storage, addr string) *Server {
	return &Server{
		addr: addr,
		st:   st,
		srv:  grpc.NewServer(),
	}
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("Failed to listen: %v", err)
	}

	pb.RegisterStorageServer(s.srv, s)
	log.Printf("Starting service at %v", s.addr)
	return s.srv.Serve(l)
}

func (s *Server) Stop() {
	s.srv.Stop()
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	key := RecordID(req.Key)
	log.Printf("GET request: key = %v", key)

	data, err := s.st.Get(key)
	status := ErrToStatus(err)

	reply := pb.GetReply{
		Status: int32(status),
		Data:   data,
	}

	if status == StatusUnknown {
		reply.Error = err.Error()
	}

	return &reply, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	key := RecordID(req.Key)
	log.Printf("PUT request: key = %v", key)

	err := s.st.Put(key, req.Data)
	status := ErrToStatus(err)
	reply := pb.PutReply{
		Status: int32(status),
	}
	if status == StatusUnknown {
		reply.Error = err.Error()
	}
	return &reply, nil
}

func (s *Server) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelReply, error) {
	key := RecordID(req.Key)
	log.Printf("DEL request: key = %v", key)

	err := s.st.Del(key)
	status := ErrToStatus(err)
	reply := pb.DelReply{
		Status: int32(status),
	}
	if status == StatusUnknown {
		reply.Error = err.Error()
	}
	return &reply, nil
}
