package dumpserver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/subiz/header"
	"google.golang.org/grpc"
)

type DumpPubsubMgr struct {
	header.UnimplementedPubsubServer
	header.UnimplementedNumberRegistryServer
	idn  int64
	lock *sync.Mutex
}

func (mgr *DumpPubsubMgr) IsSubscribed(ctx context.Context, p *header.PsMessage) (*header.Id, error) {
	return &header.Id{}, nil
}

func (mgr *DumpPubsubMgr) Fire(ctx context.Context, p *header.PsMessage) (*header.Empty, error) {
	return &header.Empty{}, nil
}

func (mgr *DumpPubsubMgr) Poll(ctx context.Context, req *header.RealtimeSubscription) (*header.PollResult, error) {
	return &header.PollResult{}, nil
}

func (me *DumpPubsubMgr) NewID2(ctx context.Context, p *header.Id) (*header.Id, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	me.idn++
	return &header.Id{Id: strconv.Itoa(int(me.idn))}, nil
}

func NewDumpPubsubMgr(port int) *DumpPubsubMgr {
	grpcServer := grpc.NewServer()
	mgr := &DumpPubsubMgr{}
	header.RegisterPubsubServer(grpcServer, mgr)
	header.RegisterNumberRegistryServer(grpcServer, mgr)
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		panic(err)
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return mgr
}
