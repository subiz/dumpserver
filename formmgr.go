package dumpserver

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/subiz/header"
	"google.golang.org/grpc"
)

type FormMgr struct {
	header.UnimplementedFormMgrServer

	lock  *sync.Mutex
	formM map[string]*header.Form
}

func (me *FormMgr) GetForm(ctx context.Context, p *header.Id) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	return &header.Response{Form: me.formM[p.GetId()]}, nil
}

func (me *FormMgr) UpdateForm(ctx context.Context, p *header.Form) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	me.formM[p.GetId()] = p
	return &header.Response{Form: p}, nil
}

func (me *FormMgr) GenerateFormLink(ctx context.Context, p *header.GenerateFormTokenRequest) (*header.Id, error) {
	return nil, nil
}

func NewFormMgr() *FormMgr {
	grpcServer := grpc.NewServer()
	mgr := &FormMgr{lock: &sync.Mutex{}, formM: map[string]*header.Form{}}
	header.RegisterFormMgrServer(grpcServer, mgr)
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 18239))
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
