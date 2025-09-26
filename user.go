package dumpserver

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/gocql/gocql"
	"github.com/subiz/header"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type UserMgr struct {
	header.UnimplementedUserMgrServer
	lock    *sync.Mutex
	userM   map[string]*header.User // accid -> convo-id -> evid -> event
	session *gocql.Session
}

func NewUserMgr(port int, dbip string) *UserMgr {
	mgr := &UserMgr{
		lock:  &sync.Mutex{},
		userM: map[string]*header.User{},
	}

	if dbip != "" {
		mgr.session = header.ConnectDB([]string{dbip}, "user")
	}
	if port > 0 {
		grpcServer := grpc.NewServer()
		header.RegisterUserMgrServer(grpcServer, mgr)

		lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			panic(err)
		}
		go func() {
			if err := grpcServer.Serve(lis); err != nil {
				panic(err)
			}
		}()
	}
	return mgr
}

func (me *UserMgr) ReadUser(ctx context.Context, p *header.Id) (*header.User, error) {

	me.lock.Lock()
	defer me.lock.Unlock()
	user := me.userM[p.GetId()]
	if user == nil {
		return &header.User{Id: p.Id}, nil
	}
	return user, nil
}

func (me *UserMgr) UpdateUser(ctx context.Context, u *header.User) (*header.User, error) {
	me.lock.Lock()
	defer me.lock.Unlock()
	user := me.userM[u.Id]
	if user == nil {
		user = u
	}
	for _, attr := range user.Attributes {
		found := false
		for _, u := range u.Attributes {
			if u.Key == attr.Key {
				found = true
			}
		}
		if !found {
			u.Attributes = append(u.Attributes, attr)
		}
	}
	me.userM[u.GetId()] = u
	return u, nil
}

func (me *UserMgr) Reset() {
	me.lock.Lock()
	defer me.lock.Unlock()

	me.userM = map[string]*header.User{}

	err := me.session.Query(`TRUNCATE user.attr_defs`).Exec()
	if err != nil {
		panic(err)
	}
}

func (me *UserMgr) AddUserAttributeDef(df *header.AttributeDefinition) {
	me.lock.Lock()
	defer me.lock.Unlock()

	data, _ := proto.Marshal(df)
	// fmt.Println("INSERT", df.GetAccountId(), df.GetKey())
	err := me.session.Query(`INSERT INTO user.attr_defs(account_id, key, data) VALUES(?,?,?)`,
		df.GetAccountId(), df.GetKey(), data).Exec()
	if err != nil {
		panic(err)
	}
}
