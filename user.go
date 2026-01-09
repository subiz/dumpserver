package dumpserver

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/idgen"
	"github.com/subiz/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type UserMgr struct {
	header.UnimplementedUserMgrServer
	lock     *sync.Mutex
	userM    map[string]*header.User // accid + uid -> user
	profileM map[string]string       // accid -> convo-id -> evid -> userid
	session  *gocql.Session
}

func NewUserMgr(port int, dbip string) *UserMgr {
	mgr := &UserMgr{
		lock:     &sync.Mutex{},
		userM:    map[string]*header.User{},
		profileM: map[string]string{},
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

func (me *UserMgr) ReadOrCreateUserByContactProfile(ctx context.Context, p *header.Id) (*header.User, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	user := me.userM[p.GetId()]
	if user == nil {
		return &header.User{Id: p.Id}, nil
	}
	return user, nil

	accid, id := p.GetAccountId(), p.GetId()
	if accid == "" {
		accid = p.AccountId
	}
	if !idgen.IsAccountID(accid) {
		return nil, log.EMissingId("account")
	}
	if (p.Channel == "subiz" && p.ChannelSource == "web") || p.Channel == "" {
		if id == "" {
			id = p.ProfileId
		}
		if id == "" {
			return nil, log.EMissingId("user")
		}
		return me.userM[p.GetId()], nil
	}

	if p.Channel != "" && p.ChannelSource != "" && p.ProfileId != "" && p.Id == "" {
		channel, source, profileid := p.Channel, p.ChannelSource, p.ProfileId
		// reading old contact profile
		if channel == "email" && source == "email" {
			profileid = header.EmailAddress(profileid)
		}

		if channel == "call" && source == "call" {
			profileid = header.PhoneNumber(profileid)
		}
		profileid = strings.TrimSpace(profileid)

		key := accid + "$$" + channel + "$$" + source + "$$" + profileid
		old := me.profileM[key]
		if old != "" {
			return me.userM[old], nil
		}

		// special case for subiz only, profile.id = user.id (why???)
		if channel == "subiz" && (source == "web" || source == "") {
			return me.userM[profileid], nil
		}

		// must create new user
		u := &header.User{}
		u.Id = idgen.NewUserID()
		u.AccountId = accid
		now := time.Now()
		u.Created = now.UnixMilli()
		u.Updated = now.UnixMilli()
		u.Attributes = []*header.Attribute{{Key: "created", Datetime: now.Format(time.RFC3339), ByType: cpb.Type_subiz.String(), Modified: time.Now().UnixMilli()}}
		u.ProfileId = profileid
		u.Channel = channel
		u.ChannelSource = source
		me.profileM[key] = u.Id
		me.userM[u.GetId()] = u
		return u, nil
	}
	return nil, log.EMissingId("user")
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
