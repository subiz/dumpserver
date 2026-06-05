package dumpserver

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/header"
	apb "github.com/subiz/header/account"
	cpb "github.com/subiz/header/common"
	ppb "github.com/subiz/header/payment"
	"google.golang.org/protobuf/proto"
)

type AccountMgr struct {
	header.UnimplementedAccountMgrServer
	header.UnimplementedPaymentMgrServer
	session *gocql.Session
}

var (
	dumpAccountMu sync.RWMutex
	dumpAccount   = defaultDumpAccount()
)

func defaultDumpAccount() *apb.Account {
	return &apb.Account{
		Name:     new("SubizTest"),
		Currency: new("VND"),
		State:    new("activated"),
		Timezone: new("+07:00"),
		BusinessHours: &apb.BusinessHours{
			WorkingDays: []*apb.BusinessHours_WorkingDay{
				{Weekday: new("Monday"), StartTime: new("08:00"), EndTime: new("12:00")},
				{Weekday: new("Monday"), StartTime: new("13:00"), EndTime: new("17:00")},
				{Weekday: new("Tuesday"), StartTime: new("08:00"), EndTime: new("12:00")},
				{Weekday: new("Tuesday"), StartTime: new("13:00"), EndTime: new("17:00")},
				{Weekday: new("Wednesday"), StartTime: new("08:00"), EndTime: new("12:00")},
				{Weekday: new("Wednesday"), StartTime: new("13:00"), EndTime: new("17:00")},
				{Weekday: new("Thursday"), StartTime: new("08:00"), EndTime: new("12:00")},
				{Weekday: new("Thursday"), StartTime: new("13:00"), EndTime: new("17:00")},
				{Weekday: new("Friday"), StartTime: new("08:00"), EndTime: new("12:00")},
				{Weekday: new("Friday"), StartTime: new("13:00"), EndTime: new("17:00")},
				{Weekday: new("Saturday"), StartTime: new("08:00"), EndTime: new("12:00")},
				{Weekday: new("Saturday"), StartTime: new("13:00"), EndTime: new("17:00")},
			},
			Holidays: []*apb.BusinessHours_Holiday{
				{Year: new(int32(2026)), Month: new(int32(5)), Day: new(int32(1)), Name: new("1/5")},
				{Year: new(int32(2026)), Month: new(int32(4)), Day: new(int32(30)), Name: new("30/4")},
				{Year: new(int32(2026)), Month: new(int32(4)), Day: new(int32(27)), Name: new("giổ tổ hùng vương")},
			},
		},
	}
}

func (mgr *AccountMgr) InviteEmails(ctx context.Context, req *header.InviteRequest) (*header.Empty, error) {
	return &header.Empty{}, nil
}

func (mgr *AccountMgr) GetInviteLink(ctx context.Context, req *header.Id) (*header.Id, error) {
	return &header.Id{}, nil
}

func (mgr *AccountMgr) RegenerateInviteLink(ctx context.Context, req *header.Id) (*header.Id, error) {
	return &header.Id{}, nil
}

func (mgr *AccountMgr) CheckInviteLink(ctx context.Context, req *header.Id) (*header.InvitationLink, error) {
	return &header.InvitationLink{}, nil
}

func (mgr *AccountMgr) JoinAccount(ctx context.Context, req *header.JoinAccountRequest) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) RequestOTP(ctx context.Context, req *header.Id) (*header.Empty, error) {
	return &header.Empty{}, nil
}

func (mgr *AccountMgr) LoginUsingOTP(ctx context.Context, req *header.LoginRequest) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) CheckEmailUsed(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) UpdateAgentProfile(ctx context.Context, req *header.AgentProfile) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) GetAgentProfile(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) ListAgentProfileAccounts(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) Login(ctx context.Context, req *header.LoginRequest) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) OldLogin(ctx context.Context, req *header.LoginRequest) (*header.Response, error) {
	return &header.Response{}, nil
}

func (mgr *AccountMgr) CreateGroup(ctx context.Context, req *header.AgentGroup) (*header.AgentGroup, error) {
	return &header.AgentGroup{}, nil
}

func (mgr *AccountMgr) ListAgents(ctx context.Context, req *header.Id) (*header.Response, error) {
	agent1 := &apb.Agent{
		AccountId: &req.AccountId,
		Email:     new("agent@subiz.com"),
		Id:        new("ag1"),
		State:     new("active"),
	}

	agent2 := &apb.Agent{
		AccountId: &req.AccountId,
		Email:     new("agent2@subiz.com"),
		Id:        new("ag2"),
		State:     new("active"),
	}
	return &header.Response{Agents: []*apb.Agent{agent1, agent2}, Total: 2}, nil
}

func (mgr *AccountMgr) GetAccount(ctx context.Context, req *header.Id) (*apb.Account, error) {
	dumpAccountMu.RLock()
	acc := proto.Clone(dumpAccount).(*apb.Account)
	dumpAccountMu.RUnlock()

	acc.Id = new(req.GetAccountId())
	return acc, nil
}

func (mgr *AccountMgr) GetSubscription(ctx context.Context, req *header.Id) (*ppb.Subscription, error) {
	t := uint32(12)
	return &ppb.Subscription{
		AccountId:              new(req.GetId()),
		Plan:                   new("advanced_unlimited_agent"),
		BillingCycleMonth:      &t,
		Started:                new(int64(time.Now().UnixMilli() - 86400000)),
		Ended:                  new(int64(time.Now().UnixMilli() + 465*86400000)),
		FpvCreditUsd:           new(int64(9201411625030429000)),
		FpvMarketingBalanceVnd: new(int64(9201411625030429000)),
		FpvNovatBalanceUsd:     new(int64(9201411625030429000)),
		Limit: &cpb.Limit{
			MaxZaloPersonals:    3,
			UseZaloPersonals:    1,
			MaxZaloOas:          1000,
			MaxFanpages:         1000,
			MaxTiktoks:          1000,
			MaxInstagrams:       1000,
			MaxGoogleBusinesses: 1000,
			UseTicket:           1,
			MaxAgents:           1000,
			MaxSegments:         1000,
			MaxAutomations:      10000,
			UseAutomation:       1,
			UseChatbotAi:        1,
		},
	}, nil

}

func (mgr *AccountMgr) ListActiveAccountIds(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{Ids: []string{"acpxkgumifuoofoosble"}}, nil
}

// UpsertAccount update account
func (mgr *AccountMgr) UpsertAccount(acc *apb.Account) {
	dumpAccountMu.Lock()
	dumpAccount = proto.Clone(acc).(*apb.Account)
	dumpAccountMu.Unlock()
}

func (me *AccountMgr) NewID(ctx context.Context, p *header.Id) (*header.Id, error) {
	accid, scope := p.GetAccountId(), p.GetId()
	unlock := header.KLock("id#" + accid + "." + scope)
	var id int64
	err := me.session.Query(`SELECT id FROM account.ids WHERE account_id=? AND scope=?`, accid, scope).Scan(&id)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		err = nil
	}
	if err != nil {
		time.Sleep(5 * time.Second)
		unlock()
		return me.NewID(ctx, p)
	}

	// hard coded start scope
	if scope == "product" {
		// avoid collision
		if id < 240000 {
			id = 240000
		}
	}
	id++
	if scope == "order" {
		// random skip
		id += int64(rand.Int() % 5)
	}
	err = me.session.Query(`INSERT INTO account.ids(account_id,scope,id,created) VALUES(?,?,?,?)`, accid, scope, id, time.Now().UnixMilli()).Exec()
	if err != nil {
		unlock()
		return me.NewID(ctx, p)
	}
	unlock()
	return &header.Id{Id: strconv.Itoa(int(id))}, nil
}

func NewAccountMgr(port int) *AccountMgr {
	mgr := &AccountMgr{}
	mgr.session = header.ConnectDB([]string{"db-0"}, "account")
	grpcServer := header.NewShardServer2(port, 1)
	header.RegisterAccountMgrServer(grpcServer, mgr)
	header.RegisterPaymentMgrServer(grpcServer, mgr)
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
