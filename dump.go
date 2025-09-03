package dumpserver

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/goutils/conv"
	"github.com/subiz/header"
	apb "github.com/subiz/header/account"
	"google.golang.org/protobuf/proto"
)

type AccountMgr struct {
	header.UnimplementedAccountMgrServer
	session *gocql.Session
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
	agent := &apb.Agent{
		AccountId: &req.AccountId,
		Email:     conv.S("agent@subiz.com"),
		Id:        conv.S("ag1"),
		State:     conv.S("active"),
	}
	return &header.Response{Agents: []*apb.Agent{agent}, Total: 1}, nil
}

func (mgr *AccountMgr) GetAccount(ctx context.Context, req *header.Id) (*apb.Account, error) {
	return &apb.Account{Name: conv.S("SubizTest"), Id: conv.S(req.GetAccountId()), Currency: conv.S("VND"), State: conv.S("activated")}, nil
}

func (mgr *AccountMgr) ListActiveAccountIds(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{Ids: []string{"acpxkgumifuoofoosble"}}, nil
}

// UpsertAccount update account
func (mgr *AccountMgr) UpsertAccount(acc *apb.Account) {
	bh, _ := proto.Marshal(acc.GetBusinessHours())
	ls, _ := proto.Marshal(acc.GetLeadSetting())
	uas, _ := proto.Marshal(acc.GetUserAttributeSetting())
	ii, _ := proto.Marshal(acc.GetInvoiceInfo())

	err := mgr.session.Query("INSERT INTO account.accounts(id, address, business_hours,city, country, created,date_format, facebook, lang, lead_setting, user_attribute_setting, locale, logo_url, logo_url_128, modified, name, owner_id, phone, referrer_from,  state, supported_locales, timezone, twitter, url, zip_code, currency, currency_locked, login_locked, deleted_by_agent, invoice_info,primary_payment_method) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", acc.GetId(), acc.GetAddress(), bh, acc.GetCity(), acc.GetCountry(), acc.GetCreated(), acc.GetDateFormat(), acc.GetFacebook(), acc.GetLang(), ls, uas, acc.GetLocale(), acc.GetLogoUrl(), acc.GetLogoUrl_128(), acc.GetModified(), acc.GetName(), acc.GetOwnerId(), acc.GetPhone(), acc.GetReferrerFrom(), acc.GetState(), acc.GetSupportedLocales(), acc.GetTimezone(), acc.GetTwitter(), acc.GetUrl(), acc.GetZipCode(), acc.GetCurrency(), acc.GetCurrencyLocked(), acc.GetLoginLocked(), acc.GetDeletedByAgent(), ii, acc.GetPrimaryPaymentMethod()).Exec()
	if err != nil {
		panic(err)
	}
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
