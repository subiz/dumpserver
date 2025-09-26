package dumpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/idgen"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type ConvoMgr struct {
	header.UnimplementedConversationMgrServer

	lock     *sync.Mutex
	lastId   int64
	messages map[string]map[string]map[string]*header.Event // accid -> convo-id -> evid -> event
	convos   map[string]map[string]*header.Conversation     // accid -> convo-id -> convo
	OnEvent  func(ev *header.Event)
}

func (me *ConvoMgr) OnAIAgentUpdated(ctx context.Context, req *header.AIAgent) (*header.Response, error) {
	return &header.Response{}, nil
}

func (me *ConvoMgr) ListEvents(ctx context.Context, req *header.ListConversationEventsRequest) (*header.Events, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	accid, convoid := req.GetAccountId(), req.GetConversationId()
	messages := me.messages[accid]
	if messages == nil {
		messages = map[string]map[string]*header.Event{}
		me.messages[accid] = messages
	}

	convomsgs := messages[convoid]
	if convomsgs == nil {
		convomsgs = map[string]*header.Event{}
		messages[convoid] = convomsgs
	}

	events := []*header.Event{}
	for _, ev := range convomsgs {
		events = append(events, ev)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Id < events[j].Id
	})
	return &header.Events{Events: events}, nil
}

func (me *ConvoMgr) WaitForMessage(accid, convoid, lastid string, timeoutms int64) *header.Event {
	if timeoutms == 0 {
		timeoutms = 30_000 // 30 sec
	}
	start := time.Now().Unix()
	for {
		out, _ := me.ListEvents(context.Background(), &header.ListConversationEventsRequest{AccountId: accid, ConversationId: convoid})
		for _, ev := range out.GetEvents() {
			if ev.GetType() != "message_sent" && ev.GetType() != "llm_tool_called" {
				continue
			}

			if ev.Id > lastid {
				return ev
			}
		}

		time.Sleep(100 * time.Millisecond)
		if time.Now().Unix()-start > timeoutms/1000 {
			me.lastId++
			evid := fmt.Sprintf("ev%d", me.lastId)
			return &header.Event{Id: evid, By: &header.By{Type: "system", Id: "subiz"}, Data: &header.Data{Message: &header.Message{Text: "timeout"}}} // timeout
		}
	}
	return nil
}

func (me *ConvoMgr) AssignRule(ctx context.Context, req *header.AssignRequest) (*header.RouteResult, error) {
	return &header.RouteResult{}, nil
}

func (me *ConvoMgr) SendMessage(ctx context.Context, e *header.Event) (*header.Event, error) {
	accid := e.GetAccountId()
	convoid := e.GetData().GetMessage().GetConversationId()
	if convoid == "" {
		convoid = e.GetData().GetConversation().GetId()
	}

	e = proto.Clone(e).(*header.Event)
	me.lock.Lock()
	defer me.lock.Unlock()

	me.lastId++

	e.Created = time.Now().UnixMilli()
	if e.GetType() == "message_sent" {
		if e.GetData().GetMessage().GetText() == "" {
			m := map[string]string{}
			header.CompileBlock(e.Data.Message.Block, m)
			e.Data.Message.Text = header.BlockToPlainText(e.Data.Message.Block)
			e.Data.Message.Plaintext = e.Data.Message.Text
		}
	}

	if e.GetType() == "llm_tool_called" {
		convoid = e.GetData().GetLlmToolCall().GetConversationId()
	}

	convos := me.convos[accid]
	if convos == nil {
		convos = map[string]*header.Conversation{}
		me.convos[accid] = convos
	}

	var theconvo *header.Conversation
	if convoid != "" {
		theconvo = convos[convoid]
	}
	if theconvo == nil {
		if e.GetTouchpoint() != nil {
			for _, convo := range convos {
				if convo.GetTouchpoint().GetChannel() == e.GetTouchpoint().GetChannel() &&
					convo.GetTouchpoint().GetSource() == e.GetTouchpoint().GetSource() &&
					convo.GetTouchpoint().GetId() == e.GetTouchpoint().GetId() {
					convoid = convo.GetId()
					theconvo = convo
					break
				}
			}
		}

		if theconvo == nil {
			if convoid == "" {
				convoid = idgen.NewConversationID()
			}
			theconvo = &header.Conversation{Id: convoid, Touchpoint: e.Touchpoint}
			convos[convoid] = theconvo
		}
	}

	if convoid == "" {
		b, _ := json.Marshal(e)
		fmt.Println("GOT", string(b))
		panic("no convo")
	}

	if theconvo == nil {
		theconvo = &header.Conversation{Id: convoid, Touchpoint: e.Touchpoint}
		if theconvo.GetTouchpoint().GetChannel() == "" {
			theconvo.Touchpoint = &header.Touchpoint{Channel: "subiz", Source: "web", Id: convoid}
		}
	}
	theconvo = proto.Clone(theconvo).(*header.Conversation)
	// theconvo.LastEvent = e
	if e.GetType() == "message_sent" {

		// add member if not exist
		var themem *header.ConversationMember
		for _, mem := range theconvo.Members {
			if mem.GetId() == e.GetBy().GetId() {
				themem = mem
				break
			}
		}

		if themem == nil {
			themem = &header.ConversationMember{
				Id:   e.By.Id,
				Type: e.By.Type,
			}
			theconvo.Members = append(theconvo.Members, themem)
		}

		themem.Membership = "active"
		themem.LastSent = e.Created
		theconvo.LastMessageSent = e
	}

	if e.GetType() == header.RealtimeType_conversation_membership_updated.String() {
		for _, conmem := range theconvo.Members {
			for _, mem := range e.GetData().GetConversation().GetMembers() {
				if mem.GetId() == conmem.GetId() {
					conmem.Membership = mem.GetMembership()
				}
			}
		}
	}

	if e.GetType() == "message_sent" && e.GetBy().GetType() == "agent" {
		// mark all bot as observer
		for _, mem := range theconvo.GetMembers() {
			if mem.GetType() == "bot" && mem.GetMembership() == "active" {
				mem.Membership = "observer"
			}
		}
	}

	convos[convoid] = theconvo
	messages := me.messages[accid]
	if messages == nil {
		messages = map[string]map[string]*header.Event{}
		me.messages[accid] = messages
	}

	convomsgs := messages[convoid]
	if convomsgs == nil {
		convomsgs = map[string]*header.Event{}
		messages[convoid] = convomsgs
	}

	e.Id = fmt.Sprintf("ev%d", me.lastId)
	if e.Data != nil && e.Data.Message != nil {
		e.Data.Message.ConversationId = convoid
	}
	convomsgs[e.Id] = e
	cloneE := proto.Clone(e).(*header.Event)
	if cloneE.Ref == nil {
		cloneE.Ref = &header.Data{Conversation: theconvo}
	}
	if me.OnEvent != nil {
		go me.OnEvent(cloneE)
	}
	return e, nil
}

func (me *ConvoMgr) GetFullConversation(ctx context.Context, id *header.Id) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	accid := id.GetAccountId()
	convos := me.convos[accid]
	if convos == nil {
		convos = map[string]*header.Conversation{}
		me.convos[accid] = convos
	}

	return &header.Response{Conversation: convos[id.Id]}, nil
}

func (me *ConvoMgr) GetConversation(ctx context.Context, id *header.Id) (*header.Conversation, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	accid := id.GetAccountId()
	convos := me.convos[accid]
	if convos == nil {
		convos = map[string]*header.Conversation{}
		me.convos[accid] = convos
	}

	return convos[id.Id], nil
}

func (me *ConvoMgr) UpdateConversationInfo(ctx context.Context, convo *header.Conversation) (*header.Conversation, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	accid := convo.GetAccountId()
	convos := me.convos[accid]
	if convos == nil {
		convos = map[string]*header.Conversation{}
		me.convos[accid] = convos
	}
	convo = proto.Clone(convo).(*header.Conversation)
	convos[convo.Id] = convo
	return convo, nil
}

func (me *ConvoMgr) UpdateConversationMember(ctx context.Context, req *header.ConversationMember) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	accid := req.GetAccountId()
	convos := me.convos[accid]
	if convos == nil {
		convos = map[string]*header.Conversation{}
		me.convos[accid] = convos
	}
	convo := convos[req.GetConversationId()]
	if convo == nil {
		panic("MISSING")
	}
	var oldmem *header.ConversationMember
	for _, om := range convo.GetMembers() {
		if om.GetId() == req.GetId() {
			oldmem = om
			break
		}
	}

	if oldmem == nil {
		oldmem = req
		convo.Members = append(convo.Members, req)
	}
	oldmem.Membership = req.Membership
	convos[convo.Id] = convo
	cloneE := &header.Event{
		Id:        idgen.NewEventID(),
		Created:   time.Now().UnixMilli(),
		AccountId: req.AccountId,
		Type:      header.RealtimeType_conversation_membership_updated.String(),
		Data: &header.Data{Conversation: &header.Conversation{
			Id:      req.GetConversationId(),
			Members: []*header.ConversationMember{req},
		}},
		Ref: &header.Data{Conversation: convo},
	}
	go me.OnEvent(cloneE)
	return &header.Response{Event: cloneE}, nil
}

func NewConvoMgr(port int) *ConvoMgr {
	conmgr := &ConvoMgr{
		lock:     &sync.Mutex{},
		lastId:   time.Now().UnixMilli(),
		messages: map[string]map[string]map[string]*header.Event{}, //
		convos:   map[string]map[string]*header.Conversation{},
	}

	if port > 0 {
		grpcServer := grpc.NewServer()
		header.RegisterConversationMgrServer(grpcServer, conmgr)

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
	return conmgr
}
