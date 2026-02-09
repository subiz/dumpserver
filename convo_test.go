package dumpserver

import (
	"context"
	"github.com/subiz/header"
	"testing"
)

func TestPong(t *testing.T) {
	convomgr := NewConvoMgr(18021)
	accid := "acc123"
	convoid := "cs123"
	ev, _ := convomgr.SendMessage(context.Background(), &header.Event{
		AccountId: accid,
		Type:      "message_sent",
		By:        &header.By{Id: "us123", Type: "user"},
		Data: &header.Data{
			Message: &header.Message{
				Text:           "hello",
				ConversationId: convoid,
			},
		},
	})

	convomgr.SendMessage(context.Background(), &header.Event{
		AccountId: accid,
		Type:      "message_pong",
		By:        &header.By{Id: "us123", Type: "user"},
		Data: &header.Data{
			Message: &header.Message{
				Id:             ev.GetId(),
				ConversationId: convoid,
				Pongs:          []*header.MessagePong{{MemberId: "us123", Type: "like"}},
			},
		},
	})

	out, _ := convomgr.ListEvents(context.Background(), &header.ListConversationEventsRequest{AccountId: accid, ConversationId: convoid})
	if len(out.GetEvents() != 1) {
		t.Errorf("should be 1, got %d", len(out.GetEvents()))
	}
}
