package dumpserver

import (
	"context"
	"testing"

	"github.com/subiz/header"
)

func TestPong(t *testing.T) {
	convomgr := NewConvoMgr(0)
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
	if len(out.GetEvents()) != 1 {
		t.Errorf("should be 1, got %d", len(out.GetEvents()))
	}
}

func TestListConversationsByUser(t *testing.T) {
	convomgr := NewConvoMgr(0)
	accid := "acc123"
	userid := "us123"

	for _, convoid := range []string{"cs123", "cs456"} {
		_, err := convomgr.SendMessage(context.Background(), &header.Event{
			AccountId: accid,
			Type:      "message_sent",
			By:        &header.By{Id: userid, Type: "user"},
			Data: &header.Data{Message: &header.Message{
				ConversationId: convoid,
				Text:           "hello",
			}},
		})
		if err != nil {
			t.Fatalf("send message to %s: %v", convoid, err)
		}
	}

	// These conversations must not be returned for us123.
	_, _ = convomgr.SendMessage(context.Background(), &header.Event{
		AccountId: accid,
		Type:      "message_sent",
		By:        &header.By{Id: "us456", Type: "user"},
		Data: &header.Data{Message: &header.Message{
			ConversationId: "cs789",
			Text:           "hello",
		}},
	})
	_, _ = convomgr.SendMessage(context.Background(), &header.Event{
		AccountId: "other-account",
		Type:      "message_sent",
		By:        &header.By{Id: userid, Type: "user"},
		Data: &header.Data{Message: &header.Message{
			ConversationId: "cs999",
			Text:           "hello",
		}},
	})

	res, err := convomgr.ListConversations(context.Background(), &header.ListConversationsRequest{
		AccountId: accid,
		UserId:    userid,
	})
	if err != nil {
		t.Fatalf("list conversations: %v", err)
	}

	convos := res.GetConversations()
	if len(convos) != 2 {
		t.Fatalf("expected 2 conversations, got %d", len(convos))
	}
	if convos[0].GetId() != "cs456" || convos[1].GetId() != "cs123" {
		t.Fatalf("unexpected conversations: %q, %q", convos[0].GetId(), convos[1].GetId())
	}
}
