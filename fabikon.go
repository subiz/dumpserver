package dumpserver

import (
	"context"
	"sync"

	"github.com/subiz/header"
)

type FabikonMgr struct {
	header.UnimplementedFabikonServiceServer

	lock  sync.Mutex
	posts map[string]*header.FacebookPost
}

func (me *FabikonMgr) UpsertFacebookPost(ctx context.Context, req *header.FacebookPost) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	if me.posts == nil {
		me.posts = make(map[string]*header.FacebookPost)
	}
	me.posts[req.GetId()] = req
	return &header.Response{}, nil
}

func (me *FabikonMgr) ReadFbFanpageSetting(ctx context.Context, req *header.Id) (*header.FbFanpageSetting, error) {
	return &header.FbFanpageSetting{}, nil
}

func (me *FabikonMgr) UpdateFbFanpageSetting(ctx context.Context, req *header.FbFanpageSetting) (*header.FbFanpageSetting, error) {
	return &header.FbFanpageSetting{}, nil
}

func (me *FabikonMgr) ListFbFanpageSettings(ctx context.Context, req *header.Id) (*header.FbFanpageSettings, error) {
	return &header.FbFanpageSettings{}, nil
}

func (me *FabikonMgr) ListFacebookPosts(ctx context.Context, req *header.FacebookPostRequest) (*header.Response, error) {
	return &header.Response{}, nil
}

func (me *FabikonMgr) MatchFacebookPosts(ctx context.Context, req *header.Ids) (*header.Response, error) {
	me.lock.Lock()
	defer me.lock.Unlock()

	posts := []*header.FacebookPost{}
	for _, id := range req.GetIds() {
		if post, ok := me.posts[id]; ok {
			posts = append(posts, post)
		}
	}
	return &header.Response{FacebookPosts: posts}, nil
}

func (me *FabikonMgr) ResyncFacebookPost(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{}, nil
}

func (me *FabikonMgr) RemoveFbUserLabel(ctx context.Context, req *header.User) (*header.Empty, error) {
	return &header.Empty{}, nil
}

func (me *FabikonMgr) AddFbUserLabel(ctx context.Context, req *header.User) (*header.Empty, error) {
	return &header.Empty{}, nil
}

func (me *FabikonMgr) GenerateRefLink(ctx context.Context, req *header.Id) (*header.Id, error) {
	return &header.Id{}, nil
}

func (me *FabikonMgr) SyncAdsFlow(ctx context.Context, req *header.FacebookAdsFlow) (*header.Id, error) {
	return &header.Id{}, nil
}

func (me *FabikonMgr) GetAdsAudience(ctx context.Context, req *header.MetaCustomAudience) (*header.MetaCustomAudience, error) {
	return &header.MetaCustomAudience{}, nil
}

func (me *FabikonMgr) CreateAdsAudience(ctx context.Context, req *header.MetaCustomAudience) (*header.MetaCustomAudience, error) {
	return &header.MetaCustomAudience{}, nil
}

func (me *FabikonMgr) UploadAdsAudienceUsers(ctx context.Context, req *header.CustomAudienceBatchRequest) (*header.CustomAudienceBatchResponse, error) {
	return &header.CustomAudienceBatchResponse{}, nil
}

func (me *FabikonMgr) DeleteAdsAudienceUsers(ctx context.Context, req *header.CustomAudienceBatchRequest) (*header.CustomAudienceBatchResponse, error) {
	return &header.CustomAudienceBatchResponse{}, nil
}

func (me *FabikonMgr) ListAdAccounts(ctx context.Context, req *header.Id) (*header.Response, error) {
	return &header.Response{}, nil
}

func (me *FabikonMgr) ListFbFanpageSettings2(ctx context.Context, req *header.ListPageSettingRequest) (*header.Response, error) {
	return &header.Response{}, nil
}
