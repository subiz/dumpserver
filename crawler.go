package dumpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/subiz/header"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

//service Crawler {
//  rpc Crawl(CrawlUrlRequest) returns (CrawlResponse); // cached
//  rpc ScreenShoot(CrawlUrlRequest) returns (CrawlResponse); // cached
//}

type Crawler struct {
	header.UnimplementedCrawlerServer
	lock        sync.RWMutex
	accessToken string
}

func (me *Crawler) SetCrawlerToken(token string) {
	me.lock.Lock()
	defer me.lock.Unlock()
	me.accessToken = token
}

func (me *Crawler) getCrawlerToken() string {
	me.lock.RLock()
	defer me.lock.RUnlock()
	return me.accessToken
}

func (me *Crawler) Crawl(ctx context.Context, in *header.CrawlUrlRequest) (*header.CrawlResponse, error) {
	// send http get request to https://api5.subiz.com.vn/3.1/crawls
	if in == nil {
		return nil, fmt.Errorf("nil crawl request")
	}

	if in.GetTimeout() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(in.GetTimeout())*time.Millisecond)
		defer cancel()
	}

	endpoint, err := url.Parse("https://api5.subiz.com.vn/3.1/crawls")
	if err != nil {
		return nil, err
	}

	accountId := in.GetAccountId()
	if accountId == "" {
		accountId = in.GetCtx().GetAccountId()
	}

	q := endpoint.Query()
	addString := func(key, value string) {
		if value != "" {
			q.Set(key, value)
		}
	}
	addBool := func(key string, value bool) {
		if value {
			q.Set(key, strconv.FormatBool(value))
		}
	}
	addInt := func(key string, value int64) {
		if value != 0 {
			q.Set(key, strconv.FormatInt(value, 10))
		}
	}

	addString("x-account-id", accountId)
	addString("x-access-token", me.getCrawlerToken())
	addString("account_id", accountId)
	addString("last_md5", in.GetLastMd5())
	addBool("link_only", in.GetLinkOnly())
	addBool("javascript_enabled", in.GetJavascriptEnabled())
	addInt("timeout", in.GetTimeout())
	addInt("max_depth", in.GetMaxDepth())
	addInt("max_links", in.GetMaxLinks())
	addString("link_regex", in.GetLinkRegex())
	for _, regex := range in.GetLinkExcludeRegexs() {
		if regex != "" {
			q.Add("link_exclude_regexs", regex)
		}
	}
	addInt("use_sitemap_xml", in.GetUseSitemapXml())
	addString("sitemap_url", in.GetSitemapUrl())
	addString("anchor", in.GetAnchor())
	addBool("force", in.GetForce())
	addString("url", in.GetUrl())
	addString("crawl_key", in.GetCrawlKey())
	addString("summary_model", in.GetSummaryModel())
	addBool("by_pass_domain_verification_check", in.GetByPassDomainVerificationCheck())
	addString("screenshoot", in.GetScreenshoot())
	addString("render_text", in.GetRenderText())
	endpoint.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if len(body) > 512 {
			body = body[:512]
		}
		return nil, fmt.Errorf("crawl request failed: %s: %s", resp.Status, string(body))
	}

	out := &header.CrawlResponse{}
	if len(body) == 0 {
		return out, nil
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, out); err != nil {
		if jsonErr := json.Unmarshal(body, out); jsonErr != nil {
			return nil, fmt.Errorf("decode crawl response: %w", err)
		}
	}
	return out, nil
}

func (me *Crawler) ScreenShoot(ctx context.Context, in *header.CrawlUrlRequest) (*header.CrawlResponse, error) {
	return nil, nil
}

func NewCrawler() *Crawler {
	grpcServer := grpc.NewServer()
	mgr := &Crawler{}
	header.RegisterCrawlerServer(grpcServer, mgr)
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 11246))
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
