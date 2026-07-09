package dumpserver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/subiz/header"
)

func TestCrawlerCrawlSubizDocs(t *testing.T) {
	token := os.Getenv("CRAWLER_ACCESS_TOKEN")
	if token == "" {
		t.Skip("set CRAWLER_ACCESS_TOKEN to run crawler integration test")
	}

	accountId := "acpxkgumifuoofoosble"

	crawler := &Crawler{}
	crawler.SetCrawlerToken(token)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	out, err := crawler.Crawl(ctx, &header.CrawlUrlRequest{
		AccountId:                     accountId,
		Url:                           "https://subiz.com.vn/docs/1802811302-nhan-phan-loai-khach-hang",
		SummaryModel:                  "gemini-2.5-flash",
		ByPassDomainVerificationCheck: true,
		RenderText:                    "markdown",
		Timeout:                       60000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if out.GetStatusCode() != 200 {
		t.Fatalf("status_code = %d, want 200", out.GetStatusCode())
	}

	if !hasString(out.GetLinks(), "https://app.subiz.com.vn") {
		t.Fatalf("links should include https://app.subiz.com.vn, got %v", out.GetLinks())
	}
}

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
