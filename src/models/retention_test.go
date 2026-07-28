package models

import "testing"

func TestContentItemBeforeCreateInitializesNewsRetentionOnly(t *testing.T) {
	news := ContentItem{Type: ContentTypeNews}
	if err := news.BeforeCreate(nil); err != nil {
		t.Fatalf("BeforeCreate NEWS: %v", err)
	}
	if news.NewsRetentionState == nil || *news.NewsRetentionState != "full" {
		t.Fatalf("NEWS state = %#v, want full", news.NewsRetentionState)
	}
	if news.NewsFeedRole == nil || *news.NewsFeedRole != "full_member" {
		t.Fatalf("NEWS role = %#v, want full_member", news.NewsFeedRole)
	}

	pods := ContentItem{Type: ContentTypePodcast}
	if err := pods.BeforeCreate(nil); err != nil {
		t.Fatalf("BeforeCreate PODCAST: %v", err)
	}
	if pods.NewsRetentionState != nil || pods.NewsFeedRole != nil {
		t.Fatal("Pods media must not receive News retention state")
	}
}
