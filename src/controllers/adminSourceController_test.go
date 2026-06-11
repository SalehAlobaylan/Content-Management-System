package controllers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestImageFromHTMLPrefersOpenGraph(t *testing.T) {
	body := []byte(`<html><head>
		<link rel="icon" href="/favicon.ico">
		<meta property="og:image" content="/images/source.png">
	</head></html>`)

	got := imageFromHTML(body, "https://example.com/feed.xml")
	want := "https://example.com/images/source.png"
	if got != want {
		t.Fatalf("imageFromHTML() = %q, want %q", got, want)
	}
}

func TestImageFromFeedXMLReadsChannelImage(t *testing.T) {
	body := []byte(`<rss><channel><image><url>/logo.jpg</url></image></channel></rss>`)

	got := imageFromFeedXML(body, "https://example.com/rss")
	want := "https://example.com/logo.jpg"
	if got != want {
		t.Fatalf("imageFromFeedXML() = %q, want %q", got, want)
	}
}

func TestDiscoverSourceImageURLFallsBackToFavicon(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/favicon.ico" {
			w.Header().Set("Content-Type", "image/x-icon")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><head><title>No image</title></head></html>`))
	}))
	defer server.Close()

	got := discoverSourceImageURL(&server.URL)
	want := server.URL + "/favicon.ico"
	if got == nil || *got != want {
		if got == nil {
			t.Fatalf("discoverSourceImageURL() = nil, want %q", want)
		}
		t.Fatalf("discoverSourceImageURL() = %q, want %q", *got, want)
	}
}
