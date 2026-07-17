package controllers

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestTruncateRunesPreservesValidArabicAndEmojiUTF8(t *testing.T) {
	value := strings.Repeat("مرحبا🙂", 100)
	got := truncateRunes(value, 241)
	if !utf8.ValidString(got) {
		t.Fatal("truncation produced invalid UTF-8")
	}
	if count := len([]rune(got)); count != 241 {
		t.Fatalf("rune count = %d, want 241", count)
	}
}

func TestValidateUserContentAudioChecksExtensionMimeAndMagic(t *testing.T) {
	valid := userContentAudioHeader(t, "episode.mp3", "audio/mpeg", []byte("ID3test-audio"))
	if err := validateUserContentAudio(valid); err != nil {
		t.Fatalf("valid mp3 rejected: %v", err)
	}
	mismatch := userContentAudioHeader(t, "episode.mp3", "audio/mpeg", []byte("RIFFxxxxWAVE"))
	if err := validateUserContentAudio(mismatch); err == nil {
		t.Fatal("wav bytes disguised as mp3 must be rejected")
	}
	wrongMIME := userContentAudioHeader(t, "episode.mp3", "image/png", []byte("ID3test-audio"))
	if err := validateUserContentAudio(wrongMIME); err == nil {
		t.Fatal("wrong MIME must be rejected")
	}
}

func TestValidateUserContentAudioAcceptsMIMEParameters(t *testing.T) {
	header := userContentAudioHeader(t, "episode.mp3", "audio/mpeg; charset=binary", []byte("ID3test-audio"))
	if err := validateUserContentAudio(header); err != nil {
		t.Fatalf("valid parameterized MIME rejected: %v", err)
	}
}

func TestValidateUserContentMultipartFormRejectsDuplicateAndUnexpectedParts(t *testing.T) {
	validFile := userContentAudioHeader(t, "episode.mp3", "audio/mpeg", []byte("ID3test-audio"))
	for name, form := range map[string]*multipart.Form{
		"valid": {
			Value: map[string][]string{"title": {"A title"}, "body_text": {"A body"}},
			File:  map[string][]*multipart.FileHeader{"audio_file": {validFile}},
		},
		"duplicate text": {
			Value: map[string][]string{"title": {"one", "two"}},
		},
		"unexpected text": {
			Value: map[string][]string{"title": {"one"}, "surprise": {"two"}},
		},
		"duplicate file": {
			Value: map[string][]string{"title": {"one"}},
			File:  map[string][]*multipart.FileHeader{"audio_file": {validFile, validFile}},
		},
		"unexpected file": {
			Value: map[string][]string{"title": {"one"}},
			File:  map[string][]*multipart.FileHeader{"image_file": {validFile}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateUserContentMultipartForm(form)
			if name == "valid" && err != nil {
				t.Fatalf("valid multipart form rejected: %v", err)
			}
			if name != "valid" && err == nil {
				t.Fatal("invalid multipart form was accepted")
			}
		})
	}
}

func TestDispatchAudioToAggregationStreamsMultipartAndPreservesContentID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	payload := bytes.Repeat([]byte("audio-data"), 16*1024)
	header := userContentAudioHeader(t, "episode.mp3", "audio/mpeg", append([]byte("ID3"), payload...))
	contentID := uuid.New()
	requestSeen := make(chan struct {
		contentID string
		tenantID  string
		typeName  string
		bytes     int64
	}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, "bad multipart", http.StatusBadRequest)
			return
		}
		file, _, err := r.FormFile("audio_file")
		if err != nil {
			http.Error(w, "missing audio", http.StatusBadRequest)
			return
		}
		defer file.Close()
		count, err := io.Copy(io.Discard, file)
		if err != nil {
			http.Error(w, "read failure", http.StatusBadRequest)
			return
		}
		requestSeen <- struct {
			contentID string
			tenantID  string
			typeName  string
			bytes     int64
		}{r.FormValue("content_item_id"), r.FormValue("tenant_id"), r.FormValue("content_type"), count}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	t.Setenv("AGGREGATION_BASE_URL", server.URL)

	c := userContentRequestContext(t, context.Background())
	if err := dispatchAudioToAggregation(c, contentID, "tenant-a", header); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	got := <-requestSeen
	if got.contentID != contentID.String() || got.tenantID != "tenant-a" || got.typeName != "PODCAST" {
		t.Fatalf("multipart fields = %#v", got)
	}
	if got.bytes != int64(len(payload)+3) {
		t.Fatalf("streamed byte count = %d, want %d", got.bytes, len(payload)+3)
	}
}

func TestDispatchAudioToAggregationHonorsRequestCancellation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	header := userContentAudioHeader(t, "episode.mp3", "audio/mpeg", append([]byte("ID3"), bytes.Repeat([]byte("x"), 64*1024)...))
	serverStarted := make(chan struct{}, 1)
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverStarted <- struct{}{}
		// Deliberately do not read the request body. This models a stalled
		// downstream and ensures the client-side pipe is released by context
		// cancellation rather than by a cooperative peer.
		<-releaseServer
	}))
	t.Setenv("AGGREGATION_BASE_URL", server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	c := userContentRequestContext(t, ctx)
	result := make(chan error, 1)
	go func() { result <- dispatchAudioToAggregation(c, uuid.New(), "tenant-a", header) }()
	select {
	case <-serverStarted:
	case <-time.After(time.Second):
		t.Fatal("downstream server was not reached")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Fatalf("dispatch error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch did not stop after request cancellation")
	}
	close(releaseServer)
	server.Close()
}

func userContentRequestContext(t *testing.T, ctx context.Context) *gin.Context {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/content/submit", nil).WithContext(ctx)
	return c
}

func userContentAudioHeader(t *testing.T, filename, mimeType string, payload []byte) *multipart.FileHeader {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("audio_file", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(1 << 20); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = req.MultipartForm.RemoveAll() })
	header := req.MultipartForm.File["audio_file"][0]
	header.Header.Set("Content-Type", mimeType)
	return header
}
