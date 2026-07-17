package controllers

import (
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	maxUserContentUploadBytes            int64 = 200 << 20
	maxUserContentMultipartOverheadBytes int64 = 1 << 20
	aggregationConnectTimeout                  = 10 * time.Second
	aggregationResponseHeaderTimeout           = 45 * time.Second
	aggregationHandoffTimeout                  = 2 * time.Minute
)

const (
	maxUserContentTitleRunes = 240
	maxUserContentBodyRunes  = 10_000
)

// SubmitUserContent handles POST /api/v1/content/submit.
//
// A logged-in user uploads a piece of content: a title, optional body text,
// and optionally an audio file. Text-only submissions become READY ARTICLE
// items immediately. Audio submissions become PENDING PODCAST items and are
// handed to the Aggregation service for transcoding through the same
// normalize → media → ai pipeline as ingested content, so they show up on
// the feed normalized as MP4 + thumbnail.
//
// Required multipart fields:
//   - title         text
//
// Optional multipart fields:
//   - body_text     text (used as ARTICLE body or PODCAST description)
//   - audio_file    binary (mp3/m4a/wav)
//
// Must include the user's JWT (UserAuthMiddleware sets c.Get("user_id")).
func SubmitUserContent(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	rawUserID, exists := c.Get("user_id")
	if !exists {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication required",
		})
		return
	}
	userIDStr, _ := rawUserID.(string)
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Token user id is not a valid uuid",
		})
		return
	}

	// Multipart's memory threshold is not a request-size limit. Cap the body
	// before parsing so chunked or lying Content-Length uploads cannot spill an
	// unbounded tempfile in CMS.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxUserContentUploadBytes+maxUserContentMultipartOverheadBytes)
	if err := c.Request.ParseMultipartForm(10 << 20); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.JSON(http.StatusRequestEntityTooLarge, utils.HTTPError{
				Code:    http.StatusRequestEntityTooLarge,
				Message: "Upload exceeds the maximum allowed size",
			})
			return
		}
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid multipart payload"})
		return
	}
	if c.Request.MultipartForm != nil {
		defer c.Request.MultipartForm.RemoveAll()
		if err := validateUserContentMultipartForm(c.Request.MultipartForm); err != nil {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid multipart payload"})
			return
		}
	}

	title := strings.TrimSpace(c.PostForm("title"))
	if title == "" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "title is required",
		})
		return
	}
	title = truncateRunes(title, maxUserContentTitleRunes)

	bodyText := strings.TrimSpace(c.PostForm("body_text"))
	if len([]rune(bodyText)) > maxUserContentBodyRunes {
		c.JSON(http.StatusRequestEntityTooLarge, utils.HTTPError{Code: http.StatusRequestEntityTooLarge, Message: "body_text exceeds the maximum allowed size"})
		return
	}

	var audioHeader *multipart.FileHeader
	if c.Request.MultipartForm != nil {
		if files := c.Request.MultipartForm.File["audio_file"]; len(files) == 1 {
			audioHeader = files[0]
		}
	}

	if bodyText == "" && audioHeader == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Submit at least one of body_text or audio_file",
		})
		return
	}
	if audioHeader != nil {
		if err := validateUserContentAudio(audioHeader); err != nil {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid audio_file"})
			return
		}
	}

	tenantID := utils.GetDefaultTenantID()
	idempotencyKey := ""
	if rawKey := strings.TrimSpace(c.GetHeader("Idempotency-Key")); rawKey != "" {
		if len(rawKey) > 240 {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Idempotency-Key is too long"})
			return
		}
		idempotencyKey = "user-upload:" + userID.String() + ":" + rawKey
		var existing models.ContentItem
		if err := db.Where("tenant_id = ? AND idempotency_key = ?", tenantID, idempotencyKey).First(&existing).Error; err == nil {
			// Aggregation derives its queue receipt from this stable content ID, so
			// retrying a failed handoff is safe: it either resumes the same job or
			// creates the one missing receipt. Do not strand a failed upload behind
			// an idempotency replay forever.
			if audioHeader != nil && existing.Status == models.ContentStatusFailed {
				result := db.Model(&models.ContentItem{}).
					Where("public_id = ? AND status = ?", existing.PublicID, models.ContentStatusFailed).
					Update("status", models.ContentStatusPending)
				if result.Error != nil {
					c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to resume submission"})
					return
				}
				if result.RowsAffected == 0 {
					var current models.ContentItem
					if lookupErr := db.Where("public_id = ?", existing.PublicID).First(&current).Error; lookupErr != nil {
						c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to resume submission"})
						return
					}
					c.JSON(http.StatusOK, gin.H{"id": current.PublicID.String(), "status": string(current.Status), "replayed": true})
					return
				}
				if dispatchErr := dispatchAudioToAggregation(c, existing.PublicID, tenantID, audioHeader); dispatchErr != nil {
					log.Printf("[CMS] submission retry dispatch failed for %s: %v", existing.PublicID, dispatchErr)
					_ = db.Model(&models.ContentItem{}).
						Where("public_id = ? AND status = ?", existing.PublicID, models.ContentStatusPending).
						Update("status", models.ContentStatusFailed).Error
					c.JSON(http.StatusBadGateway, utils.HTTPError{Code: http.StatusBadGateway, Message: "Failed to hand off audio to processing pipeline"})
					return
				}
				c.JSON(http.StatusAccepted, gin.H{"id": existing.PublicID.String(), "status": string(models.ContentStatusPending), "replayed": true, "resumed": true})
				return
			}
			c.JSON(http.StatusOK, gin.H{"id": existing.PublicID.String(), "status": string(existing.Status), "replayed": true})
			return
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to check submission state"})
			return
		}
	}

	now := time.Now()
	// Text submissions are NEWS/article; audio submissions are PODCAST media.
	contentType := models.ContentTypeNews
	var contentFormat *string
	if audioHeader != nil {
		contentType = models.ContentTypePodcast
	} else {
		f := string(models.ContentFormatArticle)
		contentFormat = &f
	}

	status := models.ContentStatusReady
	if audioHeader != nil {
		// Audio needs the aggregation pipeline to transcode + thumbnail
		// before the For You feed filter (MP4 + thumbnail) will accept it.
		status = models.ContentStatusPending
	}

	item := models.ContentItem{
		TenantID:    tenantID,
		Type:        contentType,
		Format:      contentFormat,
		Source:      models.SourceTypeUpload,
		Status:      status,
		Title:       &title,
		AuthorID:    &userID,
		PublishedAt: &now,
	}
	if idempotencyKey != "" {
		item.IdempotencyKey = &idempotencyKey
	}
	if bodyText != "" {
		item.BodyText = &bodyText
		excerpt := bodyText
		excerpt = truncateRunes(excerpt, 280)
		item.Excerpt = &excerpt
	}

	// The item ID is the durable handoff key. Aggregation derives its queue job
	// ID from it, so an ambiguous downstream response is recoverable when the
	// client repeats the same idempotency key and audio bytes. CMS deliberately
	// does not retain uploads after the request, so a server-side outbox could
	// not safely replay the binary payload.
	if err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&item).Error
	}); err != nil {
		if idempotencyKey != "" {
			var existing models.ContentItem
			if lookupErr := db.Where("tenant_id = ? AND idempotency_key = ?", tenantID, idempotencyKey).First(&existing).Error; lookupErr == nil {
				c.JSON(http.StatusOK, gin.H{"id": existing.PublicID.String(), "status": string(existing.Status), "replayed": true})
				return
			}
		}
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to create content item",
		})
		return
	}
	// Text-only path: nothing else to do, item is READY.
	if audioHeader == nil {
		c.JSON(http.StatusCreated, gin.H{
			"id":     item.PublicID.String(),
			"status": string(item.Status),
		})
		return
	}

	// Audio path: hand the bytes to Aggregation. If that handoff fails we
	// mark the item FAILED so it can't sit in PENDING forever, and report
	// the failure to the client.
	if err := dispatchAudioToAggregation(c, item.PublicID, tenantID, audioHeader); err != nil {
		log.Printf("[CMS] submit dispatch failed for %s: %v", item.PublicID, err)
		// Do not overwrite a READY/PROCESSING update if Aggregation accepted the
		// job and only its response was lost. FAILED is the recoverable state the
		// client can replay with the same idempotency key and original bytes.
		_ = db.Model(&models.ContentItem{}).
			Where("public_id = ? AND status = ?", item.PublicID, models.ContentStatusPending).
			Update("status", models.ContentStatusFailed).Error
		c.JSON(http.StatusBadGateway, utils.HTTPError{
			Code:    http.StatusBadGateway,
			Message: "Failed to hand off audio to processing pipeline",
		})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"id":     item.PublicID.String(),
		"status": string(item.Status),
	})
}

// truncateRunes bounds user-visible text without cutting inside a UTF-8 code
// point. CMS accepts Arabic and emoji input, so byte slicing would persist
// invalid text at a multi-byte boundary.
func truncateRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func validateUserContentMultipartForm(form *multipart.Form) error {
	if form == nil {
		return nil
	}
	for field, files := range form.File {
		if field != "audio_file" {
			return fmt.Errorf("unexpected upload field")
		}
		if len(files) != 1 {
			return fmt.Errorf("audio_file must occur exactly once")
		}
	}
	for field, values := range form.Value {
		if field != "title" && field != "body_text" {
			return fmt.Errorf("unexpected form field")
		}
		if len(values) != 1 {
			return fmt.Errorf("form field must occur exactly once")
		}
	}
	return nil
}

func dispatchAudioToAggregation(c *gin.Context, contentItemID uuid.UUID, tenantID string, header *multipart.FileHeader) error {
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if baseURL == "" {
		return fmt.Errorf("AGGREGATION_BASE_URL not configured")
	}

	src, err := header.Open()
	if err != nil {
		return fmt.Errorf("open audio upload: %w", err)
	}
	defer src.Close()

	filename := filepath.Base(header.Filename)
	if filename == "" || filename == "." {
		filename = contentItemID.String() + ".audio"
	}

	reader, writerPipe := io.Pipe()
	writer := multipart.NewWriter(writerPipe)
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		defer writerPipe.Close()
		for _, field := range [][2]string{
			{"content_item_id", contentItemID.String()},
			{"tenant_id", tenantID},
			{"content_type", string(models.ContentTypePodcast)},
		} {
			if err := writer.WriteField(field[0], field[1]); err != nil {
				writeErr <- err
				return
			}
		}
		part, err := writer.CreateFormFile("audio_file", filename)
		if err != nil {
			writeErr <- err
			return
		}
		// The outer MaxBytesReader protects the whole multipart request; this
		// second count protects the handoff if parser metadata is dishonest.
		written, err := io.Copy(part, io.LimitReader(src, maxUserContentUploadBytes+1))
		if err != nil {
			writeErr <- err
			return
		}
		if written > maxUserContentUploadBytes {
			writeErr <- fmt.Errorf("audio exceeds maximum allowed size")
			return
		}
		if err := writer.Close(); err != nil {
			writeErr <- err
		}
	}()

	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, baseURL+"/internal/jobs/user-content", reader)
	if err != nil {
		_ = reader.Close()
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if token := aggregationInternalServiceToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: aggregationConnectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   aggregationConnectTimeout,
		ResponseHeaderTimeout: aggregationResponseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: aggregationHandoffTimeout}
	resp, err := client.Do(req)
	_ = reader.Close()
	for streamErr := range writeErr {
		if streamErr != nil && err == nil {
			err = streamErr
		}
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("aggregation returned status %d", resp.StatusCode)
	}
	return nil
}

func validateUserContentAudio(header *multipart.FileHeader) error {
	ext := strings.ToLower(filepath.Ext(header.Filename))
	mediaType, _, err := mime.ParseMediaType(header.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("invalid audio MIME type")
	}
	mediaType = strings.ToLower(mediaType)
	allowedMIME := map[string]bool{
		"audio/mpeg": true, "audio/mp3": true, "audio/wav": true,
		"audio/x-wav": true, "audio/wave": true, "audio/mp4": true, "audio/x-m4a": true,
	}
	if !map[string]bool{".mp3": true, ".wav": true, ".m4a": true}[ext] || !allowedMIME[mediaType] {
		return fmt.Errorf("unsupported audio type")
	}
	file, err := header.Open()
	if err != nil {
		return err
	}
	defer file.Close()
	headerBytes := make([]byte, 16)
	n, err := io.ReadFull(file, headerBytes)
	if err != nil && err != io.ErrUnexpectedEOF {
		return err
	}
	headerBytes = headerBytes[:n]
	switch ext {
	case ".mp3":
		if len(headerBytes) >= 3 && string(headerBytes[:3]) == "ID3" {
			return nil
		}
		if len(headerBytes) >= 2 && headerBytes[0] == 0xff && headerBytes[1]&0xe0 == 0xe0 {
			return nil
		}
	case ".wav":
		if len(headerBytes) >= 12 && string(headerBytes[:4]) == "RIFF" && string(headerBytes[8:12]) == "WAVE" {
			return nil
		}
	case ".m4a":
		if len(headerBytes) >= 8 && string(headerBytes[4:8]) == "ftyp" {
			return nil
		}
	}
	return fmt.Errorf("audio content does not match extension")
}
