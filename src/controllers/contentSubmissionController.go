package controllers

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"mime/multipart"
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

	// 10 MiB total memory cap; larger uploads stream to a tempfile per
	// multipart's internal behavior. Aggregation also enforces size limits.
	if err := c.Request.ParseMultipartForm(10 << 20); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid multipart payload: " + err.Error(),
		})
		return
	}

	title := strings.TrimSpace(c.PostForm("title"))
	if title == "" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "title is required",
		})
		return
	}
	if len(title) > 240 {
		title = title[:240]
	}

	bodyText := strings.TrimSpace(c.PostForm("body_text"))

	var audioHeader *multipart.FileHeader
	if files := c.Request.MultipartForm.File["audio_file"]; len(files) > 0 {
		audioHeader = files[0]
	}

	if bodyText == "" && audioHeader == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Submit at least one of body_text or audio_file",
		})
		return
	}

	tenantID := utils.GetDefaultTenantID()

	now := time.Now()
	contentType := models.ContentTypeArticle
	if audioHeader != nil {
		contentType = models.ContentTypePodcast
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
		Source:      models.SourceTypeUpload,
		Status:      status,
		Title:       &title,
		AuthorID:    &userID,
		PublishedAt: &now,
	}
	if bodyText != "" {
		item.BodyText = &bodyText
		excerpt := bodyText
		if len(excerpt) > 280 {
			excerpt = excerpt[:280]
		}
		item.Excerpt = &excerpt
	}

	if err := db.Create(&item).Error; err != nil {
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
		db.Model(&item).Update("status", models.ContentStatusFailed)
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

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	if err := writer.WriteField("content_item_id", contentItemID.String()); err != nil {
		return err
	}
	if err := writer.WriteField("tenant_id", tenantID); err != nil {
		return err
	}
	if err := writer.WriteField("content_type", string(models.ContentTypePodcast)); err != nil {
		return err
	}

	filename := filepath.Base(header.Filename)
	if filename == "" || filename == "." {
		filename = contentItemID.String() + ".audio"
	}
	part, err := writer.CreateFormFile("audio_file", filename)
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, src); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/internal/jobs/user-content", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if token := strings.TrimSpace(os.Getenv("AGGREGATION_SERVICE_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("aggregation returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
