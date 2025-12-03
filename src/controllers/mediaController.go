package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var mediaQueryConfig = utils.QueryConfig{
	DefaultLimit: 20,
	MaxLimit:     100,
	DefaultSort: []utils.SortParam{
		{Field: "created_at", Direction: "desc"},
	},
	SortableFields: map[string]string{
		"created_at": "media.created_at",
		"updated_at": "media.updated_at",
		"type":       "media.type",
	},
	FilterableFields: map[string]string{
		"type":       "media.type",
		"url":        "media.url",
		"id":         "media.public_id",
		"created_at": "media.created_at",
		"updated_at": "media.updated_at",
	},
	SearchableFields: map[string]string{
		"url":  "media.url",
		"type": "media.type",
	},
	DefaultSearchFields: []string{"url", "type"},
	FieldDefaultOperators: map[string]string{
		"url": "contains",
	},
}

func CreateMedia(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var media models.Media

	if err := c.ShouldBindJSON(&media); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid media data",
		})
		return
	}

	transaction := db.Begin()
	if err := transaction.Create(&media).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to create media: " + err.Error(),
		})
		return
	}

	if err := transaction.Commit().Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to create media: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, utils.ResponseMessage{
		Data:    media,
		Code:    http.StatusCreated,
		Message: "Media created successfully",
	})
}

func GetMedia(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	mediaIDParam := c.Param("id")

	if mediaIDParam != "" { // fetch a specific media by ID
		parsedUUID, err := uuid.Parse(mediaIDParam)
		if err != nil {
			c.JSON(http.StatusBadRequest, utils.HTTPError{
				Code:    http.StatusBadRequest,
				Message: "Invalid media id",
			})
			return
		}

		var media models.Media
		if err := db.Preload("Post").First(&media, "public_id = ?", parsedUUID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.JSON(http.StatusNotFound, utils.HTTPError{
					Code:    http.StatusNotFound,
					Message: "Media not found",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, utils.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: "Failed to fetch media",
			})
			return
		}

		c.JSON(http.StatusOK, utils.ResponseMessage{
			Data:    media,
			Code:    http.StatusOK,
			Message: "Media fetched successfully",
		})
		return
	}

	params, err := utils.ParseQueryParams(c, mediaQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: err.Error(),
		})
		return
	}

	query := db.Model(&models.Media{})
	query = utils.ApplyQuery(query, params, mediaQueryConfig)
	query = query.Preload("Post")

	var mediaList []models.Media
	meta, err := utils.FetchWithPagination(query, params, &mediaList)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch media",
		})
		return
	}

	links := utils.BuildPaginationLinks(c, meta)

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data:    mediaList,
		Meta:    meta,
		Links:   links,
		Code:    http.StatusOK,
		Message: "Media fetched successfully",
	})
}

func DeleteMedia(c *gin.Context) {
	var mediaToStore models.Media // create a media object to store the media to be deleted

	if err := c.ShouldBindJSON(&mediaToStore); err != nil {
		// The c.ShouldBindJSON() function:
		// Reads the request body
		// Parses it as JSON
		// Maps the fields to your struct
		// Returns an error if anything goes wrong
		// This makes it easy to handle complex input data without manually parsing JSON.
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid media data",
		})
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	parsedUUID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid media id",
		})
		return
	}

	if err := db.First(&mediaToStore, "public_id = ?", parsedUUID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Media not found",
		})
		return
	}
	transaction := db.Begin()
	if err := transaction.Delete(&mediaToStore).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to delete media: " + err.Error(),
		})
		return
	}
	if err := transaction.Commit().Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to delete media: " + err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data:    mediaToStore,
		Code:    http.StatusOK,
		Message: "Media deleted successfully",
	})
}

// func GetMedia(c *gin.Context) {
// 	// Attempt to retrieve *gorm.DB from gin context
// 	var db *gorm.DB
// 	if ctxDB, exists := c.Get("db"); exists {
// 		db = ctxDB.(*gorm.DB)
// 	} else {
// 		// Fallback: open a new connection (not ideal for production but keeps handler functional)
// 		var err error
// 		db, err = utils.ConnectDB()
// 		if err != nil {
// 			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to connect to database"})
// 			return
// 		}
// 	}

// 	idParam := c.Param("id")
// 	if idParam != "" {
// 		// Return specific media by ID
// 		id, err := strconv.Atoi(idParam)
// 		if err != nil {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid media id"})
// 			return
// 		}

// 		var media models.Media
// 		if err := db.First(&media, id).Error; err != nil {
// 			if err == gorm.ErrRecordNotFound {
// 				c.JSON(http.StatusNotFound, gin.H{"error": "media not found"})
// 				return
// 			}
// 			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 			return
// 		}
// 		c.JSON(http.StatusOK, media)
// 		return
// 	}

// 	// No ID provided -> return all media
// 	var media []models.Media
// 	if err := db.Find(&media).Error; err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
// 	c.JSON(http.StatusOK, media)
// }
