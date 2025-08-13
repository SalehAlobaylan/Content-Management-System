package controllers

import (
	"content-management-system/src/models"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func CreateMedia(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)


}

func GetMedia(c *gin.Context) {
	if mediaDB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database not initialized"})
		return
	}

	mediaIDParam := c.Param("id")
	if mediaIDParam != "" { // fetch a specific media by ID
		parsedID, err := strconv.Atoi(mediaIDParam)
		if err != nil || parsedID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid media id"})
			return
		}

		var media models.Media
		if err := mediaDB.First(&media, parsedID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": "media not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch media"})
			return
		}

		c.JSON(http.StatusOK, media)
		return
	}

	// no id provided -> fetch all media
	var allMedia []models.Media
	if err := mediaDB.Find(&allMedia).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch media"})
		return
	}
	c.JSON(http.StatusOK, allMedia)
}

func DeleteMedia(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

}

var mediaDB *gorm.DB

func InitMediaController(db *gorm.DB) {
	mediaDB = db
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
