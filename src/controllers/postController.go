package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func CreatePost(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var post models.Post
	if err := c.ShouldBindJSON(&post); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Data:    post,
			Code:    http.StatusBadRequest,
			Message: err.Error(),
		})
		return
	}

	// Start database transaction (gorm begin method)
	transaction := db.Begin()

	if err := transaction.Create(&post).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data:    post,
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	// Handle many-to-many media association if PublicIDs are provided
	if len(post.Media) > 0 {
		var publicIDs []uuid.UUID
		for _, m := range post.Media {
			if m.PublicID != uuid.Nil {
				publicIDs = append(publicIDs, m.PublicID)
			}
		}
		if len(publicIDs) > 0 {
			var mediaList []models.Media
			if err := transaction.Where("public_id IN ?", publicIDs).Find(&mediaList).Error; err != nil {
				transaction.Rollback()
				c.JSON(http.StatusBadRequest, utils.HTTPError{
					Data:    post,
					Code:    http.StatusBadRequest,
					Message: "Invalid media IDs",
				})
				return
			}
			if err := transaction.Model(&post).Association("Media").Replace(mediaList); err != nil {
				transaction.Rollback()
				c.JSON(http.StatusInternalServerError, utils.HTTPError{
					Data:    post,
					Code:    http.StatusInternalServerError,
					Message: err.Error(),
				})
				return
			}
		}
	}

	// Reload with associations for response
	if err := transaction.Preload("Media").First(&post, post.ID).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data:    post,
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	if err := transaction.Commit().Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data:    post,
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, utils.ResponseMessage{
		Data:    post,
		Code:    http.StatusCreated,
		Message: "Post created successfully",
	})
}

func GetPosts(c *gin.Context) {

	title := c.Query("title") // query params
	author := c.Query("author")

	db := c.MustGet("db").(*gorm.DB)
	var posts []models.Post // add [] becuase it multiple posts

	// query:= db // it should be this but logically i think it wrong
	query := db.Model(&models.Post{})
	if title != "" { // if title params provided fetch posts with title
		query = query.Where("title LIKE ?", "%"+title+"%")
	}

	if author != "" {
		query = query.Where("author = ?", author)
	}

	// Use proper preloading for media relationships
	if err := query.Preload("Media").Find(&posts).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data:    posts,
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data:    posts,
		Code:    http.StatusOK,
		Message: "Posts fetched successfully",
	})
}

func GetPost(c *gin.Context) {
	postIDStr := c.Param("id")
	parsedID, err := uuid.Parse(postIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid post ID",
		})
		return
	}
	//      The Type Conversion Chain
	//      c.Param("postID") → Returns string (e.g., "123")
	//      strconv.Atoi() → Converts string to int (e.g., 123)
	//      uint() → Converts int to uint (e.g., 123 as unsigned integer)
	// all that maybe deleted now and not happenning anymore
	db := c.MustGet("db").(*gorm.DB)
	var post models.Post // no need to put in a slice [] becuase it single post

	if err := db.Preload("Media").First(&post, "public_id = ?", parsedID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Post not found",
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data:    post,
		Code:    http.StatusOK,
		Message: "Post fetched successfully",
	})
}

func UpdatePost(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var post models.Post
	var postIDStr = c.Param("id")
	parsedID, err := uuid.Parse(postIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid post ID",
		})
		return
	}
	if err := db.First(&post, "public_id = ?", parsedID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Post not found",
		})
		return
	}

	if err := c.ShouldBindJSON(&post); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: err.Error(),
		})
		return
	}

	if err := db.Save(&post).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data:    post,
		Code:    http.StatusOK,
		Message: "Post updated successfully",
	})

}
func DeletePost(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var post models.Post
	var postIDStr = c.Param("id")
	parsedID, err := uuid.Parse(postIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid post ID",
		})
		return
	}

	if err := db.First(&post, "public_id = ?", parsedID).Error; err != nil {

		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Post not found",
		})
		return
	}

	if err := db.Delete(&post).Error; err != nil {

		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusNoContent, utils.ResponseMessage{
		Code:    http.StatusNoContent,
		Message: "Post deleted successfully",
	})

}

// func GetPost(c *gin.Context) {
// if postDB == nil {
// 	c.JSON(http.StatusInternalServerError, gin.H{"error": "database not initialized"})
// 	return
// }
//
// postIDParam := c.Param("id")
// if postIDParam != "" { // fetch a specific post by ID
// 	parsedID, err := strconv.Atoi(postIDParam)
// 	if err != nil || parsedID <= 0 {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid post id"})
// 		return
// 	}
//
// 	var post models.Post
// 	if err := postDB.First(&post, parsedID).Error; err != nil {
// 		if errors.Is(err, gorm.ErrRecordNotFound) {
// 			c.JSON(http.StatusNotFound, gin.H{"error": "post not found"})
// 			return
// 		}
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch post"})
// 		return
// 	}
//
// 	c.JSON(http.StatusOK, post)
// 	return
// }
//
// }
