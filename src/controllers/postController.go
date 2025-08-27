package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func CreatePost(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var post models.Post
	if err := c.ShouldBindJSON(&post); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
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
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	if err := transaction.Commit().Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, utils.ResponseMessage{
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
		db = db.Where("title LIKE ?", "%"+title+"%")
	}

	if author != "" {
		db = db.Where("author = ?", author)
	}

	// Use proper preloading for media relationships
	if err := query.Preload("Media").Find(&posts).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code: http.StatusOK,
		Message: "Posts fetched successfully",
	})
}

func GetPost(c *gin.Context) {
	postIDStr := c.Param("postID")
	postIDInt, err := strconv.Atoi(postIDStr)
	if err != nil || postIDInt <= 0 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid post ID",
		})
		return
	}
	postID := uint(postIDInt)
// 		The Type Conversion Chain
// 		c.Param("postID") → Returns string (e.g., "123")
// 		strconv.Atoi() → Converts string to int (e.g., 123)
// 		uint() → Converts int to uint (e.g., 123 as unsigned integer)

	db := c.MustGet("db").(*gorm.DB)
	var post models.Post // no need to put in a slice [] becuase it single post

	if err := db.First(&post, postID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Post not found",
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code: http.StatusOK,
		Message: "Post fetched successfully",
	})
}

func UpdatePost(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var post models.Post
	var postIDStr = c.Param("postID")
	postIDInt, err := strconv.Atoi(postIDStr)
	postID := uint(postIDInt)

	if err != nil || postIDInt <= 0 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid post ID",
		})
		return
	}
	if err := db.First(&post, postID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Post not found",
		})
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code: http.StatusOK,
		Message: "Post updated successfully",
	})

}
func DeletePost(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var post models.Post
	var postIDStr = c.Param("postID")
	postIDInt, err := strconv.Atoi(postIDStr)
	postID := uint(postIDInt)

	if err != nil || postIDInt <= 0 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid post ID",
		})
		return
	}

	if err := db.First(&post, postID).Error; err != nil {

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
		Code: http.StatusNoContent,
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

// }
