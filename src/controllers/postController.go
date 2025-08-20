package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func CreatePost(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)

	var post models.Post

	c.JSON(http.StatusCreated, post)
}

func GetPosts(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var posts []models.Post // add [] becuase it multiple posts

	title:= c.Query("title")  // query params
	author:= c.Query("author")

	// query:= db // it should be this but logically i think it wrong
	query := db.Model(&models.Post{}) 
	if title != "" {  // if title params provided fetch posts with title
		db = db.Where("title LIKE ?", "%"+title+"%")
	}

	if author != "" {
		db = db.Where("author = ?",author)
	}

		// Use proper preloading for media relationships
		if err := query.Preload("Media").Find(&posts).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			})
			return
		}



	c.JSON(http.StatusOK , posts)
}

func GetPost(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var post models.Post // no need to put in a slice [] becuase it single post

	postID:= c.Param("id")

	if err := db.First(&post, postID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code: http.StatusNotFound,
			Message: "Post not found",
		})
		return
	}

	c.JSON(http.StatusOK, post)
}

func UpdatePost(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var post models.Post

	c.JSON(http.StatusOK, post)

}
func DeletePost(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var post models.Post

	c.JSON(http.StatusNoContent, post)

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