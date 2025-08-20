package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func CreatePage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page

	if err:= c.ShouldBindJSON(&page); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code: http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, utils.ResponseMessage{
		Code: http.StatusCreated,
		Message: "Page created successfully",
	})

}
func GetPage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page

	// catch error with HTTPError

	
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code: http.StatusOK,
		Message: "Page retrieved successfully",
	})
}
func UpdatePage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page  // if you don't define it here it will only delete the id and not the page

	pageID:= c.Param("id")
	// catch error with HTTPError


	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code: http.StatusOK,
		Message: "Page updated successfully",
	})
}
func DeletePage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page

	pageID:= c.Param("id")
	// catch error with HTTPError


	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code: http.StatusOK,
		Message: "Page deleted successfully",
	})
}
