package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func CreatePage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page

	if err:= c.ShouldBindJSON(&page); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Data: page,
			Code: http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	transaction := db.Begin()
	if err:= transaction.Create(&page).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data: page,
			Code: http.StatusInternalServerError,
			Message: "Failed to create page: " + err.Error(),
		})
		return
	}

	if err:= transaction.Commit().Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data: page,
			Code: http.StatusInternalServerError,
			Message: "Failed to create page: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, utils.ResponseMessage{
		Data: page,
		Code: http.StatusCreated,
		Message: "Page created successfully",
	})

}
func GetPage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page
	pageID:= c.Param("id")

	// catch error with HTTPError
	if _, err:= strconv.Atoi(pageID); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Data: page,
			Code: http.StatusBadRequest,
			Message: "Invalid page ID",
		})
		return
	}

	if err:= db.First(&page, pageID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code: http.StatusNotFound,
			Message: "Page not found",
		})
		return
	}
	c.MustGet("db").(*gorm.DB).First(&page, pageID)
	
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data: page,
		Code: http.StatusOK,
		Message: "Page retrieved successfully",
	})
}
func UpdatePage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page  // if you don't define it here it will only delete the id and not the page

	pageID:= c.Param("id")
	// catch error with HTTPError
	if _, err:= strconv.Atoi(pageID); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code: http.StatusBadRequest,
			Message: "Invalid page ID",
		})
		return
	}
	if err:= db.First(&page, pageID).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Data: page,
			Code: http.StatusNotFound,
			Message: "Page not found",
		})
		return
	}
	if err:= c.ShouldBindJSON(&page); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Data: page,
			Code: http.StatusBadRequest,
			Message: "Invalid page data",
		})
		return
	}

	transaction := db.Begin()
	if err:= transaction.Save(&page).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code: http.StatusInternalServerError,
			Message: "Failed to update page: " + err.Error(),
		})
		return
	}
	if err:= transaction.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data: page,
			Code: http.StatusInternalServerError,
			Message: "Failed to update page: " + err.Error(),
		})
		return
	}


	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data: page,
		Code: http.StatusOK,
		Message: "Page updated successfully",
	})
}
func DeletePage(c *gin.Context) {
	db:= c.MustGet("db").(*gorm.DB)
	var page models.Page

	pageID:= c.Param("id")
	if _, err:= strconv.Atoi(pageID); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Data: page,
			Code: http.StatusBadRequest,
			Message: "Invalid page ID",
		})
		return
	}
	if err:= c.ShouldBindJSON(&page); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Data: page,
			Code: http.StatusBadRequest,
			Message: "Invalid page data",
		})
		return
	}
		if err:= db.First(&page, pageID).Error; err != nil {
			c.JSON(http.StatusNotFound, utils.HTTPError{
				Data: page,
				Code: http.StatusNotFound,
				Message: "Page not found",
			})
			return
		}
	transaction := db.Begin()	
	if err:= transaction.Delete(&page).Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code: http.StatusInternalServerError,
			Message: "Failed to delete page: " + err.Error(),
		})
		return
	}
	if err:= transaction.Commit().Error; err != nil {
		transaction.Rollback()
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Data: page,
			Code: http.StatusInternalServerError,
			Message: "Failed to delete page: " + err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Data: page,
		Code: http.StatusOK,
		Message: "Page deleted successfully",
	})
}
