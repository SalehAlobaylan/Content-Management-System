package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Removed campaigns cannot acquire new authority even with an old client or
// qualification row. Rollout requires the recorded no-obligation data gate.
func ContentResetDiscontinued(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	contentResetDiscontinued(c)
}

func InternalContentResetDiscontinued(c *gin.Context) { contentResetDiscontinued(c) }

func contentResetDiscontinued(c *gin.Context) {
	c.JSON(http.StatusGone, gin.H{
		"error": "Content Reset campaigns are discontinued; use bounded Pods reset or Feed Recovery",
		"code":  "content_reset_discontinued", "replacement": "/admin/pods-reset/plans",
	})
}

func GetContentResetCapabilities(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"admission_closed": true, "execution_enabled": false,
		"reason":      "Content Reset campaigns are discontinued",
		"replacement": "/admin/pods-reset/plans",
		"operations": gin.H{
			"clear":       gin.H{"preview": false, "execute": false},
			"fresh_start": gin.H{"preview": false, "execute": false},
			"empty":       gin.H{"preview": false, "execute": false},
		},
		"supported_scope_kinds": []string{},
	})
}
