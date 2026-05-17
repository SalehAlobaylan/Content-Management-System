package controllers

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

// RestartService responds 202 Accepted and then exits the process after a
// short delay so the HTTP response can flush. The process must be supervised
// (Cranl, systemd, Docker restart-policy, etc.) for it to actually come back
// up; locally via `./start.sh` it will simply die.
func RestartService(c *gin.Context) {
	c.JSON(http.StatusAccepted, gin.H{
		"message": "Restart accepted. Service is shutting down — supervisor must bring it back.",
		"service": "cms",
	})

	go func() {
		time.Sleep(250 * time.Millisecond)
		log.Println("[CMS] Restart requested via /admin/restart — exiting")
		os.Exit(0)
	}()
}
