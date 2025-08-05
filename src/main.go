package main

import (
    // "content-management-system/src/models" // needs it for automigrate
    "content-management-system/src/routes"
    "content-management-system/src/utils"


	"log"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/joho/godotenv/autoload"
	"gorm.io/gorm"
)

func SetupRoutes(router *gin.Engine, db *gorm.DB) {
	routes.SetupPostRoutes(router, db)
	routes.SetupMediaRoutes(router, db)
	routes.SetupPageRoutes(router, db)
}

func main() {

	db, err:= utils.ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}





	env := os.Getenv("ENV")
	if env == "" { //if env is not set, set it to development as default
		env = "development"
	}

	utils.AutoMigrate(db) // use it in development 


	router := gin.Default()

	
	SetupRoutes(router, db)



	if err :=router.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}


}