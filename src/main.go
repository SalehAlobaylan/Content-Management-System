package main

import (
    "content-management-system/src/models" // needs it for automigrate
    "content-management-system/src/routes"
    "content-management-system/src/utils"


	"log"
	"os"
	// "fmt"

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


	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get database instance: %v", err)
	}

	defer sqlDB.Close()



	env := os.Getenv("ENV")
	if env == "" { //if env is not set, set it to development as default
		env = "development"
	}
	if env == "development" {
		log.Println("%Migrating and seeding data...")
		if err := utils.AutoMigrate(db, &models.Page{}, &models.Post{}, &models.Media{}); err != nil{ // use it in development 
			log.Fatalf("Failed to migrate database: %v", err)
		} 
		if err := utils.SeedData(db); err != nil{ // use it in development 
			log.Fatalf("Failed to seed data: %v", err)
		} 
	}
	if env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}



	router := gin.Default()

	
	SetupRoutes(router, db)



	if err :=router.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}


}