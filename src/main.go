package main

import (
    "content-management-system/src/models"
    "content-management-system/src/routes"
    "content-management-system/src/utils"


	"log"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/joho/godotenv/autoload"
)

func main() {

	db, err:= utils.ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	utils.AutoMigrate(db) // new



	utils.LoadEnv() // new
	env := os.Getenv("ENV")


	router := gin.Default()

	routes.SetupRoutes(router, db)



	if err :=router.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}


}