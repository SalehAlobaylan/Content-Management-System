package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	const port = 8090
	fs := http.FileServer(http.Dir("docs"))
	http.Handle("/", fs)

	log.Printf("Serving Swagger UI from http://localhost:%d\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}
