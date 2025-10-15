package main

import (
	"fmt"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

func main() {
	StartServer()
}

func StartServer() {
	r := router.New()
	r.GET("/", helloHandler)
	r.POST("/bson", bsonHandler)
	fmt.Println("Server running on http://localhost:8080")
	fasthttp.ListenAndServe(":8080", r.Handler)
}
