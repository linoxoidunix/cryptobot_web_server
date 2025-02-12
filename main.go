package main

import (
	"context"
	"cryptobot_server/redis"
	"cryptobot_server/websocket"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

var ctx = context.Background()

func main() {
	// Инициализация клиента Redis
	rdb := redis.NewClient()

	// Проверка подключения
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Error connecting to Redis: %v", err)
	}

	// HTTP server for WebSocket
	//http.HandleFunc("/ws", websocket.WSHandler)

	// Запуск сервера
	// log.Println("Starting server on 0.0.0.0:10999")
	// if err := http.ListenAndServe("0.0.0.0:10999", nil); err != nil {
	// 	log.Fatal("Error starting server:", err)
	// }

	// Создаем новый роутер Gin
	r := gin.Default()

	// Маршрут для получения списка транзакций по TradeID
	r.GET("/transactions/:tradeID", redis.GetTransactions)

	// HTTP server for WebSocket
	go func() {
		http.HandleFunc("/ws", websocket.WSHandler)
		log.Println("Starting WebSocket server on 0.0.0.0:10999")
		if err := http.ListenAndServe("0.0.0.0:10999", nil); err != nil {
			log.Fatal("Error starting WebSocket server:", err)
		}
	}()

	// Запуск сервера
	log.Println("Starting server on 0.0.0.0:8080")
	if err := r.Run("0.0.0.0:8080"); err != nil {
		log.Fatalf("could not start server: %v", err)
	}
}
