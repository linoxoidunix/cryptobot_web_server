package main

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Order представляет одну запись в списке ордеров.
type Order struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

// OrderBook представляет книгу ордеров.
type OrderBook struct {
	BuyOrders  []Order `json:"buyOrders"`
	SellOrders []Order `json:"sellOrders"`
}

// WebSocketServer содержит подключенных клиентов и логику.
type WebSocketServer struct {
	clients   map[*websocket.Conn]bool // Подключенные клиенты
	upgrader  websocket.Upgrader       // Для обновления соединения до WebSocket
	mu        sync.Mutex               // Защита доступа к clients
	broadcast chan OrderBook           // Канал для отправки сообщений
}

// NewWebSocketServer создает новый экземпляр WebSocket-сервера.
func NewWebSocketServer() *WebSocketServer {
	return &WebSocketServer{
		clients:   make(map[*websocket.Conn]bool),
		upgrader:  websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		broadcast: make(chan OrderBook),
	}
}

// HandleConnections обрабатывает новые подключения WebSocket.
func (server *WebSocketServer) HandleConnections(w http.ResponseWriter, r *http.Request) {
	conn, err := server.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Ошибка подключения клиента:", err)
		return
	}
	defer conn.Close()

	server.mu.Lock()
	server.clients[conn] = true
	server.mu.Unlock()

	log.Println("Клиент подключен")

	// Ожидание завершения соединения.
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Println("Клиент отключился:", err)
			server.mu.Lock()
			delete(server.clients, conn)
			server.mu.Unlock()
			break
		}
	}
}

// StartBroadcasting запускает передачу данных подключенным клиентам.
func (server *WebSocketServer) StartBroadcasting() {
	for {
		orderBook := <-server.broadcast

		// Преобразуем OrderBook в JSON.
		jsonData, err := json.Marshal(orderBook)
		if err != nil {
			log.Println("Ошибка сериализации данных:", err)
			continue
		}

		// Отправляем данные всем клиентам.
		server.mu.Lock()
		for client := range server.clients {
			err := client.WriteMessage(websocket.TextMessage, jsonData)
			if err != nil {
				log.Println("Ошибка отправки сообщения клиенту:", err)
				client.Close()
				delete(server.clients, client)
			}
		}
		server.mu.Unlock()
	}
}

func GenerateRandomOrder(priceMin, priceMax, amountMin, amountMax float64) Order {
	return Order{
		Price:  priceMin + rand.Float64()*(priceMax-priceMin),
		Amount: amountMin + rand.Float64()*(amountMax-amountMin),
	}
}

// GenerateOrderBook генерирует случайные данные OrderBook.
func GenerateOrderBook() OrderBook {
	var buyOrders, sellOrders []Order

	// Генерация случайных buyOrders.
	for i := 0; i < 10; i++ { // Генерируем 10 случайных ордеров на покупку.
		buyOrders = append(buyOrders, GenerateRandomOrder(100, 110, 1, 20))
	}

	// Генерация случайных sellOrders.
	for i := 0; i < 10; i++ { // Генерируем 10 случайных ордеров на продажу.
		sellOrders = append(sellOrders, GenerateRandomOrder(111, 120, 1, 20))
	}

	return OrderBook{
		BuyOrders:  buyOrders,
		SellOrders: sellOrders,
	}
}

// main запускает сервер.
func main() {
	server := NewWebSocketServer()

	// Обработчик WebSocket.
	http.HandleFunc("/orderbook", server.HandleConnections)

	// Отдельная горутина для рассылки данных клиентам.
	go server.StartBroadcasting()

	// Отдельная горутина для отправки новых данных.
	go func() {
		for {
			time.Sleep(2 * time.Second) // Каждые 2 секунды отправляем новые данные.
			server.broadcast <- GenerateOrderBook()
		}
	}()

	serverConfig := &http.Server{
		Addr: ":10999", // Используем сокращенную запись, которая включает и IPv4, и IPv6
	}

	log.Println("Сервер запущен на ws://localhost:10999/orderbook")
	log.Fatal(serverConfig.ListenAndServe())
}
