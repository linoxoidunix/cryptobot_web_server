package redis

import (
	"context"
	"cryptobot_server/aot"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

var ctx = context.Background()

// Redis client
var rdb *redis.Client

func NewClient() *redis.Client {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	return rdb
}

func GetClient() *redis.Client {
	if rdb == nil {
		log.Println("Redis client not initialized, initializing now...")
		NewClient()
	}
	return rdb
}

func SaveTransactionToRedis(transactionId string, transaction *aot.Transaction) {
	transactionData := map[string]interface{}{
		"TradingPair":       transaction.TradingPair,
		"ExchangeId":        transaction.ExchangeId.String(),
		"MarketType":        transaction.MarketType.String(),
		"TransactionAction": transaction.TransactionAction.String(),
	}

	// Проверка, существует ли уже запись
	for field, value := range transactionData {
		exists, err := GetClient().HSetNX(ctx, transactionId, field, value).Result()
		if err != nil {
			log.Printf("Error checking/saving transaction to Redis: %v", err)
			return
		}
		if !exists {
			log.Printf("Field '%s' already exists for transaction ID: %s", field, transactionId)
		}
	}
}

func AddTransactionToListIfNotExists(listKey string, transactionKey string) {
	// Проверка существования ключа в списке
	items, err := rdb.LRange(ctx, listKey, 0, -1).Result()
	if err != nil {
		log.Printf("Error retrieving list from Redis: %v", err)
		return
	}

	// Проверка на существование элемента
	for _, item := range items {
		if item == transactionKey {
			log.Printf("Transaction key '%s' already exists in list '%s'", transactionKey, listKey)
			return
		}
	}

	// Добавление элемента в список, если его ещё нет
	if err := rdb.LPush(ctx, listKey, transactionKey).Err(); err != nil {
		log.Printf("Error adding transaction to list: %v", err)
	} else {
		log.Printf("Transaction key '%s' added to list '%s'", transactionKey, listKey)
	}
}

func NewExchangeId(s string) (aot.ExchangeId, error) {
	log.Printf("Try get type exchangeid of: %s", s)

	// Используем маппинг для получения значения перечисления из строки
	if val, ok := aot.ExchangeId_value[s]; ok {
		return aot.ExchangeId(val), nil
	}
	return aot.ExchangeId_EXCHANGE_ID_INVALID, fmt.Errorf("invalid ExchangeId: %s", s)
}

func NewMarketType(s string) (aot.MarketType, error) {
	// Используем маппинг для получения значения перечисления из строки
	if val, ok := aot.MarketType_value[s]; ok {
		return aot.MarketType(val), nil
	}
	return aot.MarketType_MARKET_TYPE_INVALID, fmt.Errorf("invalid ExchangeId: %s", s)
}

func NewTransactionAction(s string) (aot.TransactionAction, error) {
	// Используем маппинг для получения значения перечисления из строки
	if val, ok := aot.TransactionAction_value[s]; ok {
		return aot.TransactionAction(val), nil
	}
	return aot.TransactionAction_SELL, fmt.Errorf("invalid ExchangeId: %s", s)
}

func getTransactionsForTradeID(tradeID uint64) ([]aot.Transaction, error) {
	// Формируем ключ Redis для списка транзакций этого трейда
	redisKey := fmt.Sprintf("trade:%d:transactions", tradeID)

	log.Printf("Fetching transactions for tradeID: %d with redisKey: %s", tradeID, redisKey)

	// Получаем все транзакции для данного трейда
	transactionKeys, err := rdb.LRange(ctx, redisKey, 0, -1).Result()
	if err != nil {
		log.Printf("Error fetching transaction keys for tradeID %d: %v", tradeID, err)
		return nil, fmt.Errorf("failed to get transaction keys: %v", err)
	}
	log.Printf("Found %d transaction keys for tradeID %d", len(transactionKeys), tradeID)

	// Считываем транзакции из Redis
	var transactions []aot.Transaction
	for _, key := range transactionKeys {
		log.Printf("Fetching transaction data for key: %s", key)

		// Получаем хэш данных транзакции
		data, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			log.Printf("Error fetching transaction data for key %s: %v", key, err)
			return nil, fmt.Errorf("failed to get transaction data for key %s: %v", key, err)
		}

		// Преобразуем данные в структуру Transaction
		exchangeId, err := NewExchangeId(data["ExchangeId"]) // Используем функцию NewExchangeId для преобразования строки в перечисление
		if err != nil {
			log.Printf("Error converting ExchangeId for key %s: %v", key, err)
			return nil, fmt.Errorf("failed to convert ExchangeId: %v", err)
		}

		marketType, err := NewMarketType(data["MarketType"]) // Используем функцию NewMarketType для преобразования строки в перечисление
		if err != nil {
			log.Printf("Error converting MarketType for key %s: %v", key, err)
			return nil, fmt.Errorf("failed to convert MarketType: %v", err)
		}

		transactionAction, err := NewTransactionAction(data["TransactionAction"]) // Используем функцию NewTransactionAction для преобразования строки в перечисление
		if err != nil {
			log.Printf("Error converting TransactionAction for key %s: %v", key, err)
			return nil, fmt.Errorf("failed to convert TransactionAction: %v", err)
		}

		transaction := aot.Transaction{
			TradingPair:       data["TradingPair"],
			ExchangeId:        exchangeId,
			MarketType:        marketType,
			TransactionAction: transactionAction,
		}

		transactions = append(transactions, transaction)
	}
	log.Printf("Successfully fetched %d transactions for tradeID %d", len(transactions), tradeID)
	return transactions, nil
}

// API Handler для получения списка транзакций по TradeID
func GetTransactions(c *gin.Context) {
	// Логируем входящий запрос
	log.Printf("Incoming request for TradeID: %s", c.Param("tradeID"))

	// Получаем TradeID из параметров запроса
	tradeIDParam := c.Param("tradeID")
	tradeID, err := strconv.ParseUint(tradeIDParam, 10, 64)
	if err != nil {
		log.Printf("Error parsing TradeID '%s': %v", tradeIDParam, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid TradeID"})
		return
	}

	// Логируем успешный парсинг TradeID
	log.Printf("Parsed TradeID: %d", tradeID)

	// Получаем транзакции из Redis
	transactions, err := getTransactionsForTradeID(tradeID)
	if err != nil {
		log.Printf("Error fetching transactions for TradeID %d: %v", tradeID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Convert []aot.Transaction to []*aot.Transaction
	transactionPtrs := make([]*aot.Transaction, len(transactions))
	for i := range transactions {
		transactionPtrs[i] = &transactions[i]
	}

	trade := aot.Trade{
		Id:           tradeID,
		Transactions: transactionPtrs,
	}

	// Сериализуем объект trade
	data, err := proto.Marshal(&trade)
	if err != nil {
		log.Printf("Error marshalling trade data: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize data"})
		return
	}

	// Логируем сериализованные данные
	log.Printf("Serialized data: %x", data)

	// Десериализация и логирование результата
	var deserializedTrade aot.Trade
	if err := proto.Unmarshal(data, &deserializedTrade); err != nil {
		log.Printf("Error deserializing trade data: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to deserialize data"})
		return
	}

	// Логируем десериализованный объект
	log.Printf("Deserialized Trade: ID=%d, Transactions=%v", deserializedTrade.Id, deserializedTrade.Transactions)

	c.Header("Access-Control-Allow-Origin", "*")                   // Разрешить запросы с любого домена
	c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS") // Разрешить необходимые методы
	c.Header("Access-Control-Allow-Headers", "Content-Type")       // Разрешить заголовки
	c.Header("Content-Type", "application/x-protobuf")

	if c.Request.Method == http.MethodOptions {
		// Возвращаем успешный ответ для предварительных запросов (OPTIONS)
		c.Writer.WriteHeader(http.StatusOK)
		return
	}

	// Возвращаем сериализованные данные клиенту
	c.Writer.WriteHeader(http.StatusOK)
	_, err = c.Writer.Write(data)
	if err != nil {
		log.Printf("Error writing response: %v", err)
	}
}
