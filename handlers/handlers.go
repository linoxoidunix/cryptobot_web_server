package handlers

import (
	"context"
	"cryptobot_server/aot"
	"cryptobot_server/redis"
	"fmt"
	"log"
	"strconv"

	"google.golang.org/protobuf/proto"
)

var ctx = context.Background()

var TopicHandlers = map[string]func(chan []byte, interface{}){
	"orderbook":        handleOrderBook,
	"pnl":              handlePNL,
	"wallet":           handleWallet,
	"trade":            handleTrade,
	"trade_dictionary": handleTradeDictionary,
}

func handleOrderBook(messageChannel chan []byte, data interface{}) {
	log.Println("Handling orderbook data:", data)
	messageChannel <- data.([]byte)
}

func handlePNL(messageChannel chan []byte, data interface{}) {
	log.Println("Handling PNL data:", data)
	messageChannel <- data.([]byte)
}

func handleWallet(messageChannel chan []byte, data interface{}) {
	log.Println("Handling wallet data:", data)
	messageChannel <- data.([]byte)
}

func handleTrade(messageChannel chan []byte, data interface{}) {
	log.Println("Handling trade data:", data)
	messageChannel <- data.([]byte)
}

func handleTradeDictionary(messageChannel chan []byte, data interface{}) {
	log.Println("Handling TradeDictionary message:", data)

	var trades aot.Trades

	bytes, ok := data.([]byte)
	if !ok {
		log.Println("Type assertion failed for TradeDictionary")
		return
	}

	if err := proto.Unmarshal(bytes, &trades); err != nil {
		log.Fatalf("Failed to unmarshal: %v", err)
	}

	for tradeID, trade := range trades.Trades {
		fmt.Printf("Trade ID: %d\n", tradeID)
		for idTransaction, transaction := range trade.Transactions {
			fmt.Printf("  Trading Pair: %s\n", transaction.TradingPair)
			fmt.Printf("  Exchange: %s\n", transaction.ExchangeId.String())
			fmt.Printf("  Market Type: %s\n", transaction.MarketType.String())
			fmt.Printf("  Transaction Action: %s\n\n", transaction.TransactionAction.String())

			redisKey := fmt.Sprintf("trade:%d:transaction:%d", tradeID, idTransaction)
			redis.SaveTransactionToRedis(redisKey, transaction)

			redis.AddTransactionToListIfNotExists(fmt.Sprintf("trade:%s:transactions", strconv.FormatUint(tradeID, 10)), redisKey)

		}
	}

	log.Println("Successfully processed TradeDictionary message")
}
