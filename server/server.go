package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var PORT=8080

type Request struct{
	OrderId string `json:"order_id"`
	BotId string `json:"bot_id"`
	Symbol string `json:"symbol"`
	Price float64 `json:"price"`
	Quantity int `json:"quantity"`
	Action string `json:"action"`
	Ordertype string `json:"order_type"`
	Timestamp int64 `json:"timestamp"`
}

type Response struct{
	OrderId string 
	Status string
	ProcessedAt int64 
}

var upgrader =websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool{
		return true
	},
}

func wsHandler(w http.ResponseWriter,r *http.Request){
	connection,err:=upgrader.Upgrade(w,r,nil)
	if err!=nil{
		log.Fatal("Failed to upgrade the connection to websocket")
	}
	defer connection.Close()
	for{
		_,message,err:=connection.ReadMessage();
		if err!=nil{
			fmt.Println("error in receiving the message from the client")
		}
		var request Request
		Err:=json.Unmarshal(message,&request)
		if Err!=nil{
			fmt.Println("request send by the client is not in the correct format")
			//continue
		}
		fmt.Println("request received")
		fmt.Println(request)
		response:=Response{
			OrderId: request.OrderId,
			Status: "accepted",
			ProcessedAt: time.Now().UnixMilli(),
		}
		Message,ERR:=json.Marshal(response)
		if ERR!=nil{
			fmt.Println("failed to parse the response to json")
			continue
		}
		ERROR:= connection.WriteMessage(websocket.TextMessage,Message)
		if ERROR!=nil{
			fmt.Println("failed to send the response to the client")
		}else{
			fmt.Println("response successfully send to server")
		}
	}
}

func main(){
	http.HandleFunc("/trade",wsHandler)
	fmt.Printf("websocket server started at port: %d",PORT)
	err:=http.ListenAndServe("localhost:8080",nil)
	if err!=nil{
		log.Fatal("Error in starting the server")
	}
	// fmt.Println("Server started successfully")
}