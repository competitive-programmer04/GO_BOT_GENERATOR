package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// type RequestMessage struct{
// 	bot_id string `json:"botId"`
// 	action string `json: "action"`
// }
type EngineResponse struct{
	OrderId string `json:"order_id"`
	Status string `json:"status"`
	ProcessedAt int64 `json:"processed_at"`
}

func main(){
	ini_start_bot:=1;
	ini_end_bot:=10;
	serverURL:= url.URL{Scheme:"ws",Host:"localhost:8080",Path:"/trade"};
	connection,_,err:= websocket.DefaultDialer.Dial(serverURL.String(),nil);
	if err!=nil{
		log.Fatal("cannot create a websocket connection with the server");
		fmt.Println(err);
	}else{
		fmt.Println("successfully established a websocket connection with the server")
	}
	defer connection.Close();
	orderChannel:=make(chan string,100000);
	// time_stamp:=make(map[string]int64)
	var pendingOrders sync.Map
	go func(){
		// var requestMessage RequestMessage;
		for message:= range orderChannel{
			connection.WriteMessage(websocket.TextMessage,[]byte(message))
		};
	}()
	go func(){
		time.Sleep(time.Second);
		currentTime:=time.Now().UnixMilli()
		for{
			pendingOrders.Range(func(key, value interface{})bool{
				orderId:=key.(string);
				sentTime:=value.(int64);
				if currentTime-sentTime>=2000{
					fmt.Println("the server took more than 2 second to response so it will not be accepted")
					pendingOrders.Delete(orderId)
				}
				return true;
			})
		}
	}()
	go func(){
		for{
			_,message,err:=connection.ReadMessage()
			if err!=nil{
				fmt.Println("Connection dropped by the server")
				return;
			}
			var response EngineResponse;
			parseErr:=json.Unmarshal(message,response)
			if parseErr!=nil{
				fmt.Println("the response given by the server is not in the desired form")
				continue;
			}
			if response.OrderId==""{
				print("server not return which bot request he processed necessary for calculating latency")
				continue
			}
			// latency:=time_stamp[response.OrderId]-response.ProcessedAt;
			// Success:=1;
			sentTimeInterface,exists:=pendingOrders.Load(response.OrderId)
			if exists{
				fmt.Println("server returns the response in under 2 seconds");
				latency:=response.ProcessedAt-sentTimeInterface.(int64);
				success:=1;
			}else{
				fmt.Println("server returns the response after 2seconds so it is of no use");
				latency:=2000;
				success:=0;
			}
		}
	}()
	ticker:=time.NewTicker(time.Second)
	defer ticker.Stop()
	for{
		select{
	case t:=<-ticker.C:
		for i:=ini_start_bot;i<=ini_end_bot/10;i++{
			botId:=fmt.Sprintf("bot-%d",i)
			go func(bot_id string){
				for{
					now:=time.Now().UnixMilli();
					orderId:=fmt.Sprintf("%s-%s",botId,now);
					price:=100.0+rand.Float64()*50.0;
					quantity:=rand.Intn(100)+10;
					orderType:=[]string{"Limit","Market"}
					randomOrderType:=orderType[rand.Intn(len(orderType))];
					symbols:=[]string{"AAPL","TSLA","GOOG","MSFT","AMZN"};
					randomSymbol:=symbols[rand.Intn(len(symbols))];
					requestMethods:=[]string{"Buy","Sell","Cancel"};
					randomRequestMethod:=requestMethods[rand.Intn(len(requestMethods))];
					// orderType="";
					// if rand.Intn(2)<=1{
					// 	orderType:="Limit"
					// }else{
					// 	orderType="Market"
					// }
					jsonpayload:=fmt.Sprintf("{order_id:%s,bot_id:%s,symbol:%s,price:%.2f,quantity:%d,action:%s,order_type:%s,timestamp:%d}",orderId,bot_id,
					randomSymbol,price,quantity,randomRequestMethod,randomOrderType,now);
					//time_stamp[orderId]=now
					pendingOrders.Store(orderId,now)
				    orderChannel<-jsonpayload
				    time.Sleep(10*time.Millisecond);
				}
			}(botId)
			ini_start_bot=ini_end_bot;
			ini_end_bot=2*ini_end_bot;
		}
	 }
	}
}