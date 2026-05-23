package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// type RequestMessage stru3ct{
// 	bot_id string `json:"botId"`
// 	action string `json: "action"`
// }
type ServerResponse struct{
	OrderId string `json:"order_id"`
	Status string `json:"status"`
	ProcessedAt int64 `json:"processed_at"`
}

type ClientRequest struct{
	order_id string
	bot_id string
	symbol string
	price float64
	quantity int
	action string
	order_type string
	timestamp int64
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

	rdb:=redis.NewClient(&redis.Options{
		Addr:"localhost:6379",
		Password:"",
		DB:0,
		Protocol:2,
	})
	ctx:=context.Background()
	
	checkingRequest:=make(map[string]ClientRequest)
	orderChannel:=make(chan string,100000);
	resultChannel:=make(chan string,100000);
	// time_stamp:=make(map[string]int64)
	var pendingOrders sync.Map
	done:=make(chan struct{});
	ticker:=time.NewTicker(time.Second)
	defer ticker.Stop()
	go func(){
		for{
		select{
	case t:=<-ticker.C:
		fmt.Printf("Current second: %d",t.Second())
		for i:=ini_start_bot;i<=ini_end_bot/10;i++{
			botId:=fmt.Sprintf("bot-%d",i)
			func(bot_id string){
				for{
					now:=time.Now().UnixMilli();
					orderId:=fmt.Sprintf("%s-%d",botId,now);
					price:=100.0+rand.Float64()*50.0;
					quantity:=rand.Intn(100)+10;
					// orderType:=[]string{"Limit","Market"}
					// randomOrderType:=orderType[rand.Intn(len(orderType))];
					orderType:="Limit"
					symbols:=[]string{"AAPL","TSLA","GOOG","MSFT","AMZN"};
					randomSymbol:=symbols[rand.Intn(len(symbols))];
					requestMethods:=[]string{"Buy","Sell","Cancel"};
					randomRequestMethod:=requestMethods[rand.Intn(len(requestMethods))];
					negativeTesting:=rand.Intn(100)+1;
					if negativeTesting<5{
						switch (negativeTesting){
						case 1:
							price = -100.0+rand.Float64()*50
							//break
						case 2:
							quantity = rand.Intn(-100)-10
							//break
						case 3:
							orderType = "Market"
							//break
						case 4:
							corruptRequestMethod:=[]string{"Hold","Wait","Steal"}
							randomRequestMethod = corruptRequestMethod[rand.Intn(len(corruptRequestMethod))]
							//break
						}
					}
					// orderType="";
					// if rand.Intn(2)<=1{
					// 	orderType:="Limit"+
					// }else{
					// 	orderType="Market"
					// }
					request:=ClientRequest{
						order_id: orderId,
						bot_id: botId,
						symbol: randomSymbol,
						price: price,
						quantity: quantity,
						action: randomRequestMethod,
						order_type: orderType,
						timestamp: now,
					}
					message,err:=json.Marshal(request)
					if err!=nil{
						fmt.Println("error in converting request to byte array")
						continue
					}
					pendingOrders.Store(orderId,now)
					checkingRequest[orderId]=request
				    orderChannel<-string(message)
				    time.Sleep(100*time.Millisecond);
					// jsonpayload:=fmt.Sprintf("{order_id:%s,bot_id:%s,symbol:%s,price:%.2f,quantity:%d,action:%s,order_type:%s,timestamp:%d}",orderId,bot_id,
					// randomSymbol,price,quantity,randomRequestMethod,randomOrderType,now);
					//time_stamp[orderId]=now
				}
			}(botId)
			ini_start_bot=ini_end_bot;
			ini_end_bot=2*ini_end_bot;
		}
	case <-done:
		fmt.Println("closing the connection between our bots and server");
		return;
	 }
	}
	}()
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
	// done:=make(chan struct{});
	go func(){
		for{
			_,message,err:=connection.ReadMessage()
			if err!=nil{
				fmt.Println("Connection dropped by the server")
				close(done)
				return;
			}
			var response ServerResponse;
			parseErr:=json.Unmarshal(message,&response)
			if parseErr!=nil{
				fmt.Println("the response given by the server is not in the desired form")
				continue
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
				requestSend:=checkingRequest[response.OrderId]
				if requestSend.price<=0 || requestSend.quantity<=0 || requestSend.order_type=="Market" || !slices.Contains([]string{"Buy","Sell","Cancel"},requestSend.action){
					if response.Status == "rejected"{
						latency:=response.ProcessedAt-sentTimeInterface.(int64);
				        success:=1;
						result:=fmt.Sprintf("%d,%d",latency,success)
						resultChannel<-result
					}else{
						latency:=response.ProcessedAt-sentTimeInterface.(int64);
						success:=0
						result:=fmt.Sprintf("%d,%d",latency,success)
						resultChannel<-result
					}
				}else{
					latency:=response.ProcessedAt-sentTimeInterface.(int64);
					success:=1;
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}
			}else{
				fmt.Println("server returns the response after 2seconds so it is of no use");
				latency:=2000;
				success:=0;
				result:=fmt.Sprintf("%d,%d",latency,success)
				resultChannel<-result
			}
			delete(checkingRequest,response.OrderId);
		}
	}()
	go func(){
		ticker:=time.NewTicker(100*time.Millisecond)
		batch:=make([]interface{},0,1000)
		for{
			select{
			case result:=<-resultChannel:
				batch=append(batch,result)
				if len(batch)>=1000{
					rdb.RPush(ctx,"telemetryQueue",batch...)
					batch=batch[:0]
				}
			case t:=<-ticker.C:
				fmt.Printf("Current second: %d",t.Second())
				if len(batch)>0{
					rdb.RPush(ctx,"telemetryQueue",batch...)
					batch=batch[:0]
				}
			}
		}

	}()
}