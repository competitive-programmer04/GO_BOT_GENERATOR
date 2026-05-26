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

type GeneralServerResponse struct{
	Event string `json:"event"`
	ProcessedAt int64 `json:"processed_at"`
}

type FilledResponse struct{
	Event string `json:"event"`
	BuyOrderId string `json:"buy_order_id"`
	SellOrderId string `json:"sell_order_id"`
	FilledQuantity int `json:"filled_quantity"`
	MatchPrice float64 `json:"match_price"`
	ProcessedAt int64 `json:"processed_at"`
}

type PartiallyFilledResponse struct{
	Event string `json:"event"`
	BuyOrderId string `json:"buy_order_id"`
	SellOrderId string `json:"sell_order_id"`
	MatchPrice float64 `json:"match_price"`
	FilledQuantity int `json:"filled_quantity"`
	BuyRemaining int `json:"buy_remaining"`
	SellRemaining int `json:"sell_remaining"`
	ProcessedAt int64 `json:"processed_at"`
}

type AcknowledgedResponse struct{
	Event string `json:"event"`
	OrderId string `json:"order_id"`
	BotId string `json:"bot_id"`
	ProcessedAt int64 `json:"processed_at"`
}

type CancelledResponse struct{
	Event string `json:"event"`
	OrderId string `json:"order_id"`
	BotId string `json:"bot_id"`
	ProcessedAt int64 `json:"processed_at"`
}

type RejectedEvent struct{
	Event string `json:"event"`
	IncomingOrderId string `json:"incoming_order_id"`
	RestingOrderId string `json:"resting_order_id"`
	ProcessedAt int64  `json:"processed_at"`
}

type InvalidRequest struct{
	Event string `json:"event"`
	OrderId string `json:"order_id"`
	BotId string `json:"bot_id"`
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

type Order struct{
	timestamp int64
	status string 
}

func main(){
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
	
	var checkingRequest sync.Map
	orderChannel:=make(chan string,100000);
	resultChannel:=make(chan string,100000);
	var pendingOrders sync.Map
	fillChannel:=make(chan string)
	done:=make(chan struct{});
	ticker:=time.NewTicker(time.Second)
	defer ticker.Stop()
	ini_start_bot:=1;
	ini_end_bot:=10;
	go func(){
		for{
		select{
	case t:=<-ticker.C:
		fmt.Printf("Current second: %d",t.Second())
		for i:=ini_start_bot;i<=ini_end_bot/10;i++{
			botId:=fmt.Sprintf("bot-%d",i)
			go func(bot_id string){
				for{
					now:=time.Now().UnixMilli();
					orderId:=fmt.Sprintf("%s-%d",botId,now);
					price:=100.0+rand.Float64()*50.0;
					quantity:=rand.Intn(100)+10;
					orderType:="limit"
					symbols:=[]string{"AAPL","TSLA","GOOG","MSFT","AMZN"};
					randomSymbol:=symbols[rand.Intn(len(symbols))];
					requestMethods:=[]string{"buy","sell","cancel"};
					randomRequestMethod:=requestMethods[rand.Intn(len(requestMethods))];
					negativeTesting:=rand.Intn(100)+1;
					if negativeTesting<5{
						switch (negativeTesting){
						case 1:
							price = -100.0+rand.Float64()*50
						case 2:
							quantity = rand.Intn(-100)-10
						case 3:
							orderType = "market"
						case 4:
							corruptRequestMethod:=[]string{"hold","wait","steal"}
							randomRequestMethod = corruptRequestMethod[rand.Intn(len(corruptRequestMethod))]
						}
					}
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
				    orderChannel<-string(message)
					checkingRequest.Store(orderId,request)
					order:=Order{
						timestamp: request.timestamp,
						status: "",
					}
					pendingOrders.Store(orderId,order)
				    time.Sleep(100*time.Millisecond);
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
		for{
			price:=100+rand.Float64()*100
		    quantity:=rand.Intn(100)+50
			drainLoop:
			for{
				select{
				case<-fillChannel:
				default:
					break drainLoop
				}
			}
			now:=time.Now().UnixMilli()
			orderId:=fmt.Sprintf("bot-%d-%d",1,now)

			order1:=fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:sell,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",1),price-float64(10*1),quantity,now)

			SendandWait:=func(order string)bool{
				orderChannel<-order
				pendingOrders.Store(orderId,Order{timestamp: now,status: ""})
				select{
				case<-fillChannel:
					return true
				case <-time.After(2*time.Second):
					return false
				}
			}
			if !(SendandWait(order1)){
				orderChannel<-fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:cancel,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",1),price-float64(10*1),quantity,now)
			pendingOrders.Store(orderId,Order{timestamp: time.Now().UnixMilli(),status: ""})
			continue
			}
			now=time.Now().UnixMilli()
			orderId=fmt.Sprintf("bot-%d-%d",2,now)
			order2:=fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:sell,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",2),price-float64(10*3),quantity,now)

			if !(SendandWait(order2)){
				orderChannel<-fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:cancel,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",1),price-float64(10*1),quantity,now)
			pendingOrders.Store(orderId,Order{timestamp: time.Now().UnixMilli(),status: ""})
				orderChannel<-fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:cancel,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",2),price-float64(10*3),quantity,now)
			pendingOrders.Store(orderId,Order{timestamp: time.Now().UnixMilli(),status: ""})
			continue
			}
			now=time.Now().UnixMilli()
			orderId=fmt.Sprintf("bot-%d-%d",3,now)
			order3:=fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:sell,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",3),price-float64(10*2),quantity,now)
			if !(SendandWait(order3)){
				orderChannel<-fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:cancel,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",1),price-float64(10*1),quantity,now)
			pendingOrders.Store(orderId,Order{timestamp: time.Now().UnixMilli(),status: ""})
				orderChannel<-fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:cancel,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",2),price-float64(10*3),quantity,now)
			pendingOrders.Store(orderId,Order{timestamp: time.Now().UnixMilli(),status: ""})
				orderChannel<-fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:cancel,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",3),price-float64(10*2),quantity,now)
			pendingOrders.Store(orderId,Order{timestamp: time.Now().UnixMilli(),status: ""})
			continue
			}
			now=time.Now().UnixMilli()
			orderId=fmt.Sprintf("bot-%d-%d",4,now)
			order4:=fmt.Sprintf("{order_id:%s,bot_id:%s,price:%f,quantity:%d,symbol:IICPC_PRIO,action:buy,timestamp:%d,order_type:limit}",
		    orderId,fmt.Sprintf("sniperbot-%d",4),price,quantity,now)
			orderChannel<-order4
			pendingOrders.Store(orderId,Order{timestamp: now,status: ""})
		}
	}()
	go func(){
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
				sentOrder:=value.(Order);
				if currentTime-sentOrder.timestamp>2000 && sentOrder.status==""{
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
				close(done)
				return;
			}
			var response GeneralServerResponse;
			parseErr:=json.Unmarshal(message,&response)
			if parseErr!=nil{
				fmt.Println("the response returned by the server is not in the desired form")
				continue;
			}
			switch (response.Event){
			case "filled":
				var filled_response FilledResponse
				err:=json.Unmarshal(message,&filled_response)
				if err!=nil{
					fmt.Println("the response returned by the server is not in the correct format")
					continue
				}
				order1,buy_order_exists:=pendingOrders.Load(filled_response.BuyOrderId)
				order2,sell_order_exists:=pendingOrders.Load(filled_response.SellOrderId)
				if !buy_order_exists || !sell_order_exists{
					fmt.Println("engine returned the response of a non existing request")
					latency:=time.Now().UnixMilli()-filled_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					checkingRequest.Delete(filled_response.BuyOrderId)
					checkingRequest.Delete(filled_response.SellOrderId)
					continue
				}
				pendingOrders.Store(filled_response.BuyOrderId,Order{timestamp: order1.(Order).timestamp,status: "filled"})
				pendingOrders.Store(filled_response.SellOrderId,Order{timestamp: order2.(Order).timestamp,status: "filled"})
				req1,_:=checkingRequest.Load(filled_response.BuyOrderId)
				req2,_:=checkingRequest.Load(filled_response.SellOrderId)
				buy_req:=req1.(ClientRequest)
				sell_req:=req2.(ClientRequest)
				if (buy_req.action=="cancel"||sell_req.action=="cancel")||(buy_req.action==sell_req.action)||
				(buy_req.symbol!=sell_req.symbol)||(buy_req.quantity!=sell_req.quantity)||(buy_req.bot_id==sell_req.bot_id){
					fmt.Println("response is wrong")
					latency:=time.Now().UnixMilli()-filled_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					pendingOrders.Delete(buy_req.order_id)
					pendingOrders.Delete(sell_req.order_id)
					checkingRequest.Delete(buy_req.order_id)
					checkingRequest.Delete(sell_req.order_id)
					continue
				}
				order_symbol:=buy_req.symbol
				if order_symbol=="IICPC_PRIO"{
					if sell_req.bot_id=="sniperbot-2"&&filled_response.MatchPrice==sell_req.price{
					   latency:=time.Now().UnixMilli()-filled_response.ProcessedAt
					   success:=1
					   result:=fmt.Sprintf("%d,%d",latency,success)
					   resultChannel<-result
					}else{
						latency:=time.Now().UnixMilli()-filled_response.ProcessedAt
					   success:=0
					   result:=fmt.Sprintf("%d,%d",latency,success)
					   resultChannel<-result
					}
				}else{
					if buy_req.price>=sell_req.price{
						latency:=time.Now().UnixMilli()-filled_response.ProcessedAt
					   success:=1
					   result:=fmt.Sprintf("%d,%d",latency,success)
					   resultChannel<-result
					}else{
						latency:=time.Now().UnixMilli()-filled_response.ProcessedAt
					   success:=0
					   result:=fmt.Sprintf("%d,%d",latency,success)
					   resultChannel<-result
					}
				}
				pendingOrders.Delete(buy_req.order_id)
				pendingOrders.Delete(sell_req.order_id)
				checkingRequest.Delete(buy_req.order_id)
				checkingRequest.Delete(sell_req.order_id)

			case "partially_filled":
				var partially_filled_response PartiallyFilledResponse
				err:=json.Unmarshal(message,&partially_filled_response)
				if err!=nil{
					fmt.Printf("engine not returned the response in desired format")
					continue
				}
				order1,buy_order_exists:=pendingOrders.Load(partially_filled_response.BuyOrderId)
				order2,sell_order_exists:=pendingOrders.Load(partially_filled_response.SellOrderId)
				if !buy_order_exists||!sell_order_exists{
					fmt.Println("engine returned the response of a non existing request")
					latency:=time.Now().UnixMilli()-partially_filled_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					checkingRequest.Delete(partially_filled_response.BuyOrderId)
					checkingRequest.Delete(partially_filled_response.SellOrderId)
					continue
				}
				pendingOrders.Store(partially_filled_response.BuyOrderId,Order{timestamp: order1.(Order).timestamp,status: "partially_filled"})
				pendingOrders.Store(partially_filled_response.SellOrderId,Order{timestamp: order2.(Order).timestamp,status: "partially_filled"})
				req1,_:=checkingRequest.Load(partially_filled_response.BuyOrderId)
				req2,_:=checkingRequest.Load(partially_filled_response.SellOrderId)
				buy_req:=req1.(ClientRequest)
				sell_req:=req2.(ClientRequest)
				if (buy_req.action=="cancel"||sell_req.action=="cancel")||(buy_req.action==sell_req.action)||
				(buy_req.symbol!=sell_req.symbol)||(buy_req.bot_id==sell_req.bot_id){
					fmt.Println("response is wrong")
					latency:=time.Now().UnixMilli()-partially_filled_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					pendingOrders.Delete(buy_req.order_id)
					pendingOrders.Delete(sell_req.order_id)
					checkingRequest.Delete(buy_req.order_id)
					checkingRequest.Delete(sell_req.order_id)
					continue
				}
				if (buy_req.price>=partially_filled_response.MatchPrice&&partially_filled_response.MatchPrice<=sell_req.price)&&
				(buy_req.price>=sell_req.price)&&(partially_filled_response.BuyRemaining+partially_filled_response.FilledQuantity==buy_req.quantity)&&
				(partially_filled_response.SellRemaining+partially_filled_response.FilledQuantity==sell_req.quantity)&&(buy_req.bot_id!=sell_req.bot_id){
					   latency:=time.Now().UnixMilli()-partially_filled_response.ProcessedAt
					   success:=1
					   result:=fmt.Sprintf("%d,%d",latency,success)
					   resultChannel<-result
					   buy_req.quantity=partially_filled_response.BuyRemaining
					   sell_req.quantity=partially_filled_response.SellRemaining
					   checkingRequest.Store(buy_req.order_id,buy_req)
					   checkingRequest.Store(sell_req.order_id,sell_req)
				}else{
					   latency:=time.Now().UnixMilli()-partially_filled_response.ProcessedAt
					   success:=0
					   result:=fmt.Sprintf("%d,%d",latency,success)
					   resultChannel<-result
				pendingOrders.Delete(buy_req.order_id)
				pendingOrders.Delete(sell_req.order_id)
				checkingRequest.Delete(buy_req.order_id)
				checkingRequest.Delete(sell_req.order_id)
				}
			case "acknowledgement":
				var ack_response AcknowledgedResponse
				err:=json.Unmarshal(message,&ack_response)
				if err!=nil{
					fmt.Println("The response returned by the engine is not in the correct format")
					continue
				}
				order1,order_exists:=pendingOrders.Load(ack_response.OrderId)
				if !order_exists{
					fmt.Println("engine returned the response of a non existing request")
					latency:=time.Now().UnixMilli()-ack_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					checkingRequest.Delete(ack_response.OrderId)
					continue
				}
				pendingOrders.Store(ack_response.OrderId,Order{timestamp: order1.(Order).timestamp,status: "acknowledgement"})
				req,_:=checkingRequest.Load(ack_response.OrderId)
				request:=req.(ClientRequest)
				if request.symbol=="IICPC_PRIO"{
					fillChannel<-"acknowledgement"
					latency:=time.Now().UnixMilli()-ack_response.ProcessedAt
					success:=1
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}else{
					latency:=time.Now().UnixMilli()-ack_response.ProcessedAt
					success:=1
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}
			case "cancelled":
				var cancel_response CancelledResponse
				err=json.Unmarshal(message,&cancel_response)
				if err!=nil{
					fmt.Println("engine not returned the response in the desired format")
					continue
				}
				order,order_exists:=pendingOrders.Load(cancel_response.OrderId)
				if !order_exists{
					fmt.Println("engine returned the response of a non existing event")
					latency:=time.Now().UnixMilli()-cancel_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					checkingRequest.Delete(cancel_response.OrderId)
					continue
				}
				pendingOrders.Store(cancel_response.OrderId,Order{timestamp: order.(Order).timestamp,status: "cancelled"})
				req,_:=checkingRequest.Load(cancel_response.OrderId)
				request:=req.(ClientRequest)
				if request.bot_id==cancel_response.BotId{
					latency:=time.Now().UnixMilli()-cancel_response.ProcessedAt
					success:=1
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}else{
					latency:=time.Now().UnixMilli()-cancel_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}
				pendingOrders.Delete(request.order_id)
				checkingRequest.Delete(request.order_id)
			case "rejected":
				var reject_response RejectedEvent
				err=json.Unmarshal(message,&reject_response)
				if err!=nil{
					fmt.Println("the response returned by the engine is not in the correct format")
					continue
				}
				order1,in_order_exists:=pendingOrders.Load(reject_response.IncomingOrderId)
				order2,res_order_exists:=pendingOrders.Load(reject_response.RestingOrderId)
				if !in_order_exists||!res_order_exists{
					fmt.Println("server returned the response of a non existing event")
					latency:=time.Now().UnixMilli()-reject_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					checkingRequest.Delete(reject_response.IncomingOrderId)
					continue
				}
				pendingOrders.Store(reject_response.IncomingOrderId,Order{timestamp: order1.(Order).timestamp,status: "rejected"})
				pendingOrders.Store(reject_response.RestingOrderId,Order{timestamp: order2.(Order).timestamp,status: "rejected"})
				req1,_:=checkingRequest.Load(reject_response.IncomingOrderId)
				req2,_:=checkingRequest.Load(reject_response.RestingOrderId)
				incoming_request:=req1.(ClientRequest)
				resting_req,_:=req2.(ClientRequest)
				if (incoming_request.action!="cancel"&&resting_req.action!="cancel")&&(incoming_request.action!=resting_req.action)&&
				(incoming_request.symbol==resting_req.symbol)&&(incoming_request.bot_id==resting_req.bot_id){
					latency:=time.Now().UnixMilli()-reject_response.ProcessedAt
					success:=1
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					pendingOrders.Delete(incoming_request.order_id)
					checkingRequest.Delete(incoming_request.order_id)
				}else{
					latency:=time.Now().UnixMilli()-reject_response.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}
			case "invalid_request":
				var invalid_request InvalidRequest
				err:=json.Unmarshal(message,&invalid_request)
				if err!=nil{
					fmt.Println("the response returned by the engine is not is the correct format")
					continue
				}
				order,order_exists:=pendingOrders.Load(invalid_request.OrderId)
				if !order_exists{
					fmt.Println("engine returned the response of a non existing event")
					latency:=time.Now().UnixMilli()-invalid_request.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
					checkingRequest.Delete(invalid_request.OrderId)
					continue
				}
				pendingOrders.Store(invalid_request.OrderId,Order{timestamp: order.(Order).timestamp,status: "invalid request"})
				req,_:=checkingRequest.Load(invalid_request.OrderId)
				request:=req.(ClientRequest)
				if request.price<=0 || request.quantity<=0 || request.order_type=="market" || slices.Contains([]string{"hold","wait","steal"},request.action){
					latency:=time.Now().UnixMilli()-invalid_request.ProcessedAt
					success:=1
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}else{
					latency:=time.Now().UnixMilli()-invalid_request.ProcessedAt
					success:=0
					result:=fmt.Sprintf("%d,%d",latency,success)
					resultChannel<-result
				}
				pendingOrders.Delete(request.order_id)
				checkingRequest.Delete(request.order_id)
			default:
				fmt.Println("the event in the response field doen not match with the above events so the rseponse is wrong")
				latency:=time.Now().UnixMilli()-response.ProcessedAt
				success:=0
				result:=fmt.Sprintf("%d,%d",latency,success)
				resultChannel<-result
			}
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