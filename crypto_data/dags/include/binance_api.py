import requests , math, time
from typing import Union,Optional

"""
Functions for getting data from the Binance US API
"""

def __op_is_sell(transaction:dict)->Optional[bool]: 
     """
     #Sell Operation ("m" is true): The trade represents selling BTC for USDT.   
     #Buy Operation ("m" is false): The trade represents buying BTC with USDT.
     """                                         
     opr_agr = transaction.get("m",None)
     if opr_agr == None:
           return None
     return transaction["m"] == True

def min_to_ms(min:Union[int,float])->int:
     return int(min*60*1000)

def __binance_crypto_price(symbol:str, api_time_interval_ms:int ,end_time_unix:int)->Optional[float]:
     UNIX_TIME_INTERVAL:int = api_time_interval_ms # in ms, approx 10s
     kline_data_url = "https://api.binance.us/api/v3/klines"
     params:dict = {
          "symbol": symbol,
          "interval": "1m",
          "startTime": end_time_unix - UNIX_TIME_INTERVAL,
          "endTime" :  end_time_unix,
          "limit": 1
     }
     response = requests.get(url=kline_data_url, params=params)
     if response.status_code == 200:
            json_result:list[list[Union[str,float]]] = response.json()
     else:
            print(F"Failure in getting aggregate trading data: {response.status_code}, response: {response.text}")
            return None
     if not json_result:
          return None
     return float(json_result[0][1])

def binance_server_time()->Optional[int]:
        response = requests.get("https://api.binance.us/api/v3/time")
        if response.status_code == 200:
             json_result:dict = response.json()
             return json_result.get("serverTime",None)
        else:
            return None

def binance_trading_volume(time_window_min: Union[int,float] , crypto_token:str,api_time_interval_ms:int ,end_unix_time:int = 0)-> Optional[ dict[str, Union[int,float]] ]:
    print("binance func")
    bin_api_func :float = time.time()
    MAX_REQUEST_TIMEFRAME = api_time_interval_ms # type: ignore /// 60-90k ms is the limit time (based on some test) where you can get all aggregata trading without hitting the 1000 results limit on the binance API
    time_window_ms = min_to_ms(time_window_min)

    requests_needed:int = math.ceil(time_window_ms/MAX_REQUEST_TIMEFRAME)

    if end_unix_time == 0:
        end_time: int  =int(time.time() * 1000)
    else:
        end_time = end_unix_time
    
    total_transactions:int = 0
    requests_hitting_limit:int = 0 #number of requests hitting the 1000 responses API limit
    final_price: Optional[float] = __binance_crypto_price(symbol=crypto_token,end_time_unix=end_time, api_time_interval_ms= api_time_interval_ms) #price of the crypto token at the latest date in the timeframe
    if final_price == None:
         return None 

    total_sell_coins: float = 0.0
    total_sell_usd:float  = 0.0
    total_buy_coins:float  = 0.0
    total_buy_usd:float  = 0.0
    
    for i in range(requests_needed):
        params:dict = {
            "symbol": crypto_token,
            "startTime" : end_time - MAX_REQUEST_TIMEFRAME,  #em ms , a maior janela que chega perto do limite de 1000 respostas é 90000 ms
            "endTime":    end_time,
            "limit" : 1000
        }
      
        response: requests.Response = requests.get("https://api.binance.us/api/v3/aggTrades", params=params)
        if response.status_code == 200:
                json_result:list[dict] = response.json()
        else:
                return None 
        num_transactions:int = len(json_result)
        total_transactions += num_transactions
    
        if num_transactions == 1000:
              requests_hitting_limit+=1
              
        for transaction in json_result:
                price:float = float(transaction.get("p",0.0))
                quantity: float = float(transaction.get("q",0.0))
                
                if __op_is_sell(transaction):
                    total_sell_coins += quantity
                    total_sell_usd += quantity * price 
                else:
                    total_buy_coins += quantity
                    total_buy_usd += quantity * price 

        end_time -= MAX_REQUEST_TIMEFRAME
    print(f"a total of {requests_needed} api requests is needed")
    start_price: Optional[float] = __binance_crypto_price(symbol=crypto_token,end_time_unix=end_time, api_time_interval_ms = api_time_interval_ms) #price of the crypto token at the earliest date in the timeframe
    if start_price == None:
         return None 

    return {
            f"{crypto_token}_START_PRICE": start_price, 
            f"{crypto_token}_END_PRICE": final_price, 
            f"{crypto_token}_COINS_SOLD": round(total_sell_coins,3),
            f"{crypto_token}_USD_SOLD": round(total_sell_usd,3),
            f"{crypto_token}_COINS_BOUGHT": round(total_buy_coins,3),
            f"{crypto_token}_USD_BOUGHT": round(total_buy_usd,3),
            f"{crypto_token}_NET_FLOW": round(total_buy_usd - total_sell_usd,3),
            f"{crypto_token}_TOTAL_AGGRT_TRANSACTIONS": total_transactions
    }

if __name__ == "__main__":
   #print(binance_trading_volume(30, crypto_token="SOLUSDT"))
   cur_time = binance_server_time()
   print(int(1000* time.time()))
   print(cur_time)
   #return_data = __binance_crypto_price("BTCUSDT",cur_time) # type: ignore
   #print(return_data)
  