import logging.config,json,os,time,math
import pandas as pd
from .binance_api import binance_trading_volume , min_to_ms
from datetime import datetime , timedelta

"""
max time back of 2,75 years should take around 230 hours to complete (almost 10 days)

"""
def setup_logging():
   
    with open(os.path.join("logger_config.json"), "r") as f:
            log_config = json.load(f)
            logging.config.dictConfig(config=log_config)
    #except:
     #   raise Exception("wasnt able to find/open the file logger_config.json, this makes logging impossible")

#setup_logging()

class CryptoDataETL():
 
    SUPPORTED_CRYPTO_TOKENS:set[str] = {"BTC", "ETH", "SOL"}
    MAX_TIME_FRAME_HOURS = 3   #how far back will data be collected in hours, equivalent to 2,75 years 
    DATA_TIME_WINDOW_MIN = 5 #how many minutes of data does each column represent
    TRADE_API_TIME_INTERVAL = 100000 #in ms, the time window for getting aggregated transaction data
    DATA_CHUNK_NUM_ROWS = 10000 #how many rows of data are stored in each chunk of the dataframe, 10k rows means each chunk covers 834 hours
    MAX_ROW_NUM:int  = math.ceil((MAX_TIME_FRAME_HOURS * 60)/DATA_TIME_WINDOW_MIN) #max number of rows for the CSV
    
    crypto_token: str
    __latest_unix_time: int #the latest/most recent unix time present in the dataset
    __logger: logging.Logger

    def __init__(self,crypto_token:str,enable_logs:bool = True, )->None:
        if crypto_token not in self.SUPPORTED_CRYPTO_TOKENS:
           raise IOError("Crypo token name not supported")
        else:     
           self.crypto_token = crypto_token + "USDT" #binance api needs USDT to get the token value in USD
    
        self.__latest_unix_time:int = -1
        if enable_logs:
            self.__logger = logging.getLogger(f"crypto_data_etl_{crypto_token}")

    def __create_crypto_dataframe(self, time_frame_hours:int|float,crypto_token: str, end_unix_time:int = 0)-> pd.DataFrame | None: 
        """
        Method that returns a pd Dataframe or None (if no data was able to be returned) with each column being data about price , trading volume
        and net-flow of a given crypto asset

        Args:
          time_frame_hours (int|float): how many hours backwards from the start time should the dataset cover, each column of the df spams 5min

          crypto_token (str) : crypto asset that the data will belong to, needs to be on binance token format (ex. BTCUSDT)

          end_unix_time (int, default 0): time from which the backwards scan in time will start from, if no arg is given it will start from current
          binance server time
        
        """
        if not isinstance(time_frame_hours,int) and not isinstance(time_frame_hours,float):
            raise TypeError("Input time frame isnt an int or float")
        if time_frame_hours > self.MAX_TIME_FRAME_HOURS:
            raise IOError(f"Amount of hours exceeds MAX_TIME_FRAME of {self.MAX_TIME_FRAME_HOURS} hours ({self.MAX_TIME_FRAME_HOURS/24/365} years)")
        
        time_frame_mins:int | float = time_frame_hours * 60
        num_rows:int = math.ceil(time_frame_mins/self.DATA_TIME_WINDOW_MIN) #how many time_window rows will be needed to cover the entire time_frame passed as arg

        columns_list:list[str] = [   
                    "DATE", 
                    f"{crypto_token}_START_PRICE",
                    f"{crypto_token}_END_PRICE",
                    f"{crypto_token}_COINS_BOUGHT",
                    f"{crypto_token}_COINS_SOLD",
                    f"{crypto_token}_NET_FLOW",
                    f"{crypto_token}_TOTAL_AGGRT_TRANSACTIONS"
                                ]
        df = pd.DataFrame(columns = columns_list)
        
        if end_unix_time == 0:
            start_date = datetime.now().replace(microsecond=0)
            cur_unix_time: int = int(time.time() * 1000)
        else:
            start_date = datetime.fromtimestamp(end_unix_time /1000).replace(microsecond=0)
            cur_unix_time = end_unix_time

        for i in range(num_rows): #time window loop, each iteration adds a row to the df
            row_timer:float = time.time()
          
            cur = crypto_token
            api_return_time:float = time.time()
            data:dict | None = binance_trading_volume(
                            time_window_min=self.DATA_TIME_WINDOW_MIN,
                            end_unix_time= cur_unix_time,  # type: ignore
                            crypto_token= cur,
                            api_time_interval_ms= self.TRADE_API_TIME_INTERVAL
                    )
            print(f"it took the time {time.time()- api_return_time} to get a return from api")
            if data == None: #in case the row data is incomplete, just skip that time frame 
                continue
            currency_data:list[float|int|datetime] = [
                            start_date, 
                            data.get(f"{cur}_START_PRICE",None),
                            data.get(f"{cur}_END_PRICE",None),
                            data.get(f"{cur}_COINS_BOUGHT",None),
                            data.get(f"{cur}_COINS_SOLD",None),
                            data.get(f"{cur}_NET_FLOW",None),
                            data.get(f"{cur}_TOTAL_AGGRT_TRANSACTIONS",None)
                        ]
            
            cur_unix_time  -= min_to_ms(self.DATA_TIME_WINDOW_MIN)
            df.loc[i] = currency_data # type: ignore
            start_date -= timedelta(minutes=self.DATA_TIME_WINDOW_MIN)
            print(f"it took the time {time.time()- row_timer} to get a DF row")
        
        if df.shape[0] == 0:
            self.__logger.info(f"Dataframe with end_unix_time of {end_unix_time} wasnt able to be processed")
            return None
        return df       

    def __add_crypto_dataframes(self, newer_data:pd.DataFrame , older_data: pd.DataFrame)->pd.DataFrame:
        """
        Adds 2 Dataframes , one with recent data and other with older data. \n
        If both DFs combined surpass the row limit, the end of the older DF is dropped

        OBS:
            The lower the index the more recent the data is
            Dates on the newer data df need to be more recent than on the older data
        
        Args:
            newer_data(pd.DataFrame) : DF with more recent data 
            older_data(pd.DataFrame) : DF with older data 
        
        Returns -> pd.DataFrame (concatenated DF)
        """
        if not isinstance(newer_data, pd.DataFrame) or not isinstance( older_data, pd.DataFrame):
            raise TypeError("input data isnt a pandas Dataframe")

        newer_date = newer_data.at[0,"DATE"]
        old_date = older_data.at[0,"DATE"]
        if old_date > newer_date:
            raise IOError("most recent date from the older dataframe is more recent than the dates from the newer dataframe")
        
        older_data_row_num:int = older_data.shape[0] 
        newer_data_row_num:int = newer_data.shape[0]

        if older_data_row_num + newer_data_row_num <= self.MAX_ROW_NUM:
            return pd.concat(objs=[newer_data,older_data], axis= 0, ignore_index=True)    
        else:
            rows_to_remove: int = (older_data_row_num + newer_data_row_num) - self.MAX_ROW_NUM   
        
            first_row_to_remove:int = older_data_row_num - rows_to_remove 
            older_data = older_data.drop(index=[i for i in range(first_row_to_remove, older_data_row_num)]) 

            return pd.concat(objs=[newer_data,older_data], axis= 0, ignore_index=True)

    def __seconds_to_unix(self, seconds:float)->int:
        return int(seconds*1000)

    def __get_num_chunks(self,time_frame_hours:float)->int:
        rows_per_chunk:int = self.DATA_CHUNK_NUM_ROWS
        hours_per_chunk:int= math.ceil(rows_per_chunk/12) #12 rows make up 1 hour, since each row = 5min of data
        chunks:int = math.ceil(time_frame_hours/hours_per_chunk)
       
        return chunks

    def __get_data_chunks(self,hours_per_chunk:float,chunks_of_data:int, cur_unix_time:int)->pd.DataFrame:
        """
        This function tries to extract a dataframe of the specified time period by breaking down the extraction into smaller chunks and adding them
        
        """
        unix_time_per_chunk:int = int(hours_per_chunk * 60 * 60 * 1000)  #how many ms (unix time) were covered by the chunk of data
        extracted_chunks:int = 0
        first_data_chunk : bool = True
        df: pd.DataFrame = pd.DataFrame()

        chunk_tries:int = 0
        max_chunk_tries:int = 20
        while extracted_chunks < chunks_of_data:
            chunk_start: float = time.time()
            if chunk_tries > max_chunk_tries:
                self.__logger.exception(f"tried to extract the chunk number {extracted_chunks+1}, {max_chunk_tries} times but none were sucessful, shutting down program")
                raise Exception(f"tried to extract the chunk number {extracted_chunks+1}, {max_chunk_tries} times but none were sucessful, shutting down program")
            
            try:
                    chunk_df: pd.DataFrame | None = self.__create_crypto_dataframe(
                            time_frame_hours=hours_per_chunk,
                            crypto_token=self.crypto_token,
                            end_unix_time = cur_unix_time # type: ignore
                    )
                    if not isinstance(chunk_df, pd.DataFrame):
                        raise ValueError("data-chunk returned is none")
                    
                    if first_data_chunk:
                        df = chunk_df
                        first_data_chunk = False
                    else:
                        df = self.__add_crypto_dataframes(newer_data=df, older_data=chunk_df)
                    
                    extracted_chunks += 1
                    cur_unix_time -= unix_time_per_chunk 
                    chunk_tries = 0
                    print(f"sucessful chunk , it took the following time: {time.time() -  chunk_start }")

            except Exception as e:
                    self.__logger.info(f"tried to extract the chunk number {extracted_chunks+1}, csv currently has {extracted_chunks} chunks ,exception was {e}")
                    self.__logger.exception("failed to get a data chunk, re-trying")
                    chunk_tries+=1
                    time.sleep(1)
        
        return df

    def set_unix_time_from_df(self,df:pd.DataFrame)->None:
        """
        Sets the instance variable (__latest_unix_time) to the latest date that was processed by a dataframe 
        Plus an offset the size of each crypto trade API call timespam (TRADE_API_TIME_INTERVAL) in ms

        Args:
           df (pd.Dataframe) : the existing DF of which we will take the data and find the latest date already processed
        
        """
        
        if not isinstance(df, pd.DataFrame):
            self.__logger.exception("in function set_unix_time_from_df: Input param df isnt of type Pandas Dataframe")
            raise TypeError("in function set_unix_time_from_df: Input param df isnt of type Pandas Dataframe")
        
        if df.shape[0] == 0:
            self.__logger.exception("in function set_unix_time_from_df: Input dataframe is empty")
            raise TypeError("in function set_unix_time_from_df: Input dataframe is empty")
        
        newest_date: datetime = df.at[0, "DATE"]
        self.__latest_unix_time = (int(datetime.timestamp(newest_date)) * 1000) + self.TRADE_API_TIME_INTERVAL

    def create_dataset(self)-> pd.DataFrame:
        """
        Creates and fills an empty CSV dataset with a certain amount of binance data for a certain crypto token

        Args:
                save_df_func (Callable[pd.Dataframe] -> None) : function to save the pandas dataframe in some way,
                either saving locally or writing on some cloud storage
        
        Return -> (int) unix time in ms when the function was called (used to know how behind cur time the dataset is)
                
        
        """

        if self.__latest_unix_time != -1:
            raise Exception("dataset is not empty, please use the update_dataset function to update the existing data")

        MAX_TIME_FRAME_HOURS:int =  3 #self.MAX_TIME_FRAME_HOURS
        
        CHUNKS_OF_DATA:int = self.__get_num_chunks(MAX_TIME_FRAME_HOURS) #in how many data chunks we are going to split the extraction 
        hours_per_chunk: float = MAX_TIME_FRAME_HOURS/CHUNKS_OF_DATA #before 3, after 2.5
        print(f"chunks of data {CHUNKS_OF_DATA}")
        cur_unix_time:int = self.__seconds_to_unix(time.time())
        self.__latest_unix_time = cur_unix_time

        df:pd.DataFrame = self.__get_data_chunks(
                hours_per_chunk = hours_per_chunk,
                chunks_of_data = CHUNKS_OF_DATA,
                cur_unix_time  = cur_unix_time
        )
        
        return df 
        
    def update_dataset(self, df: pd.DataFrame)->pd.DataFrame:
        """
        updates the dataset with data from current time to the latest_unix_time in the dataset, 
        a valid instance var of the latest_unix_time must be set before calling this method
        
        """
        if self.__latest_unix_time == -1:
            raise Exception("last unix time covered by the data is empty, please provide the timestamp for an existing dataset of the use create_dataset func")
        
        if not isinstance(df, pd.DataFrame):
            self.__logger.exception("in function update_dataset:  Input param df isnt of type Pandas Dataframe")
            raise TypeError("Input param df isnt of type Pandas Dataframe")

        cur_unix_time:int = self.__seconds_to_unix(time.time())
        unix_time_dif:int = cur_unix_time - self.__latest_unix_time
    
        time_frame_hours:float = unix_time_dif / 3600000  #unix time in ms to hours 
        num_of_chunks: int = self.__get_num_chunks(time_frame_hours)
        hours_per_chunk:float = time_frame_hours/num_of_chunks
        
        print(hours_per_chunk)
        updated_df: pd.DataFrame = self.__get_data_chunks( #df for the new data
            hours_per_chunk=hours_per_chunk,
            chunks_of_data= num_of_chunks,
            cur_unix_time= cur_unix_time
        )

        end_unix_time:int = self.__seconds_to_unix(time.time())
        self.__latest_unix_time = end_unix_time
        df = self.__add_crypto_dataframes(newer_data=updated_df,older_data=df)

        return df
        

