import logging.config,time,math
import pandas as pd
from .binance_api import binance_trading_volume , min_to_ms
from datetime import datetime , timedelta
from airflow.models import Variable


"""
1 hour of processing can get around 104 hours of data

max time back of 2,75 years should take around 230 hours to complete (almost 10 days)

max time of 1.5 years should take 126 hours to complete (5.2 days)
"""

class CryptoDataETL():

    #rewrite this class to be independent of airflow env variables
    
    SUPPORTED_CRYPTO_TOKENS:set[str] = {"BTC", "ETH", "SOL"}
    #var below could be init parameter
    MAX_TIME_FRAME_HOURS = float(Variable.get("MAX_TIME_FRAME_HOURS"))   #how far back will data be collected in hours, equivalent to 2,75 years 
     #var below could be init parameter
    MINS_PER_ROW = int(Variable.get("MINS_PER_ROW")) #how many minutes of data does each row represent
    #var below could be init parameter
    TRADE_API_TIME_INTERVAL = int(Variable.get("TRADE_TIME_INTERVAL")) #in ms, the time window for getting aggregated transaction data
    #can be hardcoded i think
    DATA_CHUNK_NUM_ROWS = int(Variable.get("DATA_CHUNK_NUM_ROWS")) #how many rows of data are stored in each chunk of the dataframe, 10k rows means each chunk covers 834 hours
    MAX_ROW_NUM:int  = math.ceil((MAX_TIME_FRAME_HOURS * 60)/MINS_PER_ROW) #max number of rows for the CSV
    #var below could be init parameter
    HOURS_BETWEEN_DAILY_UPDATES: int = int(Variable.get("HOURS_BETWEEN_DAILY_UPDATES"))  #how many hours are there to be between each daily uppdate of the dataset by the airflow scheduler

    crypto_token: str
    __logger: logging.Logger
    #remove logging from init, as airflow already has logs and this kinda makes the class less flexible
    def __init__(self,crypto_token:str,enable_logs:bool = True)->None:
        if crypto_token not in self.SUPPORTED_CRYPTO_TOKENS:
           raise IOError("Crypo token name not supported")
        else:     
           self.crypto_token = crypto_token + "USDT" #binance api needs USDT to get the token value in USD
    
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
        num_rows:int = math.ceil(time_frame_mins/self.MINS_PER_ROW) #how many time_window rows will be needed to cover the entire time_frame passed as arg

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
                            time_window_min=self.MINS_PER_ROW,
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
            
            cur_unix_time  -= min_to_ms(self.MINS_PER_ROW)
            df.loc[i] = currency_data # type: ignore
            start_date -= timedelta(minutes=self.MINS_PER_ROW)
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
       
        if not isinstance( newer_date, pd.Timestamp):
             print("newer date isnt instance")
             newer_date = pd.to_datetime(newer_date)

        if not isinstance( old_date, pd.Timestamp):
             print("older date isnt instance")
             old_date = pd.to_datetime(old_date)
             
        print(type(newer_date), newer_date)
        print(type(old_date), old_date)

        if old_date > newer_date:
            raise IOError("most recent date from the older dataframe is more recent than the dates from the newer dataframe")
        
        older_data_row_num:int = older_data.shape[0] 
        newer_data_row_num:int = newer_data.shape[0]

        if older_data_row_num + newer_data_row_num <= self.MAX_ROW_NUM:
            new_df =  pd.concat(objs=[newer_data,older_data], axis= 0, ignore_index=True)    
            new_df["DATE"] = pd.to_datetime(new_df["DATE"])
            return new_df
        else:
            rows_to_remove: int = (older_data_row_num + newer_data_row_num) - self.MAX_ROW_NUM   
        
            first_row_to_remove:int = older_data_row_num - rows_to_remove 
            older_data = older_data.drop(index=[i for i in range(first_row_to_remove, older_data_row_num)]) 

            new_df =  pd.concat(objs=[newer_data,older_data], axis= 0, ignore_index=True)    
            new_df["DATE"] = pd.to_datetime(new_df["DATE"])
            return new_df

    def __seconds_to_unix(self, seconds:float)->int:
        return int(seconds*1000)

    def __get_num_chunks(self,time_frame_hours:float)->int:
        rows_per_chunk:int = self.DATA_CHUNK_NUM_ROWS
        hours_per_chunk:int= math.ceil(rows_per_chunk/12) #12 rows make up 1 hour, since each row = 5min of data
        chunks:int = math.ceil(time_frame_hours/hours_per_chunk)
        if chunks <= 0:
            chunks = 1
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

    def __get_df_missing_hours(self,df: pd.DataFrame)->float:
        """
        Given a df, this function calculates how many hours of data are missing between the max amount of hours 
        supposed to be in the dataset (set by an airflow env var) and the current amount of hours covered by the df rows.

        Args:
            df (pd.DataFrame): Pandas df representing the dataset

        Return -> (float) how many hours of data the df has if compared to the max supposed hours in the dataset
        """
        if not isinstance(df, pd.DataFrame):
            self.__logger.exception("in function __get_df_missing_hours: Input param df isnt of type Pandas Dataframe")
            raise TypeError("in function __get_df_missing_hours: Input param df isnt of type Pandas Dataframe")
        
        if df.shape[0] == 0:
            self.__logger.exception("in function __get_df_missing_hours: Input dataframe is empty")
            raise TypeError("in function __get_df_missing_hours: Input dataframe is empty")
        
        num_rows:int = df.shape[0]
        mins_per_row: int = self.MINS_PER_ROW
        total_hours_covered: float = (num_rows * mins_per_row)/60

        return self.MAX_TIME_FRAME_HOURS - total_hours_covered
        
    def create_dataset(self)-> pd.DataFrame:
        """
        Creates and fills an empty CSV dataset with a certain amount of binance data for a certain crypto token.
        Arguments for the amount of data are Airflow env varibles such as "MAX_TIME_FRAME_HOURS"

        Return -> (pd.DataFrame) Dataset filled with the amount of data for the period specified in the env variable
                
        """

        CHUNKS_OF_DATA:int = self.__get_num_chunks(self.MAX_TIME_FRAME_HOURS) #in how many data chunks we are going to split the extraction 
        hours_per_chunk: float = self.MAX_TIME_FRAME_HOURS/CHUNKS_OF_DATA #how many hours of data are covered by each chunk
        print(f"chunks of data {CHUNKS_OF_DATA}")
        cur_unix_time:int = self.__seconds_to_unix(time.time())

        df:pd.DataFrame = self.__get_data_chunks(  #fills the data will all the hours in the specified timeframe
                hours_per_chunk = hours_per_chunk,
                chunks_of_data = CHUNKS_OF_DATA,
                cur_unix_time  = cur_unix_time
        )
        
        return df 
          
    def update_dataset(self, df: pd.DataFrame)->pd.DataFrame:
        """
        This function can do either:
        1)    Updates an input dataframe with the remaining hours of data necessary to reach the max hours covered by the 
              dataset, set by an airflow env variable "MAX_TIME_FRAME_HOURS", or updates it according to daily updates to renew the data
        
        2)    Case the dataset already covers the max amount of hours, this function will update the dataset with new data daily,
              the amount of hours covered by this new update is set in the airflow env var "HOURS_BETWEEN_DAILY_UPDATES"      
        
        Args:
            df (pd.Dataframe): existing dataset 

        Return -> (pd.Dataframe) the updated dataset now covering the max amount of hours
        """
        
        if not isinstance(df, pd.DataFrame):
            self.__logger.exception("in function update_dataset:  Input param df isnt of type Pandas Dataframe")
            raise TypeError("Input param df isnt of type Pandas Dataframe")

        df_missing_hours:float = self.__get_df_missing_hours(df) #how many hours are missing from the df if compared to the max hours the dataset is supposed to cover
        
        
        cur_unix_time:int = self.__seconds_to_unix(time.time())
        
        if df_missing_hours <= self.HOURS_BETWEEN_DAILY_UPDATES : #in case the amount of missing hours is less than covered in a daily update, we will do a daily update
            df_missing_hours: float = min(self.HOURS_BETWEEN_DAILY_UPDATES,self.MAX_TIME_FRAME_HOURS )

        
        
        print(f" //// df missing hours {df_missing_hours} \n")
        num_of_chunks: int = self.__get_num_chunks(df_missing_hours)
        hours_per_chunk:float = df_missing_hours/num_of_chunks
            
        print(hours_per_chunk)
        updated_df: pd.DataFrame = self.__get_data_chunks( #df for the new data
                hours_per_chunk=hours_per_chunk,
                chunks_of_data= num_of_chunks,
                cur_unix_time= cur_unix_time
        )
        
        df = self.__add_crypto_dataframes(newer_data=updated_df,older_data=df)
        df.to_csv("/home/kap/airflow_test/debug.csv")
        return df















"""MAX_TIME_FRAME_HOURS = 3   #how far back will data be collected in hours, equivalent to 2,75 years 
    MINS_PER_ROW = 5 #how many minutes of data does each column represent
    TRADE_API_TIME_INTERVAL = 100000 #in ms, the time window for getting aggregated transaction data
    DATA_CHUNK_NUM_ROWS = 10000 #how many rows of data are stored in each chunk of the dataframe, 10k rows means each chunk covers 834 hours
    MAX_ROW_NUM:int  = math.ceil((MAX_TIME_FRAME_HOURS * 60)/MINS_PER_ROW) #max number of rows for the CSV
"""
        

