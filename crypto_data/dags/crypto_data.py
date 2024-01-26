from airflow.decorators import dag , task
from airflow.models import Variable
from pendulum import datetime
from include.crypto_data_etl import CryptoDataETL
import pandas as pd 
import os

@dag(
     start_date = datetime(2024,1,1),
     schedule =  "@daily",
     tags = ["crypto_data"],
     catchup = False,
)

def crypto_data_etl()->None:
    etl = CryptoDataETL(crypto_token="BTC")

    @task
    def get_first_dataset()->pd.DataFrame:
        return etl.create_dataset()
    
    @task
    def write_df_to_file(df: pd.DataFrame)->None:
        file_path: str = os.path.join(os.getcwd() ,Variable.get("df_file_name"))
        df.to_csv(file_path, index= False)
    
    df = get_first_dataset()
    write_df_to_file(df)
    

crypto_data_etl()