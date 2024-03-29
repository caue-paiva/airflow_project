from airflow.decorators import dag , task
from airflow.models import Variable
from pendulum import datetime
from include.crypto_data_etl import CryptoDataETL
import pandas as pd 
import os , json
from airflow.io.path import ObjectStoragePath
from typing import  Optional

"""
Main DAG for the Crypto Data ETL pipeline, it creates an instance of the CryptoDataETL class and uses it to create/update 
Pandas dataframes extracted and saved in S3 buckets.



Stats for extraction speed:

Rows per min: ~187 
"""
# Variable.get() retrieves airflow env variables set in the Airflow UI and is used to make the project more modular

SUPPORTED_TOKENS: set[str] = {"BTC", "SOL", "ETH"}
DATASET_METADATA_FILENAME = Variable.get("DATASET_METADATA_NAME")
CUR_DIR_PATH: str = os.getcwd()  #for some reason when airflow executes this returns the value to the folder containing the airflow project
LOCAL_METADATA_PATH: str = os.path.join(CUR_DIR_PATH , DATASET_METADATA_FILENAME)
S3_BUCKET =  ObjectStoragePath("s3://airflow-crypto-data", conn_id="aws_default")
MIN_PER_ROW: int = int(Variable.get("MINS_PER_ROW"))
S3_DATASET_NAME: str = Variable.get("S3_DATASET_NAME")

def save_metadata_locally(metadata_json:list[dict])->bool:
    with open(LOCAL_METADATA_PATH, "w") as f:
        json.dump(metadata_json, f, indent=4)
    return True

@dag(
     start_date = datetime(2024,1,1), #dag start date
     schedule =  "@daily", #schedule between automatic dag runs
     tags = ["crypto_data"], #tags to identify the dag
     catchup = False, # catchup = True will make your dags execute automatically to make up for any missed runs, better to leave this option as False to avoid problems
)                     
def crypto_data_etl()->None:
    TOKEN = "BTC" 
    etl = CryptoDataETL( #instantiating ETL class 
        crypto_token = TOKEN,
        max_time_frame_hours=  float(Variable.get("MAX_TIME_FRAME_HOURS")),
        hours_between_daily_updates= int(Variable.get("HOURS_BETWEEN_DAILY_UPDATES")),
        mins_per_row=  MIN_PER_ROW,
        max_batch_size_hours= float(Variable.get("MAX_BATCH_SIZE_HOURS"))
    )

    @task(task_id = "check_dataset_num_rows")
    def check_dataset_num_rows()-> int:  #add a fallback in case the metadata doesnt exist
        """
        Checks the num of rows of the dataset or if it exists or not
        Used for a branching decision later in the DAG
        """
        path = S3_BUCKET/ DATASET_METADATA_FILENAME

        try: #try opening S3 bucket metadata json file
            with path.open("r") as f:
                metadata:list[dict] = json.load(f)
                for in_token in metadata:
                    if in_token.get("crypto_token") == TOKEN: #if we find the correct token
                        if not in_token.get("dataset_exists"): #if dataset doesnt exist already return -1 rows
                            save_metadata_locally(metadata) #save s3 file locally for ease of acess from other funcs
                            print("found s3 bucket but metadata said there was no dataset")
                            return -1
                        num_rows: Optional[int] = in_token.get("number_of_rows", None) #get the number of rows
                        if num_rows == None:
                            raise Exception("Was not possible to find the number of rows") #if the number of rows info doesnt exist, raise except
                        save_metadata_locally(metadata)
                        print("opened s3 bucket and found data")
                        return num_rows
                raise Exception("Didnt find the correct token in the JSON file")
        except: 
             #in case the s3 doesnt have a metadata file, we need to create one locally  with all values set to zero
             with open(LOCAL_METADATA_PATH, "w") as f:
                initial_template = [
                                     {
                                        "crypto_token": f"{x}",
                                        "dataset_exists": False, 
                                        "number_of_rows": 0,
                                        "most_recent_data": "",
                                        "oldest_data": "",
                                        "total_hours_covered": 0
                                      } for x in SUPPORTED_TOKENS
                                   ]
                json.dump(initial_template, f)
             print("didnt find metadata in s3 bucket")
             return -1 
       
    @task.branch(task_id = "branch_on_dataset_size") #branches the DAG based the dataset existing or not
    def branch_on_dataset_size(dataset_rows: int)-> str:
        if dataset_rows <= 0: 
            return "get_first_dataset"
        else:
            return "fill_existing_dataset"
        
    @task(task_id = "get_first_dataset")
    def get_first_dataset()->pd.DataFrame:
        return etl.create_dataset()
    
    @task(task_id= "fill_existing_dataset")
    def fill_existing_dataset()->pd.DataFrame:
        csv_path = S3_BUCKET / f"{TOKEN}{S3_DATASET_NAME}"
        with csv_path.open("rb") as f: #reads existing csv to a df
            df = pd.read_csv(f)

        return etl.update_dataset(df) #updates and concats the older df
    
    @task(task_id = "write_df_to_file") #ds is a dag parameter for current date
    def write_df_to_file(df:pd.DataFrame)-> tuple[ObjectStoragePath, ObjectStoragePath]:
        csv_path = S3_BUCKET / f"{TOKEN}{S3_DATASET_NAME}"
        metadata_path = S3_BUCKET/ DATASET_METADATA_FILENAME

        num_rows:int = df.shape[0] #get number of rows in dataframe
        most_recent_data: str = str(df.at[0,"DATE"])
        oldest_data: str = str(df.at[ (num_rows-1) ,"DATE"])
        hours_covered: float = (num_rows * MIN_PER_ROW)/60
        
        with csv_path.open("wb") as f:
            df.to_csv(f,index=False) #saves CSV on S3
        
        with open(LOCAL_METADATA_PATH, "r") as f: #opens local metadata json file  
            metadata:list[dict] = json.load(f)
            for in_token in metadata:
                if in_token.get("crypto_token") == TOKEN: #if we find the correct token
                       in_token["dataset_exists"] = True #change the list of dicts from the json with the new row num
                       in_token["number_of_rows"] = num_rows
                       in_token["most_recent_data"] = most_recent_data
                       in_token["oldest_data"] =  oldest_data
                       in_token["total_hours_covered"] = hours_covered
                       break
            else:
                raise Exception("Didnt find the correct token in the JSON file")

        with metadata_path.open("w") as f: #writes the json file to S3
            json.dump(metadata,f, indent=4)
               
        return csv_path, metadata_path #returns a tuple of 2 ObjectStoragePath
    
    dataset_rows = check_dataset_num_rows() #dataset_rows is an Xcom arg that holds the return val of the func
    path_branch = branch_on_dataset_size(dataset_rows) # type: ignore using the rows parameters on the branch function to return the path                 
    create_dataset = get_first_dataset()  #dataset funcs return an Xcom arg for the Dataset
    update_dataset = fill_existing_dataset() 
    write_from_new = write_df_to_file(create_dataset)  # type: ignore  write functions return a path for the cloud storage for the file write
    write_from_existing = write_df_to_file(update_dataset) # type: ignore  

    path_branch >> create_dataset  >> write_from_new #need to set-up dependencies  for both branches
    path_branch >> update_dataset >> write_from_existing
    
crypto_data_etl()


"""
Testing: 

1) pipeline works when no metadata json is found (create dataset from 0 and uploads json to s3)


"""