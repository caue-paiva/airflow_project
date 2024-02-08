from airflow.decorators import dag , task
from airflow.models import Variable
from pendulum import datetime
from include.crypto_data_etl import CryptoDataETL
import pandas as pd 
import os , json

CUR_DIR_PATH: str = os.getcwd()  #for some reason when airflow executes this returns the value to the folder containing the airflow project
print(f"cur dir path {CUR_DIR_PATH}")
CSV_FILE_PATH: str =  os.path.join(CUR_DIR_PATH, "dataset" ,"placeholder.csv")
DATASET_METADATA_PATH: str = os.path.join(CUR_DIR_PATH , "dataset" ,Variable.get("DATASET_METADATA_NAME"))

def csv_from_local_files(file_path:str)->pd.DataFrame:
    return pd.read_csv(file_path)

def update_dataset_metadata(crypto_token:str,df: pd.DataFrame )->None:
    print("updating metadata")
    with open(DATASET_METADATA_PATH, "r") as f:
           metadata:list[dict] = json.load(f)
    
    num_cols:int = df.shape[0]
    for in_token in metadata:
        if in_token.get("crypto_token") == crypto_token:
            in_token["dataset_exists"] = True
            in_token["number_of_rows"] = num_cols
            with open(DATASET_METADATA_PATH, "w") as f:
               json.dump(metadata,f, indent= 4)
            return 
           
    raise Exception(f"Was not able to find the correct token {crypto_token} in the dataset metadata")
          

#def csv_from_s3()->pd.DataFrame: 
  #  pass

@dag(
     start_date = datetime(2024,1,1),
     schedule =  "@daily",
     tags = ["crypto_data"],
     catchup = False,
)


def crypto_data_etl()->None:
    TOKEN = "BTC"
    etl = CryptoDataETL(crypto_token = TOKEN)

    @task(task_id = "check_dataset_num_rows")
    def check_dataset_num_rows()->int:
        with open(DATASET_METADATA_PATH, "r") as f:
            metadata:list[dict] = json.load(f)
            for in_token in metadata:
                if in_token.get("crypto_token") == TOKEN:
                    if not in_token.get("dataset_exists"):
                        return -1
                    
                    num_rows: int| None = in_token.get("number_of_rows", None)
                    if num_rows == None:
                        raise Exception("Was not possible to find the number of rows")
                    
                    return num_rows
           
            raise Exception(f"Was not able to find the correct token {TOKEN} in the dataset metadata")
   
    @task.branch(task_id = "branch_on_dataset_size")
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
        df: pd.DataFrame = csv_from_local_files(CSV_FILE_PATH)
        return etl.update_dataset(df)
    
    @task(task_id = "write_df_to_file")
    def write_df_to_file(df: pd.DataFrame)->None:
        df.to_csv(CSV_FILE_PATH, index= False)
        update_dataset_metadata(TOKEN,df)
    
    dataset_rows = check_dataset_num_rows()
    path_branch = branch_on_dataset_size(dataset_rows) # type: ignore
    create_dataset = get_first_dataset()
    update_dataset = fill_existing_dataset()
    write_from_new = write_df_to_file(create_dataset)  # type: ignore
    write_from_existing = write_df_to_file(update_dataset) # type: ignore

    path_branch >> create_dataset  >> write_from_new
    path_branch >> update_dataset >> write_from_existing
    

crypto_data_etl()