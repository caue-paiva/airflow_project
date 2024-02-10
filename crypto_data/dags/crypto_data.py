from airflow.decorators import dag , task
from airflow.models import Variable
from pendulum import datetime
from include.crypto_data_etl import CryptoDataETL
import pandas as pd 
import os , json
from airflow.io.path import ObjectStoragePath

CUR_DIR_PATH: str = os.getcwd()  #for some reason when airflow executes this returns the value to the folder containing the airflow project
LOCAL_METADATA_PATH: str = os.path.join(CUR_DIR_PATH , "dataset" ,Variable.get("DATASET_METADATA_NAME"))
S3_BUCKET =  ObjectStoragePath("s3://airflow-crypto-data", conn_id="aws_default")
DATASET_METADATA_FILENAME = "dataset_metadata.json"

def save_metadata_locally(metadata_json:list[dict])->bool:
    with open(LOCAL_METADATA_PATH, "w") as f:
        json.dump(metadata_json, f, indent=4)
    return True

@dag(
     start_date = datetime(2024,1,1),
     schedule =  "@daily",
     tags = ["crypto_data"],
     catchup = False,
)

def crypto_data_etl()->None:
    TOKEN = "BTC" #find a want to enable multiple tokens maybe?
    etl = CryptoDataETL(crypto_token = TOKEN)

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
                            return -1
                    
                        num_rows: int| None = in_token.get("number_of_rows", None) #get the number of rows
                        if num_rows == None:
                            raise Exception("Was not possible to find the number of rows") #if the number of rows info doesnt exist, raise except
                        save_metadata_locally(metadata)
                        return num_rows
                raise Exception("Didnt find the correct token in the JSON file")
        except: 
             #in case the s3 doesnt have a metadata file, we need to create one locally  with all values set to zero
             with open(LOCAL_METADATA_PATH, "w") as f:
                initial_template = [
                                     {"crypto_token": "BTC","dataset_exists": False, "number_of_rows": 0},                                        
                                     {"crypto_token": "ETH", "dataset_exists": False,"number_of_rows": 0},                                       
                                     {"crypto_token": "SOL", "dataset_exists": False, "number_of_rows": 0}
                                   ]
                json.dump(initial_template, f)
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
    def fill_existing_dataset(ds = None)->pd.DataFrame:
        csv_path = S3_BUCKET / f"{TOKEN}_DATA_{ds}.csv"
        with csv_path.open("rb") as f: #reads existing csv to a df
            df = pd.read_csv(f)

        return etl.update_dataset(df) #updates and concats the older df
    
    @task(task_id = "write_df_to_file") #ds is a dag parameter for current date
    def write_df_to_file(df:pd.DataFrame,ds = None)-> tuple[ObjectStoragePath, ObjectStoragePath]:
        csv_path = S3_BUCKET / f"{TOKEN}_DATA_{ds}.csv"
        metadata_path = S3_BUCKET/ DATASET_METADATA_FILENAME

        num_rows:int = df.shape[0] #get number of rows in dataframe
        
        with csv_path.open("wb") as f:
            df.to_csv(f,index=False) #saves CSV on S3
        
        with open(LOCAL_METADATA_PATH, "r") as f: #opens local metadata json file  
            metadata:list[dict] = json.load(f)
            for in_token in metadata:
                if in_token.get("crypto_token") == TOKEN: #if we find the correct token
                       in_token["dataset_exists"] = True #change the list of dicts from the json with the new row num
                       in_token["number_of_rows"] = num_rows
                       break
            else:
                raise Exception("Didnt find the correct token in the JSON file")

        with metadata_path.open("w") as f: #writes the json file to S#
            json.dump(metadata,f, indent=4)
               
        return csv_path, metadata_path #returns a tuple of 2 ObjectStoragePath
    
    dataset_rows = check_dataset_num_rows()
    path_branch = branch_on_dataset_size(dataset_rows) # type: ignore
    create_dataset = get_first_dataset()
    update_dataset = fill_existing_dataset()
    write_from_new = write_df_to_file(create_dataset)  # type: ignore
    write_from_existing = write_df_to_file(update_dataset) # type: ignore

    path_branch >> create_dataset  >> write_from_new
    path_branch >> update_dataset >> write_from_existing
    

crypto_data_etl()


"""@task(task_id = "check_dataset_num_rows")
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
         @task(task_id = "write_df_to_file")
         def write_df_to_file(df: pd.DataFrame)->None:
        df.to_csv(CSV_FILE_PATH, index= False)
        update_dataset_metadata(TOKEN,df)     
            
            
            
            
            
            
            
            """
