import json , boto3 , csv 
from io import StringIO

"""
Code used on an AWS lambda function to return a semi-static HTML page with a preview of the dataset of certain crypto tokens
as well as general stats about that dataset
"""

def csv_string_to_html_table(csv_string:str, max_rows:int = 300)->str:
    csvfile = StringIO(csv_string)
    reader = csv.reader(csvfile)
    headers = next(reader)

    table = "<table>"
    table += '<tr>' + ''.join([f'<th>{header}</th>' for header in headers]) + '</tr>'
    row_count = 0
    for row in reader:
        table += '<tr>' + ''.join([f'<td>{cell}</td>' for cell in row]) + '</tr>'
        row_count += 1
        if row_count >= max_rows:
            break
    table += '</table>'
    return table
    
def lambda_handler(event, context): 
    
    s3_client = boto3.client("s3","") #region and bucket name ommited from public repo
    S3_BUCKET_NAME: str = ""
    CSV_FILE_NAME: str = "BTC_DATA.csv"
    METADATA_FILE_NAME: str = "dataset_metadata.json"
    LOCAL_HTML_FILE_PATH: str = "html_template.html"
    
    s3_csv= s3_client.get_object(
        Bucket = S3_BUCKET_NAME,
        Key = CSV_FILE_NAME
    )
    csv_str:str  =  s3_csv["Body"].read().decode("UTF-8") #string representing the CSV file
    
    
    s3_metadata = s3_client.get_object(
        Bucket = S3_BUCKET_NAME,
        Key = METADATA_FILE_NAME
    )
    
    metadata = s3_metadata["Body"].read().decode("UTF-8")
    metadata: list[dict] = json.loads(metadata)
    
    token_info_dict: dict = {}
    for token in metadata:
        token_name = token.get("crypto_token")
        token_info_dict[token_name] = {
              "dataset_exists": token.get("dataset_exists"),
              "number_of_rows": token.get("number_of_rows"),
              "most_recent_data": token.get("most_recent_data"),
              "oldest_data": token.get("oldest_data"),
              "total_hours_covered": token.get("total_hours_covered")
    }
    
    html_template : str
    try:
        with open(LOCAL_HTML_FILE_PATH, "r") as f:
            html_template = f.read()
    except Exception as e:
        raise Exception(e)
    
    df_html1: str = csv_string_to_html_table(csv_str)

    html_template = html_template.format(
        DF_BTC = df_html1,
        DF_ETH = "",
        btc_total_rows = token_info_dict.get("BTC").get("number_of_rows") ,
        btc_total_hours = token_info_dict.get("BTC").get("total_hours_covered" ),
        btc_latest_date = token_info_dict.get("BTC").get("most_recent_data"),
        btc_oldest_date = token_info_dict.get("BTC").get("oldest_data"),
        eth_total_rows =  token_info_dict.get("ETH").get("number_of_rows"),
        eth_total_hours = token_info_dict.get("ETH").get("total_hours_covered"),
        eth_latest_date = token_info_dict.get("ETH").get("most_recent_data"),
        eth_oldest_date = token_info_dict.get("ETH").get("oldest_data"),
    )
    
    return  {
        "statusCode" : 200,
        "body": html_template,
         "headers": {
         'Content-Type': 'text/html',
        }
    }
    
   
   
   
   

