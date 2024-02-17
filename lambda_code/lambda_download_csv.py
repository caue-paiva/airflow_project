import json , boto3

"""
Lambda code that returns an S3 file download URL and redirects users 
to it so they can download the dataset CSV file of the assocated token.

"""

def return_error(err_msg:str)->dict:
    return {
        'statusCode': 500, 
        'body': err_msg
    }

def lambda_handler(event, context):
    
    params: dict = event.get("queryStringParameters", {})
    if not params:
       return return_error("was not able to get query string params")
    
    token: str = params.get("token", "") 
    if not token:
       return return_error("was not able to get token from query string")
        
    token = token.upper()
        
    S3_CLIENT = boto3.client("s3","") #region and bucket name ommited from public repo
    S3_BUCKET_NAME: str = ""
    URL_EXPIRATION_SEC:int = 60  # url expires in 60 seconds, more than enough for redirecting for downloading of the data
    
    file_name = f"{token}_DATA.csv" #get filename based on the crypto token requested
    
    csv_url = S3_CLIENT.generate_presigned_url( #generates URL that can be used to download the file 
       'get_object',
        Params = {'Bucket': S3_BUCKET_NAME, 'Key': file_name},
        ExpiresIn = URL_EXPIRATION_SEC
    )
    
    return {
        'statusCode': 302,  # Status 302 indicates a redirect to the file download URL
        'headers': {
            'Location': csv_url
        },
        'body': json.dumps(f"redirect to download file: {csv_url}")
    }
