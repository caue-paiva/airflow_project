import json , boto3
from io import StringIO
import csv
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

"""
This function is associated with a GET api and receives as a param the token the graphic data will be selected from
and returns an HTML graph (created by Plotly) of the Crypto token value and its trade net-flow (Y axis) and time (X axis)

"""

def create_subplots_from_csv_string(csv_string:str, token :str ,max_rows: int =100):
    token = token.upper()
    dates = []
    token_prices = []
    token_net_flows = []
    
    csvfile = StringIO(csv_string)
    csv_reader = csv.DictReader(csvfile)
    line_count = 0
    for row in csv_reader:
        if line_count == max_rows:
            break
        dates.append(datetime.strptime(row["DATE"], "%Y-%m-%d %H:%M:%S"))
        token_prices.append(float(row[f"{token}USDT_START_PRICE"]))
        token_net_flows.append(float(row[f"{token}USDT_NET_FLOW"]))
        line_count += 1
    
    #make the 2 subplots of the graph, one with Token value x time and another with token trade net-flow x time
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1,
                        subplot_titles=('Token Price Over Time', 'Token Net Flow Over Time'))
    fig.add_trace(go.Scatter(x=dates, y=token_prices, mode='lines+markers', name='Token Price'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=token_net_flows, mode='lines+markers', name='Token Net Flow'), row=2, col=1)
     
    COLORS = ['darkblue', 'lightblue']
    
    #create dotted dark blue lines across both subplots for ease of comparison
    for i, date in enumerate(dates):
        if i % 10 != 0:
            continue 
        color = COLORS[i % len(COLORS)]
        fig.add_vline(x=date, line_width=1, line_dash="dash", line_color=color, row='all')

    return fig

def lambda_handler(event, context):
    
    params: dict = event.get("queryStringParameters", {})
    if not params:
         return  {
            "statusCode" : 500,
            "body": "error",
         }
    
    token: str = params.get("token", "") 
    if not token:
        return  {
            "statusCode" : 500,
            "body": "error",
        }
        
    token = token.upper()
    
    s3_client = boto3.client("s3","us-east-2")
    S3_BUCKET_NAME: str = "airflow-crypto-data"
    CSV_FILE: str = f"{token}_DATA.csv"
    
    
    csv_file= s3_client.get_object(
        Bucket = S3_BUCKET_NAME,
        Key = CSV_FILE
    )
    csv_str:str  =   csv_file["Body"].read().decode("UTF-8") #string representing the CSV file
    
    fig =  create_subplots_from_csv_string(csv_str, token)
    
    fig.update_layout(
        title_text='Token Price and Net Flow Over Time',
        xaxis_title='Date',
        yaxis_title='Price',
        xaxis2_title='Date',
        yaxis2_title='Net Flow',
        height=1000,  
        showlegend=False
    )

    html = fig.to_html()
    
    return  {
        "statusCode" : 200,
        "body": html,
         "headers": {
         'Content-Type': 'text/html',
        }
    }
