## Crypto Data ETL project with Airflow and AWS 

:us:

This project aims to **collect and update data on Crypto Tokens like like Bitcoin, Ethereum** and in the future Solana in the form of CSV files covering large periods of trading data.

Data collected includes: 

* Token price,

* Tokens Bought and sold

* Num. of aggregate transactions (transactions of the same token and same price are aggregated together)

* Net flow for the asset in a period

All that in slices of 5 minutes of data each.


Data is collected from the [Binance US API](https://docs.binance.us/#introduction)


# Acessing the data 

* Dataset statistics (preview of the CSV and info like how many rows are there in file and how many hours are covered) can be acessed [here](https://jr6cd1g42j.execute-api.us-east-2.amazonaws.com/stage1/dashboard)

* Bitcoin dataset can be downloaded [here](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=btc)

* Ethereum dataset can be downloaded [here](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=eth)

All data and metadata is stored on **AWS S3** and links lead to a temporary URL where you can download certain files


# Architecture of the application 

![a](architecture_diagram.png)