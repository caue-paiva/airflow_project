## Crypto Data ETL project with Airflow and AWS 

:us:

This project aims to collect and update data on Crypto Tokens like like Bitcoin, Ethereum and in the future Solana

Data collected includes: 

* Token price,

* Tokens Bought and sold

* Num. of aggregate transactions (transactions of the same token and same price are aggregated together)

* Net flow for the asset in a period

All that in slices of 5 minutes of data each.


Data is collected from the [Binance US API](https://docs.binance.us/#introduction)


test [link](https://czmejpeff7.execute-api.us-east-2.amazonaws.com/geturl?token=btc)