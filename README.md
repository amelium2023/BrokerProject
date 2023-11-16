# Interactive Brokers Python app

To run this application you need:

1. create .env file in the root folder of the project with variables from .env.example and replace the following environments variables : `TWS_USERID`, `TWS_PASSWORD`, these values are password and login from your TW account. Also use `TRADING_MODE=paper` for the env file, as this app takes stocks from the testing environment 
2. then in the root directory run `docker-compose up` command to start python app container and ib-gateway container
3. wait few minutes for gateway connecting and then if you have subscription for the API you will get apple market data

