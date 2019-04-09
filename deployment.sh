#!/bin/bash

###update image
docker build -t 127.0.0.1:5000/xuhshen/stock_mock:latest .

########################################################################
##### config internal network
#docker network create -d bridge stockmock-net #should be run at first time

##### create mount db folder 
#mkdir -p /stockmock/data/db  # should be run at first time

### run db container  
### do not need to run every time
#docker run --name stock_mongo -v /stockmock/data/db:/data/db  --network stockmock-net --restart=always  -d mongo:3.4.19-jessie

############################################################################
#### update mock container  
docker rm -f stock_mock
docker run -d --name stock_mock -e IP=stock_mongo --restart unless-stopped --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/simulation.py


#### update stock mock strategy

docker rm -f stock_strategy
docker run  -d --name stock_strategy -e IP=stock_mongo -e ACCOUNT=stock_mock_acc1  -e RATE=1 --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main.py

#### update 500etf mock strategy
docker rm -f 500ETF_strategy
docker run  -d --name 500ETF_strategy -e IP=stock_mongo -e ACCOUNT=stock_mock_acc2  -e NUMBER=2 --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main_etf.py


#### update real account strategy(hangye zhishu)
#hang ye zhishu 
docker rm -f stock_strategy_35204099
docker run -d --name stock_strategy_35204099 -e IP=stock_mongo -e ACCOUNT=account2 -e RATE=1 -e PRODUCTS=2 -e MOCK=False --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main.py

#500 etf
docker rm -f 500ETF_strategy_35200453 
docker run  -d --name 500ETF_strategy_35200453 -e IP=stock_mongo -e ACCOUNT=account1  -e NUMBER=0.001 -e MOCK=False --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main_etf.py











