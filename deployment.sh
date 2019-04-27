#!/bin/bash

##git tag -a $1 -m "my version $1"

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
############################################################################
#### update mock container  

docker rm -f stock_mock
docker run -d --name stock_mock -e IP=stock_mongo --restart unless-stopped --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/simulation.py

#### update hang ye weight container
docker rm -f stock_hy_weight_update
docker run -d --name stock_hy_weight_update -e IP=stock_mongo --restart unless-stopped --network stockmock-net 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/updateweight.py


############################################################################
#### update stock mock strategy

docker rm -f stock_strategy
docker run  -d --name stock_strategy -e IP=stock_mongo -e ACCOUNT=stock_mock_acc1  -e RATE=1 -e PRODUCTS=1 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main.py

############################################################################
#### update 500etf mock strategy
docker rm -f 500ETF_strategy
docker run  -d --name 500ETF_strategy -e IP=stock_mongo -e ACCOUNT=stock_mock_acc2  -e NUMBER=2 --network stockmock-net  --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main_etf.py

############################################################################
############################################################################
#### update real account strategy(hangye zhishu)
#hang ye zhishu 
docker rm -f stock_strategy_35204099
docker run -d --name stock_strategy_35204099 -e IP=stock_mongo -e ACCOUNT=account2 -e RATE=1 -e PRODUCTS=2 -e MOCK=False --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main.py

############################################################################
###500 etf
docker rm -f 500ETF_strategy_35200453 
docker run  -d --name 500ETF_strategy_35200453 -e IP=stock_mongo -e ACCOUNT=account1  -e NUMBER=0.001 -e MOCK=False --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main_etf.py

docker rm -f 500ETF_strategy_35204819 
docker run  -d --name 500ETF_strategy_35204819 -e IP=stock_mongo -e ACCOUNT=account3  -e NUMBER=0.001 -e MOCK=False --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main_etf.py

docker rm -f 500ETF_strategy_624006928
docker run  -d --name 500ETF_strategy_624006928 -e IP=stock_mongo -e ACCOUNT=account5  -e NUMBER=0.001 -e MOCK=False --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/main_etf.py


##########################################################################################################################
###ni hui gou

docker rm -f nhg_35200453 
docker run  -d --name nhg_35200453  -e ACCOUNT=account1  -e SERVER=http://192.168.0.100:65000 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/nhg.py

docker rm -f nhg_35204819 
docker run  -d --name nhg_35204819  -e ACCOUNT=account3  -e SERVER=http://192.168.0.100:65000 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/nhg.py

docker rm -f nhg_35204099 
docker run  -d --name nhg_35204099  -e ACCOUNT=account2  -e SERVER=http://192.168.0.100:65000 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/nhg.py

docker rm -f nhg_624006928
docker run  -d --name nhg_624006928  -e ACCOUNT=account5  -e SERVER=http://192.168.0.100:65000 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/nhg.py




docker rm -f nhg_51307088
docker run  -d --name nhg_51307088  -e ACCOUNT=account4  -e SERVER=http://192.168.0.100:5000 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/nhg.py

docker rm -f nhg_101700000217
docker run  -d --name nhg_101700000217  -e ACCOUNT=account5  -e SERVER=http://192.168.0.100:5000 --network stockmock-net --restart unless-stopped 127.0.0.1:5000/xuhshen/stock_mock:latest python /home/nhg.py



