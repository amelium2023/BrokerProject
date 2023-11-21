from IB_Implementation import *


symbol = "AAPL"
app.tickerData.append([symbol,0.0])
#T myDB = DB_Connect()
myDB = None
Init_Logger("BrokerTest")
IB_Start(myDB,False)

#Submit_to_Broker(123,symbol,'123',6,True)
while True:
	Req_Last_Market_Price(0,symbol)
	time.sleep(10)
IB_End()
