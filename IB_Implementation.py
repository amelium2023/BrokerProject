from Wrappers import *
from ibapi.client import EClient
from ibapi.wrapper import EWrapper 
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.wrapper import BarData
from datetime  import datetime, timedelta
import time
import threading


my_broker_uuid = "2f1135db-4b15-11ee-9a18-02cb462e59f2"
my_sqs_orders_name = "OrdersIB.fifo"

def run_loop():
	app.run()


class IBapi(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self) 
            self.tickerData = []
            self.dbconnection = None
            self.commissions = {}
            self.nextorderId = 1
          
        def historicalData(self, reqId:int, bar: BarData):
                #print("HistoricalData. ReqId:", reqId, "BarData.", bar)
                if len(self.tickerData)>reqId:
                    #set the new price
                    self.tickerData[reqId][1]=bar.close

        def	historicalDataEnd (self,reqId, start, end):
            if len(self.tickerData)>reqId:
                    #set the new price
                    symbol = self.tickerData[reqId][0]
                    price = self.tickerData[reqId][1]
                    if price > 0.0:
                        #TSend_Message_Price(symbol,price)
                        logger.info("Price Test Data:" + str(symbol) + " " +str(price))
              
 	
        def tickPrice(self, reqId, tickType, price, attrib):
            if tickType == 2 and reqId == 1:
                print('The current ask price is: ', price)
        def nextValidId(self, orderId: int):
            super().nextValidId(orderId)
            self.nextorderId = orderId
            print('The next valid order id is: ', self.nextorderId)
        def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
            print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)
            commission = self.commissions[orderId]
            Update_Order_Status(self.dbconnection,orderId,status,filled,lastFillPrice,commission)
        def openOrder(self, orderId, contract, order, orderState):
            self.commissions[orderId]= orderState.commission
            print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action, order.orderType, order.totalQuantity, orderState.status)
            print ('commission:', orderState.commission,orderState.commissionCurrency)
        def execDetails(self, reqId, contract, execution):
            print('Order Execute')
        def error(self, reqId, errorCode: int, errorString: str, advancedOrderRejectJson = ""):
             super().error( errorCode, errorString, advancedOrderRejectJson)
             if advancedOrderRejectJson:
                 logger.info("IB Broker Msg: " + errorString,advancedOrderRejectJson)
             else:
                logger.info("IB Broker Msg: " + errorString)


app = IBapi()


def IB_Start(dbconnection,is_live):
    app.dbconnection = dbconnection
    if is_live:
        logger.info("Connection to live account.")
        app.connect('127.0.0.1', 4001, 123)
    else:
        logger.info("Connection to paper account.")
        #app.connect('ib-gateway', 4002, 123)
        app.connect('127.0.0.1', 4002, 123) 
    time.sleep(2)
    logger.info("Starting the thread")
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    time.sleep(2)
    #check whether connection works
    while True:
        if isinstance(app.nextorderId, int):
            logger.info('Connection established!')
            break
        else:
            print('Waiting for connection')
            time.sleep(1)


def IB_End():
   
    time.sleep(2)
    app.disconnect()
    

 
def Req_Last_Market_Price(ticker_id,symbol):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    
    app.reqMarketDataType(3)

    #Request Market Data
    #app.reqMktData(1, apple_contract, '', False, False, [])
    queryTime = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d-%H:%M:%S")

    #Request Market Data
    app.reqHistoricalData(ticker_id,contract, "", "3600 S", "1 min", "TRADES",1,1,False,[])
    #app.reqMktData(3,contract,"",True,False,[])
    return 22.30
  

def Monitor_Symbols (db_conn,log_info):
    try:
        sql = "SELECT DISTINCT symbol FROM Positions P JOIN Assets A ON P.asset1 = A.id  \
        JOIN Broker_Mapping BM ON BM.asset_group_id = A.asset_group_id \
        JOIN Brokers B on B.id = BM.broker_id \
        WHERE  closed_at IS NULL AND B.uuid = %s"
 
      
        mycursor = db_conn.cursor()

        mycursor.execute(sql,[my_broker_uuid])
        myresult = mycursor.fetchall()
        if len(myresult)==0 and log_info:
            logger.info("No symbols monitored.")
        info = ""
        ticker_id = 0
        app.tickerData.clear()
        for row in myresult:
                symbol = str(row[0]).strip().upper()
               #  at the moment we don't subscribe to symbols, we just get it, because we have websocket problems
               #  wss_client.subscribe_bars(bar_data_handler, symbol)
                
                Req_Last_Market_Price(ticker_id,symbol)
                app.tickerData.append([symbol,0.0])
                ticker_id +=1
                info += symbol + ", " 

                   

        if log_info:
            logger.info("Monitoring Symbols: " + info )
        db_conn.commit()
    except  mysql.connector.Error as error:
        logger.exception("Monitor_Symbols: %s", error.msg)
    except Exception as general_error:  # This will catch any type of Exception
        logger.exception("Monitor_Symbols (General Error): %s", str(general_error))

    

def Submit_to_Broker(order_id,symbol,uuid,units,is_paper_trade):
    try:
        # Get our account information.
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = 'USD'
	

        if units > 0:
            orderside ="BUY"
        else:
            orderside = "SELL"
            units = -1* units

        #Create order object
        order = Order()
        order.action = orderside
        order.totalQuantity = units
        order.orderType = 'MKT'
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        
        #our IB order ID
        app.nextorderId+=1

        #Place order
        app.placeOrder(app.nextorderId, contract, order)
        
        
        return (Convert_IB_orderId(app.nextorderId),"")
    except Exception as general_error:  # This will catch any type of Exception
  
        return (None,str(general_error))

def Convert_IB_orderId(orderId):
    #IB order Ids are just consequitive numbers
    shortened_broker_uuid = my_broker_uuid[8:]
    return str(orderId) + "_" + shortened_broker_uuid


def Update_Order_Status(db_conn,order_id,status,filled,price,commission):
    try:
         #TODO order_id needs to have UUID appended before sent to us
         
        #set the Store Proc Params
        if str(status).upper() == "FILLED":
             status_amelium = 1
        elif str(status).upper() == "CANCELLED ":
             status_amelium = 2
        else:
            return
        broker_reference = Convert_IB_orderId(order_id)
        mycursor = db_conn.cursor()
                    
        #(broker_ref,status.value,price,units,0.0)
        logger.info("Order status has changed: " + broker_reference +", status:" + str(status_amelium) + ", price: " + str(price) + ", units: " + str(filled) + "commission: " + str(commission))
        params = (broker_reference,status_amelium,price,filled,commission) 
        mycursor.callproc("Order_Changed",params)
        db_conn.commit()
    except  mysql.connector.Error as error:
        logger.exception("Update_Order_Status failed: %s", error.msg)
            








if __name__ == '__main__':
   
      
    symbol = "AAPL"
    app.tickerData.append([symbol,0.0])
    #T myDB = DB_Connect()
    myDB = None
    Init_Logger("IB_Implementation")
    IB_Start(myDB,False)
    Req_Last_Market_Price(0,symbol)
    #Submit_to_Broker(123,symbol,'123',6,True)
    while True:
        time.sleep(2)
    IB_End()
