import time
import sqlalchemy
import pandas as pd
from binance.client import Client
from datetime import date
from datetime import datetime
import json

 
def Bot(client,pairs):
    
    open_position = {}
    future_position = {}
    future_exit = {}
    Quantity = {}
    channel_long = {}
    for pair in pairs:
        open_position[pair] = False
        future_position[pair] = False
        future_exit[pair] = False
        channel_long[pair] = False
        # client.futures_change_leverage(symbol=pair, leverage=2)
        # time.sleep(1)
    def Quantity_def(pair,price,total_pairs=len(pairs)):
        
        Available_qty_list = client.get_account()['balances']
        
        for avl_qty in Available_qty_list:
                if avl_qty['asset'] == 'USD':
                    avl_amount = avl_qty['free']
                    break
                    
        
        per_pair_price = (float(avl_amount)/((total_pairs-5)-sum(list(open_position.values()))))*0.99
        
        qty = per_pair_price/price
        decimal = check_decimals(pair)
        
        Qty = float(round(float(qty), decimal-1)) 
        return Qty
    
    def check_decimals(symbol):
        try:
            info = client.get_symbol_info(symbol)
        except:
            print("Re-trying:",symbol)
            info = client.get_symbol_info(symbol)
        if info !=  None:
            val = info['filters'][2]['stepSize']
        else:
            return 4
        decimal = 0
        is_dec = False
        for c in val:
            if is_dec is True:
                decimal += 1
            if c == '1':
                break
            if c == '.':
                is_dec = True
        return decimal
 
    log_df = pd.DataFrame(columns=['Time','Type','Pair','Qty','Price','Desc'])
    while True:
        log_df.to_csv('Log.csv',index=False)
        time.sleep(2)
        
        d = date.today().strftime("%d%b%Y")
        try:
            engine = sqlalchemy.create_engine(f'sqlite:///Live_Data/{d}.db')
        except:
            time.sleep(5)
            continue

        for pair in pairs:
            try:
                df = pd.read_sql('Stream', engine,parse_dates=['Time'],index_col=['Time'])
                df_symbol = df.loc[df['Symbol'] == pair+'T']
                df_symbol = df_symbol.iloc[-1]
            except:
                print("Unable to read from Live Data")
                time.sleep(5)
                continue

            
            try:
                with open('./History.json','r') as f:
                    history = json.load(f)
            except:
                print("Unable to read 4hr Signals")

            


            # Check for Long Trade
            if not open_position[pair]:
                
                #Going Long
                if channel_long[pair] == False:
                    if df_symbol['Close'] > history[pair+'T']['Middle']:
                        Quantity[pair] = Quantity_def(pair,df_symbol['Close'])
                        
                        try:
                            order = client.create_order(symbol=pair,
                                                    side='BUY',
                                                    type='MARKET',
                                                    quantity=Quantity[pair]*0.3)

                            
                            print("Middle Buy:::----->",pair,'Qty:::-->',Quantity[pair]*0.3)
                            log_df.loc[len(log_df)] = [str(datetime.now()),'Middle long BUY(Entry)',pair,Quantity[pair]*0.3,df_symbol['Close'],'Success']
                            channel_long[pair] = True
                        except Exception as e:
                            print(e)
                            print("Cannot place Middle BUY order for:::",pair)
                            log_df.loc[len(log_df)] = [str(datetime.now()),'Middle long BUY(Entry)',pair,Quantity[pair]*0.3,df_symbol['Close'],'Error::'+str(e)]


                else:
                    if df_symbol['Close'] > history[pair+'T']['Upper']:

                        # Long Position
                        Quantity[pair] = Quantity_def(pair,df_symbol['Close'])
                        
                        try:
                            order = client.create_order(symbol=pair,
                                                    side='BUY',
                                                    type='MARKET',
                                                    quantity=Quantity[pair]*0.7)

                            
                            print("Upper Buy:::----->",pair,'Qty:::-->',Quantity[pair]*0.7)
                            log_df.loc[len(log_df)] = [str(datetime.now()),'Upper long BUY(Entry)',pair,Quantity[pair]*0.7,df_symbol['Close'],'Success']

                            open_position[pair] = True
                        except Exception as e:
                            print(e)
                            print("Cannot place Upper BUY order for:::",pair)
                            log_df.loc[len(log_df)] = [str(datetime.now()),'Upper long BUY(Entry)',pair,Quantity[pair]*0.7,df_symbol['Close'],'Error::'+str(e)]


                    elif df_symbol['Close'] < history[pair+'T']['Middle']:
                        
                        #Exiting from Middle Channel (Stop-loss)
                        try:
                            Available_qty_list = client.get_account()['balances']
                    
                            for avl_qty in Available_qty_list:
                                if avl_qty['asset'] == pair[:-3]:
                                    qty = avl_qty['free']
                                    decimal = check_decimals(pair)
                                    print("DEcimal",decimal)
                                    Qty = round(float(qty), decimal) - float(1/10**decimal)
                                    print(Qty)
                                    break
                            try:
                            
                                order = client.create_order(symbol=pair,
                                                    side='SELL',
                                                    type='MARKET',
                                                    quantity=Qty)
                                
                                print("Middle Stop-loss hit Selling:::---->",pair,'Qty:::-->',Qty)
                                log_df.loc[len(log_df)] = [str(datetime.now()),'Middle long SELL(Stoploss)',pair,Qty,df_symbol['Close'],'Success']

                                channel_long[pair] = False

                            except Exception as e:
                                print(e)
                                print("Cannot place Middle Stop-loss hit SELL order for:::",pair)
                                log_df.loc[len(log_df)] = [str(datetime.now()),'Middle long SELL(Stoploss)',pair,Qty,df_symbol['Close'],'Error::'+str(e)]

                        except:
                            print("Unable to fetch quantity")
            
            #Selling and Future
            if open_position[pair]:
                
                #Selling on Cash
                if future_position[pair] == False:
                    if df_symbol['Close'] < history[pair+'T']['Middle']:

                        try:
                            Available_qty_list = client.get_account()['balances']
                    
                            for avl_qty in Available_qty_list:
                                if avl_qty['asset'] == pair[:-4]:
                                    qty = avl_qty['free']
                                    decimal = check_decimals(pair)
                                    print("DEcimal",decimal)
                                    Qty = round(float(qty), decimal) - float(1/10**decimal)
                                    print(Qty)
                                    break
                            try:
                            
                                order = client.create_order(symbol=pair,
                                                    side='SELL',
                                                    type='MARKET',
                                                    quantity=Qty)
                                
                                print("Sell:::---->",pair,'Qty:::-->',Qty)
                                # open_position[pair] = False
                                log_df.loc[len(log_df)] = [str(datetime.now()),'Upper Long SELL(Exit)',pair,Qty,df_symbol['Close'],'Success']

                                
                                #--------- Shorting Entry on Futures
                                try:
                                    order = client.futures_create_order(symbol=pair, side='SELL', type='MARKET', quantity=Qty)
                                    print("Future Sell:::---->",pair,'Qty:::-->',Qty)
                                    future_position[pair] = True
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short SELL(Entry)',pair,Qty,df_symbol['Close'],'Success']

                                
                                except:
                                    print(e)
                                    print("Cannot place FUTURE SELL order for:::",pair)
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short SELL(Entry)',pair,Qty,df_symbol['Close'],'Error::'+str(e)]

                            except Exception as e:
                                print(e)
                                print("Cannot place SELL order for:::",pair)
                                log_df.loc[len(log_df)] = [str(datetime.now()),'Upper Long SELL(Exit)',pair,Qty,df_symbol['Close'],'Error::'+str(e)]

                        
                        except:
                            print("Unable to fetch quantity")

                
                else:
                    if future_exit == False:

                        # Future Target Exit Half Qty
                        if df_symbol['Close'] < history[pair+'T']['Lower']:
                            try:
                                Available_qty_list = client.get_account()['balances']
                        
                                for avl_qty in Available_qty_list:
                                    if avl_qty['asset'] == pair[:-4]:
                                        qty = avl_qty['free']
                                        decimal = check_decimals(pair)
                                        print("Decimal",decimal)
                                        Qty = round(float(qty), decimal) - float(1/10**decimal)
                                        Qty = Qty/2
                                        break
                                try:
                                
                                    order = client.futures_create_order(symbol=pair,
                                                        side='BUY',
                                                        type='MARKET',
                                                        quantity=Qty)
                                    
                                    print("BUY FUTURE Target:::---->",pair,'Qty:::-->',Qty)
                                    
                                    
                                    future_exit[pair] = True
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short SELL(Target)',pair,Qty,df_symbol['Close'],'Success']


                                except Exception as e:
                                    print(e)
                                    print("Cannot place BUY FUTURE TargetELL order for:::",pair)
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short SELL(Target)',pair,Qty,df_symbol['Close'],'Error::'+str(e)]

                            except:
                                print("Unable to fetch quantity")
                       
                        #------- Future Stoploss Exit Completely
                        elif df_symbol['Close'] > history[pair+'T']['Middle']:
                            
                            try:
                                Available_qty_list = client.get_account()['balances']
                        
                                for avl_qty in Available_qty_list:
                                    if avl_qty['asset'] == pair[:-4]:
                                        qty = avl_qty['free']
                                        decimal = check_decimals(pair)
                                        print("DEcimal",decimal)
                                        Qty = round(float(qty), decimal) - float(1/10**decimal)
                                        print(Qty)
                                        break
                                try:
                                
                                    order = client.futures_create_order(symbol=pair,
                                                        side='BUY',
                                                        type='MARKET',
                                                        quantity=Qty)
                                    
                                    print("BUY FUTURE Stop-loss:::---->",pair,'Qty:::-->',Qty)
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short BUY(Stoploss)',pair,Qty,df_symbol['Close'],'Success']

                                    
                                    open_position[pair] = False
                                    future_exit[pair] = False
                                    channel_long[pair] = False

                                except Exception as e:
                                    print(e)
                                    print("Cannot place BUY FUTURE Stop-loss order for:::",pair)
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short BUY(Stoploss)',pair,Qty,df_symbol['Close'],'Error::'+str(e)]

                            except:
                                print("Unable to fetch quantity")
                    
                    
                    #--- Exiting Other Half Future
                    else:
                        if df_symbol['Close'] > history[pair+'T']['Middle']:
                            try:
                                Available_qty_list = client.get_account()['balances']
                        
                                for avl_qty in Available_qty_list:
                                    if avl_qty['asset'] == pair[:-4]:
                                        qty = avl_qty['free']
                                        decimal = check_decimals(pair)
                                        print("DEcimal",decimal)
                                        Qty = round(float(qty), decimal) - float(1/10**decimal)
                                        print(Qty)
                                        break
                                try:
                                
                                    order = client.futures_create_order(symbol=pair,
                                                        side='BUY',
                                                        type='MARKET',
                                                        quantity=Qty)
                                    
                                    print("BUY FUTURE Exit :::---->",pair,'Qty:::-->',Qty)
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short BUY(Exit)',pair,Qty,df_symbol['Close'],'Success']

                                    
                                    open_position[pair] = False
                                    future_exit[pair] = False
                                    channel_long[pair] = False

                                except Exception as e:
                                    print(e)
                                    print("Cannot place BUY FUTURE Exit order for:::",pair)
                                    log_df.loc[len(log_df)] = [str(datetime.now()),'Future Short BUY(Exit)',pair,Qty,df_symbol['Close'],'Error::'+str(e)]

                            
                            except:
                                print("Unable to fetch quantity")

                

                    
        time.sleep(5)
        
          
 

Info = {"apiKey":"","secretKey":"","comment":"Bot"}
client = Client(Info['apiKey'],Info['secretKey'],{"verify": True, "timeout": 20}, tld='us')
# print(client)
d = date.today().strftime("%d%b%Y")
engine = sqlalchemy.create_engine(f'sqlite:///Live_Data/{d}.db')
# df = pd.read_sql('Stream', engine)
# print(df.loc[df['Symbol'] == 'ETHUSDT'])
pairs = ['BTCUSD','ETHUSD','BNBUSD','ADAUSD','SOLUSD','AVAXUSD','DOTUSD','MATICUSD','DOGEUSD','ATOMUSD','LTCUSD']
new_pairs = ['LINKUSD','UNIUSD','MANAUSD','AXSUSD','AAVEUSD','TRXUSD','CRVUSD','ZECUSD','APEUSD']
pairs = pairs + new_pairs


print(len(pairs))
lookback=40
qty=0.000000001
candle='4Hr'
Bot(client,pairs)
