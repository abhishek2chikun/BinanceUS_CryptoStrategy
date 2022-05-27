from numpy import can_cast
import sqlalchemy
import pandas as pd
from binance.client import Client
import pandas_ta as ta
import asyncio
from binance import AsyncClient
import json
import time

 
async def Bot(client,lower_length,upper_length,candle, qty,pairs, open_position=False):
    while True:
        Data = {}
        try:
            for pair in pairs:
                pair = pair+'T'
                
                klines = await client.get_historical_klines(pair, Client.KLINE_INTERVAL_4HOUR, "7 day ago UTC")
                Columns = ['Date','Open','High','Low','Close','Volume','Close time','Quote asset volume','No. of trades','Taker buy base','Taker buy quote','Ignore']
                df = pd.DataFrame(klines,columns = Columns)
                df.index = pd.to_datetime(df['Date'], unit='ms')
                df = df.drop(['Date','Close time','Quote asset volume','No. of trades','Taker buy base','Taker buy quote','Ignore'],axis=1)
                
                df_dd = ta.donchian(high=df.High,low=df.Low,lower_length=lower_length, upper_length=upper_length)
               
                Data[pair] = {'Upper':df_dd.iloc[-1][f'DCU_{lower_length}_{upper_length}'],'Lower':df_dd.iloc[-1][f'DCL_{lower_length}_{upper_length}'],'Middle':df_dd.iloc[-1][f'DCM_{lower_length}_{upper_length}']}
                
            Data['Time'] = str(list(df.index)[-1])
            with open('./History.json','w') as f:   
                json.dump(Data,f)
            
            print("Done")
            time.sleep(60*60)
        except Exception as e:
            print(e)
            

        


async def main():

    # initialise the client
   

    Info = {"apiKey":"5XXQn7CBPZTQk5dZjcXwnSgAaC6zJU4nwLEvfg4Dp1adQVMUL4S3TWvz39jiASs2","secretKey":"TG424Qd63QpIyWR1PPXucc0KyaTDRKzOtaaFTaTXE5fQ1yxL0qPs39p1rMi3OFTD","comment":"Bot"}
    client = await AsyncClient.create(Info['apiKey'],Info['secretKey'])

    pairs = ['BTCUSD','ETHUSD','BNBUSD','ADAUSD','SOLUSD','AVAXUSD','DOTUSD','MATICUSD','DOGEUSD','ATOMUSD','LTCUSD']
    new_pairs = ['LINKUSD','UNIUSD','MANAUSD','AXSUSD','AAVEUSD','TRXUSD','CRVUSD','ZECUSD','APEUSD']
    pairs = pairs + new_pairs
    
    lower_length =40
    upper_length = 40
    qty=1
    candle='4Hr'
    await asyncio.gather(Bot(client,lower_length,upper_length,candle,qty,pairs))
    
if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
