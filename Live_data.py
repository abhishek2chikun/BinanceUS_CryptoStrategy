import pandas as pd
import sqlalchemy
import asyncio
from binance import AsyncClient, BinanceSocketManager
from datetime import date


def createDataFrame(msg):
    df = pd.json_normalize([msg['data']])
    df = df.loc[:,['E','s','k.o','k.c','k.h','k.l']]
    df.columns = ['Time','Symbol','Open','Close','High','Low']
    df[['Open','Close','High','Low']] = df[['Open','Close','High','Low']].astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df



async def getAllData(bsm,pair,engine,prevday):
    async with bsm.multiplex_socket(pair) as stream:
        while True:
            today = date.today().strftime("%d%b%Y")
            print(today,prevday)
            if today!=prevday:
                break
            res = await stream.recv()
            frame = createDataFrame(res)
            frame.to_sql('Stream', engine, if_exists='append', index = False)
            print(frame)

async def main():
    while True:
        try:
            d = date.today().strftime("%d%b%Y")
            # d= '16Mar2022'
            engine = sqlalchemy.create_engine(f'sqlite:///Live_Data/{d}.db')
            # Info = {"apiKey":"5XXQn7CBPZTQk5dZjcXwnSgAaC6zJU4nwLEvfg4Dp1adQVMUL4S3TWvz39jiASs2","secretKey":"TG424Qd63QpIyWR1PPXucc0KyaTDRKzOtaaFTaTXE5fQ1yxL0qPs39p1rMi3OFTD","comment":"Bot"}

            # client = Client(Info['apiKey'],Info['secretKey'])

        
            client = await AsyncClient.create()
            bsm = BinanceSocketManager(client)
            pairs = ['BTCUSD','ETHUSD','BNBUSD','ADAUSD','SOLUSD','AVAXUSD','DOTUSD','MATICUSD','DOGEUSD','ATOMUSD','LTCUSD']
            new_pairs = ['CRVUSD','ZECUSD','APEUSD','LINKUSD','UNIUSD','MANAUSD','AXSUSD','AAVEUSD','TRXUSD']
            pair = pairs + new_pairs
            
            pair = [(i+'t').lower()+'@kline_4h' for i in pair]
           

            await asyncio.gather(getAllData(bsm,pair,engine,d))

        except Exception as e:
            print("Error")
            print(e)
            
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
