import pandas as pd
import numpy as np
from opdata import get_day

Sto_Basics = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)),'all.csv'))
Sto_Basics = Sto_Basics[['code','outstanding']]
Sto_Basics = Sto_Basics.set_index('code')
Sto_Outstand = Sto_Basics.to_dict(orient='dict')['outstanding']

def _checkSector(sector):
    """check each stock in sector, whether thers is 
    enough price data
    """
    return sector
    pass

def _statistic(sector):
    sectorShares = 0.0
    for code in sector:
        if Sto_Outstand.get(int(code)) and Sto_Outstand.get(int(code)) is not np.nan:
            outstand = Sto_Outstand.get(int(code))
        else:
            outstand = 1.0        
        sectorShares += outstand
    return sectorShares

def maker(sector, sectorName,start_date='2010-06-01',end_date='2018-03-30'):
    sector = _checkSector(sector)
    sectorShares = _statistic(sector)
    df = get_day('000001','2002-01-01','2018-07-01')
    del df['volume']
    df['open']=0.0
    df['close']=0.0
    df['high']=0.0
    df['low']=0.0
    df['name'] = sectorName
    for code in sector:
        if Sto_Outstand.get(int(code)) and Sto_Outstand.get(int(code)) is not np.nan:
            outstand = Sto_Outstand.get(int(code))
        else:
            outstand = 1.0 
        df_price = get_day(code,'2002-01-01','2018-07-01')
        df['open'] = df_price['open'] * outstand + df['open']
        df['close'] = df_price['close'] * outstand + df['close']
        df['high'] = df_price['high'] * outstand + df['high']
        df['low'] = df_price['low'] * outstand + df['low']
    
    df['open'] = df['open'] / sectorShares
    df['close'] = df['close'] / sectorShares
    df['high'] = df['high'] / sectorShares
    df['low'] = df['low'] / sectorShares
    df=df[df.date>=start_date]
    df=df[df.date<=end_date]
    df = df.reset_index()
    del df['index']
    return df

        