import os
import pandas as pd

df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)),'vote_industry300.csv'))
cdf=df[df.评价 == 1]
