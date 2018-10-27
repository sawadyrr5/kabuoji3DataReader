# -*- coding: UTF-8 -*-
import requests
import pandas as pd
from pandas_datareader import data
from pandas_datareader.base import _DailyBaseReader
from io import StringIO

_MAX_RETRY_COUNT = 3
_SLEEP_TIME = 0.1
_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'


class Kabuoji3DataReader(_DailyBaseReader):
    @property
    def url(self):
        return "https://kabuoji3.com/stock/file.php"

    def read(self):
        symbols = []
        years = set([y.year for y in pd.date_range(self.start, self.end, freq='D')])

        if isinstance(self.symbols, list):
            symbols = self.symbols
        elif isinstance(self.symbols, str) or isinstance(self.symbols, int):
            symbols.append(self.symbols)
        else:
            symbols = []

        dfs = []

        for symbol in symbols:
            for year in years:
                params = {
                    "code": symbol,
                    "year": year
                }

                dfs.append(
                    self._read_one_data(url=self.url, params=params)
                )

        df = pd.concat(dfs)

        return df

    def _read_one_data(self, url, params):
        s = requests.session()
        s.headers.update(
            {
                "User-Agent": _USER_AGENT
            }
        )
        body = s.post(url=url, data=params)
        lines = body.text.splitlines()

        out = '\r\n'.join(lines[1:])

        csv = StringIO(out)

        df = pd.read_csv(csv, header=0, quotechar='"')
        df["銘柄コード"] = params["code"]

        return df


def DataReader(symbols, data_source=None, start=None, end=None, **kwargs):
    if data_source == 'kabuoji3':
        return Kabuoji3DataReader(symbols=symbols, start=start, end=end, **kwargs)
    else:
        return data.DataReader(name=symbols, data_source=data_source, start=start, end=end, **kwargs)
