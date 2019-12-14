from typing import cast, List

from metaflow import FlowSpec, step, resources
import pandas as pd
import dask.dataframe as dd
import dask.array as da



class LinearFlow(FlowSpec):
    @step
    def start(self):
        # Load data
        # trades = dd.read_csv('test_data.csv', parse_dates = ['time'])
        ''' contains columns time, price, quantity (signed) and row number index '''

        # Random data
        row_number = 10_000_000
        chunk_size = 1_000_000
        number_of_days = 10
        nanoseconds_per_row = (number_of_days * 24 * 60 * 60 * 1e9) / row_number

        # dask arrays containing columns
        times = (da.ones((row_number, 1), chunks = (chunk_size, 1), dtype = 'int') * nanoseconds_per_row).cumsum(axis = 0) - 1
        prices = (da.ones((row_number, 1), chunks = (chunk_size, 1)) / 100).cumsum(axis = 0)
        quantities = (da.random.random((row_number, 1), chunks = (chunk_size, 1)) * 100).round(0).astype('int64')

        # dask dataframe
        trades = dd.concat([dd.from_dask_array(c) for c in [times, prices, quantities]], axis = 1)
        trades.columns = ['time', 'price', 'quantity']
        trades['instrument'] = 'GARAN'
        trades['time'] = dd.to_datetime(trades['time'])

        print(trades.head(10))
        print(trades.memory_usage(deep = True).head(100))
        print(trades.memory_usage(deep = True).sum().compute())

        self.trades_per_instrument_and_day: List[dd.DataFrame] = []
        for instrument in trades.instrument.unique():
            for day in trades.time.dt.date.unique():
                self.trades_per_instrument_and_day.append(
                    trades[
                        (trades.instrument == instrument) &
                        (trades.time.dt.date == day)
                    ]
                )
        # instrument_groups = trades.groupby(trades['instrument'])
        # self.trades_per_instrument = [instrument_groups.get_group(x) for x in instrument_groups.groups]
        self.next(self.per_instrument_and_day, foreach = 'trades_per_instrument_and_day')


    @resources(memory = 128, cpu = 1, gpu = 0)
    @step
    def per_instrument_and_day(self):
        # print('Len', len(self.input))
        self.instrument_trades = cast(dd.DataFrame, self.input).copy()
        print('Len', len(self.instrument_trades))
        print(self.instrument_trades.head(5))
        # Just an example computation of traded volume. This could actually be done just with groupby(day, instrument)
        self.instrument_trades['traded_volume'] = \
            self.instrument_trades['quantity'].abs().cumsum()
        print(self.instrument_trades.head(5))
        self.next(self.join_instruments)


    @step
    def join_instruments(self, inputs):
        print('Is dask df', type(inputs[0].instrument_trades) is dd.DataFrame)
        print('Is pandas df', type(inputs[0].instrument_trades) is pd.DataFrame)
        self.results = dd.concat([input.instrument_trades for input in inputs]).reset_index(drop = True)
        print('Len df', len(self.results))
        self.next(self.end)


    @step
    def end(self):
        print(self.results.head(1_000_005, npartitions = 2))
        print(self.results.tail(10))
        print('Partitions: ', self.results.npartitions)



if __name__ == '__main__':
    LinearFlow()
