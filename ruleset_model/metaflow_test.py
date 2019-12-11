from typing import cast

from metaflow import FlowSpec, step, resources
# import pandas as pd
import dask.dataframe as dd
import dask.array as da



class LinearFlow(FlowSpec):
    @step
    def start(self):
        # Load data
        # trades = dd.read_csv('test_data.csv', parse_dates = ['time'])
        ''' contains columns time, price, quantity (signed) and row number index '''

        # Random data
        times = da.ones((10_000, 1), chunks = (1_000, 1), dtype = 'int').cumsum(axis = 0)
        prices = ((da.random.random((10_000, 1), chunks = (1_000, 1)) * 100).round(0) / 100).cumsum(axis = 0)
        quantities = (da.random.random((10_000, 1), chunks = (1_000, 1)) * 100).round(0).astype('int')
        # generate dask dataframe
        trades = dd.concat([dd.from_dask_array(c) for c in [times, prices, quantities]], axis = 1)
        # name columns
        trades.columns = ['time', 'price', 'quantity']
        trades['instrument'] = 'GARAN'
        trades['time'] = dd.to_datetime(trades['time'])
        print(trades.head(10))

        self.trades_per_instrument = []
        for instrument in trades.instrument.unique():
            self.trades_per_instrument.append(trades[trades.instrument == instrument].persist())
        # instrument_groups = trades.groupby(trades['instrument'])
        # self.trades_per_instrument = [instrument_groups.get_group(x) for x in instrument_groups.groups]
        self.next(self.per_instrument, foreach = 'trades_per_instrument')


    @resources(memory = 128, cpu = 1, gpu = 0)
    @step
    def per_instrument(self):
        self.instrument_trades = cast(dd.DataFrame, self.input)
        # Just an example computation of traded volume. This could actually be done just with groupby(day, instrument)
        self.instrument_trades['traded_volume'] = \
            self.instrument_trades['quantity'].abs().\
            groupby(self.instrument_trades['time'].dt.date).\
            cumsum()
        self.next(self.join_instruments)


    @step
    def join_instruments(self, inputs):
        self.results = dd.concat([input.instrument_trades for input in inputs])
        self.next(self.end)


    @step
    def end(self):
        print(self.results)
        print('Partitions: ', self.results.npartitions)



if __name__ == '__main__':
    LinearFlow()
