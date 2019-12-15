from typing import cast, List

from metaflow import FlowSpec, step, resources
import pandas as pd
import numpy as np



class MyFlow(FlowSpec):
    @step
    def start(self):
        ''' Generate random 'trade' data and split them by instrument, date. '''
        # Generate random data
        row_number = 10_000_000
        number_of_days = 10
        nanoseconds_per_row = (number_of_days * 24 * 60 * 60 * 1e9) / row_number

        # numpy arrays containing columns
        times = (np.ones((row_number,), dtype = 'int') * nanoseconds_per_row).cumsum(axis = 0) - 1
        prices = np.ones((row_number,)).cumsum(axis = 0)
        quantities = (np.random.random((row_number,)) * 100).round(0).astype('int32') # pylint: disable = no-member

        # set up dataframe
        trades = pd.DataFrame(
            data = {'time': times, 'price': prices, 'quantity': quantities},
        )
        del times, prices, quantities
        trades['time'] = pd.to_datetime(trades['time'])
        trades['date'] = trades.time.dt.date #.astype('|S10')
        trades['instrument'] = pd.Series(['GARAN'], dtype = 'category')
        print(trades.dtypes)
        trades = trades.set_index(['date', 'instrument', 'time'])

        print(trades.head(10))
        print(trades.memory_usage(deep = True).head(100))
        print(trades.memory_usage(deep = True).sum())

        self.trades = trades

        self.next(self.split)


    @step
    def split(self):
        self.trades_per_instrument_and_day: List[pd.DataFrame] = []
        for instrument in self.trades.index.get_level_values('instrument').unique():
            for date in self.trades.index.get_level_values('date').unique():
                self.trades_per_instrument_and_day.append(
                    self.trades.loc[[instrument, date, None]]
                )

        self.next(self.per_instrument_and_day, foreach = 'trades_per_instrument_and_day')


    @resources(memory = 128, cpu = 1, gpu = 0)
    @step
    def per_instrument_and_day(self):
        ''' Process single day on single instrument. '''
        self.instrument_trades = cast(pd.DataFrame, self.input).copy()
        # Just an example computation of traded volume. This could actually be done just with groupby(day, instrument)
        self.instrument_trades['traded_volume'] = \
            self.instrument_trades['quantity'].abs().cumsum()
        self.next(self.join)


    @step
    def join(self, inputs):
        self.results = pd.concat([input.instrument_trades for input in inputs])
        self.next(self.end)


    @step
    def end(self):
        print(self.results)



if __name__ == '__main__':
    MyFlow()
