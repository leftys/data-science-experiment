from metaflow import FlowSpec, step



class LinearFlow(FlowSpec):
    @step
    def start(self):
        self._my_var = 'hello world'
        self.next(self.a)


    @step
    def a(self):
        print('the data artifact is: %s' % self._my_var)
        self.next(self.end)


    @step
    def end(self):
        print('the data artifact is still: %s' % self._my_var)



if __name__ == '__main__':
    LinearFlow()
