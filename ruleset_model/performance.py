from typing import Tuple, List

import contextlib
import resource
import time
import inspect

import objgraph
import numpy as np



def time_usage(func):
    def wrapper(*args, **kwargs):
        beg_ts = time.time()
        result = func(*args, **kwargs)
        end_ts = time.time()
        print("Elapsed time in %s: %f s" % (func, end_ts - beg_ts))
        return result
    return wrapper


@contextlib.contextmanager
def memory_usage(name):
    memory_usage = [None, None]
    memory_usage[0] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    yield memory_usage
    memory_usage[1] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print("Memory in %s: %.0f -> %.0f MB" % (name, memory_usage[0] / 1024, memory_usage[1] / 1024))


def check_dataframes(
    minimal_memory_usage: float = 10,
    show_views: bool = False,
    show_contents: bool = False,
    show_dtypes: bool = True,
):
    '''
    :param minimal_memory_usage: Minimal memory usage of the variable to be considered. In megabytes.
    '''
    already_printed_base_addresses = []
    for cls in ('DataFrame', 'Series'):
        objs = objgraph.by_type(cls)
        for obj in objs:
            memory_usage = obj.memory_usage(deep = True)
            try:
                memory_usage = memory_usage.sum()
            except:
                pass
            if memory_usage / 1e6 < minimal_memory_usage:
                continue
            object_type = cls
            if obj._is_view:
                memory_usage = 0
                object_type += ' view'
                if not show_views:
                    continue
            # Some view data can be referenced by several views (DataFrame and Series for example). Omit them.
            if id(obj.values.base) in already_printed_base_addresses:
                continue

            backrefs = find_object_in_stack(obj)

            print(f'{object_type} found on address {id(obj.values.base)} with memory usage {memory_usage / 1e6} MB. References:')
            print('\n'.join([f'\tName = {backref[0]} {backref[1]} in function {backref[3]} in file {backref[2]} ' for backref in backrefs]))
            if show_contents:
                print(obj)
            if show_dtypes:
                print('\tDtypes =', str(dict(obj.dtypes)))
            already_printed_base_addresses.append(id(obj.values.base))

            dtypes = obj.dtypes
            try:
                columns = obj.columns
            except AttributeError:
                # obj is Series
                dtypes = [dtypes]
                columns = ['Series']

            for dtype, column in zip(dtypes, columns):
                if dtype == np.object:
                    print(f'\tWARN: Object dtype found in \'{column}\', this series can take up a lot of memory')


def find_object_in_stack(obj: object) -> List[Tuple[str, str, str, str]]:
    '''
    Find the backreferences in stack trace locals.

    :return list of tuples of back reference name, type, filename and function name. Starts from the deepest frame where
    the variable was most likely defined.
    '''
    results: List[Tuple[str, str, str, str]] = []
    frame = inspect.currentframe().f_back
    while frame:
        frame_info = inspect.getframeinfo(frame)
        for var_name, var_value in frame.f_locals.items():
            if obj is var_value:
                results.append((var_name, 'function local', frame_info.filename, frame_info.function))
        if 'self' in frame.f_locals.keys():
            self = frame.f_locals['self']
            for var_name in dir(self):
                var_value = getattr(self, var_name)
                if obj is var_value:
                    results.append((var_name, 'instance variable', frame_info.filename, frame_info.function))
        frame = frame.f_back
    results.reverse()
    return results


class T:
    def test(self, m):
        self.n = m
        print(find_object_in_stack(l))

if __name__ == '__main__':
    l = [1, 2, 3]
    T().test(l)
