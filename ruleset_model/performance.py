from typing import Tuple, List

import resource
import time
import inspect



def time_usage(func):
    def wrapper(*args, **kwargs):
        beg_ts = time.time()
        result = func(*args, **kwargs)
        end_ts = time.time()
        print("Elapsed time in %s: %f s" % (func, end_ts - beg_ts))
        return result
    return wrapper


def memory_usage(func):
    def wrapper(*args, **kwargs):
        beg = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result = func(*args, **kwargs)
        end = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print("Memory in %s: %.0f -> %.0f MB" % (func, beg / 1024, end / 1024))
        return result
    return wrapper


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
