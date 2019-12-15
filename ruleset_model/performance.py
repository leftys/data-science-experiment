import resource
import time



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