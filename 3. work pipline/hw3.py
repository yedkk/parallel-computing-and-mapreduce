# I collaborated with:
#
# 1)
# 2) 
# ...
#

import dask
from multiprocessing.pool import ThreadPool
from dask import delayed
from dask.distributed import Client, wait, progress, as_completed
from typing import List, Dict, Tuple, Any
import re
import timeit


def tokenize(line: str) -> List[str]:
    """ Splits a line into words """
    trimmed = line.strip()
    return re.split("\W+", trimmed) if trimmed else []


def merge(a,b)-> List[str]:
        l = []
        while a and b:
            if a[0] < b[0]:
                l.append(a.pop(0))
            else:
                l.append(b.pop(0))
        return l + a + b


def sortfile(f: str) -> List[str]:
    """ Returns an array consisting of the sorted words in f"""
    with open(f, "r",encoding='UTF-8' ) as infile:
        words = [word for line in infile.readlines() for word in tokenize(line)]
        words.sort()
    return words


def mergesort(file_list: List[str]) ->  List[str]:
        # sort file by use map function
        l1 = c.map(sortfile, file_list)
        #store it in list
        my_iterator = as_completed(l1)
        # do the merge sort two by two
        while my_iterator.count() > 2:
            a = next(my_iterator)
            b = next(my_iterator)
            q = c.submit(merge, a, b)
            my_iterator.add(q)
        else:
        # if there only left two list, mergesort and return result directly
            a = next(my_iterator)
            b = next(my_iterator)
            q = c.submit(merge, a, b)
            return q.result()




if __name__ == "__main__":
    #asynchronous = True, n_workers = 1000, threads_per_worker = 2
    c = Client(asynchronous = True, n_workers = 1000, threads_per_worker = 2)
    start = timeit.timeit()
    q=mergesort([r"C:\Users\yedkk\Documents\GitHub\big data project\hw3\part-00000",
               r"C:\Users\yedkk\Documents\GitHub\big data project\hw3\part-00001"])
    end = timeit.timeit()
    print(q)
    print(start)
    print(end)



