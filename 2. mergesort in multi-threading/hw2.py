# I collaborated with:
#
# 1)
# 2) 
# ...
#


from dask import delayed
from dask.distributed import Client, wait, progress, as_completed
from typing import List, Dict, Tuple, Any
import re

def merge_dicts(a,b):
    c = {}
    for k, v in a.items():
        c[k] = a[k] + b.get(k, 0)
        b.update(c)
    return b

def tokenize(line: str) -> List[str]:
    """ Splits a line into words """
    trimmed = line.strip()
    return re.split("\W+", trimmed) if trimmed else []

@delayed
def count_freq(word_list:List[str], filelist:List[str])->Dict[str, int]:
    # define a empty dict
    word={}
    for i in word_list:
        x=filelist.count(i)
        word[i]=x
    return word

def count_them(word_list: List[str], file_list: List[str]) -> Dict[str, int]:
    #define the empyt list and empty dict
    word_store = []
    word_count = {}
    word=[]
    #append all word into a list
    for i in file_list:
        word_store.append(sortfile(i))
    for i in word_store:
        x=count_freq(word_list,i)
        word_count=merge_dicts(word_count,x.compute())
    return word_count


@delayed
def sortfile(f: str) -> List[str]:
    """ Returns an array consisting of the sorted words in f"""
    with open(f, "r", encoding='UTF-8' ) as infile:
        words = [word for line in infile.readlines() for word in tokenize(line)]
        words.sort()
    return words

@delayed
def sort_two(a:list,b:list)->List[str]:
    l = []
    while a and b:
        if a[0] < b[0]:
            l.append(a.pop(0))
        else:
            l.append(b.pop(0))
    return l + a + b


def mergesort(file_list: List[str]) -> Tuple[Any, List[str]]:
    word_store=[]
    for i in file_list:
        word_store.append(sortfile(i))
    while len(word_store)>1:
        new=sort_two(word_store[0],word_store[1])
        word_store=word_store[2:]
        word_store.append(new)
    else:
        new=word_store[0]
        return (new, new.compute())



path = [r"C:\Users\yedkk\Documents\GitHub\big data project\hw3\part-00000", r"C:\Users\yedkk\Documents\GitHub\big data project\hw3\part-00001"]
print(count_them(["Russia","Rome","American","China"],path))



