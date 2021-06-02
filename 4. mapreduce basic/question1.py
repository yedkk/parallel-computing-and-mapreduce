# I collaborated with:
#
# 1)
# 2)
# 3) 
# 4)
#
from mrjob.job import MRJob
class numbercount(MRJob):
    def mapper(self,key,lines):
        word = lines.split()
        for w in word:
            yield (w, 1)

    def reducer(self, key, values):
        yield (int(key), sum(values))

if __name__ == '__main__':
    numbercount.run()




