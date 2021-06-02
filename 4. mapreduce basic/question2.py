# I collaborated with:
#
# 1)
# 2)
# 3) 
# 4)
#
from mrjob.job import MRJob
class numbersourcecount(MRJob):
    def mapper(self, key, lines):
        word = lines.split()
        for w in word:
            if w == word[0]:
                yield (w, 1)
            else:
                yield (w, -1)

    def reducer(self, key, values):
        x=sum(values)
        if x>0:
            yield (int(key), x)


if __name__ == '__main__':
    numbersourcecount.run()

