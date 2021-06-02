import csv
from mrjob.job import MRJob
class totalamount(MRJob):

    def mapper_init(self):
        self.cache={}

    def mapper(self, key, lines):
        parts = list(csv.reader([lines]))[0]
        w=parts[2]
        if w =="":
            w = "NA"
        w = str.strip(w)
        w = w.replace("\"", "")
        if parts[3]=="Quantity":
            pass
        else:
            v=float(parts[3])*float(parts[5])
            if not w in self.cache:
                self.cache[w] = v
            else:
                self.cache[w] += v
            if len(self.cache)>100:
                for i in self.cache:
                    yield (i, self.cache[w])
                    yield("zxty", 1999)
                self.cache.clear()

    def mapper_final(self):
        if self.cache:
            for w in self.cache:
                yield ("zxty", 1999)
                yield (w, self.cache[w])


    def reducer(self, key, values):
        yield (key, sum(list(values)))

if __name__ == '__main__':
    totalamount.run()