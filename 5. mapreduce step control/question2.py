import csv
from mrjob.job import MRJob
from mrjob.job import MRStep
class totalamount2(MRJob):

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
                    yield (i, self.cache[i])
                self.cache.clear()

    def mapper_final(self):
        if self.cache:
            for w in self.cache:
                yield (w, self.cache[w])


    def combiner(self, key, values):
        yield (str.strip(key), sum(list(values)))

    def return_final(self,key, values):
        yield (sum(list(values)),key)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init, mapper=self.mapper, mapper_final=self.mapper_final,combiner=self.combiner),
            MRStep(reducer=self.return_final)]

if __name__ == '__main__':
    totalamount2.run()