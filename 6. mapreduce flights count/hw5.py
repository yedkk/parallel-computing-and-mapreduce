import csv
from mrjob.job import MRJob
from mrjob.job import MRStep

class flight(MRJob):

    def mapper_init(self):
        self.cache={}

    def mapper(self, key, lines):
        parts = list(csv.reader([lines]))[0]
        if parts[0]== "DEST_COUNTRY_NAME":
            pass
        else:
            start = parts[0].replace("\"", "")
            end = parts[1].replace("\"", "")
            start = str.strip(start)
            end = str.strip(end)
            yield (start,["end",end])
            yield (end, ["start", start])

    def reducer(self, key, values):
        values=list(values)
        start=[]
        end=[]
        for i in values:
            if i[0] == "start":
                start.append(i[1])
            else:
                end.append(i[1])
        yield key, [start,end]

    def mapper2(self, key, values):
        a = values[0]
        b = values[1]
        for i in a:
            for j in b:
                if i != j:
                    yield (i,j), 1

    def reducer2(self,key,values):
        yield tuple(key), sum(values)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init, mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)]

if __name__ == '__main__':
    flight.run()
