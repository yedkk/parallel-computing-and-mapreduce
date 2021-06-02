x={"A":5,"b":10}
y={"A":5,"b":10}
def merge_dicts(a,b):
    c = {}
    for k, v in a.items():
        c[k] = a[k] + b.get(k, 0)
        b.update(c)
    return b

print(merge_dicts(x,y))