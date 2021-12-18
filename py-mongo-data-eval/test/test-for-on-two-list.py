
l1=["a","b","c"]
l2=["X","Y","Z"]


st = ",".join(e for e in l1)
print("l1=",st)

st = ",".join(e for e in l2)
print("l2=",st)

st=",".join(l1[i]+'='+l2[i] for i in range(len(l1)))
print("l1=l2 > ",st)
