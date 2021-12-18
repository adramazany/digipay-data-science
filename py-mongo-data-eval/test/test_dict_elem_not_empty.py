d=dict()
d["elm1"]="has value"
d["elm2"]=""
#d["elm3"]
print(d)
if "elm1" in d : print("elm1 exists")
if "elm2" in d : print("elm2 exists")
if "elm3" in d : print("elm3 exists")
if not d["elm2"] : print("elm2 empty")
if "elm3" in d and not d["elm3"] : print("elm3 empty")
