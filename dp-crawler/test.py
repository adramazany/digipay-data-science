def HowManyNegative(*args):
    negCount=0
    for n in args:
        if n<0:
            negCount+=1
    return negCount

print(HowManyNegative(-1,-1,14,9,32,-12))
print(HowManyNegative(1,2,4))
print(HowManyNegative(-32,8,-17,99,-3))