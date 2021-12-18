import time

print("before sleep : ",time.strftime("%H:%M:%S",time.localtime()))
time.sleep(5)
print("after sleep 5 second :",time.strftime("%H:%M:%S",time.localtime()))

