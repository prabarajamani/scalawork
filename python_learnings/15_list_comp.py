#List comperhension

arr = [1,2,3,4,5,6]
odd = []

for i in arr:
    if i%2 ==1:
        odd.append(i)

print(odd)

odd = [i for i in arr if i%2 == 1]
print(odd)