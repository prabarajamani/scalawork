#from pyspark.sql.connect.plan import Range

#for i in 'apple':
#    print(i)

e_count =0
o_count =0
for j in range(1,5):
    if(j%2==0):
        print("Even")
        e_count = e_count+1
    else:
        print("Odd")
        o_count = o_count + 1
#    print(j)
print(e_count)
print(o_count)


num = 5

for u in range(num): # 0 --> 4 will loop
    print(u)

for k in range(10, 50 , 2): # 3 arguments start with 10 and loop till 49 and use step up value
    print(k)

for j in range(100, 50, -1):# will loop in reverse value
    print(j)

#nested loops:

n = int(input("Enter n : "))

for j in range(1, n+1):
    for i in range(1, j+1):
        print(i, end=' ')
    print()