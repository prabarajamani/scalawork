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