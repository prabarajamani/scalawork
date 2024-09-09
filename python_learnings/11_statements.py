#Statements

# type of statements
#1. simple
#2. compound ( multiple line of statement)
#3. Empty statement


#Condition statement

if 5 ==1 :
    print("5 is equal")
else:
    print("first else")
a =1
b =2

if (a == b):
    print("second if")
elif(b ==2):
    print(" b is equal to 2")
else:
    print("other wise")

if (5 == 3):{
    print("normal code")
}

print("done")

# if condition is true, the block will be consider from intetation :

num = -10

if num > 0:
    print("positive")
elif num == 0:
    print("zero")
else:
    print("negative")


 # empty statement

if 5 == 5:
    pass #empty statement
print("pass")

#jump statement

for i in range(1,11):
    if i == 5:
        continue #jump statement
    print(i)


#break statement

for i in range(1,11):
    if i == 5:
        break #break statement
    print(i)
