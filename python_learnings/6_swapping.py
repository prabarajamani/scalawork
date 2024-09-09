#Swapping
a = 5
b = 7
print(a, b)


#code
# change the value of a = 7 & b = 5 (Interchange the value) without assigning in variable
temp = a
a = b
b = temp
print(a, b)
a, b = b, a #this is swapping
print(a, b)