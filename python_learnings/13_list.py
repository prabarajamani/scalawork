#List in python

lst = [1,2,3,4,5,6,0]

print(type(lst))# list data type

print(lst[0])# 1
print(lst[-1]) # :

#LENGTH of list
print(len(lst)) #7

#insert element in list
lst.append(7)# will add a value in list at last
print(lst)

# add list of values
lst1 = [8,9,0]
lst.extend(lst1)
print(lst)

#insert the element in any specific position in list
lst.insert(0,0)
print(lst)

#count
print(lst.count(0))
print("yes")

#index
print("Index : ", lst.index(0))# will give the index val

# min and max
print(lst)
print(max(lst))

#pop -- it will remove value, based on the index
lst.pop(0)
print(lst)

#remove -- it will find the first occurred value and remove from the list
lst.remove(0)
print(lst)

# empty a list
lst.clear()
print(lst)
