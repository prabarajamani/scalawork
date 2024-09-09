#nested if

age = 18
eat_pizza = False
exercise = False


if (age < 30):
    if eat_pizza:
        print("unfit")
    else:
        print("fit")
else:
    if exercise:
        print("fit")
    else:
        print("unfit")


#Ternaory operator:
#to avoid writing multiple line code using if, we can use ternaroy operator:

print("child" if(age<18) else print("adult"))

print(("unfit" if(eat_pizza) else "fit") if(age<30) else ("fit" if (exercise) else print("unfit")))
