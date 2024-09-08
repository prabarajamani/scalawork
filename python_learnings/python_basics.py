import os

def hello_world():
    print("Hello World")

def some_program():
    hello_world()
    print("Bye World")
    print('single quote')
    v = 1
    s = "one"
    #print(v)
    v = "three"
    print(v)
    print(__name__)


some_program()