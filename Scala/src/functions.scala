object functions {

  object Math {

    def add(x: Int , Y: Int) =
      x + Y;

    def square(x: Int) = x*x;
  }

  def add(x: Int, y: Int): Int = {
    return x + y;
  }

  def substract(x: Int, y: Int): Int = {
    x - y;
  }

  def multiply(x: Int, y: Int): Int = x * y;


  def divide(x: Int, y: Int): Int =
    x/y;

  def main(args: Array[String]): Unit = {
    println(add(5,6))
    println(substract(10,6))
    println(multiply(10,6))
    println(divide(10,2))
    println(Math.add(5,6))
    println(Math.square(5))
    println(Math square 3) /*if it single arqument we can call the function with space*/
    /*
    output
    11
    4
    60
    5
    11
    25
    9*/
  }

}
