object default_val {


  object Math {

    def +(x: Int , Y: Int) =
      x + Y;

    def **(x: Int) = x*x;
  }


  def main(args: Array[String]): Unit = {

    val add = (x : Int, y: Int) => x + y;
    println(add(100, 200))
    val sum = 10 + 20;
    println(Math.+(5,6))
    println(Math.**(5))
    println(Math ** 3)
  }
}
