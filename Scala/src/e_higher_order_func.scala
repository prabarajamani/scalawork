object e_higher_order_func {

  /*take function as a arguments */

  def math(x: Double, y: Double, f: (Double, Double)=> Double): Double = f(x, y); /*f is a anonymous function*/
  def math(x: Double, y: Double, z: Double, f: (Double, Double)=> Double): Double = f(f(x, y),z);

  def main(args: Array[String]): Unit = {
    val result = math(50,20,(x,y) => x+y)
    val result1 = math(50,20,10, (x,y) => x min y)
    println("higher order function")
    println(result)
    println(result1)
    /*
    output
    70.0
    10.0
    */
  }
}
