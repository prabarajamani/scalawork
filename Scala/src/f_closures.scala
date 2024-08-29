
import java.util.Date


/*A closures is a function which uses one or more variable declared outside this function*/
/*closure will take the latest variable values */

object f_closures {

  var number = 10;
  var num = 10;
  val add = (x: Int) => x+ number;
  val add1 = (x: Int) => x+ num;

  def main(args: Array[String]) = {
    num = 100
    println(add(20));// o/p --> 30
    println(add1(20)); // o/p --> 120
  }
}
