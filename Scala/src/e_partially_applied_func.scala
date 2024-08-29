import  java.util.Date

object e_partially_applied_func {

  def log(date : Date, message: String): Unit = {
    println(date + " - " + message)
  }

  def main(args: Array[String]):Unit = {

    val sum = (a: Int, b: Int, c:Int) => a + b + c

    val f = sum(10, 20, _ : Int)
    val g = sum(10, _ : Int , _ : Int)

    println(f(100)) // o/p --> 130
    println(g(100, 200)) // o/p --> 310

    val date = new Date;
    val newlog = log(date, _ : String);
    newlog(" The meessage"); // o/p --> Thu Aug 29 19:59:25 IST 2024 -  The meessage

  }
}
