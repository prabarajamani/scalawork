object if_else {


  def main(arg: Array[String]): Unit = {
    println("if_else start")
    var fruit = "apple"
    if (fruit == "apple")
      println("apple")
    else
      println("no fruit")

    var x = 80
    if(x >= 80)
       println("Grade A")
    else if (x >= 65 && x <= 80)
    println("grade B")
    else if (x >= 50 && x <= 65)
      println("grade C")
    else if (x >= 35 && x <= 50)
      println("grade D")
    else println ("fail")
  }

}
