object g_array {

  val myarray: Array[Int] = new Array[Int](4);
  val myarray2 = new Array[Int](5);
  def main(args : Array[String]) = {

    myarray(0) = 1;
    myarray(1) = 2;
    myarray(2) = 3;
    myarray(3) = 4;
    println(myarray)

    for ( x <- myarray){
      println(x);
    }

    for (x <- 0 to myarray.length - 1){
      println(myarray(x));
    }
  }
}
