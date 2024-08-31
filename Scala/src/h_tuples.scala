object h_tuples {


  val mytuple = (1,2,"hello", true);
  val mytuple2 = new Tuple3(1, 2, "Hello");
  val mytuple3 = new Tuple3(1, "Hello", (2,3));

  def main(args: Array[String])={

    println(mytuple._1);//1
    println(mytuple._2);//2
    println(mytuple._3);//hello
    println(mytuple._4);//true
    println(mytuple2._3);//hello
    println(mytuple3._3);//(2,3)
    println(mytuple3._3._2);//3


    mytuple.productIterator.foreach{
      i => println(i);
        /*1
        2
        hello
        true*/
    }
  }

}
