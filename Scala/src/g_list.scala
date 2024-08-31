object g_list {

  val mylist: List[Int] = List(1,2,3,4,5);
  val names: List[String] = List("PRABA", "TOM", "JERRY");

  def main(args: Array[String])={

    println(0 :: mylist); // :: --> cols is used to concat the values before the list ( o/p --> List(0, 1, 2, 3, 4, 5)
    println(mylist); // o/p --> List(1, 2, 3, 4, 5)
    println(names); // o/p --> List(PRABA, TOM, JERRY)
    println(mylist.head);
    println(names.tail);
    println(mylist.tail);
    println(names.isEmpty);
    println(mylist.reverse);
    println(List.fill(5)(2));
    println(mylist.max);


    mylist.foreach( println)
    var sum : Int = 1;
    mylist.foreach(sum += _) //
    println(sum);

    for (name <- names){

      println( "for loop" + name)
    }
  }
}
