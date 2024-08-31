object i_map_filter {

  val lst = List(1,2,3);
  val mymap = Map(1 -> "tom", 2 -> "praba", 3 -> "prem");

  def main(args:Array[String]): Unit = {

    println(lst.map(x => x/0.2));//List(5.0, 10.0, 15.0)
    println(lst.map(x => "hi" * x));//List(hi, hihi, hihihi)
    println(mymap.mapValues(x => "hi" + x));//MapView(<not computed>)
    println("hello".map(_.toUpper));//HELLO
    println(List(List(1,2,3), List(1,2,3)).flatten);//List(1, 2, 3, 1, 2, 3)
    println(lst.flatMap(x => List(x, x+1)));//List(1, 2, 2, 3, 3, 4)
    println(lst.map(x => List(x, x+1)));//List(List(1, 2), List(2, 3), List(3, 4))
    println(lst.filter(x => x%2!=0))//List(1, 3)
  }
}
