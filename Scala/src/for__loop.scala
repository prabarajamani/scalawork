object for__loop {

  def main(arg: Array[String]): Unit = {

    for (i <- 1 to 10) {
      println(i)
    }

    for (i <- 1.to(5)) {
      println("i using to "  + i)
    }

    for (i <- 1 until 5 ) {
      println("i using until "  + i)
    }

    for (i <- 1 to 5; j <- 1 to 3 ) {
      println("i using until "  + i + " j : " + j)
    }

    val lst = List(1,2,3,4,5);

    for (i <- lst ){
      println (" print using list " + i)
    }

    for (i <- lst; if i < 3 ){
      println (" print using filter " + i)
    }

    val result = for {i <- lst; if i < 3 } yield
    {
      i*i
    }

    println("result : " + result)
  }

  /*
  output
  1
  2
  3
  4
  5
  6
  7
  8
  9
  10
  i using to 1
  i using to 2
  i using to 3
  i using to 4
  i using to 5
  i using until 1
  i using until 2
  i using until 3
  i using until 4
  i using until 1 j : 1
  i using until 1 j : 2
  i using until 1 j : 3
  i using until 2 j : 1
  i using until 2 j : 2
  i using until 2 j : 3
  i using until 3 j : 1
  i using until 3 j : 2
  i using until 3 j : 3
  i using until 4 j : 1
  i using until 4 j : 2
  i using until 4 j : 3
  i using until 5 j : 1
  i using until 5 j : 2
  i using until 5 j : 3
  print using list 1
  print using list 2
  print using list 3
  print using list 4
  print using list 5
  print using filter 1
  print using filter 2
  result : List(1, 4)
*/
}
