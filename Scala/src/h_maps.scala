object h_maps {

  val mymap:  Map[Int, String] = Map(1 -> "scala", 2 -> "python", 3 -> "spark")

  def main(args: Array[String])={

    println(mymap);
    println(mymap(1));
    println(mymap.keys);
    println(mymap.values);
    println(mymap.isEmpty);

    mymap.keys.foreach{ key =>

      println("keys - " + key);
      println("values - " + mymap(key));
    }

  }

}
