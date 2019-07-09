package scalaOne.scalaprograms

import org.apache.spark._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders._;
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._;

object Hackathon2 {
  
  case class Insure(id1: String, id2: String,yr: String, stcd: String, srcnum: String,
                    network: String, url: String);
  
  def main(args:Array[String]) {
    
  val ss = SparkSession.builder().appName("Hackathon").master("local[*]").getOrCreate();
  val sc = ss.sparkContext;
  val sqlc = ss.sqlContext;
  sc.setLogLevel("ERROR");
  
   def masking(insurance_data:String):String =
   {
    val mask = insurance_data.replace("a","x").replace("b","y").replace("c","z").replace("d","l").replace("m","n").replace("n","o").replaceAll("[0-9]","1")
    return mask
  }  
  
  val insuranceinfo1 = sc.textFile("hdfs://localhost:54310/user/hduser/Hack2/insuranceinfo1.csv", 2)
  
  val insuranceinfo2 = sc.textFile("hdfs://localhost:54310/user/hduser/Hack2/insuranceinfo2.csv", 2)
  
  val insuranceinfo_header1=insuranceinfo1.first()
  
  val insuranceinfo1_rawdata = insuranceinfo1.filter { x => x!= insuranceinfo_header1 }
  
  val insuranceinfo1_sampledata =insuranceinfo1_rawdata.take(10)
  
  insuranceinfo1_sampledata.foreach(println)
  
  val insuranceinfo_header2=insuranceinfo2.first()
  
  val insuranceinfo2_rawdata = insuranceinfo2.filter{ x => x != insuranceinfo_header2}
  
  val insuranceinfo2_sampledata =insuranceinfo2_rawdata.take(10)
  
  insuranceinfo2_sampledata.foreach(println)
  
  val insuredatamerged = insuranceinfo1_rawdata.union(insuranceinfo2_rawdata)
  
  val insuredatamergeddistinct=insuredatamerged.distinct()
  
  insuredatamergeddistinct.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
  
  val custs_states = sc.textFile("hdfs://localhost:54310/user/hduser/Hack2/custs_states.csv", 2)
  
  val state_filter = custs_states.map(x=>x.split(",")).filter(l =>l.length==2)
  
  val states_info = state_filter.take(5).map(x => x.foreach (println))
  
  val customer_filter = custs_states.map(x=>x.split(",")).filter(l =>l.length==5)
  
  val customer_info = customer_filter.take(5).map(x => x.foreach (println))
  
  import ss.implicits._
 
  import org.apache.spark.sql.functions.udf
  
  ss.udf.register("mas",masking _)
  ss.catalog.listFunctions.filter('name like "%masking%").show(false)
  
  val insurance_data = insuredatamergeddistinct.map(x=>x.split(",")).map(p=>Insure(p(0),p(1),p(2),p(3),p(4),p(5),masking(p(6))))
   
  val ins = ss.createDataFrame(insurance_data)
  
  ins.createOrReplaceTempView("insurance")
  
  val results = sqlc.sql("select id1,id2,concat(stcd,network) as ,url from insurance")
  
  results.show(10, false)
  
  results.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/Hack2/results.json");
  results.write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/Hack2/result_parquet.parquet");
   
  }
}
