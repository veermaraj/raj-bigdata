package raj.spark.practice



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkPractice {

  def main(args: Array[String]): Unit ={

   Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession.builder().appName("spark practice").master("local").getOrCreate()
    val array =Array(1,2,3,4,5,6,7,8,9)
   // val rdd =spark.sparkContext.parallelize(array)
   case class person(Id:Int ,Name:String)
    val df = spark.createDataFrame(Seq((1, "raj"), (2, "test"))).toDF("Id", "Name")
   /* val df2 = ds.withColumn("Name".(split($"Name", ""))).filter($"Name" =!= "")
    df.select("name".explode())
    F*/
    
  }
}