package main.scala.raj.spark.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
object studentDetails {
  case class student(student_id:Int,exam_center_id:Int,subject: String, year:Int, quarter:Int, score:String, grade:String)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val  df =spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/students.csv")
    val subjectResults =df.filter(row=>row.getAs("subject").equals("Spanish")
                   && row.getAs("year") == 2007)
    //&& Integer.parseInt(row.getAs("year") )== 2007)
    //subjectResults.show(20)
    df.printSchema()

    //println("Total record in csv : "+df.count())
    val ds =df.as[student]
   // ds.show(100)
   // ds.select($"subject").show()
    /*ds.filter(x=>x.student_id>200 && x.subject=="Math").show()
    ds.groupBy("subject").avg("score").show()
    ds.groupBy($"subject").min("score").show()*/
    //ds.orderBy($"student_id".desc,$"subject").show()
    //ds.select().show()
    //val ds1 = ds.groupBy($"subject").count()
    // val columns =List("A",   "A+",    "B",   "C",   "D",  "E",  "X")
    /*val ds1 = ds.groupBy("Subject").pivot("year").agg(round(avg("score")
      ,2).alias("Average"),
                   round(stddev("score"),2).alias("StdDev"))*/

    //val ds2 =ds.groupBy("subject").agg(max("score").cast("Int"))










    spark.close()

  }

}
