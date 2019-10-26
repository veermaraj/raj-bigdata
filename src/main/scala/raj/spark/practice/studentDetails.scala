package main.scala.raj.spark.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object studentDetails {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =SparkSession.builder().appName("Student")
        .master("local[*]").getOrCreate();

    val  ds =spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/students.csv")
    val subjectResults =ds.filter(row=>row.getAs("subject").equals("Spanish"))

    subjectResults.show(20)
    ds.printSchema()
    println(subjectResults.count())
    println("Total record in csv : "+ds.count())
    spark.close()

  }

}
