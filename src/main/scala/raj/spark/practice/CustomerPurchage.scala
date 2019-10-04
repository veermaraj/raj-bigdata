package raj.spark.practice

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import scala.math.max

object CustomerPurchage extends Serializable {

  def customerInformation(customer:String): (Int,Float)={
    val fields =customer.split(",")
    val customerId = fields(0).toInt
    val amount =fields(2).toFloat
    (customerId,amount)
  }
  def main(args:Array[String]):  Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val  spark = SparkSession.builder().appName("customer").master("local[*]").getOrCreate()
    val  customerData = spark.sparkContext.textFile("../SparkScala/customer-orders.csv")

    val customerinfo = customerData.map(customerInformation)
    val data =customerinfo.reduceByKey(_+_)
    val data1=data.d
    data1.
    data.foreach(println)

    val df = spark.createDataFrame(data).toDF("customerID","purchage")
    df.show()
    df.printSchema()



  }

}
