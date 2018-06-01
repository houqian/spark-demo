package org.houqian.spark.test

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._


/**
  * daas、ff对数
  *
  * @author : houqian
  * @since : 2018/4/17
  * @version : 1.0
  */
object DiffDassAndFF extends App {
  val spark = SparkSession.builder.appName("DiffDassAndFF")
    .master("local[2]")
    .getOrCreate()

  val folder = "/Users/finup/Desktop/nifi_data_monitor/"

  val qianzhan = spark.read.avro(folder + "qianzhan_2018-04-23.0.avro").toDF()
  qianzhan.createOrReplaceTempView("qianzhan")
  spark.sql("select * from qianzhan order by update_time desc limit 10").show()
  println("上面是钱站最新10条：")
//  qianzhan.write.option("header", "false").csv("/Users/finup/Desktop/qianzhan.csv")

  val aiqianjin = spark.read.avro(folder + "aiqianjin_2018-04-23.0.avro").toDF()
  aiqianjin.createOrReplaceTempView("aiqianjin")
  spark.sql("select * from aiqianjin order by actiontime desc limit 10").show()
  println("上面是爱钱进最新10条：")
  //  aiqianjin.write.option("header", "false").csv("/Users/finup/Desktop/aiqianjin.csv")


}
