package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/15 16:59
 */
object RDD转换DataSet {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD2DataSet")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //读取本地文件
    val fileRDD = sc.textFile("e:/temp/score.txt")
    val studentRDD = fileRDD.map(line => {
      val lines = line.split("\t")
      Student(lines(0), lines(1), lines(2).toDouble)
    })
    //注册UDF
    getPoint(spark)
    //转换DataSet

    import spark.implicits._

    val ds = studentRDD.toDS()
    //创建临时表
    ds.createOrReplaceTempView("student")

    val ds1 = spark.sql(
      """
        |select id,name,score,
        |getPoint(score) point
        |from student
        |""".stripMargin)
    ds1.show()



  }
  def getPoint(spark:SparkSession) = {
    spark.udf.register("getPoint", (score: Double) => {
      if (score >= 60) 4
      else 0
    })
  }
  case class Student (id:String, name:String,score:Double)
}

