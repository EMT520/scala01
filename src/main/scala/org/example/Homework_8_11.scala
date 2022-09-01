package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/11 20:13
 */
object Homework_8_11 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Analysis")
    val sc = new SparkContext(conf)
    //1.读取原始日志数据
    val fileRDD: RDD[String] = sc.textFile("e:/temp/user_visit_action.txt")
    KeyWord(fileRDD: RDD[String])
    Order(fileRDD: RDD[String])
  }
  //作业1:统计每个用户搜索关键字Top3

  def KeyWord(fileRDD: RDD[String])= {

    val RDD1 = fileRDD.filter(line => {
      val datas = line.split("_")
      datas(5) != "null"
    }).map(line => {
      val datas = line.split("_")
      ((datas(1), datas(5)), 1)
    }).reduceByKey(_ + _)
    val RDD2 = RDD1.map(x => {
      (x._1._1, (x._1._2, x._2))
    }).groupByKey().mapValues(_.toList.sortBy(_._2).reverse.take(3))
    RDD2.foreach(println)

  }

  //作业2:统计每个城市下单最多的Top10
  def Order(fileRDD: RDD[String]) = {

    val RDD1 = fileRDD.filter(line => {
      val datas = line.split("_")
      datas(8) != "null"
    }).map(line => {
      val datas = line.split("_")
      (datas(12), 1)
    }).reduceByKey(_ + _).sortBy(_._2, false).take(10).foreach(println)

  }
}
