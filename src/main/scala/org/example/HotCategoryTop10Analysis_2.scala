package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @description 方案②：一次性统计每个品类点击的次数，下单的次数和支付的次数：
 *              （品类，（点击总数，下单总数，支付总数））
 *              方案①中使用cogroup性能可能较低。
 *              解决方案：
 *              1、转换元组
 *              ①(品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
 *              ②(品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
 *              ③(品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
 *              2、聚合
 *              ①，②，③合并后再聚合
 *              3、修改程序代码
 *              （1）在CategoryLog类中重载方法：
def cogroup(clickCountRDD: RDD[(String, (Int, Int, Int))],
              orderCountRDD: RDD[(String, (Int, Int, Int))],
              payCountRDD: RDD[(String, (Int, Int, Int))]): RDD[(String, (Int, Int, Int))] = {
    val cogroupRDD = clickCountRDD.union(orderCountRDD).union(payCountRDD)
    val analysisRDD = cogroupRDD.reduceByKey(
      (k1, k2) => {
        (k1._1 + k2._1, k1._2 + k2._2, k1._3 + k2._3)
      })
    analysisRDD
  }
 *              （2）修改改主方法：
 *              将以下代码段
 *              //2.1统计品类的点击数量:(品类ID,点击数量)
 *              val clickLinesRDD=categoryLog.filter(6,"-1")
 *              val clickCountRDD=categoryLog.reduce(6,clickLinesRDD)
 *              //2.2统计品类的下单数量:(品类ID,下单数量)
 *              val orderLinesRDD=categoryLog.filter(8,"null")
 *              val orderCountRDD=categoryLog.reduceByMultiple(8,orderLinesRDD)
 *              //2.3 统计品类的支付数量:(品类ID,支付数量)
 *              val payLinesRDD =categoryLog.filter(10,"null")
 *              val payCountRDD =categoryLog.reduceByMultiple(10,payLinesRDD)
 *              修改为:
 *              //2.1统计品类的点击数量:(品类ID,(点击数量,0,0))
 *              val clickLinesRDD=categoryLog.filter(6,"-1")
 *              val clickCountRDD=categoryLog.reduce(6,clickLinesRDD).map({
 *              case(id,num) =>{
 *              (id,(num,0,0))
 *              }}
 *              )
 *              //2.2统计品类的下单数量:(品类ID,(0,下单数量,0))
 *              val orderLinesRDD=categoryLog.filter(8,"null")
 *              val orderCountRDD=categoryLog.reduceByMultiple(8,orderLinesRDD).map({
 *              case(id,num) =>{
 *              (id,(0,num,0))
 *              }})
 *              //2.3 统计品类的支付数量:(品类ID,(0,0,支付数量))
 *              val payLinesRDD =categoryLog.filter(10,"null")
 *              val payCountRDD =categoryLog.reduceByMultiple(10,payLinesRDD).map({
 *              case(id,num) =>{
 *              (id,(0,0,num))}})
 * @author hadoop
 * @date 2022/8/11 16:55
 */

class CategoryLog1(fileRDD: RDD[String]) extends Serializable {
  /**
   * 筛选行为数据
   *
   * @param index     行为下标
   * @param condition 筛选条件
   * @return *_*_*_..._*
   */
  def filter(index: Int, condition: String): RDD[String] = {
    val linesRDD = fileRDD.filter(line => {
      val datas = line.split("_")
      datas(index) != condition
    })
    linesRDD
  }

  /**
   * 行为聚合
   *
   * @param index    行为下标
   * @param linesRDD 数据集合
   * @return RDD(key,count)
   */
  def reduce(index: Int, linesRDD: RDD[String]): RDD[(String, Int)] = {
    val tuplesRDD = linesRDD.map(line => {
      val datas = line.split("_")
      (datas(index), 1)
    }).reduceByKey(_ + _)
    tuplesRDD
  }

  /**
   * 行为聚合(多项目)
   *
   * @param index    行为下标
   * @param linesRDD 数据集合
   * @return RDD(key,count)
   */
  def reduceByMultiple(index: Int, linesRDD: RDD[String]): RDD[(String, Int)] = {
    val tuplesRDD = linesRDD.flatMap(line => {
      val datas = line.split("_")
      val items = datas(index).split(",")
      items.map((_, 1))
    }).reduceByKey(_ + _)
    tuplesRDD
  }

  /**
   * 连接分组
   *
   * @param clickCountRDD
   * @param orderCountRDD
   * @param payCountRDD
   * @return RDD(key,(x,y,z))
   */
  def cogroup(clickCountRDD: RDD[(String, (Int, Int, Int))],
              orderCountRDD: RDD[(String, (Int, Int, Int))],
              payCountRDD: RDD[(String, (Int, Int, Int))]): RDD[(String, (Int, Int, Int))] = {
    val cogroupRDD = clickCountRDD.union(orderCountRDD).union(payCountRDD)
    val analysisRDD = cogroupRDD.reduceByKey(
      (k1, k2) => {
        (k1._1 + k2._1, k1._2 + k2._2, k1._3 + k2._3)
      })
    analysisRDD
  }
}

object HotCategoryTop10Analysis_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CategoryLog1]))
    val sc = new SparkContext(conf)
    //1.读取原始日志数据
    val fileRDD: RDD[String] = sc.textFile("e:/temp/user_visit_action.txt")
    //2.获取各项参数
    val categoryLog = new CategoryLog1(fileRDD)
    //2.1统计品类的点击数量:(品类ID,(点击数量,0,0))
    val clickLinesRDD = categoryLog.filter(6, "-1")
    val clickCountRDD = categoryLog.reduce(6, clickLinesRDD).map({
      case (id, num) => {
        (id, (num, 0, 0))
      }
    }
    )
    //2.2统计品类的下单数量:(品类ID,(0,下单数量,0))
    val orderLinesRDD = categoryLog.filter(8, "null")
    val orderCountRDD = categoryLog.reduceByMultiple(8, orderLinesRDD).map({
      case (id, num) => {
        (id, (0, num, 0))
      }
    })
    //2.3 统计品类的支付数量:(品类ID,(0,0,支付数量))
    val payLinesRDD = categoryLog.filter(10, "null")
    val payCountRDD = categoryLog.reduceByMultiple(10, payLinesRDD).map({
      case (id, num) => {
        (id, (0, 0, num))
      }
    })
    //3.连接分组(品类ID,(点击数量,下单数量,支付数量))
    val analysisRDD = categoryLog.cogroup(clickCountRDD, orderCountRDD, payCountRDD)
    //4.排名
    //4.1 元组排名,先比较第一个,再比较第二个,再比较第三个,以此类推
    println("===元组排名===")
    analysisRDD.sortBy(_._2, false).take(10).foreach(println)
    //4.2 综合排序,点击数*20%+下单数*30%+支付数*50%
    val resultRDD = analysisRDD.mapValues({
      case (x, y, z) => {
        (x * 0.2 + y * 0.3 + z * 0.5)
      }
    })
    println("===综合排名===")
    resultRDD.sortBy(_._2, false).take(10).foreach(println)

  }

}
