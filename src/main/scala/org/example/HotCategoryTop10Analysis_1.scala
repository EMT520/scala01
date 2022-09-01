package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description 电商网站的用户行为数据，主要包含用户的 4 种行为：搜索，点击，下单，支付。数据规则如下：
 *              ● 数据文件中每行数据采用下划线分隔数据
 *              ● 每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
 *              ● 如果搜索关键字为 null,表示数据不是搜索数据
 *              ● 如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据
 *              ● 针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个，id 之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
 *              ● 支付行为和下单行为类似
 *              数据格式:
 *              日期_用户ID_Session ID_页面ID_动作时间_搜索关键字_点击品类_点击产品ID_下单品类ID_下单产品ID_支付品类ID_支付产品ID_城市ID
 * @author hadoop
 * @date 2022/8/11 14:40
 */
/*
方案①：分别统计每个品类点击的次数，下单的次数和支付的次数：
（品类，点击总数）（品类，下单总数）（品类，支付总数)
 */
/*
原始日志处理类
 */
class CategoryLog(fileRDD:RDD[String]) extends Serializable {
  /**
   * 筛选行为数据
   * @param index 行为下标
   * @param condition 筛选条件
   * @return *_*_*_..._*
   */
  def filter(index:Int, condition:String):RDD[String] ={
    val linesRDD=fileRDD.filter(line=>{
      val datas=line.split("_")
      datas(index)!=condition
    })
    linesRDD
  }

  /**
   * 行为聚合
   * @param index 行为下标
   * @param linesRDD 数据集合
   * @return RDD(key,count)
   */
  def reduce (index:Int, linesRDD:RDD[String]):RDD[(String,Int)]={
    val tuplesRDD = linesRDD.map(line=>{
      val datas=line.split("_")
      (datas(index),1)
    }).reduceByKey(_+_)
    tuplesRDD
  }

  /**
   * 行为聚合(多项目)
   * @param index 行为下标
   * @param linesRDD 数据集合
   * @return RDD(key,count)
   */
  def reduceByMultiple (index:Int, linesRDD:RDD[String]):RDD[(String,Int)]={
    val tuplesRDD = linesRDD.flatMap(line=>{
      val datas=line.split("_")
      val items = datas(index).split(",")
      items.map((_,1))
    }).reduceByKey(_+_)
    tuplesRDD
  }

  /**
   * 连接分组
   * @param clickCountRDD
   * @param orderCountRDD
   * @param payCountRDD
   * @return RDD(key,(x,y,z))
   */
  def cogroup(clickCountRDD: RDD[(String,Int)],
              orderCountRDD: RDD[(String,Int)],
              payCountRDD: RDD[(String,Int)]):RDD[(String,(Int,Int,Int))]={
    val cogroupRDD=clickCountRDD.cogroup(orderCountRDD,payCountRDD)
    val analysisRDD=cogroupRDD.mapValues({
      case(clickIter,orderIter,payIter)=>{
        (clickIter.sum,orderIter.sum,payIter.sum)
      }
    })
    analysisRDD
  }
}
object HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CategoryLog]))
    val sc=new SparkContext(conf)
    //1.读取原始日志数据
    val fileRDD:RDD[String]=sc.textFile("e:/temp/user_visit_action.txt")
    //2.获取各项参数
    val categoryLog=new CategoryLog(fileRDD)
    //2.1统计品类的点击数量:(品类ID,点击数量)
    val clickLinesRDD=categoryLog.filter(6,"-1")
    val clickCountRDD=categoryLog.reduce(6,clickLinesRDD)
    //2.2统计品类的下单数量:(品类ID,下单数量)
    val orderLinesRDD=categoryLog.filter(8,"null")
    val orderCountRDD=categoryLog.reduceByMultiple(8,orderLinesRDD)
    //2.3 统计品类的支付数量:(品类ID,支付数量)
    val payLinesRDD =categoryLog.filter(10,"null")
    val payCountRDD =categoryLog.reduceByMultiple(10,payLinesRDD)
    //3.连接分组(品类ID,(点击数量,下单数量,支付数量))
    val analysisRDD=categoryLog.cogroup(clickCountRDD,orderCountRDD,payCountRDD)
    //4.排名
    //4.1 元组排名,先比较第一个,再比较第二个,再比较第三个,以此类推
    println("===元组排名===")
    analysisRDD.sortBy(_._2,false).take(10).foreach(println)
    //4.2 综合排序,点击数*20%+下单数*30%+支付数*50%
    val resultRDD = analysisRDD.mapValues({
      case(x,y,z)=>{
        (x*0.2+y*0.3+z*0.5)
      }
    })
    println("===综合排名===")
    resultRDD.sortBy(_._2,false).take(10).foreach(println)

  }

}
