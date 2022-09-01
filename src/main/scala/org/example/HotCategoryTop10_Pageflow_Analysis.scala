package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @description 使用样例类
 *              前面案例1,2,3都可以使用样例类来实现
 * @author hadoop
 * @date 2022/8/12 14:57
 */
/**
 * 样例类 用户访问动作表
 */
//case class UserVisitAction(
//                          date:String,  //用户点击行为的日期
//                          userId:Long,  //用户的ID
//                          sessionId:String, //Session的ID
//                          pageId:Long, //某个页面的ID
//                          actionTime:String, //动作的时间点
//                          searchKeyword:String, //用户搜索的关键词
//                          clickCategoryId:Long, //某一个商品品类ID
//                          clickProductId:Long, //某一个商品的ID
//                          orderCategoryIds:String, //一次订单中所有品类的ID集合
//                          orderProductIds:String, //一次订单中所有商品的ID集合
//                          payCategoryIds:String, //一次支付中所有品类的ID集合
//                          payproductIds:String, //一次支付中所有商品的ID集合
//                          cityId:Long //城市ID
//                          )
object HotCategoryTop10_Pageflow_Analysis {
  /**
   * 统计每个页面的访问次数
   * @param actionRDD
   * @return
   */
  def getPageVisitNumber(actionRDD:RDD[UserVisitAction]):Map[Long,Int]={
    val pvRDD = actionRDD.map(action=>{
      (action.pageId,1)
    }).reduceByKey(_+_)
    pvRDD.collect().toMap
  }
  /**
   * 统计A跳转到B的次数：（（1，2），10）=>从页1跳转到页2共十次
   * 1.根据session进行分组
   * 2.分组后，根据访问时间进行排序（升序）
   * 3.拉链：（1，2，3，4）=>（（1，2），（2，3），（3，4））
   * 4.聚合
   * @param actionRDD
   * @return
   */
  def getPageJumpNumber(actionRDD: RDD[UserVisitAction]):RDD[((Long,Long),Int)] ={
    val pvRDD:RDD[((Long,Long),Int)]=actionRDD.groupBy(_.sessionId).mapValues(iter=>{
      val sortList=iter.toList.sortBy(_.actionTime)
      val pageList=sortList.map(_.pageId)
      pageList.zip(pageList.tail).map((_,1))
    }).map(_._2).flatMap(list=>list).reduceByKey(_+_)
    pvRDD
  }
  /**
   * 按条件筛选跳转页面
   * 如：我们只统计页1-页2，页2-页3=>筛选条件List（（1，2），（3，4））
   *
   * @param filterList 筛选条件
   * @param actionRDD
   * @return
   */
  def getPageJumpNumber(filterList:List[(Int,Int)],actionRDD:RDD[UserVisitAction]):RDD[((Long,Long),Int)] = {
    val pvRDD:RDD[((Long,Long),Int)]=actionRDD.groupBy(_.sessionId).mapValues(iter=>{
      val sortList=iter.toList.sortBy(_.actionTime)
      val pageList=sortList.map(_.pageId)
      val zipList=pageList.zip(pageList.tail)
      zipList.filter(t=>{
        filterList.contains(t)
      }).map((_,1))
    }).map(_._2).flatMap(list=>list).reduceByKey(_+_)
    pvRDD
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10_PageflowAnalysis")
    val sc=new SparkContext(conf)
    //1.读取原始日志数据
    val fileRDD:RDD[String] = sc.textFile("e:/temp/user_visit_action.txt")
    val actionRDD=fileRDD.map(
      action=>{
        val datas=action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionRDD.cache()
    val a=getPageVisitNumber(actionRDD)
    val filterList=List((1,2),(2,3))
    val b=getPageJumpNumber(filterList,actionRDD)
    b.foreach({
      case((p1,p2),sum)=>{
        val lon=a.getOrElse(p1,0)
        val pv={
          if(lon>0){sum.toDouble/lon}else{0}
        }
        println(s"页面${p1}跳转到页面${p2}单跳转换率为:${pv*100}%")

      }
    })
  }

}
