package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @description （1）页面单跳转化率
 *              计算页面单跳转化率，什么是页面单跳转换率，比如一个用户在一次 Session 过程中访问的页面路径。如3,5,7,9,10,21，那么页面 3 跳到页面 5 叫一次单跳，7-9 也叫一次单跳，那么单跳转化率就是要统计页面点击的概率。
 *              比如：计算 3-5 的单跳转化率，先获取符合条件的 Session 对于页面 3 的访问次数（PV）为 A，然后获取符合条件的 Session 中访问了页面 3 又紧接着访问了页面 5 的次数为 B，那么 B/A 就是 3-5 的页面单跳转化率。
 *              （2）统计页面单跳转化率意义
 *              产品经理和运营总监，可以根据这个指标，去尝试分析，整个网站，产品，各个页面的表现怎么样，是不是需要去优化产品的布局；吸引用户最终可以进入最后的支付页面。数据分析师，可以此数据做更深一步的计算和分析。企业管理层，可以看到整个公司的网站，各个页面的之间的跳转的表现如何，可以适当调整公司的经营战略或策略
 * @author hadoop
 * @date 2022/8/12 14:10
 */
/*
本题难点在于如何统计A页面跳转到B页面的次数。
如页1跳转到页2次数为10，页4跳转到页9的次数为2=>（（（1，2），10），（（4，9），2））。
1、根据SessionID筛选出一个Session访问的所有页面
（SessionID，（页面ID，动作时间））
2、按SessionID分组
3、对每个SessionID的List操作：
（1）按动作时间排序，统计出每个SessionID连续访问的页面
（2）去掉时间字段
（3）拉链
（4）转换
4、将List取出并压平聚合
 */
object HotCategoryTop10_PageflowAnalysis {
  /**
   * 统计每个页面的访问次数
   * @param fileRDD
   * @return
   */
  def getPageVisitNumber(fileRDD:RDD[String]):Map[Int, Int] = {
    val pvRDD = fileRDD.map(line=>{
      val datas=line.split("_")
      ((datas(3).toInt,1))
    }).reduceByKey(_+_)
    pvRDD.collect().toMap
  }

  /**
   * 统计A跳转到B的次数：（（1，2），10）=>从页1跳转到页2共十次
   * 1.根据session进行分组
   * 2.分组后，根据访问时间进行排序（升序）
   * 3.拉链：（1，2，3，4）=>（（1，2），（2，3），（3，4））
   * 4.聚合
   * @param fileRDD
   * @return
   */
  def getPageJumpNumber(fileRDD: RDD[String]):RDD[((Int, Int), Int)] ={
    val pvRDD=fileRDD.map(line=>{
      val datas=line.split("_")
      (datas(2),(datas(3).toInt,datas(4)))
    }).groupByKey().mapValues(iter=>{
      val sortList=iter.toList.sortBy(_._2)
      val pageList=sortList.map(_._1)
      pageList.zip(pageList.tail).map((_,1))
    }).map(_._2).flatMap(list=>list).reduceByKey(_+_)
    pvRDD
  }

  /**
   * 按条件筛选跳转页面
   * 如：我们只统计页1-页2，页2-页3=>筛选条件List（（1，2），（3，4））
   *
   * @param filterList 筛选条件
   * @param fileRDD
   * @return
   */
  def getPageJumpNumber(filterList:List[(Int,Int)],fileRDD:RDD[String]):RDD[((Int,Int),Int)] = {
    val pvRDD=fileRDD.map(line=>{
      val datas=line.split("_")
      (datas(2),(datas(3).toInt,datas(4)))
    }).groupByKey().mapValues(iter=>{
      val sortList=iter.toList.sortBy(_._2)
      val pageList=sortList.map(_._1)
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
    val a=getPageVisitNumber(fileRDD)
    //val b=getPageJumpNumber(fileRDD)
    val filterList = List((1,2),(2,3))
    val b=getPageJumpNumber(filterList,fileRDD)
    b.foreach({
      case((p1,p2),sum)=>{
        val lon=a.getOrElse(p1,0)
        val pv={
          if(lon>0)(sum.toDouble/lon)else{0}
        }
        println(s"页面${p1}跳转到页面${p2}单跳转换率为:${pv*100}%")
      }
    })
  }

}
