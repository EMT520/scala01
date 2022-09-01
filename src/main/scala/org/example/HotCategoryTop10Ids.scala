package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @description 在需求一的基础上，增加每个品类用户 session 的点击统计
 *              【思路】
 *              （1）统计top10热门品类 =>[15,2,20,12,11,17,7,9,19,13]
 *              （2）过滤原始文件，保留热门品类top10的点击量
 *              （3）获取top10品类击量=> （ 品类ID，sessionId ）,sum)
 *              （4）转换=>(品类ID,(sessionId ）,sum)
 *              （5）按同类商品进行分组
 *              (6）将分组的值进行排序
 * @author hadoop
 * @date 2022/8/12 10:48
 */
object HotCategoryTop10Ids {
  /**
   * 获取Top10品类ID
   * @param fileRDD 文件RDD
   * @return
   */
  def GetTop10(fileRDD: RDD[String]):Array[String] = {

    //2.获取各项参数
    val cogroupRDD: RDD[(String, (Int, Int, Int))] = fileRDD.flatMap(line => {
      val datas: Array[String] = line.split("_")
      if (datas(6) != "-1") {
        //点击:(品类ID,(1,0,0))
        List((datas(6), (1, 0, 0)))
      } else if (datas(8) != "null") {
        //下单:(品类ID,(0,1,0))
        val ids: Array[String] = datas(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      } else if (datas(10) != "null") {
        //支付(品类ID,(0,0,1))
        val ids: Array[String] = datas(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })
    //聚合
    val reduceRDD = cogroupRDD.reduceByKey((k1, k2) => {
      (k1._1 + k2._1, k1._2 + k2._2, k1._3 + k2._3)
    })
    //4.排名
    //4.1 元组排名,先比较第一个,再比较第二个,再比较第三个,以此类推
    println("===元组排名===")
    val Top10RDD=reduceRDD.sortBy(_._2, false).take(10)
    //获取top10品类ID
    Top10RDD.map(_._1)


  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc=new SparkContext(conf)
    //1.读取原始日志数据
    val fileRDD:RDD[String] = sc.textFile("e:/temp/user_visit_action.txt")
    //2.获取Top10品类id
    val top10CategoryIds=GetTop10(fileRDD)
    //3.过滤原始文件，保留热门品类top10的点击量
    val filterRDD=fileRDD.filter(line=>{
      val datas=line.split("_")
      datas(6)!="-1" && top10CategoryIds.contains(datas(6))
    })
    //4.获取top10品类点击量=>(品类ID，sessionId ）,sum)
    val reduceRDD=filterRDD.map(line=>{
      val datas=line.split("_")
      ((datas(6),datas(2)),1)
    }).reduceByKey(_+_)
    //5.转换=>(品类ID，（sessionId ）,sum)
    val resultRDD=reduceRDD.map({
      case((cid,sid),sum)=>{
        (cid,(sid,sum))
      }
    })
    //6.按同类商品进行分组
    val groupRDD=resultRDD.groupByKey()
    //7.同类商品按点击量排名，取top10
    val sortRDD=groupRDD.mapValues(_.toList.sortBy(_._2).reverse.take(10))
    //8.打印输出
    sortRDD.foreach(list=>{
      println("品类："+list._1)
      list._2.foreach(println)
    })
  }
}
