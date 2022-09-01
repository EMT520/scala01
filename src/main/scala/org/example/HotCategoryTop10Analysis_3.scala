package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description 方案③：使用累加器的方式聚合数据
 *              方案①，②中存在大量的shuffle操作（因为获取点击数量、下单数量和支付数量时都使用了reduceByKey宽依赖）
 *              解决方案：
 *              重写代码：
 * @author hadoop
 * @date 2022/8/11 19:13
 */
object HotCategoryTop10Analysis_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc=new SparkContext(conf)
    //1.读取原始日志数据
    val fileRDD:RDD[String] = sc.textFile("e:/temp/user_visit_action.txt")
    //2.获取各项参数
    val cogroupRDD: RDD[(String, (Int, Int, Int))] =fileRDD.flatMap(line=>{
      val datas: Array[String] =line.split("_")
      if(datas(6)!="-1"){
        //点击:(品类ID,(1,0,0))
        List((datas(6),(1,0,0)))
      }else if(datas(8)!="null"){
        //下单:(品类ID,(0,1,0))
        val ids: Array[String] =datas(8).split(",")
        ids.map(id=>(id,(0,1,0)))
      }else if(datas(10)!="null"){
        //支付(品类ID,(0,0,1))
        val ids: Array[String] =datas(10).split(",")
        ids.map(id=>(id,(0,0,1)))
      }else{
        Nil
      }
    })
    //聚合
    val analysisRDD=cogroupRDD.reduceByKey((k1,k2)=>{
      (k1._1+k2._1,k1._2+k2._2,k1._3+k2._3)
    })
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
