package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/12 16:39
 */
object 双value类型 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("双value")
    val sc=new SparkContext(conf)
    //1.intersection:对源RDD和参数RDD求交集后返回一个新的RDD
    val rdd1=sc.makeRDD(List(1,2,3,4))
    val rdd2=sc.makeRDD(List(3,4,5,6))
    val rdd=rdd1.intersection(rdd2)
    rdd.foreach(println)
    println

    //2.union:对源RDD和参数RDD求并集后返回一个新的RDD
    val rdd3: RDD[Int] =rdd1.union(rdd2)
    rdd3.foreach(println)
    println

    //3.subtract:以一个RDD元素为主，去除两个RDD中的重复元素，将其他元素保留下来，求差集
    val rdd4=rdd1.subtract(rdd2)
    rdd4.foreach(println)
    println

    //4.zip：将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的key为第一个RDD中的元素，
    // value为第二个RDD中的相同位置的元素
    val rdd5=rdd1.zip(rdd2)
    rdd5.foreach(println)
    sc.stop()



  }

}
