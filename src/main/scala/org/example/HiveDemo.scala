package org.example

import org.apache.spark.sql.SparkSession

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/16 15:21
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    //连接hive，在创建SparkSession的代码中添加Hive的支持，enableHiveSupport
    val spark=SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()
    val df=spark.sql(
      """
        |select user_id,click_category_id,order_category_ids,pay_category_ids
        |from user_visit_action limit 10
        |""".stripMargin)
    val rdd=df.rdd
    rdd.foreach(println)
  }

}
