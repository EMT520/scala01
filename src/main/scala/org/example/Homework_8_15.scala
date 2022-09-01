package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/15 23:32
 */
case class UserVisitAction(
                            date:String,  //用户点击行为的日期
                            userId:Long,  //用户的ID
                            sessionId:String, //Session的ID
                            pageId:Long, //某个页面的ID
                            actionTime:String, //动作的时间点
                            searchKeyword:String, //用户搜索的关键词
                            clickCategoryId:Long, //某一个商品品类ID
                            clickProductId:Long, //某一个商品的ID
                            orderCategoryIds:String, //一次订单中所有品类的ID集合
                            orderProductIds:String, //一次订单中所有商品的ID集合
                            payCategoryIds:String, //一次支付中所有品类的ID集合
                            payproductIds:String, //一次支付中所有商品的ID集合
                            cityId:Long //城市ID
                          )
object Homework_8_15 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val fileRDD: RDD[String] = sc.textFile("e:/temp/user_visit_action.txt")
    val actionRDD = fileRDD.map(
      action => {
        val datas = action.split("_")
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
    val ds = actionRDD.toDS()
    ds.createOrReplaceTempView("UserVisitAction")
    //清洗数据
    spark.sql(
      """
        |select count(actionTime) from UserVisitAction
        |where clickCategoryId <> '-1'
        |or orderCategoryIds <> 'null'
        |or payCategoryIds <> 'null'
        |""".stripMargin).show()

    //获取点击量
    val clickDS = spark.sql(
      """
        |select clickCategoryId as click_id,count(clickCategoryId) as click_num
        |from UserVisitAction
        |where clickCategoryId !=-1
        |group by clickCategoryId
        |""".stripMargin).createOrReplaceTempView("click")
    //获取下单量
    val OrderDS = spark.sql(
      """
        |select order_id,count(order_id) as order_num
        |from (select orderCategoryIds
        |from UserVisitAction
        |where orderCategoryIds !='null') order
        |lateral view explode(split(orderCategoryIds,',')) as order_id
        |group by order_id
        |""".stripMargin).createOrReplaceTempView("order")
    //获取支付量
    val PayDS = spark.sql(
      """
        |select pay_id,count(pay_id) as pay_num
        |from (select payCategoryIds
        |from UserVisitAction
        |where payCategoryIds !='null') pay
        |lateral view explode(split(payCategoryIds,',')) as pay_id
        |group by pay_id
        |""".stripMargin).createOrReplaceTempView("pay")
    //综合排序
    spark.sql(
      """
        |select click_id as id,(click_num*0.2+order_num*0.3+pay_num*0.5) as num
        |from click,order,pay
        |where click.click_id=order.order_id and click.click_id=pay.pay_id
        |order by num desc
        |limit 10
        |""".stripMargin).show()
    //优化排序
    spark.sql(
      """
        |select click_id as id,click_num,order_num,pay_num
        |from click,order,pay
        |where click.click_id=order.order_id and click.click_id=pay.pay_id
        |order by click_num desc,order_num desc,pay_num desc
        |limit 10
        |""".stripMargin).show()
    //综合排序（通过UDF）
    com_sum(spark)
    val sort2 = spark.sql(
      """
        |select click_id,com_sum(click_num,order_num,pay_num) sum
        |from click join order
        |on (click_id=order_id)
        |join pay on (order_id=pay_id)
        |order by sum
        |limit 10
        |""".stripMargin)
    sort2.show()


  }

  def com_sum(spark: SparkSession) = {
    spark.udf.register("com_sum", (x: Int, y: Int, z: Int) => {
      x * 0.2 + y * 0.3 + z * 0.5
    })
  }

}
