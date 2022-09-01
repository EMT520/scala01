package org.Spark

import org.apache.spark.sql.DataFrame
import org.example.SparkOnHiveUtil
object HotCategoryTop10Hive {
  //从hive表中清洗数据
  def getData(): DataFrame = {
    val sql =
      """
        |select session_id,click_category_id,order_category_ids,
        |pay_category_ids from user_visit_action
        |where click_category_id<>'-1'
        |or order_category_ids[0]<>'null'
        |or pay_category_ids[0]<>'null'
        |""".stripMargin
    val df = SparkOnHiveUtil.query(sql)
    df
  }
  //提取点击量
  def getClickSum(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("temp")
    val sql =
      """
        |select click_category_id c_id,count(1) click_sum
        |from user_visit_clean where click_category_id <> '-1'
        |group by c_id
        |""".stripMargin
    val ds = SparkOnHiveUtil.query(sql)
    ds
  }
  //提取下单量
  def getOrderSum(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("temp")
    val sql =
      """
        |select c_id,count(1) order_sum from
        |(select explode(order_category_ids) as c_id
        |from temp where order_category_ids[0] <> 'null') a
        |group by c_id
        |""".stripMargin
    val ds = SparkOnHiveUtil.query(sql)
    ds
  }
  //提取支付量
  def getPaySum(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("temp")
    val sql =
      """
        |select c_id,count(1) pay_sum from
        |(select explode(pay_category_ids) as c_id
        |from temp where pay_category_ids[0] <> 'null') a
        |group by c_id
        |""".stripMargin
    val ds = SparkOnHiveUtil.query(sql)
    ds
  }
  //热门品类top10
  def getTop10Category(clickDF: DataFrame, ordefDF: DataFrame, payDF: DataFrame): DataFrame = {
    clickDF.createOrReplaceTempView("t_click")
    ordefDF.createOrReplaceTempView("t_order")
    payDF.createOrReplaceTempView("t_pay")
    val sql =
      """
        |select a.c_id,a.click_sum,b.order_sum,c.pay_sum
        |from t_click a join t_order b
        |on(a.c_id = b.c_id)
        |join t_pay c on(b.c_id = c.c_id)
        |order by a.click_sum desc,
        |b.order_sum desc,c.pay_sum desc
        |limit 10
        |""".stripMargin
    val ds = SparkOnHiveUtil.query(sql)
    ds
  }
  //提取品类的用户点击量
  def getCategoryAndSessionID(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("temp")
    val sql =
      """
        |select c_id,s_id,sum(num) click_sum from
        |(select click_category_id c_id,session_id s_id,1 as num
        |from temp where click_category_id <> '-1') a
        |group by c_id,s_id
        |""".stripMargin
    val ds = SparkOnHiveUtil.query(sql)
    ds
  }
  //提取热门品类的用户点击量
  def getTop10CategoryWithSessionIDTop10(top10Category: DataFrame, top10SessionID: DataFrame): DataFrame = {
    top10Category.createOrReplaceTempView("t_category")
    top10SessionID.createOrReplaceTempView("t_session")
    val sql =
    """
      |select c_id,s_id,click_sum
      |from (select a.c_id,b.s_id,b.click_sum,
      |row_number() over(
      |partition by a.c_id
      |order by b.click_sum desc) rank
      |from t_category a join t_session b
      |on (a.c_id=b.c_id)) a
      |where rank <=10
      |""".stripMargin
    val ds = SparkOnHiveUtil.query(sql)
    ds
  }
  def main(args: Array[String]): Unit = {
    val df = getData()
    val clickDF = getClickSum(df)
    val orderDF = getOrderSum(df)
    val payDF = getPaySum(df)
    val top10Category = getTop10Category(clickDF, orderDF, payDF)
    val top10SessionID = getCategoryAndSessionID(df)
    val top10 = getTop10CategoryWithSessionIDTop10(top10Category, top10SessionID)
    top10.show()
    org.example.HotCategory_SaveToMysql.saveMysql("user_visit_action_top10", top10)
  }
}
//    val sql =
//      """
//        |select a.c_id,b.s_id,b.click_sum from
//        |t_category a join t_session b
//        |on(a.c_id = b.c_id)
//        |""".stripMargin