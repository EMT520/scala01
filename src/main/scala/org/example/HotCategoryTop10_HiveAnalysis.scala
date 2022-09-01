package org.example

import org.apache.spark.sql.DataFrame

/**
 * @description 4.2.2.1 数据清洗
 *              1、需求：
 *              按照每个品类的点击、下单、支付的量来统计热门品类。
 *              （1）综合排名 = 点击数x20%+下单数x30%+支付数x50%
 *              （2）先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
 *              从需求得知，从数据仓库中清洗出点击、下单、支付的商品量即可。
 * @author hadoop
 * @date 2022/8/16 16:44
 */
object HotCategoryTop10_HiveAnalysis {
  //创建表user_visit_clean
  def createTable()={
    val dml=
      """
        |create table if not exists user_visit_clean(
        | click_category_id string,
        | order_category_ids array<string>,
        | pay_category_ids array<string>
        |)
        |""".stripMargin
    SparkOnHiveUtil.createTable(dml)
  }

  //从表user_visit_action中获取点击、下单、支付的量,这个表在hive里面
  def getData():DataFrame={
    val sql=
      """
        |select click_category_id,order_category_ids,pay_category_ids
        | from user_visit_action
        | where click_category_id <> '-1'
        | or order_category_ids[0] <> 'null'
        | or pay_category_ids[0] <> 'null'
        |""".stripMargin
    val df=SparkOnHiveUtil.query(sql)
    df
  }

  //将清洗的数据保存到user_visit_clean
  def insertData(dataFrame:DataFrame)={
    dataFrame.createOrReplaceTempView("temp")
    val sql="insert overwrite table user_visit_clean select * from temp"
    SparkOnHiveUtil.update(sql)
  }

  def main(args: Array[String]): Unit = {
    //创建表user_visit_clean
    createTable()
    //从表中user_visit_action中获取点击、下单、支付的量
    val df=getData()
    //将清洗的数据保存到user_visit_clean
    insertData(df)
    //查询结果
    SparkOnHiveUtil.query("select * from user_visit_clean limit 10").show()
  }

}
