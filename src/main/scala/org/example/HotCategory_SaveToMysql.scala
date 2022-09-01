package org.example

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
 * @description 对点击、下单、支付的量数据进行处理，将处理后的数据保存数据仓库
 * @author hadoop
 * @date 2022/8/17 9:44
 */
object HotCategory_SaveToMysql {
  //从user_visit_clean表中获取点击量
  def getClickCount():RDD[Row] = {
    val sql=
      """
        |select click_category_id,1,0,0 from user_visit_clean
        |where click_category_id <> '-1'
        |""".stripMargin
    val df=SparkOnHiveUtil.query(sql)
    df.rdd
  }

  //获取下单量
  def getOrderCount():RDD[Row]={
    val sql=
      """
        |select order_category_id,0,1,0 from(
        | select order_category_ids
        | from user_visit_clean
        | where order_category_ids[0] <> 'null'
        |) t
        |lateral view explode(order_category_ids) tt as order_category_id
        |""".stripMargin
    val df=SparkOnHiveUtil.query(sql)
    df.rdd
  }

  //获取支付量
  def getPayCount():RDD[Row] = {
    val sql=
      """
        |select pay_category_id,0,0,1 from(
        | select pay_category_ids
        | from user_visit_clean
        | where pay_category_ids[0] <> 'null'
        |) t
        |lateral view explode(pay_category_ids) tt as pay_category_id
        |""".stripMargin
    val df=SparkOnHiveUtil.query(sql)
    df.rdd
  }

  //将DataFrame数据保存到Mysql
  def saveMysql(table: String,dataFrame: DataFrame)={
    val props=new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")
    val url="jdbc:mysql://localhost:3306/hotcategory_top10?useSSL=false&serverTimezone=UTC"
    dataFrame.write.mode(SaveMode.Overwrite).jdbc(url,table,props)
  }

  def main(args: Array[String]): Unit = {
    //获取点击量
    val clickRDD=getClickCount()
    //获取下单量
    val orderRDD = getOrderCount()
    //获取支付量
    val payRDD=getPayCount()
    //合并RDD为[品类ID，点击量，下单量，支付量]
    val rdd=clickRDD.union(orderRDD).union(payRDD)
    //将RDD转化为（品类ID，（点击总量，下单总量，支付总量））
    val resultRDD =rdd.groupBy(_.get(0)).mapValues(itr=>{
      itr.map(row=>{
        (row.getInt(1),row.getInt(2),row.getInt(3))
      }).reduce((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))
    })
    //将RDD转换为DF后，保存到Mysql中
    import SparkOnHiveUtil.spark.implicits._
    val df=resultRDD.map(e=>{
      UserVisitTop(e._1.toString,e._2._1,e._2._2,e._2._3)
    }).toDF()
    saveMysql("tb_user_visit_top",df)


  }
  case class UserVisitTop(category_id:String,click_number:Int,order_number:Int,pay_number:Int)


}
