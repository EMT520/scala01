package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description Spark on Hive 工具类
 * @author hadoop
 * @date 2022/8/16 16:28
 */
object SparkOnHiveUtil {
  val spark=SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()

  //创建数据库
  def createDatabase(database: String)={
    spark.sql("create database"+database)
  }

  //打开数据库
  def openDatabase(database: String)={
    spark.sql("use"+database)
  }

  //创建表
  def createTable(dml:String)={
    spark.sql(dml)
  }

  //执行更新操作
  def update(sql:String)={
    spark.sql(sql)
  }

  //执行查询操作
  def query(sql:String):DataFrame={
    val df=spark.sql(sql)
    df
  }

}
