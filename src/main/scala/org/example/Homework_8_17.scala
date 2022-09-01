package org.example

import org.apache.spark.sql.DataFrame

/**
 * @description 需求3：计算页面跳转率
 * @author hadoop
 * @date 2022/8/17 23:40
 */
object Homework_8_17 {
  //获取SessionID访问页面的顺序并排序
  def getData():DataFrame = {
    val sql=
      """
        |select session_id as s_id,page_id as p_id,
        |row_number() over(partition by session_id order by action_time) rank
        |from user_visit_action
        |""".stripMargin
    SparkOnHiveUtil.query(sql)
  }
  //获取每个页面的访问次数
  def getPageSum():DataFrame={
    val sql =
      """
        |select page_id as p_id,count(1) sum
        |from user_visit_action
        |group by page_id
        |""".stripMargin
    SparkOnHiveUtil.query(sql)
  }
  def getJump(df:DataFrame):DataFrame = {
    df.createOrReplaceTempView("temp")
    val sql=
      """
        |select t1.s_id,t1.p_id pb_id,t2.p_id pa_id
        |from temp t1 join temp t2
        |on (t1.s_id=t2.s_id and t1.rank+1=t2.rank)
        |""".stripMargin
    SparkOnHiveUtil.query(sql)
  }
  //页面跳转的次数
  def getJUmpSum(df:DataFrame):DataFrame = {
    df.createOrReplaceTempView("temp")
    val sql=
      """
        |select pb_id,pa_id,count(1) as sum
        |from temp
        |group by pb_id,pa_id
        |""".stripMargin
    SparkOnHiveUtil.query(sql)
  }
  def getRate(dfPageSum:DataFrame,dfJumpSum:DataFrame):DataFrame = {
    dfPageSum.createOrReplaceTempView("PageSum")
    dfJumpSum.createOrReplaceTempView("JumpSum")
    val sql =
      """
        |select j.pb_id,j.pa_id,j.sum/t.sum*100 as res
        |from PageSum t join JumpSum j
        |on (t.p_id=j.pa_id)
        |""".stripMargin
    SparkOnHiveUtil.query(sql)
  }

  def main(args: Array[String]): Unit = {
    //从hive中清洗数据
    val df1=getData()
    //获取每个页面的访问次数
    val df2=getPageSum()
    //获取页面跳转
    val df3=getJump(df1)
    //获取页面跳转次数
    val df4=getJUmpSum(df3)
    //计算跳转率
    val df5=getRate(df2,df4)
    df5.show(false)

  }

}
