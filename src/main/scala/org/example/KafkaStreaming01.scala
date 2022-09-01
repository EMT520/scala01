package org.example


import org.apache.hbase.thirdparty.com.google.common.eventbus.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description TODO
 * @author hadoop
 * @date 2022/8/18 19:32
 */
object KafkaStreaming01 {
  def main(args: Array[String]): Unit = {
    //创建sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ks01")
    //创建StreamingContext，时间间隔为1秒（采集周期）
    val ssc=new StreamingContext(sparkConf,Seconds(1))

    /**
     * 配置kafka消费者参数
     * bootstrap.servers  kafka的服务节点和端口号，用于连接kafka
     * key.deserializer   网络传输要序列化，接收则要反序列化，这里指定反序列化的类
     * value.deserializer 指定反序列化的类
     * group.id           消费者组ID：ID相同的消费者在一个组
     * enable.auto.commit kafka是否自动提交偏移量，这里填否，交由Spark管理
     */
    val kafkaParms=Map[String,Object](
      "bootstrap.servers" ->"master:9092",
      "key.deserializer" ->classOf[StringDeserializer],
      "value.deserializer" ->classOf[StringDeserializer],
      "group.id" ->"wordcount",
      "enable.auto.commit" ->(false:java.lang.Boolean)
    )
    //定义一个主题数组，内可包含多个主题，此处只有一个
    val topics=Array("wordcount")
    //创建kafka数据源
    val lineDStream:InputDStream[ConsumerRecord[String,String]] =
      KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](topics,kafkaParms))
    //取出一行，只要value
    val lineRDD:DStream[String]=lineDStream.map(_.value())
    //分离单词
    val wordRDD:DStream[String]=lineRDD.flatMap(_.split("\\s+"))
    //转换（单词，1）
    val wordAndOneRDD:DStream[(String,Int)]=wordRDD.map((_,1))
    //统计
    val resultRDD:DStream[(String,Int)]=wordAndOneRDD.reduceByKey(_+_)
    //foreachRDD(func) 最通用的输出操作，把func作用于从stream生成的没一个RDD
    resultRDD.foreachRDD(rdd=>rdd.foreach(println))
    ssc.start()
    ssc.awaitTermination()
  }

}


