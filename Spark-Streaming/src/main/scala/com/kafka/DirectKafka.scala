
package com.kafka

import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

/***
  * 启动zookeeper
  * cd /root/kafka/kafka_2.11-1.0.0/bin
  * ./zookeeper-server-start.sh /root/kafka/kafka_2.11-1.0.0/config/zookeeper.properties
  *
  * 启动kafka
  *  cd /root/kafka/kafka_2.11-1.0.0/bin
  *  ./kafka-server-start.sh /root/kafka/kafka_2.11-1.0.0/config/server.properties
  *
  * 启动kafka生产者
  * ./kafka-console-producer.sh --broker-list master:9092 --topic kafka_test
  *
  *  提交任务
  * spark-submit --class com.kafka.DirectKafka \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --executor-cores 1 \
  * /root/datafile/SparkStreamingKafka-1.0-SNAPSHOT-jar-with-dependencies.jar \
  * master:9092 kafka_test
  */

object DirectKafka {
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
      System.err.println(
        s"""
           |DirectKafka  <brokers><topics>
           | <brokers> is a list of one or more kafka brokers
           | <topics> is a list of one or more kafka topics
         """.stripMargin)
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkStreaming-DirectKafka")
    val sc = new SparkContext(sparkConf)

    val Array(brokers, topics) = args
    val ssc = new StreamingContext(sc, Seconds(2))
    val topicset = topics.split(",").toSet
    val KafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, KafkaParams, topicset)

    directKafkaStream.print()

    var offsetRanges = Array.empty[OffsetRange]
    directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        for (o <- offsetRanges) {
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
        rdd.take(10).foreach(println)
      }

    ssc.start()
    ssc.awaitTermination()

  }
}


