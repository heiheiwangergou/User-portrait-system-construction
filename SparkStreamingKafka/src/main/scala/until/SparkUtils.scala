package main.scala.until
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.common.TopicAndPartition
import main.scala.until.ParamsUtils
import org.apache.spark.SparkConf

object SparkUtils{
  def apply(ssc: StreamingContext): SparkUtils = new SparkUtils(ssc)  // 调用class里面的构造器  这个在伴生对象里面才有

  def getStreaming(appName: String): StreamingContext = {
    val conf = new SparkConf().setAppName(appName)
      .set("spark.testing.memory","2147480000")
      .setMaster("local[*]")
    new StreamingContext(conf, Seconds(2))
  }
}


class SparkUtils(ssc: StreamingContext) {
// 传入ssc 返回线上数据

  def getDirectStream (topics: Set[String]):InputDStream[(String,String)] = {

    def readFromOffsets(list: List[(String, Int, Int)]): Map[TopicAndPartition, Long] = {
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      for (offset <- list) {
        val tp = TopicAndPartition(offset._1, offset._2) //topic和分区数
        fromOffsets += (tp -> offset._3) // offset位置
       }
      fromOffsets
    }

    val dauTopic = topics.head // ParamsUtils.kafka.KAFKA_TOPIC.toString  //"countly_imp"
    val dauOffsetList = List((dauTopic,1,1651983),
      (dauTopic,2,1625775),
      (dauTopic,3,1625780),
      (dauTopic,4,1625761),
      (dauTopic,5,1651990),
      (dauTopic,6,1625791),
      (dauTopic,7,1625776),
      (dauTopic,8,1625766))
     val dauFromOffsets = readFromOffsets(dauOffsetList)
   val dauMessageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

    val size = dauOffsetList.size

    val inpDS: InputDStream[(String, String)] = if (size > 0) {
      //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, ParamsUtils.kafka.KAFKA_PARAMS, dauFromOffsets, dauMessageHandler)
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, ParamsUtils.kafka.KAFKA_PARAMS, ParamsUtils.kafka.KAFKA_TOPIC)
    }

    inpDS
  }

}


