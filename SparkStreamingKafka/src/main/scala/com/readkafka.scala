
package main.scala.com
import com.alibaba.fastjson.JSON
import main.scala.until.dataschema
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import scalikejdbc.config.{DBs, DBsWithEnv}
import scalikejdbc._
import main.scala.until.ParamsUtils
import main.scala.until.SparkUtils

object readkafka {
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      System.err.println("please input data args")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkStreaming-test")
      .setMaster("local[*]")
      .set("spark.testing.memory","2147480000")
    val sc = new SparkContext(sparkConf)

//topic : spark_example_topic , countly_event ,countly_imp
//broker : 172.31.2.6:9292,172.31.2.7:9292,172.31.2.8:9292

//    val ssc = new StreamingContext(sc, Seconds(2))
    val ssc = new StreamingContext(sc, Seconds(2))

    val messages = new SparkUtils(ssc).getDirectStream(ParamsUtils.kafka.KAFKA_TOPIC)
    messages.print()

//    SparkUtils.apply(null).getDirectStream()

//-------------------------------------------------------------------------------

    // messages 从kafka获取数据,将数据转为RDD
    messages.foreachRDD((rdd, batchTime) => {
      import org.apache.spark.streaming.kafka.HasOffsetRanges
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges   // 获取偏移量信息
      /**
        * OffsetRange 是对topic name，partition id，fromOffset(当前消费的开始偏移)，untilOffset(当前消费的结束偏移)的封装。
        * *  所以OffsetRange 包含信息有：topic名字，分区Id，开始偏移，结束偏移
        */
      println("===========================> count: " + rdd.map(x => x + "1").count())
     // offsetRanges.foreach(offset => println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))
      for (offset <- offsetRanges) {
        // 遍历offsetRanges,里面有多个partition
        println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)
        DBs.setupAll()
        // 将partition及对应的untilOffset存到MySQL中
        val saveoffset = DB localTx {
          implicit session =>
           sql"DELETE FROM offsetinfo WHERE topic = ${offset.topic} AND partitionname = ${offset.partition}".update.apply()
            sql"INSERT INTO offsetinfo (topic, partitionname, untilOffset) VALUES (${offset.topic},${offset.partition},${offset.untilOffset})".update.apply()
        }
      }
    })

    // 处理从kafka获取的message信息
    val parameter = messages.flatMap(line => {
      //获取服务端事件日期 reqts_day
      val reqts_day = try {
        new DateTime(JSON.parseObject(line._2).getJSONObject("i").getLong("timestamp") * 1000).toDateTime.toString("yyyy-MM-dd HH:mm:ss")
      } catch {
        case ex: Exception => "(unknown)"
      }

      //获取 设备号
      val cookieid = try {
        JSON.parseObject(line._2).getJSONObject("d").get("d")    //将Json字符串转化为相应的对象  .getString("kid")
      } catch {
        case ex: Exception => "(unknown)"
      }

      //组合成一个字符串
      val data = reqts_day + "##" + cookieid
      Some(data)       //some是一定有值的, some.get获取值,如果没有值,会报异常
    }).map(_.split("##")).map(x => (x(0),x(1)))

    println("------------------")

    parameter.foreachRDD{ rdd =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      // 转换成DataFrame
      val SaveParameter = rdd.map(w => dataschema(w._1.toString,w._2.toString)).toDF("data_date","cookies_num")
      // 注册视图
      SaveParameter.createOrReplaceTempView("dau_tmp_table")
      val insertsql =sqlContext.sql("select * from dau_tmp_table")
      insertsql.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/userprofile_test","dau_tmp_table",ParamsUtils.mysql.mysqlProp)

    }

    messages.print()
    ssc.start()
    ssc.awaitTermination()

  }
}


