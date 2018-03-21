package com.jacoffee.codebase.spark.streaming.stateful

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec, Time, StreamingContext}
import com.jacoffee.codebase.spark.utils.SparkUtils._
import com.jacoffee.codebase.spark.streaming.DirectKafkaInputDStream
import scala.concurrent.duration.Duration

object KafkaWordCounter {

  private def buildSparkConf() = {
    val sparkConf = new SparkConf()
    sparkConf.set(DURATION, "10s")
    sparkConf.set(CP_DIR, "hdfs://localhost/user/allen/streaming/wordcount")
    sparkConf.set(STATE_SNAPSHOT_DIR, "hdfs://localhost/user/allen/streaming/wordcount-snapshot")
    sparkConf.set(STATE_SNAPSHOT_CP_DURATION, "50s")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.setAppName("Kafka Word Count")
    sparkConf.setMaster("local[*]")
    sparkConf
  }

  /*
      [when using getOrElse, can not register any function after stream initialized](https://stackoverflow.com/questions/35090180/why-does-spark-throw-sparkexception-dstream-has-not-been-initialized-when-res)
  */
  private def mappingFunction(
    time: Time, key: String, valueOpt: Option[Int], wordCountState: State[Int]
  ): Option[(String, Int)] = {
    val thresholdCount = 10
    // key在StateMap中有值, 相当于调用StateMap.getOption(keyType).nonEmpty
    // 首先判断该key对应的次数, 如果大于10, 则移除
    if (wordCountState.exists) {
      // StateMap.get(keyType)
      val existingCount = wordCountState.get

      // 出现次数超过10次, 则移除
      if (existingCount > thresholdCount) {
         wordCountState.remove()
         None
      } else {
        val newCount = valueOpt.getOrElse(1) + existingCount
        // 更新状态
        wordCountState.update(newCount)
        Some(key -> newCount)
      }
    } else {
      // 首次出现, 则计1
      val newCount = valueOpt.getOrElse(1)
      wordCountState.update(newCount)
      Some(key -> newCount)
    }
  }

  private def getInitialState(sc: SparkContext): RDD[(String, Int)] = {
    val snapShotDir = sc.getConf.get(STATE_SNAPSHOT_DIR)
    val path = new Path(snapShotDir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      sc.objectFile[(String, Int)](snapShotDir)
    } else {
      sc.emptyRDD
    }
  }

  private def mappedDataOutput(kafkaDirectStream: DStream[(String, Int)]) {
    kafkaDirectStream.mapWithState(StateSpec.function(mappingFunction _)).foreachRDD { mappedData =>
      mappedData.collect().foreach {
        case (word, count) =>
          println(s"Word ${word}, count ${count}")
      }
    }
  }

  private def snapShotOutput(kafkaDirectStream: DStream[(String, Int)]) {
    val sc = kafkaDirectStream.context.sparkContext
    val snapShotStream =
      kafkaDirectStream.mapWithState(
        StateSpec.function(mappingFunction _).initialState(getInitialState(sc))
      ).stateSnapshots()

    snapShotStream.foreachRDD { (stateRDD, time) =>
      // SparkUtils.periodicSnapShotDump(stateRDD, time)
      stateRDD.collect().foreach {
        case (word, count) =>
          println(s"Word ${word}, count ${count}")
      }
      println(" --------------------------- ")
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = buildSparkConf()

    require(sparkConf.contains(CP_DIR), s"${CP_DIR} is missing")
    require(sparkConf.contains(DURATION), s"${DURATION} is missing")
    require(sparkConf.contains(STATE_SNAPSHOT_DIR), s"${STATE_SNAPSHOT_DIR} is missing")

    val kafkaParams =
      Map[String, String](
        "group.id" -> "kafka-word-count-group",
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181"
      )

    // DO create new one every time we start
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Duration(sparkConf.get(DURATION)))
    cleanCheckpoint(sparkContext, sparkConf.get(CP_DIR))

    val kafkaDirectStream =
      DirectKafkaInputDStream.create(ssc, "kafka-word-count", kafkaParams).map {
        case (k, v) => (v, 1)
      }

    kafkaDirectStream.foreachStateRDD[Int, (String, Int)](
      StateSpec.function(mappingFunction _).initialState(getInitialState(sparkContext)),
      (mappedData, time) => mappedData.collect().foreach {
        case (word, count) => println(s"Word ${word}, count ${count}")
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
// scalastyle:on