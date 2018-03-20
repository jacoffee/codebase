package com.jacoffee.codebase.spark.streaming

import com.jacoffee.codebase.utils.CommonUtils
import com.jacoffee.codebase.zk.ZookeeperClient
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.utils.ZKGroupTopicDirs
import org.apache.commons.lang3.StringUtils
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory
import com.jacoffee.codebase.spark.utils.SparkUtils._

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * + DirectKafkaStream with offset submit support
  * + ZK offset submit is done via apache curator (client library for Zookeeper)
*/
class DirectKafkaInputDStream(
  ssc: StreamingContext, topic: String, kafkaParams: Map[String, String]
) extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  // state update may be a issue, can not incorporate with the offset submit
  def build() = {
    require(kafkaParams.contains("group.id"), "Consumer group id should not be empty")
    require(kafkaParams.contains("zookeeper.connect"), "Zookeeper connect should not be empty")

    val consumerGroupId = kafkaParams.get("group.id").get
    val zkConnect = kafkaParams.get("zookeeper.connect").get

    val zkClient = ZookeeperClient.connect(zkConnect)
    val getFromOffsetsFunc: ZookeeperClient => Map[TopicAndPartition, Long] = _.getFromOffsets(consumerGroupId, topic)

    val directKafkaInputDStream =
      CommonUtils.safeRelease(zkClient)(getFromOffsetsFunc)() match {
        case Success(storedFromOffsets) =>
          // try to get from offset from zookeeper or create a new one
          val directKafkaInputDStream =
            if (storedFromOffsets.nonEmpty) {
              val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
              KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
                ssc, kafkaParams, storedFromOffsets, messageHandler
              )
            } else {
              logger.info(s"Create direct stream with topics: ${topic}")
              KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
            }

          directKafkaInputDStream
        case Failure(e) =>
          logger.error(s"Exception when creating DirectKafkaInputDStream", e)
          throw e
      }

    directKafkaInputDStream.foreachRDD { (rdd, time) =>
      val zkClient = ZookeeperClient.connect(zkConnect)
      CommonUtils.safeRelease(zkClient)(
        _.commitFromOffset(consumerGroupId, rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
      )()
    }

    directKafkaInputDStream
  }

}


object DirectKafkaInputDStream {

  def build(ssc: StreamingContext, topic: String, kafkaParams: Map[String, String]) = {
    new DirectKafkaInputDStream(ssc, topic, kafkaParams).build()
  }

}
