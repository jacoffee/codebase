package com.jacoffee.codebase.zk

import kafka.common.TopicAndPartition
import kafka.utils.ZKGroupTopicDirs
import org.apache.commons.lang3.StringUtils
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils
import org.apache.spark.streaming.kafka.OffsetRange
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions.iterableAsScalaIterable

class ZookeeperClient private (val client: CuratorFramework) extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  // invoke before stream really starts
  def commitFromOffset(consumerGroupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    require(StringUtils.isNoneBlank(consumerGroupId), "Consumer group id should not be empty")

    offsetRanges.foreach { offsetRange =>
      val groupTopicDirs = new ZKGroupTopicDirs(consumerGroupId, offsetRange.topic)
      // offset path
      val consumerOffSetPath = s"${groupTopicDirs.consumerOffsetDir}/${offsetRange.partition}"

      try {
        if (client.checkExists().forPath(consumerOffSetPath) == null) {
          client.create.creatingParentsIfNeeded().forPath(consumerOffSetPath)
        }

        client.setData.forPath(
          consumerOffSetPath,
          offsetRange.fromOffset.toString.getBytes("UTF-8")
        );
      } catch {
        case e: Exception =>
          logger.error(s"Exception during commit from offset ${offsetRange.fromOffset} for topic" +
              s"${offsetRange.topic}, partition ${offsetRange.partition}", e)
      }

      logger.info(s"Committed from offset ${offsetRange.fromOffset} for topic ${offsetRange.topic}, " +
          s"partition ${offsetRange.partition}")
    }
  }

  def getFromOffsets(consumerGroupId: String, topic: String): Map[TopicAndPartition, Long] = {
    var fromOffsets = Map[TopicAndPartition, Long]()

    // /consumers/{consumer-group-id}/offsets/topics/
    val topicDirs = new ZKGroupTopicDirs(consumerGroupId, topic)

    val childPath =
      if (client.checkExists().forPath(topicDirs.consumerOffsetDir) != null ) {
        client.getChildren().forPath(topicDirs.consumerOffsetDir).toList
      } else {
        Nil
      }

    childPath.map { partition =>
      val completePath = s"${topicDirs.consumerOffsetDir}/${partition}"
      try {
        val currentOffset =
          new String(client.getData().forPath(completePath), "UTF-8").toInt
        fromOffsets += (new TopicAndPartition(topic, partition.toInt) -> currentOffset.toInt)
      } catch {
        case e: Exception =>
          logger.error(s"Exception during reading consumer offsets from path ${completePath}", e)
          // get operation only happens when stream starts, so we want to make sure it is successful
          throw e
      }
    }
    fromOffsets
  }

  def close(): Unit = {
    logger.info("Zookeeper is closing")
    CloseableUtils.closeQuietly(client)
  }

}

object ZookeeperClient {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def connect(connectString: String) = {
    logger.info("Initializing CuratorFramework")
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy)
    client.start()
    new ZookeeperClient(client)
  }

}
