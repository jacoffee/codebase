package com.jacoffee.codebase.spark.streaming

import java.util.concurrent.TimeUnit
import com.google.common.base.Stopwatch
import com.jacoffee.codebase.utils.CommonUtils
import com.jacoffee.codebase.zk.ZookeeperClient
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time, StateSpec}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{KafkaUtils, HasOffsetRanges, OffsetRange}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag
import scala.util.{Failure, Success}
import kafka.serializer.Decoder
import com.jacoffee.codebase.zk.ZookeeperClient

// scalastyle:off
class RichDirectKafkaInputDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag, V2: ClassTag](
  ssc: StreamingContext, topic: String, kafkaParams: Map[String, String]
) extends Serializable {

  require(kafkaParams.contains("group.id"), "Consumer group id should not be empty")
  require(kafkaParams.contains("zookeeper.connect"), "Zookeeper connect should not be empty")

  private val consumerGroupId = kafkaParams.get("group.id").get
  private val zkConnect = kafkaParams.get("zookeeper.connect").get

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  // when to checkpoint
  private var zeroTime: Time = null
  // to save offset
  private var offsetRanges = Array[OffsetRange]()
  private val DEFAULT_CHECKPOINT_DURATION_MULTIPLIER = 10

  // private val checkpointDuration = self.slideDuration * DEFAULT_CHECKPOINT_DURATION_MULTIPLIER

  private var dumpPathOpt: Option[String] = None

  def stateSnapshotPath(stateSnapshotPath: String): this.type = {
    this.dumpPathOpt = Some(stateSnapshotPath)
    this
  }

  private var self: DStream[(K, V)] = null

  def init(): Unit = {
    val zkClient = ZookeeperClient.connect(zkConnect)
    val getFromOffsetsFunc: ZookeeperClient => Map[TopicAndPartition, Long] = _.getFromOffsets(consumerGroupId, topic)

    val directKafkaInputDStream =
      CommonUtils.safeRelease(zkClient)(getFromOffsetsFunc)() match {
        case Success(storedFromOffsets) =>
          // try to get from offset from zookeeper or create a new one
          val directKafkaInputDStream =
            if (storedFromOffsets.nonEmpty) {
              val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
              KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
                ssc, kafkaParams, storedFromOffsets, messageHandler
              )
            } else {
              logger.info(s"Create direct stream with topics: ${topic}")
              KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, Set(topic))
            }

          directKafkaInputDStream
        case Failure(e) =>
          logger.error(s"Exception when creating DirectKafkaInputDStream", e)
          throw e
      }
    self = directKafkaInputDStream
  }

  init()

  def foreachRDDWithOffSet[StateType: ClassTag, MappedType: ClassTag](
    spec: StateSpec[K, V2, StateType, MappedType],
    mappedFunc: (K, V) => (K, V2),
    rddHandler: (RDD[(K, StateType)], Time) => Unit
  ): Unit = {
    val consumerGroupId = kafkaParams.get("group.id").get
    val zkConnect = kafkaParams.get("zookeeper.connect").get

    /**
      * Note that the typecast to HasOffsetRanges will only succeed if it is done in the first method
      * called on the directKafkaStream, not later down a chain of methods.
      */
    val stateSnapshots: DStream[(K, StateType)] =
      self.transform { stateRDD =>
        offsetRanges = stateRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        stateRDD
      }.map {
        case (k, v) => mappedFunc(k, v)
      }.mapWithState(spec).stateSnapshots()

    stateSnapshots.foreachRDD { (rdd, time) =>

      /*
      if (zeroTime == null) {
        zeroTime = time
      }

      if (
          time.milliseconds != zeroTime.milliseconds &&
          (time - zeroTime).isMultipleOf(checkpointDuration)
      ) {
        if (dumpPathOpt.nonEmpty) {
          val dumpPath = dumpPathOpt.get
          SparkUtils.cleanCheckpoint(rdd.context, dumpPath)
          logger.info(s"Checkpointing data into ${dumpPath}")
          rdd.repartition(5).saveAsObjectFile(dumpPath)
        }
      }
      */

      val stopwatch = new Stopwatch().start()
      rddHandler(rdd, time)
      stopwatch.stop()
      val millis = stopwatch.elapsed(TimeUnit.MILLISECONDS)

      logger.info(
        s"Finish streaming rdd handling in batch {}, completed in {}",
        List(new DateTime(time.milliseconds).toString("yyyy-MM-dd HH:mm:ss"), millis.toString): _*
      )

      val zkClient = ZookeeperClient.connect(zkConnect)
      CommonUtils.safeRelease(zkClient)(
        _.commitFromOffset(consumerGroupId, offsetRanges)
      )()
    }
  }

}
// scalastyle:on
