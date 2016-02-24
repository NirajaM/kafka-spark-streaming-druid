package pipeline.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import pipeline.common.SerDeUtil
import kafka.serializer.DefaultDecoder
import kafka.serializer.DefaultDecoder
import org.apache.spark.storage.StorageLevel
import com.metamx.tranquility.spark.BeamRDD._
import pipeline.druid.EventRDDBeamFactory
import scala.collection.immutable.Map
import pipeline.model.avro.KafkaEvent
import org.apache.spark.rdd.RDD
import pipeline.druid.EventRDDBeamFactory

object SparkStreamingTasks {

  def streamFromKafka() = {
    val sparkConf = new SparkConf()
      .setAppName("test-spark")
      .setMaster("yarn-client")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "pipeline.common.MyKyroRegistrator")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaConf = Map(
      "metadata.broker.list" -> "kafka:9092",
      "zookeeper.connect" -> "kafka:2181",
      "group.id" -> "test-spark-kafka-consumer",
      "zookeeper.connection.timeout.ms" -> "1000")

    val zkQuorum = "kafka:2181"
    val group = "test-spark-kafka-consumer"
    val topic = "test"

    val lines = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaConf, Set(topic))
    val rawStream = lines.map(_._2)

    val deserialisedDStream = rawStream map { stream => SerDeUtil.deserialiseEvent(stream) }

    deserialisedDStream foreachRDD { kafkaEventRDD =>
      {
        val druidEventRDD = kafkaEventRDD map { kafkaEvent =>
          {
            Map(
              "ip" -> kafkaEvent.getIp(),
              "website" -> kafkaEvent.getWebsite(),
              "timestamp" -> kafkaEvent.getTime())
          }
        }
        
        druidEventRDD.propagate(new EventRDDBeamFactory)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}