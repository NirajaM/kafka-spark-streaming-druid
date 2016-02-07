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
    ssc.checkpoint("checkpoint")

    val kafkaConf = Map(
      "metadata.broker.list" -> "192.168.99.100:9092",
      "zookeeper.connect" -> "192.168.99.100:2181",
      "group.id" -> "test-spark-kafka-consumer",
      "auto.offset.reset" -> "smallest",
      "zookeeper.connection.timeout.ms" -> "1000")

    val zkQuorum = "192.168.99.100:2181"
    val group = "test-spark-kafka-consumer"
    val topic = "test"

    val lines = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaConf, Set(topic))
    val rawStream = lines.map(_._2)

    val deserialisedStream = rawStream map { stream => SerDeUtil.deserialiseEvent(stream) }
    
    val druidDStream = deserialisedStream map { kafkaEvent =>
      {
        val eventFieldsMap = Map(
          "ip" -> kafkaEvent.getIp(),
          "website" -> kafkaEvent.getWebsite(),
          "time" -> kafkaEvent.getTime())
        eventFieldsMap
      }
    }

    val beamFactory = new EventRDDBeamFactory()
    druidDStream.foreach(rdd => {
      println("In propogate")
      println("In propogate : first")
      println("First Map "+rdd.first())
      rdd.propagate(beamFactory)
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}