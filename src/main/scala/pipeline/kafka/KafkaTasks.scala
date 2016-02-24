package pipeline.kafka

import java.util.Date
import kafka.producer.Producer
import java.util.Properties
import kafka.producer.ProducerConfig
import scala.util.Random
import kafka.producer.KeyedMessage
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.producer.KeyedMessage
import java.util.concurrent.ThreadPoolExecutor
import pipeline.model.avro.KafkaEvent
import pipeline.common.SerDeUtil
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object KafkaTasks {
  
  def main(args: Array[String]): Unit = {  
    KafkaTasks.produce()
    //KafkaTasks.consume()
  }
  
  def produce() = {

    val props = new Properties()
    props.put("metadata.broker.list", "10.24.48.147:6092")
    props.put("value.serializer.class", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "sync")
    val config = new ProducerConfig(props)

    val producer = new Producer[String, Array[Byte]](config)

    val rnd = new Random()

    for (i <- Range(1, 10000)) {
      val runtime = DateTime.now().toString(ISODateTimeFormat.dateTime())
      val ip = "192.168.2." + rnd.nextInt(255);
      val website = "www.example.com/" + rnd.nextInt(2);

      var kafkaEvent = new KafkaEvent()
      kafkaEvent.setIp(ip)
      kafkaEvent.setTime(runtime)
      kafkaEvent.setWebsite(website)
      
      println("Producing : " + kafkaEvent)

      val data = new KeyedMessage[String, Array[Byte]]("test", ip, SerDeUtil.serializeEvent(kafkaEvent))
      producer.send(data)
      
      Thread.sleep(1000)
    }

    producer.close()
  }

  def consume() = {
    
    val props = new Properties()
    props.put("zookeeper.connect", "10.24.48.147:2181");
    props.put("group.id", "test-consumer");
    props.put("zookeeper.session.timeout.ms", "4000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    val config = new ConsumerConfig(props)
    
    val consumer = Consumer.create(config)
    
    val consumerMap = consumer.createMessageStreams(Map[String,Int]("test" -> 1))
    val stream = consumerMap.get("test").get.apply(0)
    
    val message = stream.foreach(streamMessage => {
      val message = SerDeUtil.deserialiseEvent(streamMessage.message)
      println("Consuming : " + message)
    })
    
    consumer.shutdown()
  }
}