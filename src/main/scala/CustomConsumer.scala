
import java.util
import java.util._
import scala.collection.JavaConverters._


import org.apache.kafka.clients.consumer.KafkaConsumer

object CustomConsumer extends App {
  @throws[Exception]
  val topicName = "Topic"
  val groupName = "TopicGroup"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put("group.id", groupName)
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "CustomDeserializer")
  val consumer = new KafkaConsumer[String, Sample](props)

  consumer.subscribe(util.Arrays.asList(topicName))

  while (true) {
    val records = consumer.poll(1000).asScala.iterator
    for (recordValue <- records)
      println(" id= " + String.valueOf(recordValue.value.getID) + " Name = " + recordValue.value.getName)
  }




  consumer.close()

}
