import java.util._
import org.apache.kafka.clients.producer._

object CustomProducer extends App{
  @throws[Exception]
    val topicName = "Topic"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092,localhost:9093")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "CustomSerializer")
    val producer: KafkaProducer[String,Sample] = new KafkaProducer[String,Sample](props)
    val userId = new Sample(1, "hello")
    val name = new Sample(2, "knoldus")
    producer.send(new ProducerRecord(topicName, "SUP", userId)).get
    producer.send(new ProducerRecord(topicName, "SUP", name)).get
    System.out.println("CustomProducer Completed.")

    producer.close()

}