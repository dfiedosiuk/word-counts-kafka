import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

object WordCount extends App {

  val propsConsumer = new Properties()
  propsConsumer.put("bootstrap.servers", "localhost:9092")
  propsConsumer.put("group.id", "test")
  propsConsumer.put("enable.auto.commit", "true")
  propsConsumer.put("auto.commit.interval.ms", "1000")
  propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val propsProducer = new Properties()
  propsProducer.put("bootstrap.servers", "localhost:9092")
  propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


  val consumer = new KafkaConsumer[String, String](propsConsumer)
  val producer = new KafkaProducer[String, String](propsProducer)


  consumer.subscribe(Pattern.compile("input"))

  while (true) {
    val records = consumer.poll(Duration.ofMillis(100))
    records.forEach { record =>
      producer.send(new ProducerRecord[String, String]("output",record.value, record.value.split(" ").groupBy(identity).mapValues(_.length).mkString(" , ")))
    }
  }

  producer.close();
  consumer.close();

}
