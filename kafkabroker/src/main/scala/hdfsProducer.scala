import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

trait hdfsProducer {

  val DefaultZookeeperConnection = "127.0.0.1:2181"
  val DefaultKafkaConnection = "127.0.0.1:9092"
  val mandatoryOptions: Map[String, Any] = Map(
    "bootstrap.servers" -> "127.0.0.1:9092",
    "acks" -> "all",
    "batch.size" -> 16384,
    "linger.ms" -> 1,
    "buffer.memory" -> 33554432,
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")

  def extractOptions(properties: Map[String, Any]): Properties = {
    val props = new Properties()
    properties.foreach { case (key, value) => props.put(key, value.toString) }
    props
  }

  def getProducer(properties: Map[String, Any]): KafkaProducer[String, String] = {
    new KafkaProducer[String, String](extractOptions(properties))
  }

  def close(kafkaProducer: KafkaProducer[String, String]): Unit = kafkaProducer.close()

  def send(kafkaProducer: KafkaProducer[String, String], topic: String, message: String): Unit = {
    val record = new ProducerRecord(topic, "", message)
    kafkaProducer.send(record)
  }
}