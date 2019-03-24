
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._



import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



object consumerStream {

  def main(args: Array[String]) {
//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordCount <zkQuorum><group> <topics> <numThreads>")
//      System.exit(1)
//    }

    //val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("streamAppSarra").setMaster("local[2]").set("spark.executor.memory","1g");
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")


println("we start now !!!!!!!!!!!!!!!!!!!!")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("hdfsLogs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))

    ssc.start()
    ssc.awaitTermination()
  }
}
