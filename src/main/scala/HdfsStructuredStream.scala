import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object HdfsStructuredStream {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._
  /*val fileStreamDf = spark.readStream
    .option("header", "true")
    .schema(schema)
    .csv("/tmp/input")*/


}
