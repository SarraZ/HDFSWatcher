
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf


object HDFSFileStream {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Please provide 2 parameters <directoryToMonitor> <microbatchtime>")
      System.exit(1)
    }
    val directoryToMonitor = args(0)
    val microBatchTime = args(1).toInt
    val sparkConf = new SparkConf().setAppName("HDFSFileStream")
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(microBatchTime))

    //logInfo("Value of microBatchTime " + microBatchTime)
//    logInfo("DirectoryToMonitor " + directoryToMonitor)
    println("DirectoryToMonitor " + directoryToMonitor)

    val directoryStream = sparkStreamingContext.textFileStream(directoryToMonitor)

//    logInfo("After starting directoryStream")
    directoryStream.foreachRDD { fileRdd =>
      println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
      if (fileRdd.count() != 0)
        processNewFile(fileRdd)
    }

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
//    logInfo("Exiting HDFSFileStream.main")
  }

  def processNewFile(fileRDD: RDD[String]): Unit = {
    println("*******************************************")
    println("Entering processNewFile " )
    println("*******************************************")
    fileRDD.foreach{ line =>
      println(line)
    }
    println("*******************************************")
    println("Exiting processNewFile " )
    println("*******************************************")

  }
}