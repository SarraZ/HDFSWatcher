import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.fs._

object hadoopWatcher {

  def pathExists(path: String, sc:SparkContext):Boolean = {
    val conf =sc.hadoopConfiguration
    val fs= FileSystem.get(conf)
    fs.exists(new Path (path))
  }



  def main(args: Array[String]): Unit = {
    if (args.length != 0) {
      println("Please provide 2 parameters <directoryToMonitor> <microbatchtime>")
      System.exit(1)
    }
//    val directoryToMonitor =  "hdfs://localhost:9000/hdfs_watcher" //args(0)
    val directoryToMonitor = args(0) //  "/home/sarra/work/HDFSWatcher/hdfs_watcher"
    val microBatchTime =args(1).toInt
    val sparkConf = new SparkConf().setAppName("HDFSFileStream").setMaster("local[*]")
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(microBatchTime))

    //logInfo("Value of microBatchTime " + microBatchTime)
    //    logInfo("DirectoryToMonitor " + directoryToMonitor)
    println("DirectoryToMonitor " + directoryToMonitor)

    val directoryStream = sparkStreamingContext.textFileStream(directoryToMonitor)
    println (" HERE WE START")
    directoryStream.foreachRDD { fileRDD =>
      println( "number of files : ",fileRDD.count())
      if (fileRDD.count() != 0){
        println("FILE RDD INFO : ")
        val filemetadata= fileRDD.toDebugString.split('\n')
        val filename = filemetadata(2).split(" ")(3)
        println("filename received is :", filename)
        if (pathExists(filename,sc)){
          val filePath = new Path(filename)
        }
      }
    }








      sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }

}