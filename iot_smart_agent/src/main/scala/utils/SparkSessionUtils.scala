package utils


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}





object SparkSessionUtils {

  final val batchSeconds = 2


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val session = SparkSession.builder().master("local[*]").appName("iotSa").getOrCreate()

  val sc = session.sparkContext

  val ssc = new StreamingContext(sc, Seconds(batchSeconds))

  val sql = session.sqlContext

}