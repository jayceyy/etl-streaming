package com.bizhi.etl

import com.bizhi.sink.HDFSSink
import com.bizhi.util.PropUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{max, min}
import org.slf4j.LoggerFactory

object SparkKafka2HDFS {

  val LOGGER = LoggerFactory.getLogger(SparkKafka2HDFS.getClass)

  import org.apache.spark.sql._

  def getBatchInfo(dataSource: Dataset[Row]): (Long, String) = {
    val offset = dataSource.select("partition", "offset")
      .groupBy("partition")
      .agg(max("offset") as "end_offset", min("offset") as "start_offset")
      .collect()
    val partitions = offset.map(_.getAs[Int]("partition"))
    val startOffsets = offset.map(_.getAs[Long]("start_offset"))
    val endOffsets = offset.map(_.getAs[Long]("end_offset"))
    val offsetRange = partitions.zip(startOffsets).zip(endOffsets).map(o => o._1._1 -> (o._1._2, o._2)).toMap
    val batchOffsetRangeString = offsetRange.map { case (p, (s, e)) => s"$p:$s-$e" }.mkString("{", ",", "}")
    val count = offsetRange.values.map(v => v._2 - v._1).sum
    (count, batchOffsetRangeString)
  }



  def main(args: Array[String]): Unit = {

    val env = args(0)
    val path1 = args(1)
    val path2 = args(2)

    println("env:"+env)
    println("path1:"+path1)
    println("path2:"+path2)

    // 加载配置文件
    var brokers:String = null
    var topics:String = null
    if("test".equals(env)){
      brokers = new PropUtils("kafka.properties").getProperty("test.kafka.brokers")
      topics = new PropUtils("kafka.properties").getProperty("test.kafka.topics")
    }else{
      brokers = new PropUtils("kafka.properties").getProperty("product.kafka.brokers")
      topics = new PropUtils("kafka.properties").getProperty("product.kafka.topics")
    }
    println("kafka.brokers2:" + brokers)
    println("kafka.topics2:" + topics)


    val spark = SparkSession
      .builder()
      .appName("SparkKafka2HDFS")
      .config(new SparkConf())
      .enableHiveSupport()
      .getOrCreate()

    val dataSource: Dataset[Row] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .option("security.protocol", "SSL")
      .option("ssl.truststore.location", path1)
      .option("ssl.truststore.password", "tUCoKGLZiY7Bkjr4")
      .option("ssl.keystore.location", path2)
      .option("ssl.keystore.password", "tUCoKGLZiY7Bkjr4")
      .option("ssl.key.password", "tUCoKGLZiY7Bkjr4")
      .load()
      .selectExpr("CAST(value AS STRING)")

    // 使用Foreach sink，将数据写入指定文件，同一天的数据写入到一个文件中
    val fileSinkQuery =
      dataSource.writeStream
        .foreachBatch((batchData: Dataset[Row], batchId: Long) => {
          // 获取每批次数据量和offset
          val (count, batchOffsetRangeString) = getBatchInfo(batchData)
          LOGGER.info(s"批次:$batchId ,数据量:$count")
          LOGGER.debug(s"批次:$batchId ,offset:$batchOffsetRangeString")
        })
        .foreach(new HDFSSink())
        .outputMode("append")
        // TODO 加入checkpoint路径
        // .option("checkpointLocation", "hdfs checkpoint path")
        .start()

    fileSinkQuery.awaitTermination()


  }
}
