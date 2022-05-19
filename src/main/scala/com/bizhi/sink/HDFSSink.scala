package com.bizhi.sink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.slf4j.LoggerFactory

class HDFSSink extends ForeachWriter[Row] {

  private val logger = LoggerFactory.getLogger(this.getClass)

  //文件存储在hdfs上的真实地址
  private var hdfsPath: String = _
  private var output: FSDataOutputStream  = _
  private var pathNew: Path = _
  private var fs: FileSystem = _

  override def open(partitionId: Long, epochId: Long): Boolean = {

    //获取当前日期
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val currentDate =  dateFormat.format(new Date())
    // TODO hdfs路径
    hdfsPath = "hdfs:xxx/"+currentDate+"/part-"+partitionId
    pathNew = new Path(hdfsPath)

    // 不存在以当前日期命名的文件则创建；
    // 存在则先判断文件大小，超过设置(主要是避免产生过多小文件)则将该文件重命名，
    // 重命名成功后则重新创建以当前日期命名的文件
    try{
      fs = getHDFSFileSystem()
      if (!fs.exists(pathNew)){
        fs.create(pathNew).close()
      }else{
        if(fs.getFileStatus(this.pathNew).getLen >= 256*1024*1024){
          val isModify = fs.rename(pathNew,new Path(this.hdfsPath+"-"+System.currentTimeMillis()))
          if(isModify){
            fs.create(pathNew).close()
          }
        }
      }
      output = fs.append(pathNew)
    } catch{
      case e: Exception =>
        logger.error("HDFSSink>process>>>>>"+e.printStackTrace())
    }
    true

  }

  override def process(value: Row): Unit = {
    val log = value.getAs("value").toString
    if(log.trim().length()==0){
      return
    }
    try{
      if(output==null){
        output = fs.append(pathNew)
      }
      output.write(log.getBytes("UTF-8"))
      output.write("\n".getBytes("UTF-8")) //换行
    } catch{
      case e: Exception =>
        logger.error("HDFSSink>process>>>>>"+e.printStackTrace())
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    try{
      if(output!=null){
        output.close()
      }
    } catch {
      case e: Exception =>
        logger.error("HDFSSink>close>>>>>"+e.printStackTrace())
    }
  }

  def getHDFSFileSystem(): FileSystem ={
    var fs: FileSystem = null
    val conf = new Configuration()
    conf.setBoolean("dfs.support.append", true)
    // TODO hdfs路径
    //conf.set("fs.defaultFS", "hdfs://localhost:8020")

    try {
      fs = FileSystem.get(conf)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    fs
  }
}
