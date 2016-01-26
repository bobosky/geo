package com.caishi.geo

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util

import com.mongodb.{ServerAddress, MongoClient}
import com.mongodb.client.model.{UpdateOptions, Filters}
import com.mongodb.client.result.UpdateResult
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.Document
import redis.clients.jedis.JedisPool
import scala.util.parsing.json.JSON

/**
 * 计算每个用户 geo中心:KMeans 聚类
 * Created by root on 15-10-9.
 */
object GeoKMeans {
  def main(args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage: GeoKMeans <hdfsDirs> <numCenter> <numIterations> <position:home or office> <mongoRemotes:ip:port,ip:port> <mongoDb> <collection>")
      System.exit(1)
    }

    val Array(hdfsDirs, numCenter, numIterations, position,mongoRemotes, mongoDb,collection) = args
//    val hdfsDirs ="hdfs://10.4.1.4:9000/dw/2015/10/15/topic_common_event/*,hdfs://10.4.1.4:9000/dw/2015/10/14/topic_common_event/*"
//    val numCenter = 1
//    val numIterations = 10
//    val position="office"
//    val mongoRemotes="10.1.1.134:27017"
//    val mongoDb = "mydb"
//    val collection ="test"
//    val conf = new SparkConf().setAppName("mllib-geo").setMaster("local[2]")
    val conf = new SparkConf().setAppName("mllib-geo")

    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)

    //装载数据
    val data = sc.textFile(hdfsDirs.toString)
    val parsedData = data.map(line => {
      val tmp = line.split("\u0001")
      if (tmp.length > 4) {
        val uid = tmp(0).toString
        if (!uid.isEmpty) {
          JSON.parseFull(tmp(4)) match {
            case Some(map: Map[String, Any]) => {
              val judge : Boolean = position.toString match {
                case "home" => Util.checkHome(Util.transfTimeToHour(map("time").toString))._1
                case "office" => Util.checkOffice(Util.transfTimeToHour(map("time").toString))._1
              }
              if (judge) {
                val pos = map.get("position").get.asInstanceOf[Map[String, String]]
                if (pos != None) {
                  if (pos.get("lon") != None && pos.get("lat") != None) {
                    uid + "_" + pos.get("lat").get.asInstanceOf[Double] + "_" + pos.get("lon").get.asInstanceOf[Double]
                  }
                }
              } else {
                None
              }
            }
          }
        }
      }
    }).filter(m => m != None && m != Nil).map(p => {
      val t = p.toString.split("_",2)
      (t(0), p.toString)
    })
    val userPointData = parsedData.reduceByKey("\n" + _.trim + "\n" + _.trim).collect()
    sc.stop()
    userPointData.foreach(tmp =>{
      val uid = tmp._1
      val c = new SparkConf().setAppName("mllib-geo-kmean-"+uid).setMaster("local[1]")
      c.set("spark.driver.allowMultipleContexts","true")
      val meanSc = new SparkContext(c)
      val pointData = meanSc.parallelize(tmp._2.split("\n")).filter(_.split("_").length==3).map(_.split("_",2)(1)).filter(_.split("_")(0).toDouble != 0.0)
      if(pointData.count() > 0) {
//        pointData.foreach(println)
        val meanData = pointData.map(x => Vectors.dense(x.split('_').map(_.toDouble)))
        val model = KMeans.train(meanData, numCenter.toInt, numIterations.toInt)
        var resData : String = ""
        for (c <- model.clusterCenters) {
          resData = resData + c(0) +"," + c(1)+"_"
        }
        if(resData.endsWith("_"))
          resData = resData.dropRight(1)
        val d: Document = new Document("userId", uid).append(position, resData)
        // upsert data
        val result : UpdateResult =MongoUtil.getInstance(mongoRemotes).getDatabase(mongoDb).getCollection(collection).updateMany(Filters.eq("userId", uid), new Document("$set", d), new UpdateOptions().upsert(true))
        result.getModifiedCount
      }
      meanSc.stop()
    })
  }
}

object Util{
  /**
   * 检查时间段是否在家中: 22-23  00-06
   * @param hour
   * @return
   */
  def checkHome(hour : Int): (Boolean,Int) ={
    if((hour >= 0 && hour <=6) || (hour >=22 && hour <=23) )
      (true,hour)
    else
      (false,hour)
  }

  /**
   * 检查时间戳是否在上班时间段  10-12   14-17
    */
  def checkOffice(hour : Int): (Boolean,Int) ={
    if((hour >= 10 && hour <=12) || (hour >=14 && hour <=17) )
      (true,hour)
    else
      (false,hour)
  }

  /**
   * 将时间戳转换为24小时制
   * @param t 时间戳 s
   * @return
   */
  def transfTimeToHour(t : String): Int ={
    val tmp = new BigDecimal(t).toPlainString.split("\\.")(0)
    var ts:Long = 0l
    if(tmp.length<10)
      ts = new BigDecimal(tmp).toPlainString.toLong*1000*1000
    else if(tmp.length == 10 )
      ts = new BigDecimal(tmp).toPlainString.toLong*1000
    else if(tmp.length == 13)
      ts = new BigDecimal(tmp).toPlainString.toLong
    val hour = new SimpleDateFormat("HH").format(ts).toInt
    hour
  }
}
