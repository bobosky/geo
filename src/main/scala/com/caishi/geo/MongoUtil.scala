package com.caishi.geo

import java.util

import com.mongodb.{ServerAddress, MongoClient}
import com.mongodb.client.model.{UpdateOptions, Filters}
import com.mongodb.client.result.UpdateResult
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson

import scala.Array

/**
 * mongodb 辅助类
 * Created by root on 15-10-17.
 */
object MongoUtil {
  @transient private var instance : MongoClient = _

  def getInstance(address : String) : MongoClient = {
    if(instance == null){
      val adds : Array[ServerAddress] = address.split(",").map(rem => new ServerAddress(rem.split(":")(0),rem.split(":")(1).toInt))
      val t : util.ArrayList[ServerAddress] = new util.ArrayList[ServerAddress]()
      adds.foreach(s => t.add(s))

      instance = new MongoClient(t)
    }
    instance
  }

  /**
   * 更新or添加
   */
  def upsert(db:MongoDatabase, table:String,condition : Map[String,String],newBson : Bson):Unit ={
    val tab : MongoCollection[_] = db.getCollection(table)
    val d: Document = new Document("name", "name2").append("pos", 111)
    val result : UpdateResult =tab.updateMany(Filters.eq(condition("before").toString, condition("end").toString), new Document("$set", d), new UpdateOptions().upsert(true))
    result.getModifiedCount
  }
}
