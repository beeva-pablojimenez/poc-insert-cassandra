package com.beeva.poc.cassandra.massive_insertion

import org.apache.spark.{ SparkConf, SparkContext }
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import java.text.SimpleDateFormat

//lectura de datos con spark y calculo de maximos mínimos y media de temperaturas
object CassandraSpark extends App {
  val format = new SimpleDateFormat("HH:mm:ss.SSSZ")
  case class EventTemperature(weatherstation_id: String, date: String, event_time:Long, temperature:String) {
    override def toString = printf("id:%s , date:%s , time:%s , temperature:%s event", weatherstation_id, date, format.format(event_time), temperature).toString
  }
  
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

  val sc = new SparkContext("local", "testingCassy", conf)
  val rdd = sc.parallelize(1 to 10)

  
  var tableN = sc.cassandraTable("test", "temperature_by_day")
  println("first " +tableN.first())
  
  var ini = System.currentTimeMillis()
  val table = sc.cassandraTable("test", "temperature_by_day").select("weatherstation_id","date", "event_time", "temperature").where("weatherstation_id = ? and date=?", "Station-9", "2015-10-13")
  println("query "+table.first()+ " "+(System.currentTimeMillis()-ini)+" mls")
  
  
  ini = System.currentTimeMillis()
  val min = table.map { x => (x.get[String]("temperature").toInt, 1) }.reduce((a,b) => {
    if(a._1<b._1) a else b}
  )
  println("min:"+min)
  println("min "+(System.currentTimeMillis()-ini)+" mls")
  
 
  ini = System.currentTimeMillis()
  val max = table.map { x => (x.get[String]("temperature").toInt, 1) }.reduce((a,b) => {
    if(a._1>b._1) a else b}
  )
  println("max:"+max)
  println("max "+(System.currentTimeMillis()-ini)+" mls")
  
  ini = System.currentTimeMillis()
  val go = table.map { x => (x.get[String]("temperature").toInt, 1) }.reduce((a,b) => ((a._1 + b._1), (a._2 + b._2)))
  println("go "+(System.currentTimeMillis()-ini)+" mls avg:"+ go._1/go._2)
  
  println("rows count: "+table.count())
}