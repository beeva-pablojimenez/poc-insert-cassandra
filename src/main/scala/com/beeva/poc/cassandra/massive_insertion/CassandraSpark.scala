package com.beeva.poc.cassandra.massive_insertion

import org.apache.spark.{ SparkConf, SparkContext }
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import java.text.SimpleDateFormat

//lectura de datos con spark y calculo de maximos mínimos y media de temperaturas
object CassandraSpark extends App {
  val format = new SimpleDateFormat("HH:mm:ss.SSSZ")
  
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local", "testingCassy", conf)
  
  var ini = System.currentTimeMillis()
  val date = RandomObject.getDate()
  val table = sc.cassandraTable("test", "temperature_by_day").select("weatherstation_id","date", "event_time", "temperature").where("weatherstation_id = ? and date=?", RandomObject.getRandomID(), RandomObject.getDateStr(date))
  println("First Row: "+table.first()+ " "+(System.currentTimeMillis()-ini)+" mls")
  
  
  ini = System.currentTimeMillis()
  val min = table.map { x => (x.get[String]("temperature").toInt, 1) }.reduce((a,b) => {
    if(a._1<b._1) a else b}
  )
  println("Min:"+min._1+" "+(System.currentTimeMillis()-ini)+" mls")
   
  ini = System.currentTimeMillis()
  val max = table.map { x => (x.get[String]("temperature").toInt, 1) }.reduce((a,b) => {
    if(a._1>b._1) a else b}
  )
  println("Max:"+max._1+" "+(System.currentTimeMillis()-ini)+" mls")
  
  ini = System.currentTimeMillis()
  val avg = table.map { x => (x.get[String]("temperature").toInt, 1) }.reduce((a,b) => ((a._1 + b._1), (a._2 + b._2)))
  println("Average: "+ avg._1/avg._2 + " "+(System.currentTimeMillis()-ini)+" mls")
  
  println("Rows count: "+table.count())
}