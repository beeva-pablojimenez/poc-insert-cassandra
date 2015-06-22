package com.beeva.poc.cassandra.massive_insertion

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.config.Config

//creacion de valores aleatorios
object RandomObject {
  
  val r = scala.util.Random
  val maxStationsID = 100
  val station = "Station-"
  
  val temperature = 24
  val temperature_base = 20
  
  val days = 365
  val format = DateTimeFormat.forPattern("yyyy-MM-dd")

  def getRandomID():String = {
    station + r.nextInt(maxStationsID)
  }
  
  def getRandomTemperature():String = {
    if (r.nextInt(2)<1) {
      (temperature_base + r.nextInt(temperature)).toString()
    } else {
      (temperature_base - r.nextInt(temperature)).toString()
    }
  } 
  
  def getDate():DateTime = {
    var date = new DateTime
    date = date.plusDays(r.nextInt(days))
    date
  }
  
  def getDateStr(date: DateTime):String ={
    format.print(date)
  }
  
  def createInsertStatement(config: Config, event: Messages.WeatherEvent): Insert =  {
    val date = RandomObject.getDate()
    val params = Map("weatherstation_id" ->event.weatherstation_id, "date" -> event.date, "event_time" -> event.event_time, "temperature" -> event.temperature)
    val order = config.getStringList("order")
    var statement = QueryBuilder.insertInto(config.getString("keyspace"), config.getString("table"))
    var tot = order.size()-1
    var ini = 0
    for (o <-ini to tot) {
      statement.value(order.get(o), params.get(order.get(o)).get)
    }
    statement
  }
}