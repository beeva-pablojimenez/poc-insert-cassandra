package com.beeva.poc.cassandra.massive_insertion

//mensajes usados entre actores
object Messages {
  case class Insert(statement:String) {}
  case object DoInsert
  case object TaskInsert
  case object TaskShutdown
  case object InsertDone
  case object Done
  case object Failed
  case class WeatherEvent(weatherstation_id:String, date:String, event_time:Long, temperature:String){}
}