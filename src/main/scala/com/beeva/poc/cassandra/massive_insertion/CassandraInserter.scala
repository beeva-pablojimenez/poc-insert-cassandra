package com.beeva.poc.cassandra.massive_insertion

import com.datastax.driver.core.querybuilder.Insert
import akka.actor.Actor
import com.typesafe.config.Config
import com.datastax.driver.core.querybuilder.QueryBuilder
import akka.actor.ActorRef
import akka.actor.ActorLogging

//actor que crea una sentencia insert y la manda ejecutar, muere tras la ejecucion
class CassandraInserter(config: Config, router: ActorRef, controller: ActorRef) extends Actor with ActorLogging{
 def receive = {
    case Messages.DoInsert =>
      val date = RandomObject.getDate()
      val event = Messages.WeatherEvent(RandomObject.getRandomID(), RandomObject.getDateStr(date),date.getMillis, RandomObject.getRandomTemperature())
      val statement = RandomObject.createInsertStatement(config, event)
      router ! Messages.Insert(statement.toString())
    case Messages.InsertDone =>
      stop()
  }
  
  def stop(): Unit = {
    controller ! Messages.Done
    context.stop(self)
  }
}