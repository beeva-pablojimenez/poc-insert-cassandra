package com.beeva.poc.cassandra.massive_insertion

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorLogging

//Se ocupa de crear un millon de actores para insertar datos y de parar el sistema
class CassandraInsertTask(router: ActorRef, controller: ActorRef) extends Actor with ActorLogging{
  def receive = {
    case Messages.TaskInsert => {
      log.debug("Start sending "+self.path)
      var a = 0
      for (a <- 1 until 1000000) {
        val inserter = context.system.actorOf(Props(new CassandraInserter(context.system.settings.config.getConfig("model.timeseries"), router, controller)))
        inserter ! Messages.DoInsert
      }
      log.debug("Sended 1.000.000 by "+self.path)
    }
    case Messages.TaskShutdown => {
      log.debug("Stopping system...")
      context.system.shutdown()
    }
  }  
  def stop(): Unit = {    
    context.stop(self)
  }
}