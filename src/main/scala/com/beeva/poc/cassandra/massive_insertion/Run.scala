package com.beeva.poc.cassandra.massive_insertion

import akka.actor.ActorSystem
import akka.actor.Props
import java.util.Date
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Run extends scala.App {

  val system = ActorSystem("ClusterSystem")
  val router = system.actorOf(Props[CassandraPool], "pool")
  val controller = system.actorOf(Props[CassandraController], "controller")
  
  //tiempo de la prueba en minutos
  system.scheduler.scheduleOnce(20 minute, system.actorOf(Props(new CassandraInsertTask(router, controller))),Messages.TaskShutdown)
  //empieza en 10seg, se ejecuta cada 60seg
  system.scheduler.schedule(10 seconds, 1 minute, system.actorOf(Props(new CassandraInsertTask(router, controller))),Messages.TaskInsert)
   
}