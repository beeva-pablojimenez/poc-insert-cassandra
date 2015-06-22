package com.beeva.poc.cassandra.massive_insertion

import akka.routing.ActorRefRoutee
import akka.routing.Router
import akka.routing.RoundRobinRoutingLogic
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.Actor
import akka.actor.OneForOneStrategy
import scala.concurrent.duration.DurationInt
import akka.actor.SupervisorStrategy
import akka.routing.RoundRobinPool
import akka.cluster.routing.ClusterRouterPoolSettings
import akka.cluster.routing.ClusterRouterPool
import akka.routing.ConsistentHashingPool

//implementa un pool de conexiones, con round robin y supervision tipo restart
class CassandraPool extends Actor {
  
  import context.dispatcher
  
  val restart = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange=1.minute){
    case _: Exception => SupervisorStrategy.Restart
  }
  
  val router = context.actorOf(RoundRobinPool(10, supervisorStrategy = restart).props(routeeProps = Props(classOf[CassandraConnection],context.system.settings.config.getConfig("storage"))), "router")
  
  def receive = {
    case w: Messages.Insert  =>  router.tell(w, sender) 
  }
  
}

