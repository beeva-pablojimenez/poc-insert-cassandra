package com.beeva.poc.cassandra.massive_insertion

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorLogging

// controla y cuenta los row inserted en un proceso
class CassandraController extends Actor with ActorLogging{
  var count = 0 
  def receive ={
    case Messages.Done =>
      count +=1
      if(count % 1000000 ==  0)
        log.debug("Inserted:"+count)
   }
}